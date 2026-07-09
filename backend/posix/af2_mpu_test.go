// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package posix

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
)

type arrival struct {
	n int
	L int64
}

// disp is the recorded per-part outcome of classify(): PLACED@offset or STASHED.
type disp struct {
	stashed bool
	off     int64 // final data-file offset when !stashed
}

// simulate replays a part-arrival sequence through classify(), recording each
// part's disposition and mutating the index exactly as uploadPartAtOffset does
// (StashNext bump for stashed parts; Stride resolved from part #1 or, once two
// distinct part numbers are seen, the max of their lengths). classify() never
// rejects — size/uniformity errors are deferred to Complete — so simulate always
// completes.
func simulate(parts []arrival) (dispositions map[int]disp, idx *otterMpuIndex) {
	idx = &otterMpuIndex{}
	dispositions = map[int]disp{}
	for _, p := range parts {
		off, d := idx.classify(p.n, p.L)
		rec := otterMpuPart{PartNumber: p.n, Len: p.L, ETag: "e"}
		if d == dispStash {
			rec.Stashed = true
			rec.StashOffset = idx.StashNext
			idx.StashNext += p.L
			dispositions[p.n] = disp{stashed: true}
		} else {
			rec.Offset = off
			dispositions[p.n] = disp{stashed: false, off: off}
		}
		// Mirror uploadPartAtOffset's stride resolution.
		if idx.Stride == 0 {
			if p.n == 1 {
				idx.Stride = p.L
			} else {
				maxLen, distinct := p.L, 1
				for i := range idx.Parts {
					if idx.Parts[i].PartNumber == p.n {
						continue
					}
					distinct++
					if idx.Parts[i].Len > maxLen {
						maxLen = idx.Parts[i].Len
					}
				}
				if distinct >= 2 {
					idx.Stride = maxLen
				}
			}
		}
		idx.put(rec)
	}
	return dispositions, idx
}

func TestOtterMpuClassify(t *testing.T) {
	const S = int64(8)
	cases := []struct {
		name     string
		arrivals []arrival
		want     map[int]disp // expected disposition per part number
	}{
		{
			name:     "uniform in-order, short final",
			arrivals: []arrival{{1, S}, {2, S}, {3, 3}},
			// Part 3 is short (!=stride) so it is stashed and folded at Complete.
			want: map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {stashed: true}},
		},
		{
			name:     "uniform out-of-order (full-size part first)",
			arrivals: []arrival{{2, S}, {1, S}, {3, 3}},
			// Part 2 arrives before the stride is known -> stashed; part 1 sets the
			// stride; part 3 (short tail) -> stashed. Folded at Complete.
			want: map[int]disp{1: {off: 0}, 2: {stashed: true}, 3: {stashed: true}},
		},
		{
			name:     "first-arriving part is the short tail, then a full part",
			arrivals: []arrival{{3, 3}, {1, S}},
			// No longer a fallback: the short tail is stashed (it arrived before
			// part #1), part 1 places at 0. Complete folds the tail.
			want: map[int]disp{3: {stashed: true}, 1: {off: 0}},
		},
		{
			name:     "non-final short part then a higher part",
			arrivals: []arrival{{1, S}, {2, 4}, {3, S}},
			// part 2 (!=stride) stashed; part 3 (==stride) placed. The genuine
			// non-uniform interior is rejected at Complete, not here.
			want: map[int]disp{1: {off: 0}, 2: {stashed: true}, 3: {off: 16}},
		},
		{
			name:     "part larger than stride",
			arrivals: []arrival{{1, S}, {2, S + 1}},
			// Oversize is stashed (errors deferred to Complete).
			want: map[int]disp{1: {off: 0}, 2: {stashed: true}},
		},
		{
			name:     "two short parts",
			arrivals: []arrival{{1, S}, {2, 4}, {3, 4}},
			want:     map[int]disp{1: {off: 0}, 2: {stashed: true}, 3: {stashed: true}},
		},
		{
			name:     "re-upload same part, same size",
			arrivals: []arrival{{1, S}, {2, S}, {1, S}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}},
		},
		{
			name:     "re-upload final part, smaller",
			arrivals: []arrival{{1, S}, {2, S}, {3, 3}, {3, 2}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {stashed: true}},
		},
		{
			name:     "reverse order, short tail first (max-of-2 pins S)",
			arrivals: []arrival{{3, 3}, {2, S}, {1, S}},
			// Part 3 (tail) stashed alone; part 2 is the 2nd distinct part so S is
			// resolved as max(3,8)=8 and part 2 is placed at offset 8 (no longer
			// stashed); part 1 places at 0. This is the win over the part-#1-only rule.
			want: map[int]disp{3: {stashed: true}, 2: {off: 8}, 1: {off: 0}},
		},
		{
			name:     "stride resolved without part #1 (max-of-2)",
			arrivals: []arrival{{2, S}, {3, 3}, {4, S}},
			// Part 2 stashed alone; part 3 (tail) is the 2nd distinct part, pinning
			// S=max(8,3)=8 (itself stashed as the short part); part 4 then places at
			// (4-1)*8=24 via the fast path — all without part #1 ever arriving.
			want: map[int]disp{2: {stashed: true}, 3: {stashed: true}, 4: {off: 24}},
		},
		{
			name:     "retried short tail does not pin stride",
			arrivals: []arrival{{3, 3}, {3, 3}, {1, S}},
			// A re-upload of the same short part is NOT a second distinct sample, so
			// S stays unresolved until part #1 arrives and anchors offset 0.
			want: map[int]disp{3: {stashed: true}, 1: {off: 0}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, idx := simulate(tc.arrivals)
			for n, want := range tc.want {
				g, ok := got[n]
				if !ok {
					t.Fatalf("part %d missing disposition", n)
				}
				if g.stashed != want.stashed {
					t.Fatalf("part %d stashed = %v, want %v", n, g.stashed, want.stashed)
				}
				if !want.stashed && g.off != want.off {
					t.Fatalf("part %d offset = %d, want %d", n, g.off, want.off)
				}
				// A placed part's record must carry the offset; a stashed part must
				// not leak a final offset.
				rec, _ := idx.find(n)
				if rec == nil {
					t.Fatalf("part %d missing from index", n)
				}
				if want.stashed {
					if !rec.Stashed {
						t.Fatalf("part %d record not marked stashed", n)
					}
					if rec.Offset != 0 {
						t.Fatalf("part %d stashed but carries offset %d", n, rec.Offset)
					}
				} else if rec.Offset != want.off {
					t.Fatalf("part %d record offset = %d, want %d", n, rec.Offset, want.off)
				}
			}
		})
	}
}

// TestOtterMpuClassifyStridePinning verifies a single non-#1 part does not pin
// the stride, that part #1 anchors offset 0, and that a confirmed stride drives
// placement. (The two-distinct-part resolution is covered by
// TestOtterMpuClassifyMaxOfTwo.)
func TestOtterMpuClassifyStridePinning(t *testing.T) {
	idx := &otterMpuIndex{}
	// A higher-numbered part arriving ALONE does NOT pin the stride.
	if _, d := idx.classify(2, 100); d != dispStash {
		t.Fatalf("part 2 before part 1: disp = %v, want stash", d)
	}
	if idx.Stride != 0 {
		t.Fatalf("stride set by a single non-#1 part: %d", idx.Stride)
	}
	// Simulate part 1 placing and setting the stride.
	off, d := idx.classify(1, 64)
	if d != dispPlace || off != 0 {
		t.Fatalf("part 1: off=%d disp=%v, want 0/place", off, d)
	}
	idx.Stride = 64
	// Now a stride-equal part places; a short part stashes.
	if off, d := idx.classify(3, 64); d != dispPlace || off != 128 {
		t.Fatalf("part 3 (==stride): off=%d disp=%v, want 128/place", off, d)
	}
	if _, d := idx.classify(4, 10); d != dispStash {
		t.Fatalf("part 4 (short): disp=%v, want stash", d)
	}
}

// TestOtterMpuClassifyMaxOfTwo verifies the stride is resolved as the max across
// the first two DISTINCT part numbers (so a short tail arriving first no longer
// forces full parts to be stashed), and that a re-uploaded part number is not a
// second distinct sample.
func TestOtterMpuClassifyMaxOfTwo(t *testing.T) {
	const S = int64(8)

	// Short tail first, then a full part: the second distinct part pins S=8 and is
	// placed (not stashed) at its offset — even before part #1 arrives.
	_, idx := simulate([]arrival{{3, 3}, {2, S}})
	if idx.Stride != S {
		t.Fatalf("stride after {tail,full} = %d, want %d", idx.Stride, S)
	}

	// max() not min(): if the full part arrives first then the tail, S is still 8.
	_, idx = simulate([]arrival{{2, S}, {3, 3}})
	if idx.Stride != S {
		t.Fatalf("stride after {full,tail} = %d, want %d", idx.Stride, S)
	}

	// A retried short part is not a second distinct sample: S stays unresolved.
	idx = &otterMpuIndex{}
	if _, d := idx.classify(3, 3); d != dispStash {
		t.Fatalf("part 3 alone: disp=%v, want stash", d)
	}
	idx.put(otterMpuPart{PartNumber: 3, Len: 3, ETag: "e", Stashed: true})
	if _, d := idx.classify(3, 3); d != dispStash {
		t.Fatalf("part 3 retried: disp=%v, want stash", d)
	}
	if idx.Stride != 0 {
		t.Fatalf("stride pinned by a retried part: %d, want 0", idx.Stride)
	}
}

func TestOtterMpuIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")

	idx := &otterMpuIndex{Stride: 8, StashNext: 11}
	idx.put(otterMpuPart{PartNumber: 1, Offset: 0, Len: 8, ETag: `"a"`})
	idx.put(otterMpuPart{PartNumber: 2, Offset: 8, Len: 3, ETag: `"b"`})
	// A stashed part: bytes in the .stash file, no final offset.
	idx.put(otterMpuPart{PartNumber: 3, Len: 8, ETag: `"c"`, Stashed: true, StashOffset: 3})

	if err := writeMpIndexAtomic(path, idx); err != nil {
		t.Fatalf("write index: %v", err)
	}
	// No unique-named temp file may linger in the dir.
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("durable-write temp left behind: %s", e.Name())
		}
	}

	got, err := readMpIndex(path)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if got.Stride != 8 || got.StashNext != 11 || len(got.Parts) != 3 {
		t.Fatalf("round-trip index = %+v", got)
	}
	if p, _ := got.find(2); p == nil || p.Offset != 8 || p.Len != 3 || p.ETag != `"b"` {
		t.Fatalf("part 2 round-trip = %+v", p)
	}
	if p, _ := got.find(3); p == nil || !p.Stashed || p.StashOffset != 3 || p.Len != 8 {
		t.Fatalf("part 3 (stashed) round-trip = %+v", p)
	}
}

func TestOtterMpuRecompute(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")

	// A data file with a leading gap then a known range, to prove we hash the
	// requested [offset,len) and nothing else.
	part := bytes.Repeat([]byte("otter-part-bytes"), 64) // 1024 bytes
	buf := make([]byte, 100+len(part))
	copy(buf[100:], part)
	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	// MD5 of the range matches a direct MD5 of the part bytes.
	gotMD5, err := recomputePartMD5(f, 100, int64(len(part)))
	if err != nil {
		t.Fatalf("recompute md5: %v", err)
	}
	h := md5.New()
	h.Write(part)
	if want := backend.GenerateEtag(h); gotMD5 != want {
		t.Fatalf("md5 = %s, want %s", gotMD5, want)
	}

	// CRC64NVME of the range matches a direct HashReader over the part bytes.
	gotCRC, err := recomputePartCRC64NVME(f, 100, int64(len(part)))
	if err != nil {
		t.Fatalf("recompute crc: %v", err)
	}
	hr, _ := utils.NewHashReader(bytes.NewReader(part), "", utils.HashTypeCRC64NVME)
	_, _ = io.Copy(io.Discard, hr)
	if want := hr.Sum(); gotCRC != want {
		t.Fatalf("crc64nvme = %s, want %s", gotCRC, want)
	}
}

func TestOtterMpuDataFileHidden(t *testing.T) {
	// The data file must carry the ".sgwtmp." prefix so the existing listing
	// skip rule (walk.go isSkipped; MetaTmpDir skipdirs) hides it.
	name := mpDataFileName("some/key", "11112222-3333")
	if !strings.HasPrefix(name, MetaTmpDir+".") {
		t.Fatalf("data file name %q is not hidden by the .sgwtmp skip rule", name)
	}
	if !strings.HasSuffix(name, ".data") {
		t.Fatalf("data file name %q missing .data suffix", name)
	}
}
