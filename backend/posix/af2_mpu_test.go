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
	"strings"
	"testing"
)

type arrival struct {
	n int
	L int64
}

// disp is the FINAL outcome of a part after a full replay: placed at off, or
// still buffered (stride never resolved).
type disp struct {
	buffered bool
	off      int64
}

// applyArrival mirrors uploadPartAtOffset's in-memory bookkeeping (minus file
// I/O): classify -> place or buffer -> on stride resolution, drain the buffer.
// It returns after fully applying one arrival.
func applyArrival(idx *otterMpuIndex, a arrival) {
	off, d, newStride := idx.classify(a.n, a.L)
	if d == dispPlace {
		delete(idx.pending, a.n) // a re-upload after resolution supersedes the buffer
		idx.put(otterMpuPart{PartNumber: a.n, Offset: off, Len: a.L, ETag: "e"})
	} else {
		idx.pending[a.n] = &pendingPart{length: a.L, etag: "e"}
	}
	if newStride != 0 && idx.Stride == 0 {
		idx.Stride = newStride
		for pn, pp := range idx.pending {
			idx.put(otterMpuPart{PartNumber: pn, Offset: int64(pn-1) * idx.Stride, Len: pp.length, ETag: pp.etag})
			delete(idx.pending, pn)
		}
	}
}

// simulate replays a part-arrival sequence and reports each part's FINAL
// disposition (placed@offset or buffered). classify() never rejects — size /
// uniformity errors are deferred to Complete — so simulate always completes.
func simulate(parts []arrival) (final map[int]disp, idx *otterMpuIndex) {
	idx = &otterMpuIndex{pending: map[int]*pendingPart{}}
	for _, a := range parts {
		applyArrival(idx, a)
	}
	final = map[int]disp{}
	for i := range idx.Parts {
		final[idx.Parts[i].PartNumber] = disp{off: idx.Parts[i].Offset}
	}
	for pn := range idx.pending {
		final[pn] = disp{buffered: true}
	}
	return final, idx
}

func TestOtterMpuClassify(t *testing.T) {
	const S = int64(8)
	cases := []struct {
		name     string
		arrivals []arrival
		want     map[int]disp // expected FINAL disposition per part number
	}{
		{
			name:     "uniform in-order, short final placed directly",
			arrivals: []arrival{{1, S}, {2, S}, {3, 3}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {off: 16}},
		},
		{
			name:     "uniform out-of-order (full-size part first)",
			arrivals: []arrival{{2, S}, {1, S}, {3, 3}},
			// Part 2 buffers until part 1 resolves the stride and drains it.
			want: map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {off: 16}},
		},
		{
			name:     "short tail first, then part #1",
			arrivals: []arrival{{3, 3}, {1, S}},
			// Part 3 buffers; part 1 resolves S=8 and drains it to (3-1)*8=16.
			want: map[int]disp{1: {off: 0}, 3: {off: 16}},
		},
		{
			name:     "non-final short part placed directly once stride known",
			arrivals: []arrival{{1, S}, {2, 4}, {3, S}},
			// part 2 is short but the stride is known, so it is placed at 8 (the
			// genuine non-uniform interior is rejected at Complete, not here).
			want: map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {off: 16}},
		},
		{
			name:     "part larger than stride placed directly",
			arrivals: []arrival{{1, S}, {2, S + 1}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}},
		},
		{
			name:     "two short parts placed after stride",
			arrivals: []arrival{{1, S}, {2, 4}, {3, 4}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {off: 16}},
		},
		{
			name:     "re-upload same part, same size",
			arrivals: []arrival{{1, S}, {2, S}, {1, S}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}},
		},
		{
			name:     "re-upload final part, smaller",
			arrivals: []arrival{{1, S}, {2, S}, {3, 3}, {3, 2}},
			want:     map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {off: 16}},
		},
		{
			name:     "reverse order, short tail first (max-of-2 pins S)",
			arrivals: []arrival{{3, 3}, {2, S}, {1, S}},
			// Part 3 buffers; part 2 is the 2nd distinct part so S=max(3,8)=8, part 2
			// places at 8 and part 3 drains to 16; part 1 places at 0.
			want: map[int]disp{1: {off: 0}, 2: {off: 8}, 3: {off: 16}},
		},
		{
			name:     "stride resolved without part #1 (max-of-2)",
			arrivals: []arrival{{2, S}, {3, 3}, {4, S}},
			// Part 2 buffers; part 3 (short) is the 2nd distinct part, pinning
			// S=max(8,3)=8 and placing at 16; part 2 drains to 8; part 4 places at 24.
			want: map[int]disp{2: {off: 8}, 3: {off: 16}, 4: {off: 24}},
		},
		{
			name:     "retried short tail does not pin stride",
			arrivals: []arrival{{3, 3}, {3, 3}, {1, S}},
			// A re-upload of the same short part is not a 2nd distinct sample, so S
			// stays unresolved until part #1 arrives and anchors offset 0.
			want: map[int]disp{1: {off: 0}, 3: {off: 16}},
		},
		{
			name:     "single non-#1 part stays buffered",
			arrivals: []arrival{{2, S}},
			want:     map[int]disp{2: {buffered: true}},
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
				if g.buffered != want.buffered {
					t.Fatalf("part %d buffered = %v, want %v", n, g.buffered, want.buffered)
				}
				if !want.buffered && g.off != want.off {
					t.Fatalf("part %d offset = %d, want %d", n, g.off, want.off)
				}
				if want.buffered {
					if _, i := idx.find(n); i >= 0 {
						t.Fatalf("part %d expected buffered but is placed", n)
					}
				} else {
					rec, _ := idx.find(n)
					if rec == nil {
						t.Fatalf("part %d expected placed but missing from index", n)
					}
					if rec.Offset != want.off {
						t.Fatalf("part %d record offset = %d, want %d", n, rec.Offset, want.off)
					}
				}
			}
		})
	}
}

// TestOtterMpuClassifyStridePinning verifies a single non-#1 part does not pin
// the stride, part #1 anchors offset 0, and — once the stride is known — even a
// short part is placed directly (no buffering).
func TestOtterMpuClassifyStridePinning(t *testing.T) {
	idx := &otterMpuIndex{pending: map[int]*pendingPart{}}

	// A higher-numbered part arriving ALONE does NOT pin the stride.
	if _, d, ns := idx.classify(2, 100); d != dispBuffer || ns != 0 {
		t.Fatalf("part 2 before part 1: disp=%v newStride=%d, want buffer/0", d, ns)
	}
	idx.pending[2] = &pendingPart{length: 100} // reflect the buffered part

	// Part 1 places at 0 and resolves the stride to its length.
	off, d, ns := idx.classify(1, 64)
	if d != dispPlace || off != 0 || ns != 64 {
		t.Fatalf("part 1: off=%d disp=%v newStride=%d, want 0/place/64", off, d, ns)
	}
	idx.Stride = 64 // caller persists

	// Stride known: an equal part places, and a SHORT part also places directly.
	if off, d, _ := idx.classify(3, 64); d != dispPlace || off != 128 {
		t.Fatalf("part 3 (==stride): off=%d disp=%v, want 128/place", off, d)
	}
	if off, d, _ := idx.classify(4, 10); d != dispPlace || off != 192 {
		t.Fatalf("part 4 (short): off=%d disp=%v, want 192/place", off, d)
	}
}

// TestOtterMpuClassifyMaxOfTwo verifies the stride is resolved as the max across
// the first two DISTINCT part numbers, and that a re-uploaded part number is not
// a second distinct sample.
func TestOtterMpuClassifyMaxOfTwo(t *testing.T) {
	const S = int64(8)

	_, idx := simulate([]arrival{{3, 3}, {2, S}})
	if idx.Stride != S {
		t.Fatalf("stride after {tail,full} = %d, want %d", idx.Stride, S)
	}

	_, idx = simulate([]arrival{{2, S}, {3, 3}})
	if idx.Stride != S {
		t.Fatalf("stride after {full,tail} = %d, want %d", idx.Stride, S)
	}

	// A retried short part is not a second distinct sample: S stays unresolved.
	idx = &otterMpuIndex{pending: map[int]*pendingPart{}}
	if _, d, _ := idx.classify(3, 3); d != dispBuffer {
		t.Fatalf("part 3 alone: disp=%v, want buffer", d)
	}
	idx.pending[3] = &pendingPart{length: 3}
	if _, d, _ := idx.classify(3, 3); d != dispBuffer {
		t.Fatalf("part 3 retried: disp=%v, want buffer", d)
	}
	if idx.Stride != 0 {
		t.Fatalf("stride pinned by a retried part: %d, want 0", idx.Stride)
	}
}

// TestOtterMpuAtMostOnePending is the invariant regression: across adversarial
// orderings, the pending buffer never holds more than one part. In particular
// {2,S},{3,S} (neither is #1) must resolve the stride via the union scan of
// Parts+pending, not leave both buffered.
func TestOtterMpuAtMostOnePending(t *testing.T) {
	orderings := [][]arrival{
		{{2, 8}, {3, 8}},         // two distinct non-#1 parts
		{{3, 3}, {2, 8}, {1, 8}}, // short tail first
		{{2, 8}, {3, 3}, {4, 8}}, // stride resolved without #1
		{{5, 8}, {5, 8}, {1, 8}}, // re-upload of a buffered part
	}
	for _, seq := range orderings {
		idx := &otterMpuIndex{pending: map[int]*pendingPart{}}
		maxPending := 0
		for _, a := range seq {
			applyArrival(idx, a)
			if len(idx.pending) > maxPending {
				maxPending = len(idx.pending)
			}
		}
		if maxPending > 1 {
			t.Fatalf("seq %v: max pending = %d, want <= 1", seq, maxPending)
		}
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
