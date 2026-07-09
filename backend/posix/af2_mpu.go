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

// Otter multipart write-at-offset support (LLD §4b).
//
// Instead of staging one file per part and concatenating them into the final
// object at Complete (a second full-size write, costly on AF2/SDFS), Otter
// writes every part directly at its final byte offset in ONE data file and
// reveals it with a same-directory rename at Complete — the data is written
// once.
//
// State layout (the upstream .sgwtmp tree is retained so ListMultipartUploads /
// checkUploadIDExists keep working and a single skip rule hides it):
//
//	<bucket>/.sgwtmp/multipart/<hash(key)>/objname            (plain file: the S3 key)
//	<bucket>/.sgwtmp/multipart/<hash(key)>/<uploadID>/        (per-upload state dir)
//	<bucket>/.sgwtmp/multipart/<hash(key)>/<uploadID>/index   (plain JSON: parts + stride)
//	<objdir>/.sgwtmp.<hash(key)>.<uploadID>.data             (the single data file)
//
// The data file lives in the object's OWN directory (so the reveal rename is
// same-directory, legal on SDFS) and uses the ".sgwtmp." prefix so it is already
// hidden by every listing walk. The index records each part's {offset,len,etag}
// so Complete needs no stored per-part attribute and ListParts has full fidelity.

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// otterMpuPart is one recorded part in the per-upload index.
type otterMpuPart struct {
	PartNumber  int    `json:"n"`
	Offset      int64  `json:"off"`             // final data-file offset; valid only when Stashed==false
	Len         int64  `json:"len"`             // true written length (never the declared/stride length)
	ETag        string `json:"etag"`
	Stashed     bool   `json:"stash,omitempty"` // true => bytes live in the stash file at StashOffset
	StashOffset int64  `json:"soff,omitempty"`  // byte offset within the .stash file (valid iff Stashed)
}

// otterMpuIndex is the per-upload write-at-offset index, stored as a plain JSON
// file in the upload's state dir.
type otterMpuIndex struct {
	// Stride is the confirmed uniform part size S. It is set either from part #1
	// (n==1, authoritative) OR, once two or more distinct part numbers have been
	// seen, as the maximum of their lengths — safe because at most one part (the
	// highest-numbered) may be short, so the larger of any two parts must be the
	// full stride. Zero means S is not resolvable yet (only one distinct part,
	// and not #1); S is then resolved at Complete from the authoritative ordered
	// part list. A short part NEVER sets Stride on its own.
	Stride    int64          `json:"stride"`
	StashNext int64          `json:"snext,omitempty"` // bump cursor: next free offset in the .stash file
	Parts     []otterMpuPart `json:"parts"`
}

// --- naming helpers -------------------------------------------------------

func mpKeyHash(key string) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", h)
}

// mpHashDir is <bucket>/.sgwtmp/multipart/<hash> (shared by all uploads of key).
func mpHashDir(bucket, key string) string {
	return filepath.Join(bucket, MetaTmpMultipartDir, mpKeyHash(key))
}

// mpObjNamePath is the hash-level plain file holding the S3 key, read by
// ListMultipartUploads.
func mpObjNamePath(bucket, key string) string {
	return filepath.Join(mpHashDir(bucket, key), onameAttr)
}

// mpStateDir is the per-upload state dir <hashdir>/<uploadID>.
func mpStateDir(bucket, key, uploadID string) string {
	return filepath.Join(mpHashDir(bucket, key), uploadID)
}

// mpIndexPath is the per-upload index plain file.
func mpIndexPath(bucket, key, uploadID string) string {
	return filepath.Join(mpStateDir(bucket, key, uploadID), "index")
}

// mpUploadMetaPath is the per-upload object-metadata plain file (content-type,
// user metadata) captured at Create and applied to the final object at Complete.
// It is a plain file (not a DESC attribute) because the upload-id path is a
// directory and SDFS rejects the DESC setxattr on directories.
func mpUploadMetaPath(bucket, key, uploadID string) string {
	return filepath.Join(mpStateDir(bucket, key, uploadID), "upload-meta")
}

// mpDataFileName is the single in-progress data file's basename. It uses the
// ".sgwtmp." prefix so the existing listing skip rule hides it (walk.go
// isSkipped; the MetaTmpDir skipdirs in fileToObjVersions).
func mpDataFileName(key, uploadID string) string {
	return fmt.Sprintf("%s.%s.%s.data", MetaTmpDir, mpKeyHash(key), uploadID)
}

// mpDataFilePath is the data file in the object's own directory (so the reveal
// rename at Complete is same-directory, legal on SDFS).
func mpDataFilePath(bucket, key, uploadID string) string {
	objDir := filepath.Dir(filepath.Join(bucket, key))
	return filepath.Join(objDir, mpDataFileName(key, uploadID))
}

// mpCompletingName is the claim marker the data file is renamed to at Complete.
func mpCompletingPath(bucket, key, uploadID string) string {
	return mpDataFilePath(bucket, key, uploadID) + ".completing"
}

// mpStashFileName is the per-upload stash file's basename. It mirrors
// mpDataFileName (".sgwtmp." prefix so the listing skip rule hides it) but ends
// in ".stash". Parts that cannot yet be placed at their final offset (they
// arrived before the stride was resolvable, or are shorter than the confirmed
// stride — the tail) are appended here and folded into the data file at Complete.
func mpStashFileName(key, uploadID string) string {
	return fmt.Sprintf("%s.%s.%s.stash", MetaTmpDir, mpKeyHash(key), uploadID)
}

// mpStashFilePath is the stash file in the object's own directory (so a fold /
// rename stays same-directory, legal on SDFS).
func mpStashFilePath(bucket, key, uploadID string) string {
	objDir := filepath.Dir(filepath.Join(bucket, key))
	return filepath.Join(objDir, mpStashFileName(key, uploadID))
}

// --- per-uploadID lock ----------------------------------------------------

// mpuLock returns the *sync.Mutex serializing all index/data/stash mutations for
// a single (bucket, object, uploadID), creating it on first use via LoadOrStore.
// The entry is removed by Complete/Abort on the terminal path (NOT inline on the
// upload path, which would split mutual exclusion across two mutex objects while
// waiters exist).
func (p *Posix) mpuLock(bucket, object, uploadID string) *sync.Mutex {
	key := bucket + "\x00" + object + "\x00" + uploadID
	m, _ := p.mpuLocks.LoadOrStore(key, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// mpuUnlockDelete drops the per-uploadID lock map entry. Call only on the
// terminal path (after Complete/Abort has finished and released the mutex), so
// no waiters remain that could grab a fresh, different mutex for the same key.
func (p *Posix) mpuUnlockDelete(bucket, object, uploadID string) {
	key := bucket + "\x00" + object + "\x00" + uploadID
	p.mpuLocks.Delete(key)
}

// --- index I/O ------------------------------------------------------------

func readMpIndex(path string) (*otterMpuIndex, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var idx otterMpuIndex
	if err := json.Unmarshal(b, &idx); err != nil {
		return nil, fmt.Errorf("parse mpu index: %w", err)
	}
	return &idx, nil
}

// writeAtomic writes b to a UNIQUE same-directory temp file and renames it onto
// path. The rename is what makes the swap atomic — a reader (or a retry) sees
// either the old contents or the complete new ones, never a torn write.
//
// It deliberately does NOT fsync. Otter is ack-on-write and the durability
// boundary is the AF2 checkpoint, not the gateway (design §8): a 200 means "in
// the channel," not "survives a power loss." fsync'ing in-progress MPU staging
// would hold it to a stronger guarantee than finished objects get (the final
// reveal rename isn't fsync'd either) — at exactly the fsync cost that wedges
// SDFS. Crash-consistency here comes from this rename plus the per-part MD5
// re-validation at Complete, not from fsync. A unique tmp name (pid + nanos)
// avoids the fixed-".tmp" collision under concurrent retries.
func writeAtomic(path string, b []byte) error {
	tmp := path + "." + strconv.Itoa(os.Getpid()) + "." + strconv.FormatInt(time.Now().UnixNano(), 10) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// writeMpIndexAtomic commits the index via an atomic temp+rename so a reader or a
// retry never sees a half-written index. It is not fsync'd (see writeAtomic): the
// index is rebuilt by client retries and every part is re-validated at Complete,
// so it needs consistency, not durability.
func writeMpIndexAtomic(path string, idx *otterMpuIndex) error {
	b, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal mpu index: %w", err)
	}
	return writeAtomic(path, b)
}

// writeMpUploadMeta persists the upload's object metadata as a plain JSON file via
// an atomic temp+rename (not fsync'd; readMpUploadMeta is best-effort and a failed
// Create is retried by the client).
func writeMpUploadMeta(path string, m metaProperties) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal mpu upload-meta: %w", err)
	}
	return writeAtomic(path, b)
}

// readMpUploadMeta reads the upload's object metadata plain file.
func readMpUploadMeta(path string) (metaProperties, error) {
	var m metaProperties
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("parse mpu upload-meta: %w", err)
	}
	return m, nil
}

// find returns the recorded part with the given number and its slice index, or
// (nil, -1) if absent.
func (idx *otterMpuIndex) find(n int) (*otterMpuPart, int) {
	for i := range idx.Parts {
		if idx.Parts[i].PartNumber == n {
			return &idx.Parts[i], i
		}
	}
	return nil, -1
}

// put inserts or replaces a part record.
func (idx *otterMpuIndex) put(part otterMpuPart) {
	if _, i := idx.find(part.PartNumber); i >= 0 {
		idx.Parts[i] = part
		return
	}
	idx.Parts = append(idx.Parts, part)
}

// --- placement ------------------------------------------------------------

// mpDisposition is what UploadPart should do with an incoming part.
type mpDisposition int

const (
	dispPlace mpDisposition = iota // write at the returned final offset (no-copy fast path)
	dispStash                      // append to the .stash file (caller assigns StashOffset)
)

// classify decides ONLY what UploadPart can safely decide with no global view of
// the upload. It does NOT mutate the index and it does NOT reject anything:
// final size/uniformity rejection is deferred to Complete, where the
// authoritative ordered part list is known.
//
//   - If the stride is confirmed and the part equals it, it is a full part:
//     write it once at its final offset (the no-copy fast path).
//   - If this is part #1, it is authoritative for S (and is the whole object for
//     a single-part upload), so write it at offset 0; the caller sets idx.Stride
//     after a successful write.
//   - Else if at least one OTHER distinct part is already recorded, the stride is
//     resolvable as max(this part, the others): at most one part (the last) may
//     be short, so the larger of any two parts is the full stride S. A part whose
//     length is the max is a full part — place it at (n-1)*S; a shorter part is
//     the tail — stash it. The caller persists idx.Stride after the write.
//   - Otherwise (stride unknown, only this part seen, not #1) stash it; the
//     stride is not yet resolvable. Final classification happens at Complete.
//
// A re-upload of an already-recorded part number is NOT counted as a second
// distinct sample (it would let a retried short tail masquerade as the stride).
func (idx *otterMpuIndex) classify(n int, L int64) (offset int64, disp mpDisposition) {
	if idx.Stride != 0 {
		if L == idx.Stride {
			return int64(n-1) * idx.Stride, dispPlace
		}
		return 0, dispStash
	}
	if n == 1 {
		return 0, dispPlace
	}
	// Stride not yet confirmed: resolve it from the max across >=2 distinct parts.
	otherMax, others := int64(0), 0
	for i := range idx.Parts {
		if idx.Parts[i].PartNumber == n {
			continue // a re-upload of this same part is not a second distinct sample
		}
		others++
		if idx.Parts[i].Len > otherMax {
			otherMax = idx.Parts[i].Len
		}
	}
	if others >= 1 {
		if L >= otherMax {
			return int64(n-1) * L, dispPlace // this part is the (tied) largest => full part, S==L
		}
		return 0, dispStash // shorter than a seen full part => the tail
	}
	return 0, dispStash // only this part seen and it isn't #1: cannot resolve S yet
}

// --- recompute (Complete) -------------------------------------------------

// recomputePartMD5 returns the quoted hex MD5 ETag of the data file range
// [offset, offset+length), matching backend.GenerateEtag.
func recomputePartMD5(f *os.File, offset, length int64) (string, error) {
	h := md5.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, offset, length)); err != nil {
		return "", err
	}
	return backend.GenerateEtag(h), nil
}

// recomputePartCRC64NVME returns the part's internal CRC64NVME checksum over the
// data file range, matching what UploadPart records today.
func recomputePartCRC64NVME(f *os.File, offset, length int64) (string, error) {
	hr, err := utils.NewHashReader(io.NewSectionReader(f, offset, length), "", utils.HashTypeCRC64NVME)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(io.Discard, hr); err != nil {
		return "", err
	}
	return hr.Sum(), nil
}

// offsetWriter streams io.Copy output into a file at a fixed starting offset via
// WriteAt, so a part can be written at its computed position in the data file.
type offsetWriter struct {
	f   *os.File
	off int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

// userPartChecksum returns the client-supplied checksum algorithm/value for a
// part, if any (request-header form). Trailing/chunked checksums are not honored
// on the fast path.
func userPartChecksum(input *s3.UploadPartInput) (utils.HashType, string) {
	for _, c := range []struct {
		v *string
		t utils.HashType
	}{
		{input.ChecksumCRC32, utils.HashTypeCRC32},
		{input.ChecksumCRC32C, utils.HashTypeCRC32C},
		{input.ChecksumSHA1, utils.HashTypeSha1},
		{input.ChecksumSHA256, utils.HashTypeSha256},
		{input.ChecksumCRC64NVME, utils.HashTypeCRC64NVME},
		{input.ChecksumSHA512, utils.HashTypeSha512},
		{input.ChecksumMD5, utils.HashTypeMd5},
		{input.ChecksumXXHASH64, utils.HashTypeXXHASH64},
		{input.ChecksumXXHASH3, utils.HashTypeXXHASH3},
		{input.ChecksumXXHASH128, utils.HashTypeXXHASH128},
	} {
		if c.v != nil && *c.v != "" {
			return c.t, *c.v
		}
	}
	return "", ""
}

// uploadPartAtOffset implements UploadPart for the sameDirTmp (Otter) path. The
// whole read-modify-write runs under the per-uploadID lock. A part whose final
// offset is known (confirmed stride match, or part #1) is written once at that
// offset in the data file (no-copy fast path); any other part is appended to the
// upload's single .stash file and folded at Complete. The data write precedes the
// index commit and Complete re-validates every part's MD5, so a crash can only
// cost a retry, never a silently corrupt object (no fsync needed; see writeAtomic).
func (p *Posix) uploadPartAtOffset(bucket, object, uploadID string, partNum int, length int64, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if length <= 0 {
		// A reliable offset needs a known, positive part length; reject rather
		// than risk placing parts at a wrong offset.
		return nil, s3err.GetInvalidPartErr(uploadID, int32(partNum), "")
	}

	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	idxPath := mpIndexPath(bucket, object, uploadID)
	idx, err := readMpIndex(idxPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read mpu index: %w", err)
		}
		idx = &otterMpuIndex{}
	}

	offset, disp := idx.classify(partNum, length)

	var targetPath string
	var writeOff int64
	switch disp {
	case dispPlace:
		targetPath = mpDataFilePath(bucket, object, uploadID)
		writeOff = offset
	default: // dispStash
		targetPath = mpStashFilePath(bucket, object, uploadID)
		writeOff = idx.StashNext
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}
	tf, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		return nil, fmt.Errorf("open mpu part file: %w", err)
	}
	defer tf.Close()

	hash := md5.New()
	tr := io.TeeReader(input.Body, hash)

	chAlgo, chVal := userPartChecksum(input)
	var hashRdr *utils.HashReader
	if chAlgo != "" {
		hashRdr, err = utils.NewHashReader(tr, chVal, chAlgo)
		if err != nil {
			return nil, fmt.Errorf("init part hash reader: %w", err)
		}
		tr = hashRdr
	}

	written, err := io.Copy(&offsetWriter{f: tf, off: writeOff}, tr)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		if _, ok := err.(s3err.S3Error); ok {
			return nil, err
		}
		return nil, fmt.Errorf("write part data: %w", err)
	}

	etag := backend.GenerateEtag(hash)

	rec := otterMpuPart{PartNumber: partNum, Len: written, ETag: etag}
	switch disp {
	case dispPlace:
		rec.Offset = offset
	default: // dispStash
		rec.Stashed = true
		rec.StashOffset = writeOff
		idx.StashNext += written
	}

	// Resolve/confirm the stride S so later parts take the no-copy fast path.
	// Part #1 is authoritative; otherwise S becomes resolvable the moment a second
	// distinct part number is seen, as the max of the distinct parts' true lengths
	// (current part included) — at most one part may be short, so that max is S.
	if idx.Stride == 0 {
		if partNum == 1 {
			idx.Stride = written
		} else {
			maxLen, distinct := written, 1
			for i := range idx.Parts {
				if idx.Parts[i].PartNumber == partNum {
					continue // not a second distinct sample
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
	if err := writeMpIndexAtomic(idxPath, idx); err != nil {
		return nil, fmt.Errorf("write mpu index: %w", err)
	}

	res := &s3.UploadPartOutput{ETag: &etag}
	if hashRdr != nil {
		sum := hashRdr.Sum()
		setUploadPartChecksum(res, types.ChecksumAlgorithm(strings.ToUpper(string(chAlgo))), &sum)
	}
	return res, nil
}

// completeMultipartAtOffset implements CompleteMultipartUpload for the sameDirTmp
// (Otter) path. The whole validate → fold → claim → reveal sequence runs under
// the per-uploadID lock. Stride S and the true last part N are resolved from the
// authoritative ordered client part list; stashed parts are folded into the data
// file before validation, so the validation loop then sees every part at its
// final offset and the uniform-except-shorter-last upload lands on the no-copy
// reveal branch.
func (p *Posix) completeMultipartAtOffset(input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	res := s3response.CompleteMultipartUploadResult{}
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	s3MD5, err := backend.GetMultipartMD5(parts)
	if err != nil {
		return res, "", err
	}

	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	dataPath := mpDataFilePath(bucket, object, uploadID)
	stashPath := mpStashFilePath(bucket, object, uploadID)
	completingPath := mpCompletingPath(bucket, object, uploadID)
	finalPath := filepath.Join(bucket, object)

	if _, err := p.checkUploadIDExists(bucket, object, uploadID); err != nil {
		// Idempotency: a retry after a successful completion finds the upload
		// state already removed; if the final object exists, treat as success.
		if _, statErr := os.Stat(finalPath); statErr == nil {
			p.mpuUnlockDelete(bucket, object, uploadID)
			return s3response.CompleteMultipartUploadResult{Bucket: &bucket, ETag: &s3MD5, Key: &object}, "", nil
		}
		return res, "", err
	}

	// Resume path: a crash between the claim-rename and the final reveal leaves
	// the (already-folded) data under completingPath with no dataPath. Re-run
	// validation + reveal from completingPath.
	if _, statErr := os.Stat(dataPath); os.IsNotExist(statErr) {
		if _, cstatErr := os.Stat(completingPath); cstatErr == nil {
			return p.validateRevealMpu(input, completingPath, finalPath, s3MD5, true)
		}
	}

	idx, err := readMpIndex(mpIndexPath(bucket, object, uploadID))
	if err != nil {
		if os.IsNotExist(err) {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		return res, "", fmt.Errorf("read mpu index: %w", err)
	}

	// --- pre-validate & resolve stride from the authoritative ordered list ----
	//
	// N is the TRUE last part (max listed PartNumber); S is the stride, taken
	// from part #1 if known, else from the lowest-numbered listed part's record.
	var N int
	for _, part := range parts {
		if part.PartNumber != nil && int(*part.PartNumber) > N {
			N = int(*part.PartNumber)
		}
	}
	S := idx.Stride
	if S == 0 {
		// Part #1 never arrived: resolve S from the lowest-numbered listed part.
		lowest := -1
		for _, part := range parts {
			if part.PartNumber == nil {
				continue
			}
			if lowest < 0 || int(*part.PartNumber) < lowest {
				lowest = int(*part.PartNumber)
			}
		}
		if lowest >= 0 {
			if rec, _ := idx.find(lowest); rec != nil {
				S = rec.Len
			}
		}
	}

	for _, part := range parts {
		if part.PartNumber == nil || part.ETag == nil {
			return res, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		pn := int(*part.PartNumber)
		rec, _ := idx.find(pn)
		if rec == nil {
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		isLast := pn == N
		if !isLast && rec.Len != S {
			// Genuine non-uniform interior part (or a 2nd short part).
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		if isLast && rec.Len > S {
			// The tail cannot exceed the stride.
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		if !isLast && rec.Len < backend.MinPartSize {
			return res, "", s3err.GetEntityTooSmallErr(rec.Len, backend.MinPartSize)
		}
	}

	// --- write-ahead fold of stashed parts into the data file -----------------
	//
	// For each listed part whose bytes still live in the stash file, copy them to
	// their final offset (partNum-1)*S in the data file, flip the record, and
	// persist the index. The stash is NOT dropped here — it stays until the
	// post-reveal cleanup — so a crash mid-fold either re-folds from the still-
	// present stash, or (if an index flip persisted ahead of its data) is caught
	// by the per-part MD5 check at Complete as InvalidPart → client retry. Never a
	// corrupt object, so no fsync is needed.
	idxPath := mpIndexPath(bucket, object, uploadID)
	needFold := false
	for _, part := range parts {
		if rec, _ := idx.find(int(*part.PartNumber)); rec != nil && rec.Stashed {
			needFold = true
			break
		}
	}
	if needFold {
		sf, err := os.Open(stashPath)
		if err != nil {
			return res, "", fmt.Errorf("open mpu stash file: %w", err)
		}
		df, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			sf.Close()
			return res, "", fmt.Errorf("open mpu data file: %w", err)
		}
		for _, part := range parts {
			pn := int(*part.PartNumber)
			rec, ri := idx.find(pn)
			if rec == nil || !rec.Stashed {
				continue
			}
			finalOff := int64(pn-1) * S
			if _, err := io.Copy(&offsetWriter{f: df, off: finalOff},
				io.NewSectionReader(sf, rec.StashOffset, rec.Len)); err != nil {
				df.Close()
				sf.Close()
				return res, "", fmt.Errorf("fold stashed part: %w", err)
			}
			idx.Parts[ri].Stashed = false
			idx.Parts[ri].Offset = finalOff
			idx.Parts[ri].StashOffset = 0
			if err := writeMpIndexAtomic(idxPath, idx); err != nil {
				df.Close()
				sf.Close()
				return res, "", fmt.Errorf("write mpu index after fold: %w", err)
			}
		}
		df.Close()
		sf.Close()
	}

	// Folds done: drop stray bytes beyond the parts' extent. dataSize is the parts'
	// max committed extent, NOT the raw file size, so leftover stash/scratch bytes
	// can never neuter the crash guard. The stash file is left in place until the
	// post-reveal cleanup (validateRevealMpu), so a crash before reveal can still
	// re-fold from it.
	var dataSize int64
	for _, part := range parts {
		if rec, _ := idx.find(int(*part.PartNumber)); rec != nil {
			if end := rec.Offset + rec.Len; end > dataSize {
				dataSize = end
			}
		}
	}
	if err := os.Truncate(dataPath, dataSize); err != nil {
		return res, "", fmt.Errorf("truncate mpu data file: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return res, "", fmt.Errorf("create object dir: %w", err)
	}

	// Claim the upload by renaming the data file; ENOENT => another caller won.
	if err := os.Rename(dataPath, completingPath); err != nil {
		if os.IsNotExist(err) {
			if _, statErr := os.Stat(finalPath); statErr == nil {
				p.mpuUnlockDelete(bucket, object, uploadID)
				return s3response.CompleteMultipartUploadResult{Bucket: &bucket, ETag: &s3MD5, Key: &object}, "", nil
			}
			return res, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return res, "", fmt.Errorf("claim mpu data file: %w", err)
	}

	return p.validateRevealMpu(input, completingPath, finalPath, s3MD5, false)
}

// revealKind records which Complete reveal branch was taken, for tests that must
// assert the no-copy path is actually used (invisible to byte checks).
type revealKind int

const (
	revealNoCopy  revealKind = iota // contiguous data revealed by rename, no copy
	revealCompact                   // non-contiguous layout compacted into a temp
)

// validateRevealMpu runs the post-fold validation loop against srcPath (the
// claimed completingPath, where every listed part now lives at its final
// offset), recomputes per-part MD5/CRC, and reveals the object — by rename when
// the layout is contiguous (no-copy), else by compacting the listed ranges. It
// is also the resume entry point (resume==true) when a crash left only
// completingPath. On success it cleans up state and drops the lock entry.
func (p *Posix) validateRevealMpu(input *s3.CompleteMultipartUploadInput, srcPath, finalPath, s3MD5 string, resume bool) (s3response.CompleteMultipartUploadResult, string, error) {
	res := s3response.CompleteMultipartUploadResult{}
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	idx, err := readMpIndex(mpIndexPath(bucket, object, uploadID))
	if err != nil {
		return res, "", fmt.Errorf("read mpu index: %w", err)
	}

	df, err := os.OpenFile(srcPath, os.O_RDWR, 0600)
	if err != nil {
		return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	dfi, err := df.Stat()
	if err != nil {
		df.Close()
		return res, "", fmt.Errorf("stat mpu data file: %w", err)
	}
	fileSize := dfi.Size()

	// dataSize is the parts' max committed extent (the crash guard reference),
	// never the raw file size.
	var dataSize int64
	for _, part := range parts {
		if part.PartNumber == nil {
			continue
		}
		if rec, _ := idx.find(int(*part.PartNumber)); rec != nil {
			if end := rec.Offset + rec.Len; end > dataSize {
				dataSize = end
			}
		}
	}

	type rng struct{ off, length int64 }
	ranges := make([]rng, 0, len(parts))
	last := len(parts) - 1
	var totalsize, cumulative int64
	var prevPartNum int32
	contiguous := true
	crc := ""

	for i, part := range parts {
		if part.PartNumber == nil || part.ETag == nil {
			df.Close()
			return res, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if *part.PartNumber < 1 {
			df.Close()
			return res, "", s3err.GetInvalidArgumentErr(s3err.InvalidArgCompleteMpPartNumber, fmt.Sprint(*part.PartNumber))
		}
		if *part.PartNumber <= prevPartNum {
			df.Close()
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		prevPartNum = *part.PartNumber

		rec, _ := idx.find(int(*part.PartNumber))
		// Hard error (not a silent skip) if the index has no entry or the data
		// write did not fully land — covers a crash between data write and index
		// update. dataSize is the parts' extent, so stray bytes can't mask it.
		if rec == nil || rec.Offset+rec.Len > dataSize {
			df.Close()
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		if i < last && rec.Len < backend.MinPartSize {
			df.Close()
			return res, "", s3err.GetEntityTooSmallErr(rec.Len, backend.MinPartSize)
		}

		md5sum, err := recomputePartMD5(df, rec.Offset, rec.Len)
		if err != nil {
			df.Close()
			return res, "", fmt.Errorf("recompute part md5: %w", err)
		}
		if !backend.AreEtagsSame(rec.ETag, *part.ETag) || !backend.AreEtagsSame(md5sum, *part.ETag) {
			df.Close()
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}

		pc, err := recomputePartCRC64NVME(df, rec.Offset, rec.Len)
		if err != nil {
			df.Close()
			return res, "", fmt.Errorf("recompute part crc: %w", err)
		}
		if i == 0 {
			crc = pc
		} else if crc, err = utils.AddCRCChecksum(types.ChecksumAlgorithmCrc64nvme, crc, pc, rec.Len); err != nil {
			df.Close()
			return res, "", fmt.Errorf("accumulate crc: %w", err)
		}

		if rec.Offset != cumulative {
			contiguous = false
		}
		cumulative += rec.Len
		totalsize += rec.Len
		ranges = append(ranges, rng{rec.Offset, rec.Len})
	}
	df.Close()

	// content-type / user metadata captured at Create live in the upload-meta
	// plain file (not a DESC attr, since the state dir is a directory).
	mprops, err := readMpUploadMeta(mpUploadMetaPath(bucket, object, uploadID))
	if err != nil {
		// Best-effort: proceed without object metadata if unreadable.
		mprops = metaProperties{}
	}

	if contiguous && cumulative == fileSize {
		// Data already laid out [0,total); reveal it with no copy.
		p.lastRevealKind = revealNoCopy
		cf, err := os.OpenFile(srcPath, os.O_RDWR, 0600)
		if err != nil {
			return res, "", fmt.Errorf("open completing file: %w", err)
		}
		if err := cf.Truncate(totalsize); err != nil {
			cf.Close()
			return res, "", fmt.Errorf("truncate completing file: %w", err)
		}
		if err := p.writeFinalMpuMeta(cf, bucket, object, s3MD5, mprops); err != nil {
			cf.Close()
			return res, "", err
		}
		cf.Close()
		if err := os.Rename(srcPath, finalPath); err != nil {
			return res, "", fmt.Errorf("reveal object: %w", err)
		}
	} else {
		// Non-contiguous completion (gaps/reordered/re-pinned-stride): compact the
		// listed parts in order into a same-dir temp, then reveal.
		p.lastRevealKind = revealCompact
		srcf, err := os.Open(srcPath)
		if err != nil {
			return res, "", fmt.Errorf("open completing file: %w", err)
		}
		asmPath := srcPath + ".asm"
		af, err := os.OpenFile(asmPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			srcf.Close()
			return res, "", fmt.Errorf("open assemble file: %w", err)
		}
		for _, rg := range ranges {
			if _, err := io.Copy(af, io.NewSectionReader(srcf, rg.off, rg.length)); err != nil {
				af.Close()
				srcf.Close()
				os.Remove(asmPath)
				return res, "", fmt.Errorf("assemble object: %w", err)
			}
		}
		srcf.Close()
		if err := p.writeFinalMpuMeta(af, bucket, object, s3MD5, mprops); err != nil {
			af.Close()
			os.Remove(asmPath)
			return res, "", err
		}
		af.Close()
		if err := os.Rename(asmPath, finalPath); err != nil {
			os.Remove(asmPath)
			return res, "", fmt.Errorf("reveal object: %w", err)
		}
		os.Remove(srcPath)
	}

	// Best-effort cleanup of the upload's state dir, stash, and hash dir.
	os.Remove(mpStashFilePath(bucket, object, uploadID))
	os.RemoveAll(mpStateDir(bucket, object, uploadID))
	cleanupMpHashDir(mpHashDir(bucket, object))
	p.mpuUnlockDelete(bucket, object, uploadID)

	fullObject := types.ChecksumTypeFullObject
	return s3response.CompleteMultipartUploadResult{
		Bucket:            &bucket,
		ETag:              &s3MD5,
		Key:               &object,
		ChecksumCRC64NVME: &crc,
		ChecksumType:      &fullObject,
	}, "", nil
}

// writeFinalMpuMeta writes the revealed object's must-store metadata (content
// type / user metadata captured at Create, plus the composite ETag) onto the
// final file's fd before the reveal rename, so it rides the inode.
func (p *Posix) writeFinalMpuMeta(f *os.File, bucket, object, etag string, mprops metaProperties) error {
	if err := p.storeObjectMetaProperties(f, bucket, object, mprops); err != nil {
		return fmt.Errorf("set object metadata: %w", err)
	}
	if err := p.meta.StoreAttribute(f, bucket, object, etagkey, []byte(etag)); err != nil {
		return fmt.Errorf("set etag attr: %w", err)
	}
	return nil
}

// listPartsFromIndex builds ListParts results from the per-upload index (full
// fidelity: correct sizes and ETags), honoring part-number-marker / max-parts.
func (p *Posix) listPartsFromIndex(bucket, object, uploadID string, marker, maxParts int) (s3response.ListPartsResult, error) {
	// Take the per-uploadID lock for a consistent snapshot of the index against
	// concurrent UploadPart/Complete. Stashed parts are normal records carrying
	// the true PartNumber/Len/ETag, so they surface with the right size/ETag; we
	// never emit Offset/StashOffset (only PartNumber/Size/ETag go to the client).
	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	idx, err := readMpIndex(mpIndexPath(bucket, object, uploadID))
	if err != nil && !os.IsNotExist(err) {
		return s3response.ListPartsResult{}, fmt.Errorf("read mpu index: %w", err)
	}

	var modTime time.Time
	if fi, statErr := os.Stat(mpDataFilePath(bucket, object, uploadID)); statErr == nil {
		modTime = fi.ModTime()
	}

	var recs []otterMpuPart
	if idx != nil {
		recs = append(recs, idx.Parts...)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].PartNumber < recs[j].PartNumber })

	parts := make([]s3response.Part, 0, len(recs))
	for _, r := range recs {
		if r.PartNumber <= marker {
			continue
		}
		parts = append(parts, s3response.Part{
			PartNumber:   r.PartNumber,
			ETag:         r.ETag,
			Size:         r.Len,
			LastModified: modTime,
		})
	}

	oldLen := len(parts)
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
	}
	nextpart := 0
	if len(parts) != 0 {
		nextpart = parts[len(parts)-1].PartNumber
	}

	return s3response.ListPartsResult{
		Bucket:               bucket,
		IsTruncated:          oldLen != len(parts),
		Key:                  object,
		MaxParts:             maxParts,
		NextPartNumberMarker: nextpart,
		PartNumberMarker:     marker,
		Parts:                parts,
		UploadID:             uploadID,
		StorageClass:         types.StorageClassStandard,
		ChecksumAlgorithm:    types.ChecksumAlgorithm("null"),
		ChecksumType:         types.ChecksumType("null"),
	}, nil
}

// readMpObjName returns the S3 key for a multipart hash dir, reading the plain
// objname file first (written under sameDirTmp so it survives the DESC storer
// dropping the onameAttr attribute) and falling back to the metadata storer.
func (p *Posix) readMpObjName(bucket, hashName string) (string, error) {
	b, err := os.ReadFile(filepath.Join(bucket, MetaTmpMultipartDir, hashName, onameAttr))
	if err == nil {
		return string(b), nil
	}
	mb, merr := p.meta.RetrieveAttribute(nil, bucket, filepath.Join(MetaTmpMultipartDir, hashName), onameAttr)
	if merr != nil {
		return "", merr
	}
	return string(mb), nil
}

// cleanupMpHashDir removes the hash dir's objname file and the hash dir itself
// when no other uploads for the key remain.
func cleanupMpHashDir(hashDir string) {
	entries, err := os.ReadDir(hashDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.Name() != onameAttr {
			return // another upload for this key is still in flight
		}
	}
	os.Remove(filepath.Join(hashDir, onameAttr))
	os.Remove(hashDir)
}

// ---- Af2MPUHandler: implements MPUHandler for AF2/SDFS write-at-offset -----

// Af2MPUHandler is an MPUHandler for Rubrik CDM SDFS AF2 channels. It writes
// every part directly at its final byte offset in a single data file and
// reveals the completed object with a same-directory rename — one write per
// byte, no staging copy. The stash-the-tail mechanism handles parts that
// arrive before the stride is resolvable or that are shorter than the stride.
type Af2MPUHandler struct{}

func (Af2MPUHandler) CreateMultipartUpload(_ context.Context, p *Posix, mpu s3response.CreateMultipartUploadInput, bucket, object, uploadID string) (s3response.InitiateMultipartUploadResult, error) {
	if err := os.WriteFile(mpObjNamePath(bucket, object), []byte(object), 0600); err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("write mpu objname: %w", err)
	}
	if err := writeMpUploadMeta(mpUploadMetaPath(bucket, object, uploadID), metaProperties{
		ContentType:        mpu.ContentType,
		ContentEncoding:    mpu.ContentEncoding,
		ContentDisposition: mpu.ContentDisposition,
		ContentLanguage:    mpu.ContentLanguage,
		CacheControl:       mpu.CacheControl,
		Expires:            mpu.Expires,
		Metadata:           mpu.Metadata,
	}); err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("write mpu upload-meta: %w", err)
	}
	return s3response.InitiateMultipartUploadResult{Bucket: bucket, Key: object, UploadId: uploadID}, nil
}

func (Af2MPUHandler) UploadPart(_ context.Context, p *Posix, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	length := aws.ToInt64(input.ContentLength)
	return p.uploadPartAtOffset(*input.Bucket, *input.Key, *input.UploadId, int(aws.ToInt32(input.PartNumber)), length, input)
}

func (Af2MPUHandler) CompleteMultipartUpload(_ context.Context, p *Posix, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	return p.completeMultipartAtOffset(input)
}

func (Af2MPUHandler) AbortMultipartUpload(_ context.Context, p *Posix, input *s3.AbortMultipartUploadInput) error {
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId

	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	os.Remove(mpDataFilePath(bucket, object, uploadID))
	os.Remove(mpStashFilePath(bucket, object, uploadID))
	os.RemoveAll(mpStateDir(bucket, object, uploadID))
	cleanupMpHashDir(mpHashDir(bucket, object))
	p.mpuUnlockDelete(bucket, object, uploadID)
	return nil
}

func (Af2MPUHandler) ListParts(_ context.Context, p *Posix, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	marker := 0
	if input.PartNumberMarker != nil {
		if n, err := strconv.Atoi(*input.PartNumberMarker); err == nil {
			marker = n
		}
	}
	maxParts := int(aws.ToInt32(input.MaxParts))
	return p.listPartsFromIndex(bucket, object, uploadID, marker, maxParts)
}
