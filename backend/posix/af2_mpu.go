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

// Otter multipart write-at-offset support (LLD §4b) — complete-only-fsync model.
//
// Each part is written directly at its final byte offset in ONE data file and
// the object is revealed with a same-directory rename at Complete, so the data
// is written once. Under the whole-object-retry crash contract, per-part
// durability is not needed: parts are acked as they arrive but nothing is
// fsync'd until Complete, when the assembled object's data is fsync'd once and
// the reveal rename is made durable (parent-dir fsync). Any crash before that
// single durability point loses only the hidden scratch file; the client
// re-uploads the whole object.
//
// State:
//   - The per-upload index (stride + placed part records + the pre-stride
//     buffer) is IN-MEMORY, keyed by (bucket,object,uploadID) in p.mpuIndexes.
//     It is not durable; a gateway restart drops it and the client retries.
//   - CreateMultipartUpload still writes the on-disk state dir + objname +
//     upload-meta, so checkUploadIDExists / ListMultipartUploads are unchanged.
//   - <objdir>/.sgwtmp.<hash(key)>.<uploadID>.data  — the single data file.
//   - <objdir>/.sgwtmp.<hash(key)>.<uploadID>.stash — a spill file, used ONLY
//     when a pre-stride buffered part exceeds the in-memory cap (page-cache, no
//     fsync). Common uploads never create it.
//
// A part whose final offset is not yet known (it arrived before the stride was
// resolvable) is held in the in-memory buffer (or spilled) and flushed to the
// data file the instant the stride resolves. Because the stride resolves as soon
// as part #1 or a second distinct part number is seen, at most one part is ever
// buffered.

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
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// applyFinalObjectPerms sets the revealed object's mode (and ownership when
// chown is configured) on its fd before the reveal rename, matching the standard
// posix path — otherwise Af2-completed objects would keep the in-progress data
// file's 0600 mode owned by the gateway process. Directory ownership on this
// path still follows the gateway process (the object's parent dir is created
// lazily during UploadPart, before the account is known); that is acceptable for
// the SDFS deployment (root; CDM consumes via AF2, not direct POSIX) and full
// dir-chown parity is a follow-up if a POSIX-export use case needs it.
func (p *Posix) applyFinalObjectPerms(f *os.File, acct auth.Account) error {
	if err := f.Chmod(os.FileMode(defaultFilePerm)); err != nil {
		return fmt.Errorf("chmod final object: %w", err)
	}
	if uid, gid, doChown := p.getChownIDs(acct); doChown {
		if err := f.Chown(uid, gid); err != nil {
			return fmt.Errorf("chown final object: %w", err)
		}
	}
	return nil
}

// otterMpuPart is one PLACED part (its bytes are already in the data file).
type otterMpuPart struct {
	PartNumber int
	Offset     int64  // final data-file offset
	Len        int64  // true written length (never the declared/stride length)
	ETag       string // md5 ETag computed at UploadPart
	CRC        string // part CRC32C computed at UploadPart, for O(1) full-object CRC at Complete
}

// pendingPart is a part received before its final offset was known. Its bytes
// live in RAM (data != nil) or, if larger than the cap, in the per-upload spill
// file (spilled == true). Hashes are computed on arrival so no re-read is needed.
type pendingPart struct {
	data     []byte // non-nil iff held in RAM
	spilled  bool   // true iff bytes are in the spill file at spillOff
	spillOff int64
	length   int64
	etag     string
	crc      string
}

// otterMpuIndex is the per-upload write-at-offset index. It is IN-MEMORY only
// (never serialized) under the complete-only-fsync model.
type otterMpuIndex struct {
	// Stride is the confirmed uniform part size S (0 = not yet resolved). It is
	// set from part #1 (authoritative) or, once two distinct part numbers are
	// seen, as the max of their true lengths — at most one part (the last) may be
	// short, so that max is S.
	Stride int64
	Parts  []otterMpuPart // placed parts only

	// pending holds parts awaiting stride resolution. Invariant: at most one
	// entry at a time (a re-upload of the same number overwrites the slot; the
	// second distinct number resolves the stride and drains the buffer).
	pending    map[int]*pendingPart
	pendingRAM int64 // total bytes currently held in RAM across pending (cap accounting)
	spillNext  int64 // append cursor into the per-upload spill file (disk fallback)
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

// mpStashFileName is the per-upload spill file's basename (".sgwtmp." prefix so
// the listing skip rule hides it, ".stash" suffix). Used only as a disk fallback
// when a pre-stride buffered part exceeds the in-memory cap; written without
// fsync and drained at stride resolution.
func mpStashFileName(key, uploadID string) string {
	return fmt.Sprintf("%s.%s.%s.stash", MetaTmpDir, mpKeyHash(key), uploadID)
}

// mpStashFilePath is the spill file in the object's own directory.
func mpStashFilePath(bucket, key, uploadID string) string {
	objDir := filepath.Dir(filepath.Join(bucket, key))
	return filepath.Join(objDir, mpStashFileName(key, uploadID))
}

// --- per-uploadID lock ----------------------------------------------------

// mpuLock returns the *sync.Mutex serializing all index/data/spill mutations for
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

// --- in-memory index store ------------------------------------------------

func mpuIndexKey(bucket, object, uploadID string) string {
	return bucket + "\x00" + object + "\x00" + uploadID
}

// getMpuIndex returns the in-memory index for the uploadID, creating an empty
// one if absent. The bool is false when a fresh index was created (first part of
// the upload in this process), which the caller uses to drop any scratch left by
// a prior attempt that reused this uploadID (e.g. across a gateway restart).
// Callers must hold the per-uploadID mutex.
func (p *Posix) getMpuIndex(bucket, object, uploadID string) (*otterMpuIndex, bool) {
	k := mpuIndexKey(bucket, object, uploadID)
	if v, ok := p.mpuIndexes.Load(k); ok {
		return v.(*otterMpuIndex), true
	}
	idx := &otterMpuIndex{pending: make(map[int]*pendingPart)}
	actual, loaded := p.mpuIndexes.LoadOrStore(k, idx)
	return actual.(*otterMpuIndex), loaded
}

// peekMpuIndex returns the in-memory index without creating one.
func (p *Posix) peekMpuIndex(bucket, object, uploadID string) (*otterMpuIndex, bool) {
	if v, ok := p.mpuIndexes.Load(mpuIndexKey(bucket, object, uploadID)); ok {
		return v.(*otterMpuIndex), true
	}
	return nil, false
}

// delMpuIndex drops the in-memory index (and, with it, any buffered bytes).
func (p *Posix) delMpuIndex(bucket, object, uploadID string) {
	p.mpuIndexes.Delete(mpuIndexKey(bucket, object, uploadID))
}

// --- durable helpers (Create-time upload-meta only) -----------------------

// fsyncParentDir opens the parent directory of path and fsyncs it so a rename
// into (or unlink from) that directory is durable.
func fsyncParentDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// writeDurableAtomic writes b to a UNIQUE same-directory temp file, fsyncs the
// temp fd, renames it onto path, then fsyncs the parent dir so both the bytes
// and the rename survive a crash. Used for the Create-time upload-meta (written
// once, off the per-part hot path).
func writeDurableAtomic(path string, b []byte) error {
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
	if err := f.Sync(); err != nil {
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
	return fsyncParentDir(path)
}

// writeMpUploadMeta persists the upload's object metadata as a plain JSON file,
// durably (so a crash at Create does not silently lose the upload metadata).
func writeMpUploadMeta(path string, m metaProperties) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal mpu upload-meta: %w", err)
	}
	return writeDurableAtomic(path, b)
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

// --- index helpers --------------------------------------------------------

// find returns the recorded (placed) part with the given number and its slice
// index, or (nil, -1) if absent.
func (idx *otterMpuIndex) find(n int) (*otterMpuPart, int) {
	for i := range idx.Parts {
		if idx.Parts[i].PartNumber == n {
			return &idx.Parts[i], i
		}
	}
	return nil, -1
}

// put inserts or replaces a placed part record.
func (idx *otterMpuIndex) put(part otterMpuPart) {
	if _, i := idx.find(part.PartNumber); i >= 0 {
		idx.Parts[i] = part
		return
	}
	idx.Parts = append(idx.Parts, part)
}

// lenOf returns the true length of a part number whether it is placed or still
// pending, or 0 if unknown.
func (idx *otterMpuIndex) lenOf(n int) int64 {
	if rec, _ := idx.find(n); rec != nil {
		return rec.Len
	}
	if pp, ok := idx.pending[n]; ok {
		return pp.length
	}
	return 0
}

// --- placement ------------------------------------------------------------

// mpDisposition is what UploadPart should do with an incoming part.
type mpDisposition int

const (
	dispPlace  mpDisposition = iota // write at the returned final offset (no-copy fast path)
	dispBuffer                      // hold in the pre-stride buffer (RAM or spill)
)

// classify decides where an incoming (partNum n, length L) goes, purely from the
// current index. It NEVER mutates and NEVER rejects — final size/uniformity
// rejection is deferred to Complete. It returns the final data-file offset (when
// placeable), the disposition, and the stride value confirmed BY this part
// (newStride == 0 means the stride is still unresolved).
//
//   - Once the stride is known, EVERY part is placed directly at (n-1)*S — full
//     or short. A short tail's offset is deterministic; if it turns out not to be
//     the tail, Complete's uniformity check rejects the whole upload.
//   - Part #1 is authoritative for S (place@0).
//   - Otherwise the stride resolves the moment a SECOND distinct part number is
//     seen, as max(this length, the other parts' max) — so this part can be
//     placed and the caller then drains the one buffered part.
//   - Only when no other distinct part has been seen (and this is not #1) can the
//     stride not be resolved: buffer this part.
//
// The distinct-part scan unions Parts (placed) AND pending, because a pre-stride
// part lives in pending, not Parts.
func (idx *otterMpuIndex) classify(n int, L int64) (offset int64, disp mpDisposition, newStride int64) {
	if idx.Stride != 0 {
		return int64(n-1) * idx.Stride, dispPlace, idx.Stride
	}
	if n == 1 {
		return 0, dispPlace, L
	}
	otherMax, others := int64(0), 0
	consider := func(pn int, l int64) {
		if pn == n {
			return // a re-upload of this same part is not a second distinct sample
		}
		others++
		if l > otherMax {
			otherMax = l
		}
	}
	for i := range idx.Parts {
		consider(idx.Parts[i].PartNumber, idx.Parts[i].Len)
	}
	for pn, pp := range idx.pending {
		consider(pn, pp.length)
	}
	if others >= 1 {
		S := L
		if otherMax > S {
			S = otherMax
		}
		return int64(n-1) * S, dispPlace, S
	}
	return 0, dispBuffer, 0
}

// --- byte I/O helpers -----------------------------------------------------

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
	} {
		if c.v != nil && *c.v != "" {
			return c.t, *c.v
		}
	}
	return "", ""
}

// mpuWriteErr maps a data-write error to the right S3 error.
func mpuWriteErr(err error) error {
	if errors.Is(err, syscall.ENOSPC) {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	if _, ok := err.(s3err.S3Error); ok {
		return err
	}
	return fmt.Errorf("write part data: %w", err)
}

// --- UploadPart -----------------------------------------------------------

// uploadPartAtOffset implements UploadPart for the Af2 write-at-offset path under
// the complete-only-fsync model. A part whose final offset is known (stride
// confirmed, or part #1) is written once at that offset in the data file (page
// cache, NO fsync); any other part is buffered in RAM (or spilled to disk if it
// exceeds the cap) and flushed the instant the stride resolves. The whole
// read-modify-write runs under the per-uploadID lock.
func (p *Posix) uploadPartAtOffset(bucket, object, uploadID string, partNum int, length int64, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if length <= 0 {
		// A reliable offset needs a known, positive part length; reject rather
		// than risk placing parts at a wrong offset.
		return nil, s3err.GetInvalidPartErr(uploadID, int32(partNum), "")
	}

	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	idx, loaded := p.getMpuIndex(bucket, object, uploadID)
	if !loaded {
		// Fresh in-memory index: drop any scratch a prior attempt left on disk
		// under this uploadID (e.g. across a gateway restart) so stale bytes
		// cannot survive into this attempt.
		_ = os.Remove(p.rooted(mpDataFilePath(bucket, object, uploadID)))
		_ = os.Remove(p.rooted(mpStashFilePath(bucket, object, uploadID)))
	}

	offset, disp, newStride := idx.classify(partNum, length)

	// Stream the body once: md5 (ETag) + CRC32C (+ optional user checksum).
	hash := md5.New()
	tr := io.TeeReader(input.Body, hash)

	chAlgo, chVal := userPartChecksum(input)
	var hashRdr *utils.HashReader
	var err error
	if chAlgo != "" {
		hashRdr, err = utils.NewHashReader(tr, chVal, chAlgo)
		if err != nil {
			return nil, fmt.Errorf("init part hash reader: %w", err)
		}
		tr = hashRdr
	}
	// Internal full-object checksum: CRC32C (hardware-accelerated via SSE4.2, and
	// the algorithm the aws CLI sends by default) — replaces the table-based
	// CRC64NVME. crc32Combine supports it, so the O(1) Complete accumulation is
	// unchanged.
	crcRdr, err := utils.NewHashReader(tr, "", utils.HashTypeCRC32C)
	if err != nil {
		return nil, fmt.Errorf("init part crc reader: %w", err)
	}
	tr = crcRdr

	var written int64
	switch disp {
	case dispPlace:
		dataPath := p.rooted(mpDataFilePath(bucket, object, uploadID))
		if err := os.MkdirAll(filepath.Dir(dataPath), p.newDirPerm); err != nil {
			return nil, fmt.Errorf("create object dir: %w", err)
		}
		df, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			if errors.Is(err, syscall.ENOSPC) {
				return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
			}
			return nil, fmt.Errorf("open mpu data file: %w", err)
		}
		written, err = io.Copy(&offsetWriter{f: df, off: offset}, tr)
		if err != nil {
			df.Close()
			return nil, mpuWriteErr(err)
		}
		df.Close() // NO fsync

	default: // dispBuffer
		// Evict-before-insert: a re-upload of an already-pending part must
		// reconcile the RAM counter and rewind the spill cursor so the spill file
		// is rewritten in place (never grown across retries).
		if old, ok := idx.pending[partNum]; ok {
			if old.data != nil {
				idx.pendingRAM -= old.length
			}
			if old.spilled {
				idx.spillNext = old.spillOff
			}
			delete(idx.pending, partNum)
		}

		if idx.pendingRAM+length <= p.mpuMemBufferMax {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, mpuWriteErr(err)
			}
			written = int64(len(data))
			idx.pending[partNum] = &pendingPart{data: data, length: written}
			idx.pendingRAM += written
		} else {
			spillPath := p.rooted(mpStashFilePath(bucket, object, uploadID))
			if err := os.MkdirAll(filepath.Dir(spillPath), p.newDirPerm); err != nil {
				return nil, fmt.Errorf("create object dir: %w", err)
			}
			sf, err := os.OpenFile(spillPath, os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				if errors.Is(err, syscall.ENOSPC) {
					return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
				}
				return nil, fmt.Errorf("open mpu spill file: %w", err)
			}
			spillOff := idx.spillNext
			written, err = io.Copy(&offsetWriter{f: sf, off: spillOff}, tr)
			if err != nil {
				sf.Close()
				return nil, mpuWriteErr(err)
			}
			sf.Close() // NO fsync
			idx.pending[partNum] = &pendingPart{spilled: true, spillOff: spillOff, length: written}
			idx.spillNext = spillOff + written
		}
	}

	etag := backend.GenerateEtag(hash)
	partCRC := crcRdr.Sum()

	if disp == dispPlace {
		idx.put(otterMpuPart{PartNumber: partNum, Offset: offset, Len: written, ETag: etag, CRC: partCRC})
	} else {
		pp := idx.pending[partNum]
		pp.length = written
		pp.etag = etag
		pp.crc = partCRC
	}

	// Stride just resolved: persist it and drain the (at most one) buffered part.
	if newStride != 0 && idx.Stride == 0 {
		idx.Stride = newStride
		if err := p.flushPending(bucket, object, uploadID, idx); err != nil {
			return nil, err
		}
	}

	res := &s3.UploadPartOutput{ETag: &etag}
	if hashRdr != nil {
		sum := hashRdr.Sum()
		setUploadPartChecksum(res, types.ChecksumAlgorithm(strings.ToUpper(string(chAlgo))), &sum)
	}
	return res, nil
}

// flushPending writes every buffered part into the data file at its final offset
// (n-1)*Stride (page cache, NO fsync), converting each into a placed record.
// Requires idx.Stride > 0. It subtracts from pendingRAM only for RAM-held parts
// (spilled parts never counted), and removes the spill file once drained.
func (p *Posix) flushPending(bucket, object, uploadID string, idx *otterMpuIndex) error {
	if len(idx.pending) == 0 {
		return nil
	}
	dataPath := p.rooted(mpDataFilePath(bucket, object, uploadID))
	if err := os.MkdirAll(filepath.Dir(dataPath), p.newDirPerm); err != nil {
		return fmt.Errorf("create object dir: %w", err)
	}
	df, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("open mpu data file: %w", err)
	}
	defer df.Close()

	var sf *os.File
	defer func() {
		if sf != nil {
			sf.Close()
		}
	}()

	usedSpill := false
	for pn, pp := range idx.pending {
		off := int64(pn-1) * idx.Stride
		if pp.spilled {
			if sf == nil {
				sf, err = os.Open(p.rooted(mpStashFilePath(bucket, object, uploadID)))
				if err != nil {
					return fmt.Errorf("open mpu spill file: %w", err)
				}
			}
			if _, err := io.Copy(&offsetWriter{f: df, off: off}, io.NewSectionReader(sf, pp.spillOff, pp.length)); err != nil {
				return fmt.Errorf("flush spilled part: %w", err)
			}
			usedSpill = true
		} else {
			if _, err := df.WriteAt(pp.data, off); err != nil {
				return fmt.Errorf("flush buffered part: %w", err)
			}
			idx.pendingRAM -= pp.length
		}
		idx.put(otterMpuPart{PartNumber: pn, Offset: off, Len: pp.length, ETag: pp.etag, CRC: pp.crc})
		delete(idx.pending, pn)
	}

	if usedSpill {
		if sf != nil {
			sf.Close()
			sf = nil
		}
		_ = os.Remove(p.rooted(mpStashFilePath(bucket, object, uploadID)))
		idx.spillNext = 0
	}
	return nil
}

// --- CompleteMultipartUpload ----------------------------------------------

// completeMultipartAtOffset implements CompleteMultipartUpload for the
// Af2 write-at-offset path under the complete-only-fsync model. It validates the
// client part list, resolves the stride, drains any residual buffered part,
// validates uniformity/ETags against the in-memory index (no data re-read), then
// reveals the object with a single data fsync + durable rename. The whole
// sequence runs under the per-uploadID lock.
func (p *Posix) completeMultipartAtOffset(acct auth.Account, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	res := s3response.CompleteMultipartUploadResult{}
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	// The Af2 write-at-offset path does not create object versions (it reveals a
	// single current object by rename). Versioning is disabled in the Otter
	// deployment by design — point-in-time is CDM's SLA-driven recovery points,
	// not S3 versioning. Fail loud rather than silently returning an empty
	// versionID if versioning is ever enabled on this backend.
	if p.versioningEnabled() {
		return res, "", s3err.GetAPIError(s3err.ErrNotImplemented)
	}

	s3MD5, err := backend.GetMultipartMD5(parts)
	if err != nil {
		return res, "", err
	}

	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	dataPath := p.rooted(mpDataFilePath(bucket, object, uploadID))
	finalPath := p.rooted(bucket, object)

	// Idempotency: the on-disk upload state is gone. If the final object exists,
	// a prior Complete already succeeded — return success; else it never existed.
	if _, err := p.checkUploadIDExists(bucket, object, uploadID); err != nil {
		if _, statErr := os.Stat(finalPath); statErr == nil {
			p.delMpuIndex(bucket, object, uploadID)
			p.mpuUnlockDelete(bucket, object, uploadID)
			return s3response.CompleteMultipartUploadResult{Bucket: &bucket, ETag: &s3MD5, Key: &object}, "", nil
		}
		return res, "", err
	}

	idx, ok := p.peekMpuIndex(bucket, object, uploadID)
	if !ok {
		// In-memory state lost (e.g. gateway restart). If the object somehow
		// already exists treat as success; otherwise the client must re-upload
		// the whole object.
		if _, statErr := os.Stat(finalPath); statErr == nil {
			p.mpuUnlockDelete(bucket, object, uploadID)
			return s3response.CompleteMultipartUploadResult{Bucket: &bucket, ETag: &s3MD5, Key: &object}, "", nil
		}
		return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}

	// (1) Validate the client part-list SHAPE + ORDER first. On the Otter path
	// this is the ONLY guard against an out-of-order Complete list (nothing
	// upstream enforces it), and CRC accumulation + range assembly below iterate
	// in listed order — so a mis-ordered list would otherwise assemble a corrupt
	// object and return 200.
	var prevPN int32
	for _, part := range parts {
		if part.PartNumber == nil || part.ETag == nil {
			return res, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if *part.PartNumber < 1 {
			return res, "", s3err.GetInvalidArgumentErr(s3err.InvalidArgCompleteMpPartNumber, fmt.Sprint(*part.PartNumber))
		}
		if *part.PartNumber <= prevPN {
			return res, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		prevPN = *part.PartNumber
	}

	// (2) Resolve N (true last part) and S (stride). S is the confirmed stride if
	// known, else the lowest-numbered listed part's length (part #1 never arrived).
	var N int
	for _, part := range parts {
		if int(*part.PartNumber) > N {
			N = int(*part.PartNumber)
		}
	}
	S := idx.Stride
	if S == 0 {
		lowest := -1
		for _, part := range parts {
			if lowest < 0 || int(*part.PartNumber) < lowest {
				lowest = int(*part.PartNumber)
			}
		}
		if lowest >= 0 {
			S = idx.lenOf(lowest)
		}
	}
	// Drain any residual buffered part only when the stride is resolvable; if S
	// is still 0 (e.g. the lowest listed part was never uploaded) skip the flush
	// and let validation below reject the missing part.
	if idx.Stride == 0 && S > 0 {
		idx.Stride = S
	}
	if idx.Stride > 0 {
		if err := p.flushPending(bucket, object, uploadID, idx); err != nil {
			return res, "", fmt.Errorf("flush pending parts: %w", err)
		}
	}

	// (3) Validate every listed part against the (now fully placed) index.
	for _, part := range parts {
		pn := int(*part.PartNumber)
		rec, _ := idx.find(pn)
		if rec == nil {
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		isLast := pn == N
		if !isLast && rec.Len != S {
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		if isLast && rec.Len > S {
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		if !isLast && rec.Len < backend.MinPartSize {
			return res, "", s3err.GetEntityTooSmallErr(rec.Len, backend.MinPartSize)
		}
		if !backend.AreEtagsSame(rec.ETag, *part.ETag) {
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
	}

	// (4) Truncate the data file to the listed parts' extent (drop stray/spill
	// bytes so leftover scratch can never neuter the crash guard), and remove the
	// spill file if it still exists.
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
	_ = os.Remove(p.rooted(mpStashFilePath(bucket, object, uploadID)))

	if err := os.MkdirAll(filepath.Dir(finalPath), p.newDirPerm); err != nil {
		return res, "", fmt.Errorf("create object dir: %w", err)
	}

	// (5) Accumulate the full-object CRC in listed order + reveal.
	return p.revealMpu(acct, input, idx, dataPath, finalPath, s3MD5)
}

// revealKind records which Complete reveal branch was taken, for tests that must
// assert the no-copy path is actually used (invisible to byte checks).
type revealKind int

const (
	revealNoCopy  revealKind = iota // contiguous data revealed by rename, no copy
	revealCompact                   // non-contiguous layout compacted into a temp
)

// revealMpu accumulates the full-object CRC32C from the per-part records (no
// data re-read), then reveals the object: by rename when the recorded layout is
// already contiguous [0,total) (no-copy), else by compacting the listed ranges
// into a same-dir temp. The single data fsync (and the parent-dir fsync after
// the rename) are the durability point. On success it cleans up all state.
// Callers must have validated the part list and placed every listed part.
func (p *Posix) revealMpu(acct auth.Account, input *s3.CompleteMultipartUploadInput, idx *otterMpuIndex, dataPath, finalPath, s3MD5 string) (s3response.CompleteMultipartUploadResult, string, error) {
	res := s3response.CompleteMultipartUploadResult{}
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	df, err := os.OpenFile(dataPath, os.O_RDWR, 0600)
	if err != nil {
		return res, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	dfi, err := df.Stat()
	if err != nil {
		df.Close()
		return res, "", fmt.Errorf("stat mpu data file: %w", err)
	}
	fileSize := dfi.Size() // after the caller's truncate-to-extent

	type rng struct{ off, length int64 }
	ranges := make([]rng, 0, len(parts))
	var totalsize, cumulative int64
	contiguous := true
	crc := ""

	for i, part := range parts {
		rec, _ := idx.find(int(*part.PartNumber))
		if rec == nil { // guarded by the caller; defensive
			df.Close()
			return res, "", s3err.GetInvalidPartErr(uploadID, *part.PartNumber, *part.ETag)
		}
		if i == 0 {
			crc = rec.CRC
		} else if crc, err = utils.AddCRCChecksum(types.ChecksumAlgorithmCrc32c, crc, rec.CRC, rec.Len); err != nil {
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

	// content-type / user metadata captured at Create live in the upload-meta
	// plain file (not a DESC attr, since the state dir is a directory).
	mprops, err := readMpUploadMeta(p.rooted(mpUploadMetaPath(bucket, object, uploadID)))
	if err != nil {
		mprops = metaProperties{} // best-effort
	}

	if contiguous && cumulative == fileSize {
		// Data already laid out [0,total): reveal with no copy.
		p.lastRevealKind.Store(int32(revealNoCopy))
		if err := df.Truncate(totalsize); err != nil {
			df.Close()
			return res, "", fmt.Errorf("truncate completing file: %w", err)
		}
		if err := p.writeFinalMpuMeta(df, bucket, object, s3MD5, mprops); err != nil {
			df.Close()
			return res, "", err
		}
		if err := p.applyFinalObjectPerms(df, acct); err != nil {
			df.Close()
			return res, "", err
		}
		if err := df.Sync(); err != nil { // single data-durability point
			df.Close()
			return res, "", fmt.Errorf("fsync mpu data file: %w", err)
		}
		df.Close()
		if err := os.Rename(dataPath, finalPath); err != nil {
			return res, "", fmt.Errorf("reveal object: %w", err)
		}
		if err := fsyncParentDir(finalPath); err != nil {
			return res, "", fmt.Errorf("fsync parent dir: %w", err)
		}
	} else {
		// Non-contiguous completion (gaps / reordered / re-pinned stride): compact
		// the listed ranges in order into a same-dir temp, then reveal.
		p.lastRevealKind.Store(int32(revealCompact))
		asmPath := dataPath + ".asm"
		af, err := os.OpenFile(asmPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			df.Close()
			return res, "", fmt.Errorf("open assemble file: %w", err)
		}
		for _, rg := range ranges {
			if _, err := io.Copy(af, io.NewSectionReader(df, rg.off, rg.length)); err != nil {
				af.Close()
				df.Close()
				os.Remove(asmPath)
				return res, "", fmt.Errorf("assemble object: %w", err)
			}
		}
		df.Close()
		if err := p.writeFinalMpuMeta(af, bucket, object, s3MD5, mprops); err != nil {
			af.Close()
			os.Remove(asmPath)
			return res, "", err
		}
		if err := p.applyFinalObjectPerms(af, acct); err != nil {
			af.Close()
			os.Remove(asmPath)
			return res, "", err
		}
		if err := af.Sync(); err != nil { // single data-durability point
			af.Close()
			os.Remove(asmPath)
			return res, "", fmt.Errorf("fsync assemble file: %w", err)
		}
		af.Close()
		if err := os.Rename(asmPath, finalPath); err != nil {
			os.Remove(asmPath)
			return res, "", fmt.Errorf("reveal object: %w", err)
		}
		if err := fsyncParentDir(finalPath); err != nil {
			return res, "", fmt.Errorf("fsync parent dir: %w", err)
		}
		os.Remove(dataPath)
	}

	// Cleanup: state dir, spill, hash dir, in-memory index, lock entry.
	_ = os.Remove(p.rooted(mpStashFilePath(bucket, object, uploadID)))
	os.RemoveAll(p.rooted(mpStateDir(bucket, object, uploadID)))
	cleanupMpHashDir(p.rooted(mpHashDir(bucket, object)))
	p.delMpuIndex(bucket, object, uploadID)
	p.mpuUnlockDelete(bucket, object, uploadID)

	fullObject := types.ChecksumTypeFullObject
	return s3response.CompleteMultipartUploadResult{
		Bucket:         &bucket,
		ETag:           &s3MD5,
		Key:            &object,
		ChecksumCRC32C: &crc,
		ChecksumType:   &fullObject,
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

// --- ListParts ------------------------------------------------------------

// listPartsFromIndex builds ListParts results from the in-memory index (placed
// parts + any pending part, both carrying the true PartNumber/Len/ETag),
// honoring part-number-marker / max-parts. Offsets are never emitted.
func (p *Posix) listPartsFromIndex(bucket, object, uploadID string, marker, maxParts int) (s3response.ListPartsResult, error) {
	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	idx, _ := p.peekMpuIndex(bucket, object, uploadID)

	var modTime time.Time
	if fi, statErr := os.Stat(p.rooted(mpDataFilePath(bucket, object, uploadID))); statErr == nil {
		modTime = fi.ModTime()
	}

	var recs []otterMpuPart
	if idx != nil {
		recs = append(recs, idx.Parts...)
		for pn, pp := range idx.pending {
			recs = append(recs, otterMpuPart{PartNumber: pn, Len: pp.length, ETag: pp.etag})
		}
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
// objname file first (written by the Af2 path so it survives the DESC storer
// dropping the onameAttr attribute) and falling back to the metadata storer.
func (p *Posix) readMpObjName(bucket, hashName string) (string, error) {
	b, err := os.ReadFile(p.rooted(bucket, MetaTmpMultipartDir, hashName, onameAttr))
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
// byte, no staging copy. Under the complete-only-fsync model the per-upload
// index is in-memory; nothing is fsync'd until Complete.
//
// Scope (v1): Otter serves Postgres base backup and WAL archival — first-party,
// CDM-orchestrated clients in a single trust domain. Object-lock
// (retention/legal-hold), tagging, versioning, and a retrievable checksum
// attribute are intentionally not supported on this path (retention/immutability
// and point-in-time are CDM SLA-driven, not S3-level; write-time integrity is
// still enforced via the stored ETag + per-part CRC32C validated at Complete).
type Af2MPUHandler struct{}

func (Af2MPUHandler) CreateMultipartUpload(_ context.Context, p *Posix, mpu s3response.CreateMultipartUploadInput, bucket, object, uploadID string) (s3response.InitiateMultipartUploadResult, error) {
	if err := os.WriteFile(p.rooted(mpObjNamePath(bucket, object)), []byte(object), 0600); err != nil {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("write mpu objname: %w", err)
	}
	if err := writeMpUploadMeta(p.rooted(mpUploadMetaPath(bucket, object, uploadID)), metaProperties{
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

func (Af2MPUHandler) CompleteMultipartUpload(ctx context.Context, p *Posix, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	acct, _ := ctx.Value("account").(auth.Account)
	return p.completeMultipartAtOffset(acct, input)
}

func (Af2MPUHandler) AbortMultipartUpload(_ context.Context, p *Posix, input *s3.AbortMultipartUploadInput) error {
	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId

	mu := p.mpuLock(bucket, object, uploadID)
	mu.Lock()
	defer mu.Unlock()

	os.Remove(p.rooted(mpDataFilePath(bucket, object, uploadID)))
	os.Remove(p.rooted(mpStashFilePath(bucket, object, uploadID)))
	os.RemoveAll(p.rooted(mpStateDir(bucket, object, uploadID)))
	cleanupMpHashDir(p.rooted(mpHashDir(bucket, object)))
	p.delMpuIndex(bucket, object, uploadID)
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
