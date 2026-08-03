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
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3response"
)

func ptrS(s string) *string { return &s }
func ptrI32(i int32) *int32 { return &i }
func ptrI64(i int64) *int64 { return &i }

// newOtterBackend builds a Posix backend in Otter mode (sameDirTmp + Af2Desc)
// rooted at a temp dir, with one bucket created.
func newOtterBackend(t *testing.T) (*Posix, string) {
	return newOtterBackendCap(t, 0) // 0 => 64 MiB default in-memory buffer cap
}

// newOtterBackendCap is newOtterBackend with an explicit in-memory buffer cap, so
// tests can force the disk-spill fallback for a pre-stride part.
func newOtterBackendCap(t *testing.T, bufMax int64) (*Posix, string) {
	t.Helper()
	gw := t.TempDir()
	be, err := New(gw, meta.NewAf2Desc(0), PosixOpts{
		SameDirTmp:      true,
		Af2MPU:          true,
		ForceNoTmpFile:  true,
		NewDirPerm:      0755,
		MpuMemBufferMax: bufMax,
	})
	if err != nil {
		t.Fatalf("new posix backend: %v", err)
	}
	if err := os.Mkdir(filepath.Join(gw, "buck"), 0755); err != nil {
		t.Fatalf("mkdir bucket: %v", err)
	}
	return be, "buck"
}

func createMPU(t *testing.T, be *Posix, bucket, key string) string {
	t.Helper()
	out, err := be.CreateMultipartUpload(context.Background(), s3response.CreateMultipartUploadInput{
		Bucket:      ptrS(bucket),
		Key:         ptrS(key),
		ContentType: ptrS("text/plain"),
	})
	if err != nil {
		t.Fatalf("create mpu: %v", err)
	}
	return out.UploadId
}

func uploadPart(t *testing.T, be *Posix, bucket, key, uploadID string, n int, data []byte) (string, error) {
	t.Helper()
	out, err := be.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptrS(bucket),
		Key:           ptrS(key),
		UploadId:      ptrS(uploadID),
		PartNumber:    ptrI32(int32(n)),
		ContentLength: ptrI64(int64(len(data))),
		Body:          bytes.NewReader(data),
	})
	if err != nil {
		return "", err
	}
	return *out.ETag, nil
}

func completeMPU(t *testing.T, be *Posix, bucket, key, uploadID string, parts []types.CompletedPart) (s3response.CompleteMultipartUploadResult, error) {
	t.Helper()
	res, _, err := be.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:          ptrS(bucket),
		Key:             ptrS(key),
		UploadId:        ptrS(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	return res, err
}

// TestOtterMPUFastPathOutOfOrder covers FV3 (out-of-order uniform fast path,
// byte-correct assembly, -N composite ETag) and FV2-style idempotent re-Complete.
func TestOtterMPUFastPathOutOfOrder(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "big/obj"
	uploadID := createMPU(t, be, bucket, key)

	p1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	p2 := bytes.Repeat([]byte("B"), backend.MinPartSize)
	p3 := []byte("the-short-final-tail")

	// Upload out of order: 2, 1, 3. Part 2 arrives before part #1 confirms the
	// stride, so it is stashed; part 1 places at 0 and pins S; part 3 (short tail)
	// is stashed. Complete folds both stashed parts and assembles byte-correct.
	// (TestOtterMPUShortFinalPartFirst covers the harder 3,1,2 order.)
	e2, err := uploadPart(t, be, bucket, key, uploadID, 2, p2)
	if err != nil {
		t.Fatalf("upload part 2: %v", err)
	}
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, p1)
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	e3, err := uploadPart(t, be, bucket, key, uploadID, 3, p3)
	if err != nil {
		t.Fatalf("upload part 3: %v", err)
	}

	parts := []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
		{PartNumber: ptrI32(3), ETag: ptrS(e3)},
	}
	res, err := completeMPU(t, be, bucket, key, uploadID, parts)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !strings.HasSuffix(strings.Trim(*res.ETag, `"`), "-3") {
		t.Fatalf("composite ETag = %s, want -3 suffix", *res.ETag)
	}

	// Object bytes equal the in-order concatenation.
	got, err := os.ReadFile(be.rooted(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	want := bytes.Join([][]byte{p1, p2, p3}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("object bytes mismatch: got %d bytes, want %d", len(got), len(want))
	}

	// No residue: the data file is gone.
	if _, err := os.Stat(be.rooted(mpDataFilePath(bucket, key, uploadID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("data file residue: %v", err)
	}

	// Re-Complete is idempotent (retry after success).
	res2, err := completeMPU(t, be, bucket, key, uploadID, parts)
	if err != nil {
		t.Fatalf("idempotent re-complete: %v", err)
	}
	if *res2.ETag != *res.ETag {
		t.Fatalf("re-complete ETag = %s, want %s", *res2.ETag, *res.ETag)
	}
}

// TestOtterMPUNonUniformRejected: a genuinely non-uniform interior part is
// accepted at UploadPart (it is stashed; errors are deferred) but rejected
// cleanly at Complete with no object revealed.
func TestOtterMPUNonUniformRejected(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "obj"
	uploadID := createMPU(t, be, bucket, key)

	// Parts 1 and 3 are uniform (stride), part 2 (interior) is larger -> the
	// interior is non-uniform. UploadPart no longer rejects; it stashes part 2.
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, bytes.Repeat([]byte("A"), backend.MinPartSize))
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	e2, err := uploadPart(t, be, bucket, key, uploadID, 2, bytes.Repeat([]byte("B"), backend.MinPartSize+1024))
	if err != nil {
		t.Fatalf("upload part 2 (should be accepted/stashed): %v", err)
	}
	e3, err := uploadPart(t, be, bucket, key, uploadID, 3, bytes.Repeat([]byte("C"), backend.MinPartSize))
	if err != nil {
		t.Fatalf("upload part 3: %v", err)
	}

	// Complete must reject the non-uniform interior part cleanly.
	_, err = completeMPU(t, be, bucket, key, uploadID, []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
		{PartNumber: ptrI32(3), ETag: ptrS(e3)},
	})
	if err == nil {
		t.Fatalf("expected Complete to reject the non-uniform interior part")
	}
	// No object may be revealed on the rejected path.
	if _, statErr := os.Stat(be.rooted(bucket, key)); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("object revealed despite non-uniform rejection: %v", statErr)
	}
}

// assertNoMpResidue fails if the upload's .data or .stash side files remain.
func assertNoMpResidue(t *testing.T, be *Posix, bucket, key, uploadID string) {
	t.Helper()
	if _, err := os.Stat(be.rooted(mpDataFilePath(bucket, key, uploadID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("data file residue: %v", err)
	}
	if _, err := os.Stat(be.rooted(mpStashFilePath(bucket, key, uploadID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stash file residue: %v", err)
	}
}

// TestOtterMPUShortFinalPartFirst is the headline regression: the short final
// part arrives FIRST (before any full part sets the stride). It must be stashed,
// folded at Complete, and the object assembled byte-for-byte — exactly the case
// the old arrival-order stride inference (and the max_concurrent_requests=1
// workaround) could not handle.
func TestOtterMPUShortFinalPartFirst(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "big/obj"
	uploadID := createMPU(t, be, bucket, key)

	p1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	p2 := bytes.Repeat([]byte("B"), backend.MinPartSize)
	p3 := []byte("the-short-final-tail")

	// Upload order 3, 1, 2: the short tail arrives first.
	e3, err := uploadPart(t, be, bucket, key, uploadID, 3, p3)
	if err != nil {
		t.Fatalf("upload part 3: %v", err)
	}
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, p1)
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	e2, err := uploadPart(t, be, bucket, key, uploadID, 2, p2)
	if err != nil {
		t.Fatalf("upload part 2: %v", err)
	}

	parts := []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
		{PartNumber: ptrI32(3), ETag: ptrS(e3)},
	}
	res, err := completeMPU(t, be, bucket, key, uploadID, parts)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !strings.HasSuffix(strings.Trim(*res.ETag, `"`), "-3") {
		t.Fatalf("composite ETag = %s, want -3 suffix", *res.ETag)
	}

	got, err := os.ReadFile(be.rooted(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	want := bytes.Join([][]byte{p1, p2, p3}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("object bytes mismatch: got %d bytes, want %d", len(got), len(want))
	}

	// The short-final, folded case must take the no-copy reveal branch.
	if revealKind(be.lastRevealKind.Load()) != revealNoCopy {
		t.Fatalf("reveal kind = %v, want revealNoCopy", revealKind(be.lastRevealKind.Load()))
	}
	// Both side files are gone.
	assertNoMpResidue(t, be, bucket, key, uploadID)
}

// TestOtterMPUSinglePart: a single-part upload (any size, exempt from
// MinPartSize) produces the exact object.
func TestOtterMPUSinglePart(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "single/obj"
	uploadID := createMPU(t, be, bucket, key)

	data := []byte("just-one-small-part")
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, data)
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	res, err := completeMPU(t, be, bucket, key, uploadID, []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
	})
	if err != nil {
		t.Fatalf("complete single-part: %v", err)
	}
	if !strings.HasSuffix(strings.Trim(*res.ETag, `"`), "-1") {
		t.Fatalf("composite ETag = %s, want -1 suffix", *res.ETag)
	}
	got, err := os.ReadFile(be.rooted(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("single-part object mismatch: got %q want %q", got, data)
	}
	if revealKind(be.lastRevealKind.Load()) != revealNoCopy {
		t.Fatalf("reveal kind = %v, want revealNoCopy", revealKind(be.lastRevealKind.Load()))
	}
	assertNoMpResidue(t, be, bucket, key, uploadID)
}

// TestOtterMPUStashFoldNoTail: full parts 2 and 3 arrive before part 1 (so they
// are stashed), all uniform with no short tail. Complete folds them and reveals
// contiguous via the no-copy branch.
func TestOtterMPUStashFoldNoTail(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "nt/obj"
	uploadID := createMPU(t, be, bucket, key)

	p1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	p2 := bytes.Repeat([]byte("B"), backend.MinPartSize)
	p3 := bytes.Repeat([]byte("C"), backend.MinPartSize)

	e2, err := uploadPart(t, be, bucket, key, uploadID, 2, p2)
	if err != nil {
		t.Fatalf("upload part 2: %v", err)
	}
	e3, err := uploadPart(t, be, bucket, key, uploadID, 3, p3)
	if err != nil {
		t.Fatalf("upload part 3: %v", err)
	}
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, p1)
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}

	res, err := completeMPU(t, be, bucket, key, uploadID, []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
		{PartNumber: ptrI32(3), ETag: ptrS(e3)},
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, err := os.ReadFile(be.rooted(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	want := bytes.Join([][]byte{p1, p2, p3}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("object size = %d, want %d", len(got), len(want))
	}
	if int64(len(got)) != 3*int64(backend.MinPartSize) {
		t.Fatalf("final size = %d, want %d", len(got), 3*backend.MinPartSize)
	}
	if revealKind(be.lastRevealKind.Load()) != revealNoCopy {
		t.Fatalf("reveal kind = %v, want revealNoCopy", revealKind(be.lastRevealKind.Load()))
	}
	_ = res
	assertNoMpResidue(t, be, bucket, key, uploadID)
}

// TestOtterMPUListDuringBuffer: ListParts during the pre-stride window (a short
// part uploaded before part #1) reports the buffered part with its TRUE short
// size and ETag, leaking no offset. Under the complete-only-fsync model a small
// buffered part lives in RAM, so NO spill file is created. Abort then frees state.
func TestOtterMPUListDuringBuffer(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "ls/obj"
	uploadID := createMPU(t, be, bucket, key)

	tail := []byte("short-tail-bytes")
	e3, err := uploadPart(t, be, bucket, key, uploadID, 3, tail)
	if err != nil {
		t.Fatalf("upload part 3 (buffered): %v", err)
	}

	lp, err := be.ListParts(context.Background(), &s3.ListPartsInput{
		Bucket: ptrS(bucket), Key: ptrS(key), UploadId: ptrS(uploadID), MaxParts: ptrI32(100),
	})
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(lp.Parts) != 1 {
		t.Fatalf("list parts = %d, want 1", len(lp.Parts))
	}
	if lp.Parts[0].PartNumber != 3 || lp.Parts[0].Size != int64(len(tail)) || lp.Parts[0].ETag != e3 {
		t.Fatalf("buffered part listed wrong: %+v", lp.Parts[0])
	}

	// A small buffered part stays in RAM: no spill file is created.
	if _, err := os.Stat(be.rooted(mpStashFilePath(bucket, key, uploadID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("small buffered part should not create a spill file: %v", err)
	}

	if err := be.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: ptrS(bucket), Key: ptrS(key), UploadId: ptrS(uploadID),
	}); err != nil {
		t.Fatalf("abort: %v", err)
	}
	assertNoMpResidue(t, be, bucket, key, uploadID)
}

// TestOtterMPUMisorderedCompleteRejected is the corruption guard (FV9): a
// Complete part list that is not strictly ascending must be rejected before any
// reveal, so a mis-ordered list can never be assembled into a corrupt object.
func TestOtterMPUMisorderedCompleteRejected(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "mo/obj"
	uploadID := createMPU(t, be, bucket, key)

	p1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	p2 := bytes.Repeat([]byte("B"), backend.MinPartSize)
	p3 := []byte("the-short-final-tail")
	e1, _ := uploadPart(t, be, bucket, key, uploadID, 1, p1)
	e2, _ := uploadPart(t, be, bucket, key, uploadID, 2, p2)
	e3, _ := uploadPart(t, be, bucket, key, uploadID, 3, p3)

	// List parts out of order (3,1,2): must be rejected, no object revealed.
	_, err := completeMPU(t, be, bucket, key, uploadID, []types.CompletedPart{
		{PartNumber: ptrI32(3), ETag: ptrS(e3)},
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
	})
	if err == nil {
		t.Fatalf("expected mis-ordered Complete to be rejected")
	}
	if _, statErr := os.Stat(be.rooted(bucket, key)); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("object revealed despite mis-ordered rejection: %v", statErr)
	}
}

// TestOtterMPUOversizedPartSpills (FV4): with a tiny in-memory cap, a pre-stride
// part exceeding the cap spills to a page-cache disk file, then is flushed to its
// final offset when the stride resolves; the object assembles byte-correct and
// the spill file is removed.
func TestOtterMPUOversizedPartSpills(t *testing.T) {
	be, bucket := newOtterBackendCap(t, 4) // 4-byte cap forces the spill fallback
	key := "sp/obj"
	uploadID := createMPU(t, be, bucket, key)

	p1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	p2 := bytes.Repeat([]byte("B"), backend.MinPartSize)

	// Part 2 arrives first (out of order); it exceeds the cap and spills to disk.
	e2, err := uploadPart(t, be, bucket, key, uploadID, 2, p2)
	if err != nil {
		t.Fatalf("upload part 2: %v", err)
	}
	if _, err := os.Stat(be.rooted(mpStashFilePath(bucket, key, uploadID))); err != nil {
		t.Fatalf("expected spill file for oversized pre-stride part: %v", err)
	}

	// Part 1 resolves the stride and flushes the spilled part; spill file removed.
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, p1)
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	if _, err := os.Stat(be.rooted(mpStashFilePath(bucket, key, uploadID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("spill file not removed after flush: %v", err)
	}

	res, err := completeMPU(t, be, bucket, key, uploadID, []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, err := os.ReadFile(be.rooted(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if want := bytes.Join([][]byte{p1, p2}, nil); !bytes.Equal(got, want) {
		t.Fatalf("object bytes mismatch: got %d, want %d", len(got), len(want))
	}
	_ = res
	assertNoMpResidue(t, be, bucket, key, uploadID)
}

// TestOtterMPUReuploadAccounting (FV10): re-uploading a still-buffered part must
// reconcile the RAM counter (no double-count), and a full flush must leave
// pendingRAM == 0 with an empty buffer.
func TestOtterMPUReuploadAccounting(t *testing.T) {
	// Cap fits exactly one part in RAM.
	be, bucket := newOtterBackendCap(t, int64(backend.MinPartSize)+16)
	key := "acct/obj"
	uploadID := createMPU(t, be, bucket, key)

	part := bytes.Repeat([]byte("B"), backend.MinPartSize)

	// Part 2 arrives first (out of order) and is buffered in RAM.
	if _, err := uploadPart(t, be, bucket, key, uploadID, 2, part); err != nil {
		t.Fatalf("upload part 2: %v", err)
	}
	// Re-upload part 2 (RAM->RAM): pendingRAM must not double-count.
	if _, err := uploadPart(t, be, bucket, key, uploadID, 2, part); err != nil {
		t.Fatalf("re-upload part 2: %v", err)
	}
	idx, ok := be.peekMpuIndex(bucket, key, uploadID)
	if !ok {
		t.Fatalf("in-memory index missing")
	}
	if idx.pendingRAM != int64(len(part)) {
		t.Fatalf("pendingRAM after RAM re-upload = %d, want %d", idx.pendingRAM, len(part))
	}
	if len(idx.pending) != 1 {
		t.Fatalf("pending count = %d, want 1", len(idx.pending))
	}

	// Part 1 resolves the stride and flushes the buffer.
	if _, err := uploadPart(t, be, bucket, key, uploadID, 1, part); err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	idx, _ = be.peekMpuIndex(bucket, key, uploadID)
	if idx.pendingRAM != 0 {
		t.Fatalf("pendingRAM after flush = %d, want 0", idx.pendingRAM)
	}
	if len(idx.pending) != 0 {
		t.Fatalf("pending not drained: %d", len(idx.pending))
	}
}

// TestOtterMPUMissingPartRejected: completing with a part that was never
// uploaded hard-errors (no silent short object).
func TestOtterMPUMissingPartRejected(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "obj"
	uploadID := createMPU(t, be, bucket, key)

	e1, _ := uploadPart(t, be, bucket, key, uploadID, 1, bytes.Repeat([]byte("A"), backend.MinPartSize))
	e2, _ := uploadPart(t, be, bucket, key, uploadID, 2, bytes.Repeat([]byte("B"), backend.MinPartSize))

	// Part 3 was never uploaded.
	_, err := completeMPU(t, be, bucket, key, uploadID, []types.CompletedPart{
		{PartNumber: ptrI32(1), ETag: ptrS(e1)},
		{PartNumber: ptrI32(2), ETag: ptrS(e2)},
		{PartNumber: ptrI32(3), ETag: ptrS("\"deadbeef\"")},
	})
	if err == nil {
		t.Fatalf("expected error completing with a missing part")
	}
}

// TestOtterMPUListAndAbort covers ListParts fidelity, ListMultipartUploads
// in-flight discovery (FV8), and Abort cleanup (FV5).
func TestOtterMPUListAndAbort(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "dir/obj"
	uploadID := createMPU(t, be, bucket, key)

	// Uniform part sizes so both land on the fast path.
	d1 := bytes.Repeat([]byte("x"), 1024)
	d2 := bytes.Repeat([]byte("y"), 1024)
	e1, err := uploadPart(t, be, bucket, key, uploadID, 1, d1)
	if err != nil {
		t.Fatalf("upload part 1: %v", err)
	}
	if _, err := uploadPart(t, be, bucket, key, uploadID, 2, d2); err != nil {
		t.Fatalf("upload part 2: %v", err)
	}

	// ListParts: full fidelity from the index.
	lp, err := be.ListParts(context.Background(), &s3.ListPartsInput{
		Bucket: ptrS(bucket), Key: ptrS(key), UploadId: ptrS(uploadID), MaxParts: ptrI32(100),
	})
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(lp.Parts) != 2 {
		t.Fatalf("list parts = %d, want 2", len(lp.Parts))
	}
	if lp.Parts[0].PartNumber != 1 || lp.Parts[0].Size != int64(len(d1)) || lp.Parts[0].ETag != e1 {
		t.Fatalf("part 1 = %+v", lp.Parts[0])
	}

	// ListMultipartUploads: the in-flight upload is discoverable by key (FV8).
	lmu, err := be.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: ptrS(bucket), MaxUploads: ptrI32(100),
	})
	if err != nil {
		t.Fatalf("list mpu: %v", err)
	}
	if len(lmu.Uploads) != 1 || lmu.Uploads[0].Key != key {
		t.Fatalf("list mpu = %+v, want one upload for %q", lmu.Uploads, key)
	}

	// Abort removes the data file and the upload disappears from listings (FV5).
	if err := be.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: ptrS(bucket), Key: ptrS(key), UploadId: ptrS(uploadID),
	}); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := os.Stat(be.rooted(mpDataFilePath(bucket, key, uploadID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("data file not removed on abort: %v", err)
	}
	lmu2, _ := be.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: ptrS(bucket), MaxUploads: ptrI32(100),
	})
	if len(lmu2.Uploads) != 0 {
		t.Fatalf("aborted upload still listed: %+v", lmu2.Uploads)
	}
}
