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
// rooted at a temp dir, with one bucket created. New() chdir's to the root, so
// bucket-relative paths resolve.
func newOtterBackend(t *testing.T) (*Posix, string) {
	t.Helper()
	gw := t.TempDir()
	be, err := New(gw, meta.NewAf2Desc(0), PosixOpts{
		SameDirTmp:     true,
		ForceNoTmpFile: true,
		NewDirPerm:     0755,
	})
	if err != nil {
		t.Fatalf("new posix backend: %v", err)
	}
	if err := os.Mkdir("buck", 0755); err != nil {
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
	got, err := os.ReadFile(filepath.Join(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	want := bytes.Join([][]byte{p1, p2, p3}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("object bytes mismatch: got %d bytes, want %d", len(got), len(want))
	}

	// No residue: the data file is gone.
	if _, err := os.Stat(mpDataFilePath(bucket, key, uploadID)); !errors.Is(err, fs.ErrNotExist) {
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
	if _, statErr := os.Stat(filepath.Join(bucket, key)); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("object revealed despite non-uniform rejection: %v", statErr)
	}
}

// assertNoMpResidue fails if the upload's .data or .stash side files remain.
func assertNoMpResidue(t *testing.T, bucket, key, uploadID string) {
	t.Helper()
	if _, err := os.Stat(mpDataFilePath(bucket, key, uploadID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("data file residue: %v", err)
	}
	if _, err := os.Stat(mpStashFilePath(bucket, key, uploadID)); !errors.Is(err, fs.ErrNotExist) {
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

	got, err := os.ReadFile(filepath.Join(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	want := bytes.Join([][]byte{p1, p2, p3}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("object bytes mismatch: got %d bytes, want %d", len(got), len(want))
	}

	// The short-final, folded case must take the no-copy reveal branch.
	if be.lastRevealKind != revealNoCopy {
		t.Fatalf("reveal kind = %v, want revealNoCopy", be.lastRevealKind)
	}
	// Both side files are gone.
	assertNoMpResidue(t, bucket, key, uploadID)
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
	got, err := os.ReadFile(filepath.Join(bucket, key))
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("single-part object mismatch: got %q want %q", got, data)
	}
	if be.lastRevealKind != revealNoCopy {
		t.Fatalf("reveal kind = %v, want revealNoCopy", be.lastRevealKind)
	}
	assertNoMpResidue(t, bucket, key, uploadID)
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
	got, err := os.ReadFile(filepath.Join(bucket, key))
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
	if be.lastRevealKind != revealNoCopy {
		t.Fatalf("reveal kind = %v, want revealNoCopy", be.lastRevealKind)
	}
	_ = res
	assertNoMpResidue(t, bucket, key, uploadID)
}

// TestOtterMPUListDuringStash: ListParts during the stashed window (a short part
// uploaded before part #1) reports the stashed part with its TRUE short size and
// ETag, leaking no offset. Abort then removes both side files.
func TestOtterMPUListDuringStash(t *testing.T) {
	be, bucket := newOtterBackend(t)
	key := "ls/obj"
	uploadID := createMPU(t, be, bucket, key)

	tail := []byte("short-tail-bytes")
	e3, err := uploadPart(t, be, bucket, key, uploadID, 3, tail)
	if err != nil {
		t.Fatalf("upload part 3 (stashed): %v", err)
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
		t.Fatalf("stashed part listed wrong: %+v", lp.Parts[0])
	}

	// The stash file exists pre-Complete; the data file does not yet.
	if _, err := os.Stat(mpStashFilePath(bucket, key, uploadID)); err != nil {
		t.Fatalf("stash file should exist during stashed window: %v", err)
	}

	if err := be.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: ptrS(bucket), Key: ptrS(key), UploadId: ptrS(uploadID),
	}); err != nil {
		t.Fatalf("abort: %v", err)
	}
	assertNoMpResidue(t, bucket, key, uploadID)
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
	if _, err := os.Stat(mpDataFilePath(bucket, key, uploadID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("data file not removed on abort: %v", err)
	}
	lmu2, _ := be.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: ptrS(bucket), MaxUploads: ptrI32(100),
	})
	if len(lmu2.Uploads) != 0 {
		t.Fatalf("aborted upload still listed: %+v", lmu2.Uploads)
	}
}
