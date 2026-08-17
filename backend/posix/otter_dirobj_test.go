// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific
// language governing permissions and limitations under the License.

package posix

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3response"
)

// newOtterBackendRooted mirrors the production runOtter wiring: an Af2Desc
// storer whose Rootdir is set to the absolute gateway root, so path-based
// (nil-fd) xattr syscalls are anchored there rather than the process CWD. This
// is the piece the bare newOtterBackend helper omits.
func newOtterBackendRooted(t *testing.T) (*Posix, string) {
	t.Helper()
	gw := t.TempDir()
	gwAbs, err := filepath.Abs(gw)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	desc := meta.NewAf2Desc(0)
	desc.Rootdir = gwAbs
	be, err := New(gw, desc, PosixOpts{
		SameDirTmp:     true,
		Af2MPU:         true,
		ForceNoTmpFile: true,
		NewDirPerm:     0o755,
	})
	if err != nil {
		t.Fatalf("new posix backend: %v", err)
	}
	if err := os.Mkdir(filepath.Join(gwAbs, "buck"), 0o755); err != nil {
		t.Fatalf("mkdir bucket: %v", err)
	}
	return be, "buck"
}

// TestOtterDirectoryObjectPutDelete regresses the CWD gap for the Af2Desc
// storer this deployment uses. Removing the process-wide os.Chdir means a
// directory-object PutObject — which stores its DESC metadata via a nil-fd
// xattr.Set on the freshly-created directory — must anchor that path at
// Af2Desc.Rootdir. Without rooting it fails with
//
//	xattr.Set ... sdfs_af2_file_desc: no such file or directory
//
// go test never caught this because the nil-fd directory-object path is not
// otherwise exercised with Af2Desc (the MPU tests all use the fd-based path).
func TestOtterDirectoryObjectPutDelete(t *testing.T) {
	be, bucket := newOtterBackendRooted(t)
	key := "dir/"

	if _, err := be.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(nil),
	}); err != nil {
		t.Fatalf("PutObject directory object: %v", err)
	}

	// The etag attribute must be readable back (proves the DESC blob landed on
	// the rooted directory, not a CWD-relative phantom path).
	if _, err := be.meta.RetrieveAttribute(nil, bucket, key, etagkey); err != nil {
		t.Fatalf("directory object etag not retrievable after put: %v", err)
	}

	// DeleteObject removes the directory and then its DESC attributes; the
	// attribute removal after the entry is gone must not 500.
	if _, err := be.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}); err != nil {
		t.Fatalf("DeleteObject directory object: %v", err)
	}
}

// TestOtterRegularObjectPutDelete covers the regular-object round trip and, in
// particular, DeleteObject removing DESC attributes after the data file is
// unlinked — the delete path must tolerate a vanished target rather than 500.
func TestOtterRegularObjectPutDelete(t *testing.T) {
	be, bucket := newOtterBackendRooted(t)
	key := "a/b/obj.txt"
	body := []byte("hello otter")
	clen := int64(len(body))

	if _, err := be.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		ContentLength: &clen,
		Body:          bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("PutObject regular object: %v", err)
	}

	if _, err := be.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}); err != nil {
		t.Fatalf("DeleteObject regular object: %v", err)
	}
}
