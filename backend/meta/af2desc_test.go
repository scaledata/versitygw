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

package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pkg/xattr"
	"github.com/versity/versitygw/s3err"
)

// These tests run on a normal xattr-capable filesystem (APFS/ext4/tmpfs), where
// getxattr works. That lets a fresh Af2Desc instance read back what a previous
// one wrote, exercising the real on-disk DESC blob in addition to the cache. On
// SDFS getxattr is unimplemented, so only the write-through cache serves reads;
// that path is covered by the cache-hit assertions here.

func mustCreate(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func rawBlob(t *testing.T, path string) map[string]string {
	t.Helper()
	b, err := xattr.Get(path, descXattrName)
	if err != nil {
		t.Fatalf("read raw DESC xattr on %s: %v", path, err)
	}
	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal DESC blob: %v", err)
	}
	return m
}

func retr(t *testing.T, s *Af2Desc, bucket, object, attr string) (string, error) {
	t.Helper()
	b, err := s.RetrieveAttribute(nil, bucket, object, attr)
	return string(b), err
}

// TestAf2DescRoundTrip: multiple allowlisted attributes pack into ONE DESC blob
// and round-trip both from the cache and from disk via a fresh instance.
func TestAf2DescRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "obj1")
	f := mustCreate(t, p)

	s := NewAf2Desc(0)
	for k, v := range map[string]string{
		"etag":         `"abc123"`,
		"content-type": "application/octet-stream",
		"metadata":     `{"x-amz-meta-db":"pg"}`,
	} {
		if err := s.StoreAttribute(f, dir, "obj1", k, []byte(v)); err != nil {
			t.Fatalf("store %s: %v", k, err)
		}
	}

	// Cache read-back.
	if got, err := retr(t, s, dir, "obj1", "etag"); err != nil || got != `"abc123"` {
		t.Fatalf("cache etag = %q, %v", got, err)
	}

	// One physical xattr holds all three logical keys.
	if m := rawBlob(t, p); len(m) != 3 || m["content-type"] != "application/octet-stream" {
		t.Fatalf("on-disk blob = %v (want 3 keys)", m)
	}

	// Fresh instance: read-back must come from the on-disk blob (getxattr).
	s2 := NewAf2Desc(0)
	if got, err := retr(t, s2, dir, "obj1", "metadata"); err != nil || got != `{"x-amz-meta-db":"pg"}` {
		t.Fatalf("disk metadata = %q, %v", got, err)
	}
}

// TestAf2DescCrossCallFdNil: stored with a non-nil fd, retrieved with nil fd at
// the same path — proves the cache is keyed by path, not by fd (mirrors PUT->GET).
func TestAf2DescCrossCallFdNil(t *testing.T) {
	dir := t.TempDir()
	f := mustCreate(t, filepath.Join(dir, "k"))
	s := NewAf2Desc(0)

	if err := s.StoreAttribute(f, dir, "k", "etag", []byte("E1")); err != nil {
		t.Fatalf("store: %v", err)
	}
	if got, err := retr(t, s, dir, "k", "etag"); err != nil || got != "E1" {
		t.Fatalf("nil-fd retrieve = %q, %v", got, err)
	}
}

// TestAf2DescOverwriteNewInode: re-creating the object (new inode) must not leak
// the prior object's metadata into the new blob (FV2).
func TestAf2DescOverwriteNewInode(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "seg1")
	s := NewAf2Desc(0)

	// v1: full metadata set.
	f1 := mustCreate(t, p)
	for k, v := range map[string]string{"etag": "E1", "content-type": "A", "metadata": `{"a":"1"}`} {
		if err := s.StoreAttribute(f1, dir, "seg1", k, []byte(v)); err != nil {
			t.Fatalf("v1 store %s: %v", k, err)
		}
	}
	_ = f1.Close()

	// Overwrite: new inode at the same path, only etag set.
	if err := os.Remove(p); err != nil {
		t.Fatalf("remove: %v", err)
	}
	f2 := mustCreate(t, p)
	if err := s.StoreAttribute(f2, dir, "seg1", "etag", []byte("E2")); err != nil {
		t.Fatalf("v2 store: %v", err)
	}

	if got, err := retr(t, s, dir, "seg1", "etag"); err != nil || got != "E2" {
		t.Fatalf("etag = %q, %v (want E2)", got, err)
	}
	for _, leaked := range []string{"content-type", "metadata"} {
		if _, err := retr(t, s, dir, "seg1", leaked); !errors.Is(err, ErrNoSuchKey) {
			t.Fatalf("%s leaked from v1: err=%v (want ErrNoSuchKey)", leaked, err)
		}
	}
	// On-disk blob also reflects only v2.
	if m := rawBlob(t, p); len(m) != 1 || m["etag"] != "E2" {
		t.Fatalf("on-disk blob after overwrite = %v (want only etag=E2)", m)
	}
}

// TestAf2DescDropNonAllowlisted: a non-allowlisted key is accepted-and-dropped
// on store and reported absent on retrieve; no xattr is written for it.
func TestAf2DescDropNonAllowlisted(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "o")
	f := mustCreate(t, p)
	s := NewAf2Desc(0)

	if err := s.StoreAttribute(f, dir, "o", "checksums", []byte(`{"Type":"FULL_OBJECT"}`)); err != nil {
		t.Fatalf("store checksums should no-op, got: %v", err)
	}
	if _, err := retr(t, s, dir, "o", "checksums"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("checksums retrieve err=%v (want ErrNoSuchKey)", err)
	}
	// Nothing was written to disk.
	if _, err := xattr.Get(p, descXattrName); err == nil {
		t.Fatalf("DESC xattr exists but only a dropped key was stored")
	}
}

// TestAf2DescOverCap: a write that would exceed maxLen is rejected and prior
// state is preserved.
func TestAf2DescOverCap(t *testing.T) {
	dir := t.TempDir()
	f := mustCreate(t, filepath.Join(dir, "o"))
	s := NewAf2Desc(20) // tiny cap

	// First small key fits: {"etag":"abc"} == 14 bytes.
	if err := s.StoreAttribute(f, dir, "o", "etag", []byte("abc")); err != nil {
		t.Fatalf("store etag: %v", err)
	}
	// Adding content-type blows the 20-byte cap.
	err := s.StoreAttribute(f, dir, "o", "content-type", []byte("application/octet-stream"))
	if err == nil {
		t.Fatalf("over-cap store should fail")
	}
	if err != s3err.GetAPIError(s3err.ErrMetadataTooLarge) {
		t.Fatalf("over-cap error = %v (want MetadataTooLarge)", err)
	}
	// The prior key survives; the rejected key is absent.
	if got, e := retr(t, s, dir, "o", "etag"); e != nil || got != "abc" {
		t.Fatalf("etag after over-cap = %q, %v (want abc)", got, e)
	}
	if _, e := retr(t, s, dir, "o", "content-type"); !errors.Is(e, ErrNoSuchKey) {
		t.Fatalf("content-type after over-cap err=%v (want ErrNoSuchKey)", e)
	}
}

// TestAf2DescNilFdWrite: f==nil writes (path-based) round-trip, including the
// bucket-level case (object == "").
func TestAf2DescNilFdWrite(t *testing.T) {
	dir := t.TempDir()
	s := NewAf2Desc(0)

	// Object write with nil fd.
	p := filepath.Join(dir, "o")
	mustCreate(t, p)
	if err := s.StoreAttribute(nil, dir, "o", "content-type", []byte("text/plain")); err != nil {
		t.Fatalf("nil-fd object store: %v", err)
	}
	s2 := NewAf2Desc(0)
	if got, err := retr(t, s2, dir, "o", "content-type"); err != nil || got != "text/plain" {
		t.Fatalf("disk content-type = %q, %v", got, err)
	}

	// Bucket-level write (object == "") targets the bucket dir inode.
	if err := s.StoreAttribute(nil, dir, "", "etag", []byte("B")); err != nil {
		t.Fatalf("bucket-level store: %v", err)
	}
	if got, err := retr(t, s, dir, "", "etag"); err != nil || got != "B" {
		t.Fatalf("bucket etag = %q, %v", got, err)
	}
}

// TestAf2DescDelete: single-key delete preserves siblings; DeleteAttributes
// clears everything (cache and disk).
func TestAf2DescDelete(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "o")
	f := mustCreate(t, p)
	s := NewAf2Desc(0)
	_ = s.StoreAttribute(f, dir, "o", "etag", []byte("E"))
	_ = s.StoreAttribute(f, dir, "o", "content-type", []byte("text/plain"))

	if err := s.DeleteAttribute(dir, "o", "content-type"); err != nil {
		t.Fatalf("delete content-type: %v", err)
	}
	if _, err := retr(t, s, dir, "o", "content-type"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("content-type after delete err=%v", err)
	}
	if got, err := retr(t, s, dir, "o", "etag"); err != nil || got != "E" {
		t.Fatalf("etag after sibling delete = %q, %v", got, err)
	}

	if err := s.DeleteAttributes(dir, "o"); err != nil {
		t.Fatalf("delete attributes: %v", err)
	}
	if _, err := xattr.Get(p, descXattrName); err == nil {
		t.Fatalf("DESC xattr still present after DeleteAttributes")
	}
}

// TestAf2DescListAttributesEmpty: ListAttributes returns no keys by design.
func TestAf2DescListAttributesEmpty(t *testing.T) {
	dir := t.TempDir()
	f := mustCreate(t, filepath.Join(dir, "o"))
	s := NewAf2Desc(0)
	_ = s.StoreAttribute(f, dir, "o", "etag", []byte("E"))

	attrs, err := s.ListAttributes(dir, "o")
	if err != nil {
		t.Fatalf("ListAttributes err: %v", err)
	}
	if len(attrs) != 0 {
		t.Fatalf("ListAttributes = %v (want empty)", attrs)
	}
}

// TestAf2DescConcurrent: concurrent stores to distinct objects all succeed and
// round-trip (run with -race to exercise the sharded locking).
func TestAf2DescConcurrent(t *testing.T) {
	dir := t.TempDir()
	s := NewAf2Desc(0)
	const n = 64

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("obj-%d", i)
			f, err := os.Create(filepath.Join(dir, key))
			if err != nil {
				t.Errorf("create %s: %v", key, err)
				return
			}
			defer f.Close()
			if err := s.StoreAttribute(f, dir, key, "etag", []byte(fmt.Sprintf("E%d", i))); err != nil {
				t.Errorf("store %s: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("obj-%d", i)
		want := fmt.Sprintf("E%d", i)
		if got, err := retr(t, s, dir, key, "etag"); err != nil || got != want {
			t.Fatalf("%s etag = %q, %v (want %q)", key, got, err, want)
		}
	}
}
