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

// TestAf2DescDroppedKeys verifies the deliberately-unsupported v1 keys
// (object-lock, tagging, checksums, version-id) are accepted-and-dropped:
// StoreAttribute succeeds (must NOT error — the object-write path stores these
// in sequence), the value is not retrievable, and nothing is written to disk.
func TestAf2DescDroppedKeys(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "o")
	f := mustCreate(t, p)
	s := NewAf2Desc(0)

	for key := range droppedKeys {
		if err := s.StoreAttribute(f, dir, "o", key, []byte("x")); err != nil {
			t.Fatalf("store dropped key %q should no-op, got: %v", key, err)
		}
		if _, err := retr(t, s, dir, "o", key); !errors.Is(err, ErrNoSuchKey) {
			t.Fatalf("dropped key %q retrieve err=%v (want ErrNoSuchKey)", key, err)
		}
	}
	// Only dropped keys were stored, so no DESC blob should exist on disk.
	if _, err := xattr.Get(p, descXattrName); err == nil {
		t.Fatalf("DESC xattr exists but only dropped keys were stored")
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

// TestAf2DescDeleteAfterUnlink mirrors backend/posix DeleteObject's real call
// order: the object's data file is unlinked before the metadata storer is asked
// to drop the DESC. The path-based xattr.Remove/Set then fails with ENOENT;
// both delete methods must treat that as success (the DESC is already gone with
// the file) rather than surfacing a raw error that would fail every
// DeleteObject under --af2-desc.
func TestAf2DescDeleteAfterUnlink(t *testing.T) {
	t.Run("DeleteAttributes", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "o")
		f := mustCreate(t, p)
		s := NewAf2Desc(0)
		_ = s.StoreAttribute(f, dir, "o", "etag", []byte("E"))

		if err := os.Remove(p); err != nil {
			t.Fatalf("unlink object file: %v", err)
		}
		if err := s.DeleteAttributes(dir, "o"); err != nil {
			t.Fatalf("DeleteAttributes after unlink: %v", err)
		}
	})

	t.Run("DeleteAttribute", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "o")
		f := mustCreate(t, p)
		s := NewAf2Desc(0)
		_ = s.StoreAttribute(f, dir, "o", "etag", []byte("E"))
		_ = s.StoreAttribute(f, dir, "o", "content-type", []byte("text/plain"))

		if err := os.Remove(p); err != nil {
			t.Fatalf("unlink object file: %v", err)
		}
		// Removing one of several attributes takes the xattr.Set branch, which
		// also fails ENOENT on the unlinked path and must be tolerated.
		if err := s.DeleteAttribute(dir, "o", "content-type"); err != nil {
			t.Fatalf("DeleteAttribute (set branch) after unlink: %v", err)
		}
		// Removing the last attribute takes the xattr.Remove branch.
		if err := s.DeleteAttribute(dir, "o", "etag"); err != nil {
			t.Fatalf("DeleteAttribute (remove branch) after unlink: %v", err)
		}
	})
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

// TestAf2DescRootdir verifies rooted() resolution and that a path-based
// (nil-fd) store/retrieve with a RELATIVE bucket lands under Rootdir rather
// than the process CWD. Without rooting, xattr.Set on the relative "bkt/obj"
// would target the CWD and fail ENOENT, so this test fails without the fix.
func TestAf2DescRootdir(t *testing.T) {
	// rooted() resolution.
	a := NewAf2Desc(0)
	a.Rootdir = "/root"
	if got := a.rooted("bkt", "obj"); got != "/root/bkt/obj" {
		t.Fatalf("rooted relative = %q, want /root/bkt/obj", got)
	}
	if got := a.rooted("/abs/bkt", "obj"); got != "/abs/bkt/obj" {
		t.Fatalf("rooted absolute bucket = %q, want /abs/bkt/obj (IsAbs guard)", got)
	}
	a0 := NewAf2Desc(0)
	if got := a0.rooted("bkt", "obj"); got != "bkt/obj" {
		t.Fatalf("rooted empty Rootdir = %q, want bkt/obj", got)
	}

	// Path-based store/retrieve with a relative bucket, anchored at Rootdir.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bkt"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	p := filepath.Join(root, "bkt", "obj")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := NewAf2Desc(0)
	s.Rootdir = root
	if err := s.StoreAttribute(nil, "bkt", "obj", "etag", []byte("E")); err != nil {
		t.Fatalf("rooted nil-fd store: %v", err)
	}
	// The DESC xattr must physically be at <root>/bkt/obj.
	if _, err := xattr.Get(p, descXattrName); err != nil {
		t.Fatalf("DESC xattr not written to rooted path %s: %v", p, err)
	}
	// A fresh instance with the same Rootdir reads it back from disk.
	s2 := NewAf2Desc(0)
	s2.Rootdir = root
	if got, err := retr(t, s2, "bkt", "obj", "etag"); err != nil || got != "E" {
		t.Fatalf("rooted disk read = %q, %v (want E)", got, err)
	}
}

// TestAf2DescWarmedEntrySurvivesFirstStore covers the WarmCache inode fix: a
// cache entry populated without a local fd (ino==0, as WarmCache and cold reads
// do) must not be wiped by the first fd-based StoreAttribute — the real inode is
// adopted, warmed fields kept. A genuinely changed inode (object replaced) still
// resets the entry.
func TestAf2DescWarmedEntrySurvivesFirstStore(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "seg")
	f := mustCreate(t, p)
	s := NewAf2Desc(0)

	// Simulate a WarmCache-populated entry: known fields, unknown inode.
	s.cache[filepath.Join(dir, "seg")] = &descEntry{m: map[string]string{"etag": "WARM"}}

	// First fd-based store of a different attribute must adopt the inode without
	// wiping the warmed etag.
	if err := s.StoreAttribute(f, dir, "seg", "content-type", []byte("text/plain")); err != nil {
		t.Fatalf("store: %v", err)
	}
	if got, err := retr(t, s, dir, "seg", "etag"); err != nil || got != "WARM" {
		t.Fatalf("warmed etag = %q, %v (must survive first store)", got, err)
	}

	// A genuine inode change (object replaced at the same path) still wipes.
	_ = f.Close()
	if err := os.Remove(p); err != nil {
		t.Fatalf("remove: %v", err)
	}
	f2 := mustCreate(t, p)
	if err := s.StoreAttribute(f2, dir, "seg", "etag", []byte("NEW")); err != nil {
		t.Fatalf("store2: %v", err)
	}
	if _, err := retr(t, s, dir, "seg", "content-type"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("content-type should be wiped by inode change, err=%v", err)
	}
}

// TestAf2DescRenameObjectRace exercises RenameObject concurrently with
// StoreAttribute/RetrieveAttribute on the same old/new paths. It verifies the
// shard-lock ordering added to RenameObject is deadlock-free and race-clean
// (run with -race); the test completing at all is the deadlock assertion.
func TestAf2DescRenameObjectRace(t *testing.T) {
	dir := t.TempDir()
	s := NewAf2Desc(0)

	// Pre-create both targets so the path-based xattr ops succeed.
	oldName, newName := "staging-old", "staging-new"
	mustCreate(t, filepath.Join(dir, oldName))
	mustCreate(t, filepath.Join(dir, newName))

	const iters = 200
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = s.StoreAttribute(nil, dir, oldName, "etag", []byte("E"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if err := s.RenameObject(dir, oldName, newName); err != nil {
				t.Errorf("RenameObject: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_, _ = s.RetrieveAttribute(nil, dir, newName, "etag")
		}
	}()

	wg.Wait()
}

// TestAf2DescBucketTaggingDropped verifies tagging is accepted-and-dropped, not
// cache-preserved. The real callers (CreateBucket/PutBucketTagging/
// PutObjectTagging) store under backend/posix's tagHdr = "X-Amz-Tagging", which
// is not in the descKeys allowlist, so it must round-trip as ErrNoSuchKey.
// acl/ownership, stored in the same CreateBucket sequence, stay cache-preserved.
func TestAf2DescBucketTaggingDropped(t *testing.T) {
	dir := t.TempDir()
	s := NewAf2Desc(0)

	const tagHdr = "X-Amz-Tagging" // backend/posix tagHdr constant

	if err := s.StoreAttribute(nil, dir, "", tagHdr, []byte("k=v")); err != nil {
		t.Fatalf("store tagging: %v", err)
	}
	if _, err := s.RetrieveAttribute(nil, dir, "", tagHdr); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("bucket tagging err = %v; want dropped (ErrNoSuchKey)", err)
	}

	if err := s.StoreAttribute(nil, dir, "", "acl", []byte("ACL")); err != nil {
		t.Fatalf("store acl: %v", err)
	}
	if got, err := retr(t, s, dir, "", "acl"); err != nil || got != "ACL" {
		t.Fatalf("bucket acl = %q, %v; want preserved", got, err)
	}
}
