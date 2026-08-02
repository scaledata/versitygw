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
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/pkg/xattr"
	"github.com/versity/versitygw/s3err"
)

// Af2Desc is an Otter MetadataStorer that packs the must-store S3 object
// metadata into the single AF2 "DESC" file attribute, so the metadata rides the
// AF2 partition snapshot together with the object data.
//
// On SDFS the only FUSE-writable attribute is "sdfs_af2_file_desc" -> the AF2
// file_attributes["DESC"] slot; generic xattr keys are rejected and getxattr is
// unimplemented. Af2Desc therefore:
//   - packs an allowlisted set of logical attributes into ONE JSON blob written
//     under descXattrName (see descKeys); non-allowlisted keys are accepted and
//     dropped so the blob stays within the MJF value-size cap;
//   - serves read-back from an in-process write-through cache, because SDFS
//     cannot read the attribute back. On a normal xattr-capable filesystem
//     (APFS/ext4/tmpfs) the getxattr fallback works, which is what the unit
//     tests exercise. On SDFS a cold cache (e.g. after restart) returns
//     ErrNoSuchKey, which callers already treat as "empty"; restore-time
//     read-back is the C++/Thrift af2GetPartitionMetadata path, out of scope
//     for the gateway.
//
// Deliberately unsupported in v1 (see droppedKeys). Otter v1 serves Postgres
// base backup and WAL archival — first-party, CDM-orchestrated clients in a
// single trust domain, not a general S3 service — so object-lock
// (retention/legal-hold), object tagging, the retrievable checksum attribute,
// and version-id are accepted and dropped rather than persisted. Rationale:
// retention/immutability are
// enforced by CDM's SLA-driven recovery-point lifecycle (not S3 object-lock);
// write-time integrity still holds (the ETag/MD5 is stored and every MPU part's
// checksum is validated at Complete — only the retrievable stored-checksum
// surface is dropped); no S3 lifecycle/tag-policy consumers exist; and versioning
// is disabled in this deployment. StoreAttribute cannot error on these keys — the
// normal object-write path stores them in sequence, so erroring would fail every
// qualifying PUT; if these features ever enter scope, reject at the operation
// layer (PutObjectRetention/PutObjectLegalHold/PutObjectTagging) instead.
//
// Known operational limits (v1):
//   - Unbounded cache. One entry per (bucket,object) ever touched is retained;
//     nothing evicts. Eviction is intentionally deferred: on SDFS the cache IS
//     the read path (no getxattr), so evicting an entry would make a later GET
//     return empty metadata. Bounding the cache (LRU/janitor) is safe only once
//     the getxattr / WarmCache read-back path lands; until then plan for a
//     footprint of roughly one small entry per live object.
//   - Binary user-metadata. Values are packed via json.Marshal, which replaces
//     invalid UTF-8 with U+FFFD, so arbitrary binary metadata values do not
//     round-trip byte-exactly. S3 user metadata is spec'd as printable ASCII, so
//     this only affects out-of-spec binary values.
//   - Restart read-back gap. The cache is process-local and not persisted; after
//     a gateway restart it is cold and read-back of pre-restart objects returns
//     ErrNoSuchKey (etag/content-type appear empty) until WarmCache (or a
//     getxattr read path) repopulates it. Object data + DESC stay durable in the
//     snapshot; only live re-serving is affected.
type Af2Desc struct {
	// mu guards the structure of the cache map (entry insert/lookup/move).
	mu    sync.RWMutex
	cache map[string]*descEntry // key: filepath.Join(bucket, object)

	// shards provides per-path locking so concurrent writes to different
	// objects do not serialize behind one global mutex across the (blocking)
	// setxattr syscall. Lock ordering: take the shard lock first, then acquire
	// mu only briefly inside entry(); never the reverse.
	shards []sync.Mutex

	maxLen int
}

type descEntry struct {
	// ino is the inode the cached map was last written against. A differing
	// inode on a write means a new object replaced the old one (temp-fd +
	// same-dir rename), so the cached map must be reset to avoid leaking the
	// prior object's metadata into the new one.
	ino uint64
	m   map[string]string
}

// descXattrName is the only FUSE-writable AF2 attribute on SDFS; SDFS maps it to
// file_attributes["DESC"], persisted at partition finalize. The SDFS FUSE
// handler matches this key VERBATIM and does NOT strip the "user." namespace
// (verified on-cluster 2026-06-17: setxattr "user.sdfs_af2_file_desc" -> ENOTSUP,
// raw "sdfs_af2_file_desc" -> OK). It must therefore be set WITHOUT the
// xattrPrefix that normal namespaced S3 attributes carry.
const descXattrName = "sdfs_af2_file_desc"

// DefaultDescMaxBytes is the conservative MJF max_value_size for an AF2 file
// attribute. The packed JSON blob must not exceed it.
const DefaultDescMaxBytes = 256

// descShards is the number of per-path lock shards.
const descShards = 256

// descKeys is the allowlist of logical S3 attributes packed into the DESC blob.
// The values mirror the (unexported) backend/posix attribute-key constants. Any
// key outside this set is accepted-and-dropped on store and reported absent on
// retrieve, so only must-store object metadata occupies the size-capped blob.
var descKeys = map[string]bool{
	"etag":                true,
	"content-type":        true,
	"content-encoding":    true,
	"content-disposition": true,
	"content-language":    true,
	"cache-control":       true,
	"expires":             true,
	"metadata":            true,
	// Bucket-level metadata — stored in-cache only (SDFS getxattr is
	// unimplemented, but the write-through cache keeps them alive for
	// the process lifetime, which is enough for bucket ACL/ownership).
	"acl":       true,
	"ownership": true,
	"tagging":   true,
}

// droppedKeys are object attributes that Otter v1 deliberately does NOT persist
// (accepted and dropped, not an oversight). These correspond to the backend's
// key constants for object-lock, object tagging, the retrievable checksum
// attribute, and version-id. See the Af2Desc type doc for the full rationale;
// the short version: Otter v1 serves base backup + WAL for first-party clients
// in a single trust domain, where retention/immutability come from CDM's SLA
// lifecycle (not S3 object-lock),
// write-time integrity is preserved via the stored ETag + per-part checksum
// validation, no tag-policy consumers exist, and versioning is disabled.
//
// This set is documentation/intent only — the accept-and-drop is enforced by the
// descKeys allowlist miss in StoreAttribute. It is listed explicitly so the drop
// is a reviewable decision, and so a future reader can see exactly what is not
// persisted. It must NOT be turned into a store-time error: the normal
// object-write path calls StoreAttribute for these keys in sequence, so erroring
// would fail every qualifying PUT. If these features enter scope, reject at the
// operation layer instead.
var droppedKeys = map[string]bool{
	"X-Amz-Tagging":     true, // object tagging (tagHdr)
	"object-retention":  true, // WORM retention (objectRetentionKey)
	"object-legal-hold": true, // WORM legal hold (objectLegalHoldKey)
	"checksums":         true, // retrievable stored checksum (checksumsKey)
	"version-id":        true, // versioning disabled in this deployment (versionIdKey)
}

var _ MetadataStorer = (*Af2Desc)(nil)

// NewAf2Desc creates an Af2Desc storer. maxValueBytes caps the packed blob; a
// non-positive value uses DefaultDescMaxBytes.
func NewAf2Desc(maxValueBytes int) *Af2Desc {
	if maxValueBytes <= 0 {
		maxValueBytes = DefaultDescMaxBytes
	}
	return &Af2Desc{
		cache:  make(map[string]*descEntry),
		shards: make([]sync.Mutex, descShards),
		maxLen: maxValueBytes,
	}
}

func (a *Af2Desc) shardFor(path string) *sync.Mutex {
	h := fnv.New32a()
	// hash.Write never returns an error.
	_, _ = h.Write([]byte(path))
	return &a.shards[h.Sum32()%descShards]
}

// entry returns the cache entry for path, creating an empty one if absent. The
// caller must already hold the path's shard lock.
func (a *Af2Desc) entry(path string) *descEntry {
	a.mu.RLock()
	e := a.cache[path]
	a.mu.RUnlock()
	if e != nil {
		return e
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if e = a.cache[path]; e == nil {
		e = &descEntry{m: map[string]string{}}
		a.cache[path] = e
	}
	return e
}

func inoOf(f *os.File) (uint64, bool) {
	fi, err := f.Stat()
	if err != nil {
		return 0, false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Ino), true
}

// readBlob reads and parses the on-disk DESC blob. Used as a fallback on cache
// miss; on SDFS getxattr is unimplemented (ENOTSUP/ENODATA) and this returns an
// error, which callers translate to ErrNoSuchKey.
func readBlob(f *os.File, path string) (map[string]string, error) {
	var b []byte
	var err error
	if f != nil {
		b, err = xattr.FGet(f, descXattrName)
	} else {
		b, err = xattr.Get(path, descXattrName)
	}
	if err != nil {
		return nil, err
	}
	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func mapXattrErr(err error) error {
	if errors.Is(err, syscall.ENOSPC) {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	if errors.Is(err, syscall.EROFS) {
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	}
	return err
}

// StoreAttribute packs attribute=value into the object's DESC blob and writes it
// through to the xattr. Non-allowlisted keys are accepted and dropped. When a
// non-nil f refers to a new inode, the cached map is reset first so a prior
// object's metadata cannot leak into the new object.
func (a *Af2Desc) StoreAttribute(f *os.File, bucket, object, attribute string, value []byte) error {
	if !descKeys[attribute] {
		return nil
	}

	path := filepath.Join(bucket, object)
	sh := a.shardFor(path)
	sh.Lock()
	defer sh.Unlock()

	e := a.entry(path)

	if f != nil {
		if ino, ok := inoOf(f); ok && e.ino != ino {
			e.m = map[string]string{}
			e.ino = ino
		}
	}

	prev, had := e.m[attribute]
	e.m[attribute] = string(value)

	blob, err := json.Marshal(e.m)
	if err != nil {
		a.revert(e, attribute, prev, had)
		return err
	}
	// Bucket-level entries (object=="") hold ACL/ownership JSON which can
	// exceed the MJF 256-byte cap. Skip the size check and the xattr write
	// for them — they are cache-only (SDFS getxattr is unimplemented anyway).
	if object != "" {
		if len(blob) > a.maxLen {
			a.revert(e, attribute, prev, had)
			return s3err.GetAPIError(s3err.ErrMetadataTooLarge)
		}
		if f != nil {
			err = xattr.FSet(f, descXattrName, blob)
		} else {
			err = xattr.Set(path, descXattrName, blob)
		}
		if err != nil {
			a.revert(e, attribute, prev, had)
			return mapXattrErr(err)
		}
	}
	return nil
}

// revert restores a single key to its prior state after a failed write, keeping
// the cache consistent with what is actually on disk.
func (a *Af2Desc) revert(e *descEntry, attribute, prev string, had bool) {
	if had {
		e.m[attribute] = prev
	} else {
		delete(e.m, attribute)
	}
}

// RetrieveAttribute returns the value of attribute from the object's DESC blob.
// It serves from the write-through cache; only when nothing is cached for the
// path does it consult the on-disk blob (which works on a normal xattr fs and
// fails to ErrNoSuchKey on SDFS).
func (a *Af2Desc) RetrieveAttribute(f *os.File, bucket, object, attribute string) ([]byte, error) {
	if !descKeys[attribute] {
		return nil, ErrNoSuchKey
	}

	path := filepath.Join(bucket, object)
	sh := a.shardFor(path)
	sh.Lock()
	defer sh.Unlock()

	e := a.entry(path)
	if v, ok := e.m[attribute]; ok {
		return []byte(v), nil
	}

	// A non-empty cache entry is authoritative (we either wrote it or already
	// read the full on-disk blob), so a missing key means ErrNoSuchKey. Only a
	// cold (empty) entry falls back to disk.
	if len(e.m) == 0 {
		if m, err := readBlob(f, path); err == nil {
			e.m = m
			// Record the inode alongside the cold-filled map. Without this the
			// entry's ino stays 0, and the next fd-based StoreAttribute would see
			// ino(0) != the real inode, wrongly conclude the object was replaced,
			// and wipe this freshly-read metadata.
			if f != nil {
				if ino, ok := inoOf(f); ok {
					e.ino = ino
				}
			}
			if v, ok := m[attribute]; ok {
				return []byte(v), nil
			}
		}
	}
	return nil, ErrNoSuchKey
}

// DeleteAttribute removes a single attribute from the object's DESC blob.
func (a *Af2Desc) DeleteAttribute(bucket, object, attribute string) error {
	path := filepath.Join(bucket, object)
	sh := a.shardFor(path)
	sh.Lock()
	defer sh.Unlock()

	e := a.entry(path)
	prev, had := e.m[attribute]
	delete(e.m, attribute)

	var err error
	if len(e.m) == 0 {
		err = xattr.Remove(path, descXattrName)
	} else {
		var blob []byte
		blob, err = json.Marshal(e.m)
		if err == nil {
			err = xattr.Set(path, descXattrName, blob)
		}
	}
	// ENOATTR/ENOTSUP/ENOENT => nothing is (or can be) persisted: no blob was
	// ever written, the filesystem cannot store xattrs, or the object file has
	// already been unlinked by the caller. The cache deletion stands and we
	// report success. On any real error, revert the cache so it stays consistent
	// with disk (reads are cache-authoritative), matching StoreAttribute's
	// revert-on-failure behavior.
	if errors.Is(err, xattr.ENOATTR) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		a.revert(e, attribute, prev, had)
		return mapXattrErr(err)
	}
	return nil
}

// DeleteAttributes removes all metadata for the object (the whole DESC blob).
func (a *Af2Desc) DeleteAttributes(bucket, object string) error {
	path := filepath.Join(bucket, object)
	sh := a.shardFor(path)
	sh.Lock()
	defer sh.Unlock()

	a.mu.Lock()
	delete(a.cache, path)
	a.mu.Unlock()

	err := xattr.Remove(path, descXattrName)
	// ENOATTR: no blob was ever written. ENOTSUP: the filesystem cannot store
	// xattrs. ENOENT: the object's data file was already unlinked by the caller
	// — backend/posix DeleteObject removes the data file and only then calls
	// DeleteAttributes, so the DESC xattr is already gone with it, which is
	// exactly the desired post-condition. The cache entry is cleared above in
	// every case, so report success; this mirrors XattrMeta.DeleteAttributes
	// being a documented no-op because xattrs are removed with the file.
	if errors.Is(err, xattr.ENOATTR) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.ENOENT) {
		return nil
	}
	return mapXattrErr(err)
}

// ListAttributes deliberately returns no keys. The DESC blob holds only
// must-store object metadata; the backend's ListAttributes consumers are the
// legacy X-Amz-Meta migration/cleanup paths (loadObjectMetadata,
// storeObjectMetadata), which must not see live DESC keys, and SDFS listxattr is
// unsupported.
//
// NOTE: returning an empty list also forecloses the general-object versioning
// attribute-copy path (createObjVersion); this is acceptable only because
// versioning is disabled in the Otter WAL deployment. A future general-object
// deployment must revisit this.
func (a *Af2Desc) ListAttributes(bucket, object string) ([]string, error) {
	return []string{}, nil
}

// RenameObject moves the cached entry from oldObject to newObject. The DESC
// xattr itself follows the inode across a rename, so there is no filesystem op.
func (a *Af2Desc) RenameObject(bucket, oldObject, newObject string) error {
	oldPath := filepath.Join(bucket, oldObject)
	newPath := filepath.Join(bucket, newObject)

	a.mu.Lock()
	if e, ok := a.cache[oldPath]; ok {
		a.cache[newPath] = e
		delete(a.cache, oldPath)
	}
	a.mu.Unlock()
	return nil
}
