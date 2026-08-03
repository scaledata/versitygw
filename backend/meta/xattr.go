// Copyright 2024 Versity Software
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pkg/xattr"
	"github.com/versity/versitygw/s3err"
)

var (
	// ErrNoSuchKey is returned when the key does not exist.
	ErrNoSuchKey = errors.New("no such key")
)

// XattrMeta stores object metadata directly on filesystem extended attributes.
// Rootdir anchors the bucket-relative paths used by the no-fd (path-based)
// calls below; it must be set to the gateway's rootdir (Posix no longer
// os.Chdir's there, so a bare bucket/object join would resolve against the
// process's actual CWD instead).
//
// Left empty, rooted falls back to the plain join. This is not just a
// migration fallback: cmd/versitygw/utils.go's convertXattrMetadata
// deliberately constructs a zero-value XattrMeta and passes an
// already-absolute directory as "bucket" — a different calling convention
// than the Posix integration. Do NOT set Rootdir there; it would double-root
// every path.
type XattrMeta struct {
	Rootdir string
}

// rooted joins bucket/object onto Rootdir, mirroring Posix.rooted. If bucket
// is already absolute (e.g. a caller passes an already-rooted versioning
// path), it is joined as-is without prepending Rootdir — same defense as
// Posix.rooted, for the same reason: some callers reassign "bucket" to an
// already-absolute path before the final path-construction site.
func (x XattrMeta) rooted(bucket, object string) string {
	if x.Rootdir == "" || filepath.IsAbs(bucket) {
		return filepath.Join(bucket, object)
	}
	return filepath.Join(x.Rootdir, bucket, object)
}

// RetrieveAttribute retrieves the value of a specific attribute for an object in a bucket.
func (x XattrMeta) RetrieveAttribute(f *os.File, bucket, object, attribute string) ([]byte, error) {
	if f != nil {
		b, err := xattr.FGet(f, xattrPrefix+attribute)
		if errors.Is(err, xattr.ENOATTR) {
			return nil, ErrNoSuchKey
		}
		return b, err
	}

	b, err := xattr.Get(x.rooted(bucket, object), xattrPrefix+attribute)
	if errors.Is(err, xattr.ENOATTR) {
		return nil, ErrNoSuchKey
	}
	return b, err
}

// StoreAttribute stores the value of a specific attribute for an object in a bucket.
func (x XattrMeta) StoreAttribute(f *os.File, bucket, object, attribute string, value []byte) error {
	if f != nil {
		err := xattr.FSet(f, xattrPrefix+attribute, value)
		if errors.Is(err, syscall.EROFS) {
			return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
		}
		if errors.Is(err, syscall.ENOSPC) {
			return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		return err
	}

	err := xattr.Set(x.rooted(bucket, object), xattrPrefix+attribute, value)
	if errors.Is(err, syscall.EROFS) {
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	}
	if errors.Is(err, syscall.ENOSPC) {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	return err
}

// DeleteAttribute removes the value of a specific attribute for an object in a bucket.
func (x XattrMeta) DeleteAttribute(bucket, object, attribute string) error {
	err := xattr.Remove(x.rooted(bucket, object), xattrPrefix+attribute)
	if errors.Is(err, xattr.ENOATTR) {
		return ErrNoSuchKey
	}
	if errors.Is(err, syscall.EROFS) {
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	}
	return err
}

// DeleteAttributes is not implemented for xattr since xattrs
// are automatically removed when the file is deleted.
func (x XattrMeta) DeleteAttributes(bucket, object string) error {
	return nil
}

// RenameObject is a no-op for xattr because extended attributes are stored
// on the inodes and follow the file/directory when it is renamed.
func (x XattrMeta) RenameObject(_, _, _ string) error {
	return nil
}

// ListAttributes lists all attributes for an object in a bucket.
func (x XattrMeta) ListAttributes(bucket, object string) ([]string, error) {
	attrs, err := xattr.List(x.rooted(bucket, object))
	if err != nil {
		return nil, err
	}
	attributes := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		if !isUserAttr(attr) {
			continue
		}
		attributes = append(attributes, strings.TrimPrefix(attr, xattrPrefix))
	}
	return attributes, nil
}

func isUserAttr(attr string) bool {
	return strings.HasPrefix(attr, xattrPrefix)
}

// Test is a helper function to test if xattrs are supported.
func (x XattrMeta) Test(path string) error {
	// check for platform support
	if !xattr.XATTR_SUPPORTED {
		return fmt.Errorf("xattrs are not supported on this platform")
	}

	// check if the filesystem supports xattrs
	_, err := xattr.Get(path, "user.test")
	if errors.Is(err, syscall.ENOTSUP) {
		return fmt.Errorf("xattrs are not supported on this filesystem")
	}

	return nil
}
