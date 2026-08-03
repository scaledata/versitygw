// Copyright 2024 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package posix

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/versity/versitygw/backend/meta"
)

// fakeEtagMeta is a minimal MetadataStorer that only answers etag lookups, used
// to distinguish a real (empty) directory-object marker from a plain key
// prefix. A path registered in dirObjects reports an etag; everything else
// reports meta.ErrNoSuchKey, matching how FileToObj probes directory objects.
type fakeEtagMeta struct {
	dirObjects map[string]struct{} // set of filepath.Join(bucket, object) paths
}

func (m fakeEtagMeta) RetrieveAttribute(_ *os.File, bucket, object, attribute string) ([]byte, error) {
	if attribute == etagkey {
		if _, ok := m.dirObjects[filepath.Join(bucket, object)]; ok {
			return []byte("etag"), nil
		}
	}
	return nil, meta.ErrNoSuchKey
}

func (m fakeEtagMeta) StoreAttribute(_ *os.File, _, _, _ string, _ []byte) error { return nil }
func (m fakeEtagMeta) DeleteAttribute(_, _, _ string) error                      { return nil }
func (m fakeEtagMeta) ListAttributes(_, _ string) ([]string, error)             { return nil, nil }
func (m fakeEtagMeta) DeleteAttributes(_, _ string) error                        { return nil }
func (m fakeEtagMeta) RenameObject(_, _, _ string) error                         { return nil }

// TestIsBucketEmptySameDirTmp covers the same-dir-tmp scratch cases for
// DeleteBucket's emptiness check, in particular the nested-key orphan that the
// original top-level-only fix missed: a leaked ".sgwtmp.*" scratch file under a
// nested prefix must not block bucket deletion, while a real object (at any
// depth) and an empty directory-object marker still must.
func TestIsBucketEmptySameDirTmp(t *testing.T) {
	const scratch = MetaTmpDir + ".deadbeef.abc123" // ".sgwtmp.<hash>.<rand>"

	tests := []struct {
		name string
		// files to create under the bucket root (relative paths)
		files []string
		// directories to create under the bucket root (relative paths)
		dirs []string
		// relative paths (dirs) registered as directory-object markers
		dirObjects []string
		wantEmpty  bool
	}{
		{
			name:      "truly empty bucket",
			wantEmpty: true,
		},
		{
			name:      "only a top-level scratch file",
			files:     []string{scratch},
			wantEmpty: true,
		},
		{
			name:      "only a nested orphaned scratch file",
			files:     []string{filepath.Join("a", "b", "c", scratch)},
			wantEmpty: true, // the regression this fix targets
		},
		{
			name:      "legacy .sgwtmp staging dir with scratch inside",
			files:     []string{filepath.Join(MetaTmpDir, scratch)},
			wantEmpty: true,
		},
		{
			name:      "top-level real object",
			files:     []string{"obj"},
			wantEmpty: false,
		},
		{
			name:      "nested real object",
			files:     []string{filepath.Join("a", "b", "obj")},
			wantEmpty: false,
		},
		{
			name:      "real object alongside nested scratch",
			files:     []string{"obj", filepath.Join("a", "b", scratch)},
			wantEmpty: false,
		},
		{
			name:       "empty directory-object marker",
			dirs:       []string{"foo"},
			dirObjects: []string{"foo"},
			wantEmpty:  false, // must not be deleted as if it were scratch
		},
		{
			name:       "nested empty directory-object marker under a prefix",
			dirs:       []string{filepath.Join("a", "foo")},
			dirObjects: []string{filepath.Join("a", "foo")},
			wantEmpty:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucket := t.TempDir()

			for _, d := range tc.dirs {
				if err := os.MkdirAll(filepath.Join(bucket, d), 0o755); err != nil {
					t.Fatalf("mkdir %q: %v", d, err)
				}
			}
			for _, f := range tc.files {
				full := filepath.Join(bucket, f)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatalf("mkdir for %q: %v", f, err)
				}
				if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
					t.Fatalf("write %q: %v", f, err)
				}
			}

			fm := fakeEtagMeta{dirObjects: map[string]struct{}{}}
			for _, d := range tc.dirObjects {
				fm.dirObjects[filepath.Join(bucket, d)] = struct{}{}
			}

			p := &Posix{meta: fm}

			err := p.isBucketEmpty(bucket)
			gotEmpty := err == nil
			if gotEmpty != tc.wantEmpty {
				t.Fatalf("isBucketEmpty empty=%v (err=%v), want empty=%v",
					gotEmpty, err, tc.wantEmpty)
			}
		})
	}
}
