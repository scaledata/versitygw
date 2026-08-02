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

//go:build linux

package posix

import (
	"os"
	"path/filepath"
	"testing"
)

// newTmpfileForTest creates a real named temp file under dir and wraps it in a
// tmpfile targeting bucket/objname, mirroring the non-O_TMPFILE (--disableotmp)
// write path — the commit path Otter/SDFS runs on Linux.
func newTmpfileForTest(t *testing.T, dir, bucket, objname string) *tmpfile {
	t.Helper()
	f, err := os.CreateTemp(dir, MetaTmpDir+".test.")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString("payload"); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return &tmpfile{
		f:          f,
		bucket:     bucket,
		objname:    objname,
		newDirPerm: 0o755,
	}
}

// TestFallbackLinkRecreatesParent covers fallbackLink's recreate-parent retry
// loop: when the commit rename races a concurrent removal of the object's
// parent directory (ENOENT), the loop must MkdirAll the parent and retry rather
// than failing the PUT. This is the Linux (--disableotmp) equivalent of the
// non-Linux link() retry path, and the path Otter actually exercises, so the
// coverage lives here where it runs under CI rather than on the !linux build.
func TestFallbackLinkRecreatesParent(t *testing.T) {
	t.Run("parent missing is recreated", func(t *testing.T) {
		root := t.TempDir()
		// objname nests under "sub", which does not exist yet, so the first
		// rename fails ENOENT and the retry loop must recreate it and succeed.
		tmp := newTmpfileForTest(t, root, root, filepath.Join("sub", "obj"))
		tempname := tmp.f.Name()

		if err := tmp.fallbackLink(); err != nil {
			t.Fatalf("fallbackLink: %v", err)
		}

		objPath := filepath.Join(root, "sub", "obj")
		if b, err := os.ReadFile(objPath); err != nil || string(b) != "payload" {
			t.Fatalf("object at %s = %q, err=%v; want committed payload",
				objPath, b, err)
		}
		if _, err := os.Stat(tempname); !os.IsNotExist(err) {
			t.Fatalf("temp file %s still present after commit (err=%v)",
				tempname, err)
		}
	})

	t.Run("parent present commits directly", func(t *testing.T) {
		root := t.TempDir()
		tmp := newTmpfileForTest(t, root, root, "obj")
		tempname := tmp.f.Name()

		if err := tmp.fallbackLink(); err != nil {
			t.Fatalf("fallbackLink: %v", err)
		}
		if _, err := os.Stat(filepath.Join(root, "obj")); err != nil {
			t.Fatalf("object not committed: %v", err)
		}
		if _, err := os.Stat(tempname); !os.IsNotExist(err) {
			t.Fatalf("temp file still present: %v", err)
		}
	})
}
