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

import "testing"

// TestXattrMetaRooted covers the rooted() resolution that this rootdir-aware
// change hinges on: an unset Rootdir joins as-is, an already-absolute bucket
// (e.g. a versionPath from the posix backend) is never double-rooted, and a
// relative bucket is anchored under Rootdir. The absolute-bucket guard has live
// callers (ListObjectVersions / version-copy pass an absolute versionPath), so a
// future "simplification" that drops it would silently break those lookups.
func TestXattrMetaRooted(t *testing.T) {
	cases := []struct {
		name    string
		rootdir string
		bucket  string
		object  string
		want    string
	}{
		{"empty rootdir joins as-is", "", "bkt", "obj", "bkt/obj"},
		{"relative bucket anchored", "/root", "bkt", "obj", "/root/bkt/obj"},
		{"absolute bucket not double-rooted", "/root", "/abs/vers", "vid", "/abs/vers/vid"},
		{"absolute bucket, empty rootdir", "", "/abs/vers", "vid", "/abs/vers/vid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := XattrMeta{Rootdir: tc.rootdir}
			if got := x.rooted(tc.bucket, tc.object); got != tc.want {
				t.Fatalf("rooted(%q,%q) with Rootdir=%q = %q, want %q",
					tc.bucket, tc.object, tc.rootdir, got, tc.want)
			}
		})
	}
}
