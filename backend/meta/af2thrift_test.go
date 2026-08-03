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
	"encoding/binary"
	"testing"
)

func writeI64(w *wbuf, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.b = append(w.b, b[:]...)
}

// TestParseGetMetaResponseRoundTrip builds a strict-binary af2GetPartitionMetadata
// REPLY by hand (using the same writer the request path uses) and verifies the
// parser: extracts DESC for files that have it, skips files that don't, and
// strips the channel-path prefix to produce the relative bucket/object cache key.
func TestParseGetMetaResponseRoundTrip(t *testing.T) {
	const channelPath = "/sd/mount/chan0"
	prefix := channelPath + "/"

	var w wbuf
	// Strict binary message header: version 1, type REPLY (2).
	w.b = append(w.b, 0x80, 0x01, 0x00, 0x02)
	w.str("af2GetPartitionMetadata")
	w.i32(1) // sequence id

	// Reply struct: field 0 STRUCT = success return (Af2GetMetadataResponse).
	w.field(tbStruct, 0)
	// Af2GetMetadataResponse: field 2 STRUCT = Af2FileMetadata.
	w.field(tbStruct, 2)
	// Af2FileMetadata: field 1 LIST<STRUCT> = file summaries.
	w.field(tbList, 1)
	w.byte_(tbStruct) // element type
	w.i32(2)          // two summaries

	// Summary 1: has a DESC attribute -> must appear, keyed by relative path.
	w.field(tbString, 1)
	w.str(prefix + "wal/00000001")
	w.field(tbI64, 2)
	writeI64(&w, 4096)
	w.field(tbMap, 3)
	w.byte_(tbString)
	w.byte_(tbString)
	w.i32(1)
	w.str("DESC")
	w.str(`{"etag":"abc","content-type":"text/plain"}`)
	w.stop() // end summary 1

	// Summary 2: no DESC attribute -> must be skipped.
	w.field(tbString, 1)
	w.str(prefix + "wal/00000002")
	w.field(tbMap, 3)
	w.byte_(tbString)
	w.byte_(tbString)
	w.i32(0) // empty attribute map
	w.stop() // end summary 2

	w.stop() // end Af2FileMetadata
	w.stop() // end Af2GetMetadataResponse
	w.stop() // end reply struct

	got, err := parseGetMetaResponse(w.b, channelPath)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1 (only the file with DESC); got=%v", len(got), got)
	}
	m, ok := got["wal/00000001"]
	if !ok {
		t.Fatalf("missing expected relative key %q; got=%v", "wal/00000001", got)
	}
	if m["etag"] != "abc" || m["content-type"] != "text/plain" {
		t.Fatalf("DESC fields mismatch: %v", m)
	}
}

// TestParseGetMetaResponseException verifies a TApplicationException reply is
// surfaced as an error rather than silently returning an empty map.
func TestParseGetMetaResponseException(t *testing.T) {
	var w wbuf
	// version 1, type EXCEPTION (3).
	w.b = append(w.b, 0x80, 0x01, 0x00, 0x03)
	w.str("af2GetPartitionMetadata")
	w.i32(1)
	w.field(tbString, 1)
	w.str("boom")
	w.field(tbI32, 2)
	w.i32(6)
	w.stop()

	if _, err := parseGetMetaResponse(w.b, "/sd/mount/chan0"); err == nil {
		t.Fatalf("expected an error for an EXCEPTION reply, got nil")
	}
}
