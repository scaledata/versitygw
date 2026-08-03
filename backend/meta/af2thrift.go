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

// Thrift binary codec for the af2GetPartitionMetadata call: the wire-format
// request encoder (buildGetMetaRequest) and response decoder (parse*), plus the
// minimal TBinaryProtocol read/write primitives they use. Pure functions with no
// I/O — the mTLS transport that carries these bytes lives in af2client.go.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Thrift binary protocol type constants.
const (
	tbStop   byte = 0
	tbBool   byte = 2
	tbI16    byte = 6
	tbI32    byte = 8
	tbI64    byte = 10
	tbString byte = 11
	tbStruct byte = 12
	tbMap    byte = 13
	tbSet    byte = 14
	tbList   byte = 15
)

// ── Write helpers ─────────────────────────────────────────────────────────────

type wbuf struct{ b []byte }

func (w *wbuf) byte_(v byte) { w.b = append(w.b, v) }
func (w *wbuf) bool_(v bool) {
	if v {
		w.byte_(1)
	} else {
		w.byte_(0)
	}
}
func (w *wbuf) i16(v int16) { w.b = append(w.b, byte(v>>8), byte(v)) }
func (w *wbuf) i32(v int32) {
	w.b = append(w.b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func (w *wbuf) str(s string)              { w.i32(int32(len(s))); w.b = append(w.b, s...) }
func (w *wbuf) field(typ byte, id int16)  { w.byte_(typ); w.i16(id) }
func (w *wbuf) stop()                     { w.byte_(tbStop) }

// buildGetMetaRequest encodes an af2GetPartitionMetadata call in Thrift strict
// binary protocol with TMultiplexedProtocol ("ServiceName:methodName").
//
// Af2GetMetadataRequest (af2/af2_common.thrift):
//   field 1: RequestContext (empty — all fields optional)
//   field 2: Af2Id { unique_id, af2_root, scratch_root, base_dir, snappable_type }
//   field 3: partition_id i16
//   field 4: num_shards i16 (production passes 0 — "not used"; avoids per-shard stat)
//   field 5: include_file_attributes bool = true
//
// ALL Af2Id fields are required: the server computes the metadata-shard path as
// af2_root/unique_id/<partition>/shard0. With an empty af2_root it degrades to
// "/0/shard0" and the Stat fails. af2_root/scratch_root/base_dir are derived
// from channelPath.
func buildGetMetaRequest(af2UniqueID, channelPath string, partitionID int16) []byte {
	const af2SnappableTypePostgres int32 = 3 // POSTGRESDBCLUSTER

	// channelPath = /sd/mount/<base>/<export>/channel_N
	//   base_dir    = /sd/mount/<base>/<export>   (strip /channel_N)
	//   af2_root    = /sd/mount/<base>            (strip /<export>)
	//   scratch_root= /sd/scratch/<base>          (mount→scratch)
	baseDir := strings.TrimRight(channelPath, "/")
	if idx := strings.LastIndex(baseDir, "/"); idx > 0 {
		baseDir = baseDir[:idx]
	}
	af2Root := baseDir
	if idx := strings.LastIndex(af2Root, "/"); idx > 0 {
		af2Root = af2Root[:idx]
	}
	scratchRoot := strings.Replace(af2Root, "/mount/", "/scratch/", 1)

	var body wbuf

	// The method args are an implicit struct with ONE field:
	//   af2GetPartitionMetadata(1: Af2GetMetadataRequest request)
	// so the whole request is wrapped as args field 1 (STRUCT).
	body.field(tbStruct, 1) // args field 1 = request

	// --- Af2GetMetadataRequest fields ---

	// Field 1: RequestContext (empty struct)
	body.field(tbStruct, 1)
	body.stop()

	// Field 2: Af2Id — all five fields
	body.field(tbStruct, 2)
	body.field(tbString, 1); body.str(af2UniqueID)              // unique_id
	body.field(tbString, 2); body.str(af2Root)                  // af2_root
	body.field(tbString, 3); body.str(scratchRoot)              // scratch_root
	body.field(tbString, 4); body.str(baseDir)                  // base_dir
	body.field(tbI32, 5); body.i32(af2SnappableTypePostgres)    // snappable_type
	body.stop()

	// Field 3: partition_id
	body.field(tbI16, 3)
	body.i16(partitionID)

	// Field 4: num_shards = 0 (production pattern; server does not stat data shards)
	body.field(tbI16, 4)
	body.i16(0)

	// Field 5: include_file_attributes = true
	body.field(tbBool, 5)
	body.bool_(true)

	body.stop() // end Af2GetMetadataRequest

	body.stop() // end args struct

	// TMultiplexedProtocol requires "ServiceName:methodName".
	const method = "Af2Server:af2GetPartitionMetadata"
	var msg wbuf
	msg.b = append(msg.b, 0x80, 0x01, 0x00, 0x01) // strict binary CALL
	msg.str(method)
	msg.i32(1) // sequence ID
	msg.b = append(msg.b, body.b...)
	return msg.b
}

// ── Read helpers ──────────────────────────────────────────────────────────────

type rbuf struct {
	b   []byte
	pos int
}

func (r *rbuf) byte_() (byte, error) {
	if r.pos >= len(r.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}
func (r *rbuf) i16() (int16, error) {
	if r.pos+2 > len(r.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int16(binary.BigEndian.Uint16(r.b[r.pos:]))
	r.pos += 2
	return v, nil
}
func (r *rbuf) i32() (int32, error) {
	if r.pos+4 > len(r.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int32(binary.BigEndian.Uint32(r.b[r.pos:]))
	r.pos += 4
	return v, nil
}
func (r *rbuf) i64() (int64, error) {
	if r.pos+8 > len(r.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int64(binary.BigEndian.Uint64(r.b[r.pos:]))
	r.pos += 8
	return v, nil
}
func (r *rbuf) str() (string, error) {
	n, err := r.i32()
	if err != nil {
		return "", err
	}
	if n < 0 || r.pos+int(n) > len(r.b) {
		return "", fmt.Errorf("string length out of bounds: %d", n)
	}
	s := string(r.b[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, nil
}

// skip discards one Thrift value of the given type.
func (r *rbuf) skip(typ byte) error {
	switch typ {
	case tbBool:
		_, err := r.byte_()
		return err
	case tbI16:
		_, err := r.i16()
		return err
	case tbI32:
		_, err := r.i32()
		return err
	case tbI64:
		_, err := r.i64()
		return err
	case tbString:
		_, err := r.str()
		return err
	case tbStruct:
		for {
			ft, err := r.byte_()
			if err != nil {
				return err
			}
			if ft == tbStop {
				return nil
			}
			if _, err := r.i16(); err != nil {
				return err
			}
			if err := r.skip(ft); err != nil {
				return err
			}
		}
	case tbMap:
		kt, err := r.byte_()
		if err != nil {
			return err
		}
		vt, err := r.byte_()
		if err != nil {
			return err
		}
		n, err := r.i32()
		if err != nil {
			return err
		}
		for i := 0; i < int(n); i++ {
			if err := r.skip(kt); err != nil {
				return err
			}
			if err := r.skip(vt); err != nil {
				return err
			}
		}
		return nil
	case tbList, tbSet:
		et, err := r.byte_()
		if err != nil {
			return err
		}
		n, err := r.i32()
		if err != nil {
			return err
		}
		for i := 0; i < int(n); i++ {
			if err := r.skip(et); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("cannot skip unknown Thrift type %d", typ)
	}
}

// ── Response parsing ──────────────────────────────────────────────────────────

// parseGetMetaResponse decodes the reply and returns a map from relative object
// path to DESC JSON fields for files that have a DESC attribute.
func parseGetMetaResponse(data []byte, channelPath string) (map[string]map[string]string, error) {
	r := &rbuf{b: data}

	// Strict binary message header.
	vw, err := r.i32()
	if err != nil {
		return nil, err
	}
	msgType := byte(vw & 0xff)
	if _, err := r.str(); err != nil { // method name
		return nil, err
	}
	if _, err := r.i32(); err != nil { // sequence ID
		return nil, err
	}

	if msgType == 3 { // EXCEPTION
		msg, errType := "", int32(0)
		for {
			ft, err := r.byte_()
			if err != nil || ft == tbStop {
				break
			}
			fid, err := r.i16()
			if err != nil {
				break
			}
			switch fid {
			case 1:
				msg, _ = r.str()
			case 2:
				errType, _ = r.i32()
			default:
				_ = r.skip(ft)
			}
		}
		return nil, fmt.Errorf("af2GetPartitionMetadata exception type=%d: %q", errType, msg)
	}

	prefix := strings.TrimRight(channelPath, "/") + "/"
	result := map[string]map[string]string{}

	// Result struct: field 0 (STRUCT) = success value (Af2GetMetadataResponse).
	for {
		ft, err := r.byte_()
		if err != nil || ft == tbStop {
			break
		}
		fid, err := r.i16()
		if err != nil {
			return nil, err
		}
		if fid == 0 && ft == tbStruct {
			if err := parseAf2GetMetadataResponse(r, result, prefix); err != nil {
				return nil, err
			}
		} else {
			if err := r.skip(ft); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// parseAf2GetMetadataResponse parses Af2GetMetadataResponse; field 2 is the
// Af2FileMetadata holding the file summary list.
func parseAf2GetMetadataResponse(r *rbuf, result map[string]map[string]string, prefix string) error {
	for {
		ft, err := r.byte_()
		if err != nil || ft == tbStop {
			return err
		}
		fid, err := r.i16()
		if err != nil {
			return err
		}
		if fid == 2 && ft == tbStruct {
			if err := parseFileMetadata(r, result, prefix); err != nil {
				return err
			}
		} else {
			if err := r.skip(ft); err != nil {
				return err
			}
		}
	}
}

func parseFileMetadata(r *rbuf, result map[string]map[string]string, prefix string) error {
	for {
		ft, err := r.byte_()
		if err != nil || ft == tbStop {
			return err
		}
		fid, err := r.i16()
		if err != nil {
			return err
		}
		if fid == 1 && ft == tbList {
			_, err := r.byte_() // element type (STRUCT)
			if err != nil {
				return err
			}
			count, err := r.i32()
			if err != nil {
				return err
			}
			for i := 0; i < int(count); i++ {
				if err := parseFileSummary(r, result, prefix); err != nil {
					return err
				}
			}
		} else {
			if err := r.skip(ft); err != nil {
				return err
			}
		}
	}
}

func parseFileSummary(r *rbuf, result map[string]map[string]string, prefix string) error {
	var filePath string
	var fileAttrs map[string]string
	for {
		ft, err := r.byte_()
		if err != nil || ft == tbStop {
			break
		}
		fid, err := r.i16()
		if err != nil {
			return err
		}
		switch fid {
		case 1: // file_path string
			filePath, err = r.str()
			if err != nil {
				return err
			}
		case 2: // logical_size i64
			if err := r.skip(tbI64); err != nil {
				return err
			}
		case 3: // file_attributes map<string,string>
			fileAttrs, err = parseStringMap(r)
			if err != nil {
				return err
			}
		default:
			if err := r.skip(ft); err != nil {
				return err
			}
		}
	}
	if filePath == "" || fileAttrs == nil {
		return nil
	}
	desc, ok := fileAttrs["DESC"]
	if !ok {
		return nil
	}
	m := map[string]string{}
	if json.Unmarshal([]byte(desc), &m) == nil {
		// Strip the channel directory prefix to get the relative cache key
		// (e.g. "/sd/mount/.../channel_0/warmtest/wal/f" -> "warmtest/wal/f").
		result[strings.TrimPrefix(filePath, prefix)] = m
	}
	return nil
}

func parseStringMap(r *rbuf) (map[string]string, error) {
	_, err := r.byte_() // key type
	if err != nil {
		return nil, err
	}
	_, err = r.byte_() // value type
	if err != nil {
		return nil, err
	}
	count, err := r.i32()
	if err != nil {
		return nil, err
	}
	if count < 0 || count > 1024 {
		return nil, fmt.Errorf("parseStringMap: invalid count %d", count)
	}
	m := make(map[string]string, count)
	for i := 0; i < int(count); i++ {
		k, err := r.str()
		if err != nil {
			return nil, err
		}
		v, err := r.str()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
