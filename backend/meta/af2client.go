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

// mTLS transport for the af2GetPartitionMetadata call: GetPartitionDescMap dials
// the CDM kvsnapshot service over the cluster mTLS identity, sends the encoded
// request (buildGetMetaRequest) framed with TFramedTransport, and decodes the
// reply (parseGetMetaResponse). The wire codec/parser live in af2thrift.go.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// af2KVSnapshotPort is the kvsnapshot internal Thrift service port on CDM nodes.
// Value comes from src/spec/global-config/kvsnapshot_global_config.yml
// (kvSnapshotServerInternalPort default: 9638).
const af2KVSnapshotPort = 9638

const af2DialTimeout = 10 * time.Second

// af2IOTimeout bounds the whole send/recv after a successful dial, so a
// kvsnapshot service that accepts the connection but then stalls before
// replying cannot block recvFramed's io.ReadFull forever. WarmCache is called
// synchronously at gateway startup, so an unbounded read would hang startup.
const af2IOTimeout = 30 * time.Second

// DefaultAf2CertFile and DefaultAf2KeyFile are the Rubrik cluster mTLS
// credentials used by all internal node-to-node Thrift services.
const DefaultAf2CertFile = "/var/lib/rubrik/certs/cluster.crt"
const DefaultAf2KeyFile = "/var/lib/rubrik/certs/cluster.pem"

// GetPartitionDescMap calls af2GetPartitionMetadata on the CDM node at nodeIP
// and returns a map from relative object path (bucket/object) to the unpacked
// DESC JSON fields. Only files that have a DESC attribute are included.
//
// This is the production DESC-read pattern (Af2Partition.getAf2FileSummaryList):
// it calls getPartitionMetadata with num_shards=0 and does NOT call
// af2InitPartition — the partition is assumed already mounted on nodeIP, which
// holds for the gateway because it runs on the channel's mount node
// (writer==owner) and sdfs_main keeps the partition mounted across a gateway
// restart. Calling af2InitPartition would be a mutating operation that can steal
// the claim-mount from a live writer; we deliberately avoid it.
//
// channelPath is the gateway root directory; it is stripped from AF2's absolute
// file_paths to produce cache-compatible relative keys.
// certFile and keyFile are the mTLS cluster certificate and key; pass empty
// strings to use DefaultAf2CertFile / DefaultAf2KeyFile.
func GetPartitionDescMap(nodeIP, af2UniqueID, channelPath string, partitionID int16, certFile, keyFile string) (map[string]map[string]string, error) {
	if certFile == "" {
		certFile = DefaultAf2CertFile
	}
	if keyFile == "" {
		keyFile = DefaultAf2KeyFile
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load cluster cert %s: %w", certFile, err)
	}
	caPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read cluster CA %s: %w", certFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse cluster CA %s: no certificates found (malformed or empty PEM)", certFile)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		// Server CN is <clusterUUID>.cluster.rubrik.local, not verifiable
		// against an IP. Skip hostname check; the CA pool still validates the
		// chain. Internal cluster mTLS.
		InsecureSkipVerify: true, //nolint:gosec
	}

	addr := fmt.Sprintf("%s:%d", nodeIP, af2KVSnapshotPort)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: af2DialTimeout}, "tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("TLS connect to AF2 at %s: %w", addr, err)
	}
	defer conn.Close()

	// Bound the whole request. Without this, a peer that completes the TLS
	// handshake and then goes silent leaves recvFramed's io.ReadFull blocking
	// forever, hanging synchronous startup warm-up.
	if err := conn.SetDeadline(time.Now().Add(af2IOTimeout)); err != nil {
		return nil, fmt.Errorf("set AF2 connection deadline: %w", err)
	}

	req := buildGetMetaRequest(af2UniqueID, channelPath, partitionID)
	if err := sendFramed(conn, req); err != nil {
		return nil, fmt.Errorf("send af2GetPartitionMetadata: %w", err)
	}
	resp, err := recvFramed(conn)
	if err != nil {
		return nil, fmt.Errorf("recv af2GetPartitionMetadata: %w", err)
	}
	return parseGetMetaResponse(resp, channelPath)
}

// ── Framed transport (TFramedTransport over mTLS) ─────────────────────────────
// kvsnapshot uses TBinaryProtocol + TFramedTransport: each message is prefixed
// with a 4-byte big-endian frame length.

func sendFramed(conn net.Conn, payload []byte) error {
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	_, err := conn.Write(frame)
	return err
}

func recvFramed(conn net.Conn) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > 64*1024*1024 {
		return nil, fmt.Errorf("AF2 response frame too large: %d bytes", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read frame body: %w", err)
	}
	return buf, nil
}
