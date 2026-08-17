// Copyright 2023 Versity Software
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

package main

import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/backend/router"
	"github.com/versity/versitygw/backend/s3proxy"
)

var (
	ownerMapFile    string
	selfIdxFlag     int
	forwardTimeout  time.Duration
	chanParallelism int

	// af2Warm* flags enable WarmCache at startup: pre-populate the Af2Desc
	// metadata cache from af2GetPartitionMetadata so GET/HEAD returns correct
	// metadata even on a cold cache after a restart. Set node-ip, id, and
	// partition-id together to enable it.
	af2WarmNodeIP      string
	af2WarmUniqueID    string
	af2WarmPartitionID int
	af2WarmCertFile    string
	af2WarmKeyFile     string
)

func otterCommand() *cli.Command {
	return &cli.Command{
		Name:  "otter",
		Usage: "Otter multi-channel distribution backend (local AF2 channel + forward to peer gateways)",
		Description: `Distributes S3 object writes across N AF2 channels, one per node. This node
runs the local posix/AF2 channel for the objects it owns and forwards the rest
to peer node gateways, selecting the owner by a stable hash of the object key
(WAL segments by their monotonic ordinal). Provide the gateway root directory
as the argument and the shared owner map via --owner-map.`,
		Action: runOtter,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "owner-map",
				Usage:       "path to the shared JSON owner map (slots: slot/nodeId/endpoint/channelPath, plus n/epoch)",
				EnvVars:     []string{"OTTER_OWNER_MAP"},
				Destination: &ownerMapFile,
				Required:    true,
			},
			&cli.IntFlag{
				Name:        "self-idx",
				Usage:       "this node's slot index in the owner map (0..n-1); the only per-node difference",
				EnvVars:     []string{"OTTER_SELF_IDX"},
				Destination: &selfIdxFlag,
				Required:    true,
			},
			&cli.DurationFlag{
				Name:        "forward-timeout",
				Usage:       "deadline for a forwarded byte-write to a peer; a partitioned peer fails fast instead of hanging (0 disables)",
				EnvVars:     []string{"OTTER_FORWARD_TIMEOUT"},
				Value:       30 * time.Second,
				Destination: &forwardTimeout,
			},
			&cli.IntFlag{
				Name:        "chan-parallelism",
				Usage:       "P: max concurrent byte-writes admitted into the local AF2 channel (1 = historical serialize-everything; >1 relaxes it to test throughput)",
				EnvVars:     []string{"OTTER_CHAN_PARALLELISM"},
				Value:       1,
				Destination: &chanParallelism,
			},
			// posix/AF2 flags for the local channel (subset of `posix`):
			&cli.BoolFlag{
				Name:        "af2-desc",
				Usage:       "store S3 metadata (etag, content-type, user metadata) in the AF2 DESC attribute (CDM SDFS)",
				EnvVars:     []string{"VGW_AF2_DESC"},
				Destination: &af2Desc,
			},
			&cli.IntFlag{
				Name:        "af2-desc-max-bytes",
				Usage:       "maximum size of the packed AF2 DESC metadata blob",
				EnvVars:     []string{"VGW_AF2_DESC_MAX_BYTES"},
				Value:       meta.DefaultDescMaxBytes,
				Destination: &af2DescMaxBytes,
			},
			&cli.BoolFlag{
				Name:        "same-dir-tmp",
				Usage:       "create the atomic-write temp file in the object's own directory (required for SDFS)",
				EnvVars:     []string{"VGW_SAME_DIR_TMP"},
				Destination: &sameDirTmp,
			},
			&cli.BoolFlag{
				Name:        "disableotmp",
				Usage:       "disable O_TMPFILE support for new objects (use with --same-dir-tmp on SDFS)",
				EnvVars:     []string{"VGW_DISABLE_OTMP"},
				Destination: &forceNoTmpFile,
			},
			// af2GetPartitionMetadata warm-up flags. Set node-ip, id, and
			// partition-id together to enable WarmCache at startup.
			&cli.StringFlag{
				Name:        "af2-warm-node-ip",
				Usage:       "IP of the CDM node running the AF2/kvsnapshot service (usually 127.0.0.1); with --af2-warm-id/--af2-warm-partition-id, warms the metadata cache at startup",
				EnvVars:     []string{"OTTER_AF2_WARM_NODE_IP"},
				Destination: &af2WarmNodeIP,
			},
			&cli.StringFlag{
				Name:        "af2-warm-id",
				Usage:       "AF2 unique ID (UUID) for this channel's partition (from Kosmos data_path_spec)",
				EnvVars:     []string{"OTTER_AF2_WARM_ID"},
				Destination: &af2WarmUniqueID,
			},
			&cli.IntFlag{
				Name:        "af2-warm-partition-id",
				Usage:       "AF2 partition ID for this channel (from Kosmos data_path_spec)",
				EnvVars:     []string{"OTTER_AF2_WARM_PARTITION_ID"},
				Value:       -1,
				Destination: &af2WarmPartitionID,
			},
			&cli.StringFlag{
				Name:        "af2-warm-cert",
				Usage:       "mTLS cluster certificate for kvsnapshot (default: /var/lib/rubrik/certs/cluster.crt)",
				EnvVars:     []string{"OTTER_AF2_WARM_CERT"},
				Destination: &af2WarmCertFile,
			},
			&cli.StringFlag{
				Name:        "af2-warm-key",
				Usage:       "mTLS cluster key for kvsnapshot (default: /var/lib/rubrik/certs/cluster.pem)",
				EnvVars:     []string{"OTTER_AF2_WARM_KEY"},
				Destination: &af2WarmKeyFile,
			},
			&cli.IntFlag{
				Name:        "concurrency",
				Usage:       "maximum concurrent actions allowed on the local backend",
				EnvVars:     []string{"VGW_POSIX_CONCURRENCY"},
				Value:       5000,
				Destination: &actionsConcurrency,
			},
			&cli.UintFlag{
				Name:        "dir-perms",
				Usage:       "default directory permissions for new directories",
				EnvVars:     []string{"VGW_DIR_PERMS"},
				Value:       0755,
				Destination: &dirPerms,
			},
		},
	}
}

// maybeWarmCache runs Af2Desc.WarmCache when all three warm flags are provided.
// Partial configuration (some but not all) is surfaced loudly rather than
// silently skipped, and the partition-id is bounds-checked before narrowing to
// int16. WarmCache itself is best-effort (logged, non-fatal); only a
// misconfiguration returns an error.
func maybeWarmCache(desc *meta.Af2Desc, gwroot string) error {
	nSet := 0
	if af2WarmNodeIP != "" {
		nSet++
	}
	if af2WarmUniqueID != "" {
		nSet++
	}
	if af2WarmPartitionID >= 0 {
		nSet++
	}
	switch {
	case nSet == 0:
		return nil // warm not requested
	case nSet < 3:
		fmt.Fprintf(os.Stderr, "warn: --af2-warm-* partially configured (%d/3 set); skipping WarmCache. Set --af2-warm-node-ip, --af2-warm-id and --af2-warm-partition-id together.\n", nSet)
		return nil
	}

	if af2WarmPartitionID > math.MaxInt16 {
		return fmt.Errorf("--af2-warm-partition-id %d exceeds the AF2 partition-id range (max %d)", af2WarmPartitionID, math.MaxInt16)
	}

	// AF2 reports absolute file_paths; WarmCache strips channelPath as an exact
	// string prefix to derive relative cache keys, so pass the absolute, cleaned
	// gwroot. It must be the canonical mount path AF2 walks — a relative path or
	// stray trailing slash would make every key miss and warming a silent no-op.
	gwrootAbs, err := filepath.Abs(gwroot)
	if err != nil {
		return fmt.Errorf("resolve gwroot %q to absolute: %w", gwroot, err)
	}

	if err := desc.WarmCache(af2WarmNodeIP, af2WarmUniqueID, gwrootAbs,
		int16(af2WarmPartitionID), af2WarmCertFile, af2WarmKeyFile); err != nil {
		fmt.Fprintf(os.Stderr, "warn: WarmCache failed (pre-restart metadata unavailable): %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "info: WarmCache complete\n")
	}
	return nil
}

func runOtter(ctx *cli.Context) error {
	if ctx.NArg() == 0 {
		return fmt.Errorf("no gateway root directory provided")
	}
	gwroot := ctx.Args().Get(0)

	if dirPerms > math.MaxUint32 {
		return fmt.Errorf("invalid directory permissions: %d", dirPerms)
	}
	if actionsConcurrency <= 0 {
		return fmt.Errorf("concurrency must be positive, got %d", actionsConcurrency)
	}

	om, err := router.LoadOwnerMap(ownerMapFile)
	if err != nil {
		return err
	}

	// Local AF2 channel backend (DESC metadata + same-dir-tmp for SDFS).
	var ms meta.MetadataStorer
	if af2Desc {
		desc := meta.NewAf2Desc(af2DescMaxBytes)
		gwrootAbs, err := filepath.Abs(gwroot)
		if err != nil {
			return fmt.Errorf("get absolute path of %v: %w", gwroot, err)
		}
		// Root path-based DESC xattr syscalls at the absolute gateway root; the
		// backend no longer chdir's, so a bare bucket/object would resolve
		// against the process CWD (mirrors the XattrMeta.Rootdir wiring below).
		desc.Rootdir = gwrootAbs
		if err := maybeWarmCache(desc, gwroot); err != nil {
			return err
		}
		ms = desc
	} else {
		gwrootAbs, err := filepath.Abs(gwroot)
		if err != nil {
			return fmt.Errorf("get absolute path of %v: %w", gwroot, err)
		}
		// The warm flags only take effect under --af2-desc; warn if set without
		// it so this precondition failure isn't silent.
		if af2WarmNodeIP != "" || af2WarmUniqueID != "" || af2WarmPartitionID >= 0 {
			fmt.Fprintf(os.Stderr, "warn: --af2-warm-* flags are set but --af2-desc is not; WarmCache will not run\n")
		}
		// Match runPosix: probe xattr support up front so a mount that cannot
		// store xattrs fails at startup with a clear error instead of surfacing
		// a raw ENOTSUP per-object on the first PUT/HEAD.
		if err := (meta.XattrMeta{}).Test(gwroot); err != nil {
			return fmt.Errorf("xattr check failed: %w", err)
		}
		ms = meta.XattrMeta{Rootdir: gwrootAbs}
	}
	opts := posix.PosixOpts{
		NewDirPerm:     fs.FileMode(dirPerms),
		ForceNoTmpFile: forceNoTmpFile,
		SameDirTmp:     sameDirTmp,
		// The otter deployment is the AF2 write-at-offset backend, so it selects
		// the Af2 MPU handler explicitly. This is decoupled from SameDirTmp (which
		// is only the cross-dir-rename fix) so the two concerns stay independent.
		Af2MPU:              true,
		ValidateBucketNames: disableStrictBucketNames,
		Concurrency:         actionsConcurrency,
		CopyObjectThreshold: copyObjectThreshold,
	}
	local, err := posix.New(gwroot, ms, opts)
	if err != nil {
		return fmt.Errorf("init local posix backend: %w", err)
	}

	// Forward-leg credentials come from the environment (kept off the command
	// line and out of the shared owner-map file). Refuse to start with default/
	// well-known credentials: a silent fallback would let a misconfigured node
	// forward with publicly-known keys. Only required when this node can actually
	// forward — a single-node deployment (n==1) has no peers to sign for.
	access := os.Getenv("ROOT_ACCESS_KEY")
	secret := os.Getenv("ROOT_SECRET_KEY")
	if om.N > 1 && (access == "" || secret == "") {
		return fmt.Errorf("otter: ROOT_ACCESS_KEY and ROOT_SECRET_KEY must both be set for multi-node forwarding (owner map n=%d); refusing to start with default credentials", om.N)
	}
	const region = "us-east-1"

	// One forwarding backend per non-self slot. s3proxy already re-signs each
	// request (no Host rewrite) and streams the body with UNSIGNED-PAYLOAD, so
	// it is SigV4-legal and avoids buffering the whole object.
	peers := make([]backend.Backend, om.N)
	for i, slot := range om.Slots {
		if i == selfIdxFlag {
			continue // owned locally; never forwarded to self
		}
		pxy, perr := s3proxy.New(ctx.Context,
			access, secret, slot.Endpoint, region,
			"",    // metaBucket: none (the owning gateway owns its own metadata) — validate on-cluster
			false, // anonymousCredentials
			true,  // disableChecksum: stream with unsigned payload, no seek
			true,  // disableDataIntegrityCheck
			true,  // sslSkipVerify: plaintext intra-cluster for v1 (mTLS is a follow-up)
			true,  // usePathStyle: raw IP:port endpoints
			false, // debug
			false, // gcsCompatibility
		)
		if perr != nil {
			return fmt.Errorf("init forwarder to slot %d (%s @ %s): %w", i, slot.NodeId, slot.Endpoint, perr)
		}
		peers[i] = pxy
	}

	r, err := router.New(local, peers, om, selfIdxFlag, forwardTimeout, chanParallelism)
	if err != nil {
		return err
	}

	return runGateway(ctx.Context, r)
}
