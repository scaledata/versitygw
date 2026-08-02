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
	"time"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/backend/router"
	"github.com/versity/versitygw/backend/s3proxy"
)

var (
	ownerMapFile   string
	selfIdxFlag    int
	forwardTimeout time.Duration
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
		ms = meta.NewAf2Desc(af2DescMaxBytes)
	} else {
		// Match runPosix: probe xattr support up front so a mount that cannot
		// store xattrs fails at startup with a clear error instead of surfacing
		// a raw ENOTSUP per-object on the first PUT/HEAD.
		if err := (meta.XattrMeta{}).Test(gwroot); err != nil {
			return fmt.Errorf("xattr check failed: %w", err)
		}
		ms = meta.XattrMeta{}
	}
	opts := posix.PosixOpts{
		NewDirPerm:          fs.FileMode(dirPerms),
		ForceNoTmpFile:      forceNoTmpFile,
		SameDirTmp:          sameDirTmp,
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
	// line and out of the shared owner-map file).
	access := os.Getenv("ROOT_ACCESS_KEY")
	secret := os.Getenv("ROOT_SECRET_KEY")
	if access == "" {
		access = "otter"
	}
	if secret == "" {
		secret = "ottersecret"
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

	r, err := router.New(local, peers, om, selfIdxFlag, forwardTimeout)
	if err != nil {
		return err
	}

	return runGateway(ctx.Context, r)
}
