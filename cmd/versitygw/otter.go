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
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/noderesolver"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/backend/router"
	"github.com/versity/versitygw/backend/s3proxy"
	"github.com/versity/versitygw/otter/controlgrpc"
	"github.com/versity/versitygw/otter/grant"
)

var (
	ownerMapFile   string
	selfIdxFlag    int
	forwardTimeout time.Duration

	// af2Warm* flags enable WarmCache at startup: pre-populate the Af2Desc
	// metadata cache from af2GetPartitionMetadata so GET/HEAD returns correct
	// metadata even on a cold cache after a restart. Set node-ip, id, and
	// partition-id together to enable it.
	af2WarmNodeIP      string
	af2WarmUniqueID    string
	af2WarmPartitionID int
	af2WarmCertFile    string
	af2WarmKeyFile     string

	// selfNodeId identifies this node's owned slot by CDM node id rather than by
	// position. The owner map's slot ordering is a bootstrap artifact; a grant
	// carries its own slot list, so matching by node id keeps this node pointed
	// at the right channel even when the grant reorders slots.
	selfNodeId string

	// control* configure the JFL grant-push endpoint. Empty --control-addr
	// leaves the control plane off entirely and the gateway owner-map driven.
	controlAddr     string
	controlTLS      bool
	controlCertFile string
	controlKeyFile  string
	controlCAFile   string

	// grants* configure CRDB access. Node resolution (sd.node) and durable grant
	// storage are deliberately separate flags: they are independent tables, and
	// gating both on one flag meant a missing grant table stopped peer-IP
	// resolution from ever running.
	grantsCRDBHost     string
	grantsCRDBDB       string
	grantsCRDBDurable  bool
	grantLookupTimeout time.Duration
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
				Usage:       "this node's slot index in the owner map (0..n-1); fallback when --self-node-id is not set",
				EnvVars:     []string{"OTTER_SELF_IDX"},
				Destination: &selfIdxFlag,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "self-node-id",
				Usage:       "this node's CDM node id; when set, the owned slot is the grant slot whose nodeId matches, rather than --self-idx",
				EnvVars:     []string{"OTTER_SELF_NODE_ID"},
				Destination: &selfNodeId,
			},
			&cli.StringFlag{
				Name:        "control-addr",
				Usage:       "listen address for the OtterControlService gRPC grant-push endpoint (empty disables the control plane)",
				EnvVars:     []string{"OTTER_CONTROL_ADDR"},
				Destination: &controlAddr,
			},
			&cli.BoolFlag{
				Name:        "control-tls",
				Usage:       "terminate mutual TLS on the control endpoint using --control-cert/--control-key/--control-ca",
				EnvVars:     []string{"OTTER_CONTROL_TLS"},
				Destination: &controlTLS,
			},
			&cli.StringFlag{
				Name:        "control-cert",
				Usage:       "server certificate for the control endpoint (requires --control-tls)",
				EnvVars:     []string{"OTTER_CONTROL_CERT"},
				Destination: &controlCertFile,
			},
			&cli.StringFlag{
				Name:        "control-key",
				Usage:       "server private key for the control endpoint (requires --control-tls)",
				EnvVars:     []string{"OTTER_CONTROL_KEY"},
				Destination: &controlKeyFile,
			},
			&cli.StringFlag{
				Name:        "control-ca",
				Usage:       "CA bundle used to verify JFL client certificates (requires --control-tls)",
				EnvVars:     []string{"OTTER_CONTROL_CA"},
				Destination: &controlCAFile,
			},
			&cli.StringFlag{
				Name:        "grants-crdb-host",
				Usage:       "CRDB host for sd.node peer-IP resolution; also the host used for durable grants when --grants-crdb-durable is set",
				EnvVars:     []string{"OTTER_GRANTS_CRDB_HOST"},
				Destination: &grantsCRDBHost,
			},
			&cli.StringFlag{
				Name:        "grants-crdb-db",
				Usage:       "CRDB database holding the durable grant table",
				EnvVars:     []string{"OTTER_GRANTS_CRDB_DB"},
				Value:       "defaultdb",
				Destination: &grantsCRDBDB,
			},
			&cli.BoolFlag{
				Name:        "grants-crdb-durable",
				Usage:       "read grants from CRDB on cache miss and warm the cache at startup (requires --grants-crdb-host)",
				EnvVars:     []string{"OTTER_GRANTS_CRDB_DURABLE"},
				Destination: &grantsCRDBDurable,
			},
			&cli.DurationFlag{
				Name:        "grant-lookup-timeout",
				Usage:       "deadline for a single CRDB grant lookup on the resolve path",
				EnvVars:     []string{"OTTER_GRANT_LOOKUP_TIMEOUT"},
				Value:       2 * time.Second,
				Destination: &grantLookupTimeout,
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

	r, err := router.New(local, peers, om, selfIdxFlag, forwardTimeout)
	if err != nil {
		return err
	}

	if err := startControlPlane(ctx, gwroot, r, ms, opts); err != nil {
		return err
	}

	return runGateway(ctx.Context, r)
}

// startControlPlane brings up the JFL grant-push endpoint when --control-addr is
// set, and is a no-op otherwise. On grant upsert it fires a background goroutine
// that resolves slot IPs from sd.node, builds a new posix backend rooted at the
// granted ChannelPath, and calls Router.Reconfigure to switch the write path.
//
// Without this, the gateway is owner-map driven: grant, Resolver and Reconfigure
// all exist but nothing delivers a grant, so the upsert hook never fires.
func startControlPlane(ctx *cli.Context, gwroot string, r *router.Router, ms meta.MetadataStorer, opts posix.PosixOpts) error {
	if controlAddr == "" {
		return nil
	}

	var nodeRes *noderesolver.Resolver
	var src grant.Source
	if grantsCRDBHost != "" {
		nr, nrerr := noderesolver.New(ctx.Context, grantsCRDBHost, "sd")
		if nrerr != nil {
			return fmt.Errorf("otter control: CRDB node resolver: %w", nrerr)
		}
		nodeRes = nr

		if grantsCRDBDurable {
			s, serr := grant.NewCRDBSource(ctx.Context, grant.CRDBDSN(grantsCRDBHost, grantsCRDBDB))
			if serr != nil {
				return fmt.Errorf("otter control: CRDB grant source: %w", serr)
			}
			src = s
		}
	} else if grantsCRDBDurable {
		return fmt.Errorf("otter control: --grants-crdb-durable requires --grants-crdb-host")
	}

	resolver := grant.NewResolver(
		grant.WithSource(src),
		grant.WithLookupTimeout(grantLookupTimeout),
		grant.WithUpsertHook(func(g grant.Grant) error {
			fmt.Fprintf(os.Stderr,
				"info: grant upsert access=%s bucket=%s epoch=%d n=%d - launching reconfigure\n",
				g.AccessKeyID, g.Bucket, g.Epoch, g.N())
			// Deliberately detached from the RPC: reconfiguration retries with
			// backoff and can outlive the caller's deadline. The grant is already
			// cached, so a failed apply is retried by re-pushing the same epoch.
			go applyGrant(context.Background(), g, r, nodeRes, gwroot, ms, opts)
			return nil
		}),
	)

	if src != nil {
		n, werr := resolver.Warm(ctx.Context)
		if werr != nil {
			return fmt.Errorf("otter control: warm grants from CRDB: %w", werr)
		}
		fmt.Fprintf(os.Stderr, "info: warmed %d grant(s) from CRDB\n", n)
	}

	var tlsCfg *tls.Config
	if controlTLS {
		cfg, terr := controlgrpc.ServerTLSConfig(controlCertFile, controlKeyFile, controlCAFile)
		if terr != nil {
			return fmt.Errorf("otter control: TLS config: %w", terr)
		}
		tlsCfg = cfg
	}

	srv, serr := controlgrpc.NewServer(controlgrpc.NewHandler(resolver), controlAddr, tlsCfg)
	if serr != nil {
		return serr
	}
	go func() {
		fmt.Fprintf(os.Stderr, "info: OtterControlService(gRPC) listening on %s (tls=%v)\n", controlAddr, controlTLS)
		if e := srv.Serve(); e != nil {
			fmt.Fprintf(os.Stderr, "error: OtterControlService(gRPC) server exited: %v\n", e)
		}
	}()
	return nil
}

// applyGrant retries doApplyGrant with backoff: the usual failures are transient
// (CRDB not up, granted channel path not yet mounted). On exhaustion the gateway
// stays on its previous write path rather than losing one, and re-pushing the
// same grant epoch retries.
func applyGrant(ctx context.Context, g grant.Grant, r *router.Router, nodeRes *noderesolver.Resolver, gwroot string, ms meta.MetadataStorer, opts posix.PosixOpts) {
	backoff := []time.Duration{200 * time.Millisecond, time.Second, 5 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= 2; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff[attempt-1])
		}
		if err := doApplyGrant(ctx, g, r, nodeRes, gwroot, ms, opts); err != nil {
			lastErr = err
			fmt.Fprintf(os.Stderr, "warn: grant-driven reconfigure attempt %d/3 failed: %v\n", attempt+1, err)
			continue
		}
		return
	}
	fmt.Fprintf(os.Stderr,
		"warn: grant-driven reconfigure failed after 3 attempts - staying on previous write path; re-push the grant to retry. Last error: %v\n",
		lastErr)
}

func doApplyGrant(ctx context.Context, g grant.Grant, r *router.Router, nodeRes *noderesolver.Resolver, gwroot string, ms meta.MetadataStorer, opts posix.PosixOpts) error {
	fmt.Fprintf(os.Stderr, "info: applying grant clientId=%s access=%s bucket=%s epoch=%d n=%d policy=%v\n",
		g.ClientID, g.AccessKeyID, g.Bucket, g.Epoch, g.N(), g.Policy)
	for i, s := range g.Slots {
		fmt.Fprintf(os.Stderr, "info:   slot[%d] nodeId=%s channelPath=%s endpoint=%q\n",
			i, s.NodeID, s.ChannelPath, s.Endpoint)
	}

	// Locate this node's slot. ownedIdx == -1 means the grant gives this node no
	// channel of its own, so it installs a forwarding table and writes nothing
	// locally. Router.Reconfigure treats -1 as "matches no place() result".
	ownedIdx := -1
	if selfNodeId != "" {
		for i, s := range g.Slots {
			if s.NodeID == selfNodeId {
				ownedIdx = i
				break
			}
		}
		if ownedIdx < 0 {
			fmt.Fprintf(os.Stderr, "info: grant has no owned slot for selfNodeId=%s - installing forwarding table only\n", selfNodeId)
		}
	} else if selfIdxFlag < len(g.Slots) {
		ownedIdx = selfIdxFlag
	} else {
		return fmt.Errorf("grant has %d slot(s) but --self-idx is %d and --self-node-id is unset; cannot identify this node's slot",
			len(g.Slots), selfIdxFlag)
	}

	// Same rule as startup: never fall back to well-known credentials. A grant
	// with more than one slot means this node may forward, and a silent default
	// would sign peer requests with publicly-known keys.
	access := os.Getenv("ROOT_ACCESS_KEY")
	secret := os.Getenv("ROOT_SECRET_KEY")
	if len(g.Slots) > 1 && (access == "" || secret == "") {
		return fmt.Errorf("ROOT_ACCESS_KEY and ROOT_SECRET_KEY must both be set to forward for a multi-slot grant (n=%d); refusing to build peers with default credentials", len(g.Slots))
	}
	const region = "us-east-1"

	peers := make([]backend.Backend, len(g.Slots))
	for i, s := range g.Slots {
		if i == ownedIdx {
			continue // owned locally; never forwarded to self
		}
		endpoint := s.Endpoint
		if endpoint == "" && nodeRes != nil {
			ip, found, err := nodeRes.DataIP(ctx, s.NodeID)
			if err != nil {
				return fmt.Errorf("resolve nodeId %s: %w", s.NodeID, err)
			}
			if !found {
				fmt.Fprintf(os.Stderr, "warn: grant slot[%d] nodeId=%s not found in sd.node - skipping peer\n", i, s.NodeID)
				continue
			}
			endpoint = fmt.Sprintf("http://%s:9000", ip)
			fmt.Fprintf(os.Stderr, "info: grant slot[%d] resolved dataIP=%s endpoint=%s\n", i, ip, endpoint)
		}
		if endpoint == "" {
			continue
		}
		pxy, perr := s3proxy.New(ctx, access, secret, endpoint, region,
			"",    // metaBucket
			false, // anonymousCredentials
			true,  // disableChecksum
			true,  // disableDataIntegrityCheck
			true,  // sslSkipVerify
			true,  // usePathStyle
			false, // debug
			false, // gcsCompatibility
		)
		if perr != nil {
			return fmt.Errorf("init peer for slot %d: %w", i, perr)
		}
		peers[i] = pxy
	}

	// Forwarder-only: keep the existing local backend, install the peer table.
	if ownedIdx < 0 {
		if err := r.Reconfigure(ctx, r.Local(), peers, ownedIdx, len(g.Slots)); err != nil {
			return fmt.Errorf("reconfigure (forwarder only): %w", err)
		}
		fmt.Fprintf(os.Stderr, "info: grant-driven forwarding table installed (no local write path)\n")
		return nil
	}

	ownedSlot := g.Slots[ownedIdx]
	writeRoot := gwroot
	if ownedSlot.ChannelPath != "" {
		writeRoot = ownedSlot.ChannelPath
	}
	fmt.Fprintf(os.Stderr, "info: grant-driven write path -> %s (slot[%d] nodeId=%s)\n",
		writeRoot, ownedIdx, ownedSlot.NodeID)

	newLocal, err := posix.New(writeRoot, ms, opts)
	if err != nil {
		return fmt.Errorf("init local backend at %s: %w", writeRoot, err)
	}

	if err := r.Reconfigure(ctx, newLocal, peers, ownedIdx, len(g.Slots)); err != nil {
		// Reconfigure did not take, so this backend was never installed and
		// nothing else can reach it; release it rather than leaking its root fd.
		newLocal.Shutdown()
		return fmt.Errorf("reconfigure to %s: %w", writeRoot, err)
	}

	bucketDir := filepath.Join(writeRoot, g.Bucket)
	if mkErr := os.MkdirAll(bucketDir, fs.FileMode(dirPerms)); mkErr != nil {
		fmt.Fprintf(os.Stderr, "warn: grant-driven mkdir %q: %v\n", bucketDir, mkErr)
	} else {
		fmt.Fprintf(os.Stderr, "info: grant-driven mkdir %s\n", bucketDir)
	}
	return nil
}
