# Plan: Grant-Driven Dynamic Write Path + Verbose Logging

## Context

Today the versitygw Otter gateway has a static write path: the local POSIX
backend is rooted at `gwroot` (the positional arg on startup). The control
plane (`startControlPlane`) receives grants from JFL, caches them, and does a
bucket `mkdir` in `gwroot` — but S3 writes still go to `gwroot` regardless of
what `slot.ChannelPath` the grant says.

The desired behaviour: once JFL pushes a grant, the gateway switches its local
write root to `slot.ChannelPath` from the grant's owned slot, and rebuilds peer
forwarding from CRDB-resolved per-slot IPs. A new grant (e.g. re-expose after a
channel move) supersedes the old path automatically. No restart needed.

Related memory: `otter_cdm_job_integration.md`, `otter_channel_provisioning.md`.

---

## Approach

On grant upsert the hook fires a **background reconfiguration goroutine** that:

1. Finds the owned slot by matching `slot.NodeId == selfNodeId` (`--self-node-id` flag).
2. Queries `sd.node` in CRDB (`node_id → data_ip_address`) for every slot.
3. Builds a fresh immutable `routerCfg` snapshot (new posix.Backend at `slot.ChannelPath`
   + rebuilt s3proxy peers).
4. Atomically stores the new config into the router via `atomic.Pointer[routerCfg]`.
5. `mkdir`s `channelPath/bucket`.

The hook itself returns `nil` immediately so the grant stays cached at the new epoch
regardless of whether the background reconfiguration succeeds. Transient failures
(CRDB down, channel path not yet mounted) retry up to 3× with backoff. If all retries
fail, the gateway stays on the previous write path and a `warn` line is logged — the
same grant epoch can be re-pushed by JFL to re-trigger the reconfiguration.

**posix.Backend prerequisite:** `posix.New` currently calls `os.Chdir(rootdir)` which
is process-wide. Before two posix backends can safely coexist in one process, every
relative-path call in `posix.go` must be replaced with `filepath.Join(p.rootdir, ...)`.
This is Task 0 (done first).

The static owner map becomes optional: if absent the gateway bootstraps with N=1, no
peers, writing to `gwroot`. The owner map is still useful as a pre-grant peer bootstrap
for multi-node deployments.

Scala side: `OtterGrantBuilder.resolveSlots` leaves `endpoint` empty; the gateway
resolves IPs from CRDB.

---

## Implementation Overview

### A. Self-identity — `--self-node-id` and `--self-idx` flags

**What:** New optional `--self-node-id` flag in `otter.go`. `--self-idx` changes from
`Required: true` to `Required: false, Value: 0`.

```go
var selfNodeId string

&cli.StringFlag{
    Name:        "self-node-id",
    Usage:       "CDM node ID of this gateway (matched against slot.NodeId in grants to find owned slot)",
    EnvVars:     []string{"OTTER_SELF_NODE_ID"},
    Destination: &selfNodeId,
    Required:    false,
},
// --self-idx: Required: false, Value: 0  (was Required: true)
```

Slot resolution precedence: if `selfNodeId` is set, match by nodeId; otherwise fall
back to `selfIdxFlag`.

**Why:** `--self-idx` must be non-required so the gateway can start without an owner
map. Using nodeId is more stable than an index (survives owner-map reordering).

---

### B. posix.Backend: absolute-path root (prerequisite, Task 0)

**What:** In `backend/posix/posix.go`, replace every bare relative-path call with
`filepath.Join(p.rootdir, ...)`. Remove the `os.Chdir(rootdir)` call from `New`.
All ops use `p.rootdir` explicitly: `os.Stat(filepath.Join(p.rootdir, bucket))`,
`os.Mkdir(filepath.Join(p.rootdir, bucket), ...)`, `os.Open(filepath.Join(p.rootdir, bucket, key))`, etc.

**Why:** `os.Chdir` is a process-wide syscall. If two `posix.Backend` instances
exist simultaneously (old and new during a grant-driven switch), the second `New`
call clobbers the CWD, breaking every in-flight relative-path op on the first backend.
Making all paths absolute allows multiple backend instances to coexist safely.

**Scope:** `posix.go` only. Files that already use `filepath.Join(p.rootdir, ...)` need
no change. The test suite must remain green (`go test ./backend/posix/...`).

---

### C. Router atomic config snapshot — `backend/router/router.go`

**What:** Replace four mutable struct fields (`local`, `Backend`, `peers`, `n`) with
a single `atomic.Pointer[routerCfg]`. Add `Reconfigure` method.

```go
type routerCfg struct {
    local   backend.Backend
    peers   []backend.Backend // len N; peers[selfIdx] is nil
    selfIdx int
    n       int
}

type Router struct {
    cfg            atomic.Pointer[routerCfg]
    embeddedBackend atomic.Pointer[backend.Backend] // for the embedding delegate
    chanSem        chan struct{}
    epoch          int64
    forwardTimeout time.Duration
}

// cfg() is the fast-path accessor — one atomic load per request.
func (r *Router) cfg() *routerCfg { return r.cfgPtr.Load() }

// Reconfigure atomically installs newLocal + newPeers.
// chanSem is acquired to drain any in-flight local byte-write first (P=1 invariant).
// Reads (GetObject etc.) are never blocked — they get the old or new snapshot
// depending on when their single cfg.Load() executes; both snapshots are valid.
func (r *Router) Reconfigure(newLocal backend.Backend, newPeers []backend.Backend, newN int) {
    r.chanSem <- struct{}{}
    defer func() { <-r.chanSem }()
    old := r.cfgPtr.Load()
    newCfg := &routerCfg{
        local:   newLocal,
        peers:   newPeers,
        selfIdx: old.selfIdx,
        n:       newN,
    }
    r.cfgPtr.Store(newCfg)
}
```

`pick()`, `CreateBucket`, `DeleteBucket`, and all verb overrides call `r.cfg()` once
at the top and operate on the immutable snapshot. No lock required for reads.
`chanSem` retains its existing role: serialise the byte-write body (P=1 per channel)
independently of reconfiguration.

**Why:** Go's `atomic.Pointer` (Go 1.19+, available in this module) gives a single
load/store with the Go memory model's sequential-consistency guarantee. The snapshot
pattern avoids data races on the hot path without adding any lock contention.

---

### D. CRDB node resolver — `backend/noderesolver` package

**What:** New package `backend/noderesolver` (build tag `otter_crdb`).

```go
type Resolver struct { pool *pgxpool.Pool }

func New(ctx context.Context, dsn string) (*Resolver, error)

// DataIP returns the data_ip_address for nodeId from sd.node.
// Returns ("", false, nil) when the node is not found (not an error).
func (r *Resolver) DataIP(ctx context.Context, nodeId string) (string, bool, error)

func (r *Resolver) Close()
```

Query:
```sql
SELECT data_ip_address::text FROM sd.node
WHERE cluster_id = 'cluster' AND node_id = $1
```

Reuses `grant.CRDBDSN` and `grant.DefaultCRDBCertDir`. The same `--grants-crdb-host`
flag drives both the grant source and the node resolver (one connection pool per role,
two roles for one CRDB).

---

### E. Grant upsert hook — background reconfiguration

**What:** The upsert hook in `startControlPlane` fires a goroutine and returns nil:

```go
grant.WithUpsertHook(func(g grant.Grant) error {
    fmt.Fprintf(os.Stderr,
        "info: grant upsert access=%s bucket=%s epoch=%d n=%d — launching reconfigure\n",
        g.AccessKeyID, g.Bucket, g.Epoch, g.N())
    go func() { applyGrant(ctx.Context, g, r, nodeRes, gwroot) }()
    return nil
}),
```

`applyGrant` (3× retry, 200 ms / 1 s / 5 s backoff):
1. Find owned slot (nodeId match or selfIdxFlag fallback).
2. For each slot: `nodeRes.DataIP(ctx, slot.NodeId)` → build `http://<ip>:9000`.
3. `posix.New(slot.ChannelPath, ms, opts)` — now safe because Task 0 removed Chdir.
4. Build s3proxy peers for non-self slots.
5. `r.Reconfigure(newLocal, newPeers, g.N())`.
6. `os.MkdirAll(filepath.Join(slot.ChannelPath, g.Bucket), ...)`.
7. Log write-path switch and mkdir.

On all-retries failure: `warn: grant-driven reconfigure failed after 3 attempts — staying on previous write path; re-push the grant to retry`.

**Logging:**
```
info: grant upsert  access=otter bucket=pg-wal epoch=1785309121270 n=1 — launching reconfigure
info: grant slot[0] nodeId=vm-machine-68awg5-kaahjkk channelPath=/sd/mount/... (resolving IP)
info: grant slot[0] resolved dataIP=10.27.119.187 endpoint=http://10.27.119.187:9000
info: grant-driven write path: /home/ubuntu/otter-test-gwroot -> /sd/mount/postgres_log/.../channel_0
info: grant-driven mkdir /sd/mount/postgres_log/.../channel_0/pg-wal
```

---

### F. S3 hot-path verbose logging — `backend/router/router.go`

**What:** DEBUG log lines per verb, gated on `OTTER_VERBOSE=1`:

```
debug: PUT bucket=pg-wal key=wal/000000010000000000000009 slot=0 local=true channelPath=/sd/mount/...
debug: PUT bucket=pg-wal key=wal/000000010000000000000010 slot=1 local=false forwarding=http://10.27.125.54:9000
```

Router stores the current channelPath in an `atomic.Value` (string) updated in
`Reconfigure`; `PutObject` reads it for the log line at no lock cost.

---

### G. Owner map optional + --self-idx non-required

**What:** `--owner-map`: `Required: false`. `--self-idx`: `Required: false, Value: 0`.
When `--owner-map` is absent, bootstrap with:

```go
om = &router.OwnerMap{N: 1, Epoch: 0, Slots: []router.Slot{{Slot: 0}}}
selfIdxFlag = 0
```

No peers are built at startup; peer slice is a single `nil` entry.

---

### H. Scala — `OtterGrantBuilder.resolveSlots` + test

**What:** Set `endpoint = ""` per slot; remove the `endpoint` parameter from
`resolveSlots`. Add a test that calls `resolveSlots` directly and asserts
`slot.endpoint == ""`.

```scala
def resolveSlots(ctx: ClusterContext, channels: Seq[Af2Channel]): Seq[ChannelSlot] =
  channels.zipWithIndex.map { case (channel, idx) =>
    ChannelSlot(
      slot        = idx,
      nodeId      = channel.getOwnerNode(ctx).nodeId,
      endpoint    = "",      // gateway resolves from sd.node via CRDB
      channelPath = channel.channelPath,
    )
  }
```

Update `pushOtterChannelGrant` call-site (drop `endpoint` arg).

---

## Feature Verification

### Happy path — single node, grant drives channel switch

**Given:** Gateway starts with `--self-node-id vm-machine-68awg5-kaahjkk`,
`--grants-crdb-host localhost:26257`, `--control-addr 0.0.0.0:9200`,
`--control-tls`, no `--owner-map`, no `--self-idx`. gwroot = `/tmp/otter-scratch`.

**When:** JFL pushes grant:
```
client=fcf08055 access=otter bucket=pg-wal epoch=1785309121270 n=1
slots[0]: nodeId=vm-machine-68awg5-kaahjkk channelPath=/sd/mount/postgres_log/.../channel_0 endpoint=""
```

**Then:**
1. Gateway logs `grant-driven write path: /tmp/otter-scratch → /sd/mount/postgres_log/.../channel_0`
2. Gateway mkdirs `/sd/mount/postgres_log/.../channel_0/pg-wal`
3. S3 PUT `/pg-wal/wal/000000010000000000000009` → file at
   `/sd/mount/postgres_log/.../channel_0/pg-wal/wal/000000010000000000000009`
4. `pg_stat_archiver.archived_count` increments

### Grant update — channel moves (higher epoch)

**Given:** Gateway has channelPath A from a previous grant (epoch E).

**When:** JFL pushes a new grant with epoch E+1 pointing to channelPath B.

**Then:**
1. `accepted=true`, reconfigure goroutine fires
2. Gateway logs switch A → B
3. New writes land on B; old data on A unaffected

### CRDB unavailable at grant time (retry)

**When:** CRDB is down when the grant arrives.

**Then:**
1. Hook returns nil immediately (grant cached at new epoch)
2. Background goroutine retries 3× with backoff
3. If CRDB recovers within ~6 s: reconfiguration succeeds on retry
4. If still down: `warn: grant-driven reconfigure failed after 3 attempts — staying on previous write path; re-push the grant to retry`
5. Gateway continues serving on old write path; no crash

### nodeId not in sd.node

**When:** Grant slot has `nodeId = "unknown-node"`, absent from `sd.node`.

**Then:**
1. `DataIP` returns `("", false, nil)`
2. Gateway logs `warn: grant slot[1] nodeId=unknown-node not found in sd.node — skipping peer`
3. Owned slot still reconfigured; affected peer forwarding returns 503 to client

### Stale epoch rejected

**When:** Second push of same grant with equal epoch.

**Then:** `accepted=false, detail="stale epoch"`, no reconfiguration.

---

## Tasks

| # | Task | Summary | Verify | Status |
|---|------|---------|--------|--------|
| 0 | posix absolute paths | Replace all relative-path calls in `posix.go` with `filepath.Join(p.rootdir, ...)`, remove `os.Chdir` from `New` | `go test ./backend/posix/...` green; two `posix.New` instances at different roots can coexist without CWD interference | [x] |
| 1 | noderesolver package | New `backend/noderesolver` (otter_crdb tag): CRDB pool + `DataIP(nodeId)` query against `sd.node`, unit tests with mock pool | `go test ./backend/noderesolver/...` passes | [x] |
| 2 | Router atomic config | Replace mutable fields with `atomic.Pointer[routerCfg]`; add `Reconfigure`; update `pick()`, `CreateBucket`, `DeleteBucket`, all verb overrides to use snapshot | `go test -race ./backend/router/...` — race detector clean with concurrent Reconfigure | [x] |
| 3 | Gateway hook + flags | `--self-node-id` (optional), `--self-idx`/`--owner-map` non-required; background `applyGrant` (3× retry) with noderesolver + Reconfigure + mkdir + verbose logs | Start without `--owner-map`; push grant; observe log lines + correct mkdir | [x] |
| 4 | S3 hot-path verbose logging | DEBUG PUT/GET/forward log lines in `router.go` gated on `OTTER_VERBOSE=1`; channelPath read from atomic string updated in `Reconfigure` | `OTTER_VERBOSE=1` shows per-object lines; race detector clean | [x] |
| 5 | Scala: empty endpoint + test | `resolveSlots` sets `endpoint = ""`; add unit test asserting empty endpoint from `resolveSlots`; update `pushOtterChannelGrant` call-site | `OtterGrantBuilderTest` passes + new resolveSlots test passes | [x] |
