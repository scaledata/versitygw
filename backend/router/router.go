package router

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// routerCfg is an immutable snapshot of the routing state. Reconfigure swaps it
// atomically so readers never observe a torn update: pick() and the delegate
// methods each Load() the pointer once and operate on that snapshot.
type routerCfg struct {
	local   backend.Backend   // node-local backend
	peers   []backend.Backend // len N; peers[selfIdx] is nil (owned locally)
	selfIdx int
	n       int
}

// Router is the Otter multi-channel distribution backend. Verbs it does not
// override are delegated to the current local backend (see router_delegate.go);
// per-object verbs are routed to the slot/owner computed from the object key:
// writes locally if this node owns the object, forwards to the owning node's
// gateway otherwise. The routing state lives in an atomically-swapped cfg
// snapshot so Reconfigure can install a new local backend / peer set without a
// lock on the read path.
//
// Invariants (see the design doc):
//   - writer==owner: a forward always targets the AF2 mount-node; the local AF2
//     write is the EACCES backstop if the map is stale.
//   - P=1 per channel: chanSem (weight 1) serializes byte-writes into the local
//     channel. Every write to a channel becomes a *local* write on its owner
//     (direct or forwarded), so gating the local path serializes the channel
//     globally regardless of how many entry nodes forward to it.
//   - deterministic placement: place() is a stable hash; reads recompute it, so
//     there is no per-object location map.
//   - bounded forwards: a forwarded byte-write is given a deadline so a
//     partitioned peer fails fast instead of hanging at the OS connect timeout.
type Router struct {
	cfg            atomic.Pointer[routerCfg]
	epoch          int64
	chanSem        chan struct{} // weight-1: at most one byte-write into the local channel at a time
	forwardTimeout time.Duration // deadline for a forwarded byte-write (0 = no timeout)
}

// validateCfg checks a routing configuration. selfIdx may be -1 (this node owns
// no slot — a pure forwarder); otherwise it must index a slot in [0,n). Shared
// by New and Reconfigure so a live reconfiguration is validated as strictly as
// bootstrap, failing fast instead of installing a snapshot that panics pick().
func validateCfg(local backend.Backend, peers []backend.Backend, selfIdx, n int) error {
	if local == nil {
		return fmt.Errorf("router: nil local backend")
	}
	if n <= 0 {
		return fmt.Errorf("router: n must be > 0, got %d", n)
	}
	if len(peers) != n {
		return fmt.Errorf("router: have %d peer backends but n=%d", len(peers), n)
	}
	if selfIdx != -1 && (selfIdx < 0 || selfIdx >= n) {
		return fmt.Errorf("router: selfIdx %d out of range [0,%d) (or -1 for a pure forwarder)", selfIdx, n)
	}
	for i := range peers {
		if i != selfIdx && peers[i] == nil {
			return fmt.Errorf("router: peers[%d] is nil but it is not the local slot", i)
		}
	}
	return nil
}

// New builds a Router. selfIdx is this node's slot (supplied per-node as a flag).
// peers must have length m.N; peers[selfIdx] is ignored (this node's objects are
// written through local), and every other entry must be a forwarding backend to
// that slot's gateway. forwardTimeout bounds a forwarded byte-write (0 disables).
func New(local backend.Backend, peers []backend.Backend, m *OwnerMap, selfIdx int, forwardTimeout time.Duration) (*Router, error) {
	if err := validateCfg(local, peers, selfIdx, m.N); err != nil {
		return nil, err
	}
	r := &Router{
		epoch:          m.Epoch,
		chanSem:        make(chan struct{}, 1),
		forwardTimeout: forwardTimeout,
	}
	r.cfg.Store(&routerCfg{
		local:   local,
		peers:   peers,
		selfIdx: selfIdx,
		n:       m.N,
	})
	return r, nil
}

// reconfigureDrainTimeout bounds how long Reconfigure waits to drain an
// in-flight local write before giving up, so a wedged backend cannot hang a
// handoff indefinitely.
const reconfigureDrainTimeout = 30 * time.Second

// Local returns the current node-local backend. A grant that carries no owned
// slot for this node reconfigures the peer table only, and needs the existing
// local backend to hand back to Reconfigure unchanged.
func (r *Router) Local() backend.Backend { return r.cfg.Load().local }

// Reconfigure atomically installs a new local backend and peer slice.
// chanSem is acquired to drain any in-flight local byte-write first (P=1 invariant).
// Reads (GetObject etc.) are lock-free — they load a snapshot once per request
// and operate on it; both old and new snapshots are valid concurrently.
// newSelfIdx is this node's owned slot in the new configuration, or -1 if this
// node owns no slot (a pure forwarder). It must be set from the grant, not
// preserved from the bootstrap config: in deploy mode the owned slot is selected
// by --self-node-id (matching a slot's NodeID), so the bootstrap selfIdx (0) is
// unrelated to the real slot. A stale selfIdx makes pick() route objects hashing
// to that slot at the local backend — which for a forwarder has no bucket dir
// (404 NoSuchBucket) and for a mis-indexed owner dereferences a nil peer.
// selfIdx == -1 never equals any place() result, so every object forwards.
func (r *Router) Reconfigure(ctx context.Context, newLocal backend.Backend, newPeers []backend.Backend, newSelfIdx, newN int) error {
	if err := validateCfg(newLocal, newPeers, newSelfIdx, newN); err != nil {
		return err
	}

	// Drain in-flight local byte-writes (P=1) before swapping, but bound the
	// wait: a wedged local backend is the likely reason for a regrant, and an
	// unbounded acquire here would hang the very handoff meant to move traffic
	// off the unhealthy node. NOTE: acquiring chanSem does not by itself close
	// the pick()-before-enterWrite() window that can still land a stale write on
	// the old backend — that is tracked separately; this only bounds the wait.
	dctx, cancel := context.WithTimeout(ctx, reconfigureDrainTimeout)
	defer cancel()
	select {
	case r.chanSem <- struct{}{}:
		defer func() { <-r.chanSem }()
	case <-dctx.Done():
		return fmt.Errorf("router: reconfigure timed out draining in-flight local write: %w", dctx.Err())
	}

	old := r.cfg.Load()
	r.cfg.Store(&routerCfg{
		local:   newLocal,
		peers:   newPeers,
		selfIdx: newSelfIdx,
		n:       newN,
	})

	// Release the replaced backend's resources (e.g. posix's open root fd) so
	// repeated ownership handoffs don't leak descriptors toward EMFILE. Skip
	// when the new config reuses the same instance. (Draining in-flight lock-free
	// reads on the old backend before Shutdown is part of the pick-window
	// redesign; Reconfigure has no live caller yet.)
	if old != nil && old.local != nil && old.local != newLocal {
		old.local.Shutdown()
	}
	return nil
}

func (r *Router) String() string {
	c := r.cfg.Load()
	return fmt.Sprintf("Otter router (n=%d, self=%d, epoch=%d)", c.n, c.selfIdx, r.epoch)
}

// pick returns the backend that owns (bucket,key) and whether it is local.
func (r *Router) pick(bucket, key string) (backend.Backend, bool) {
	c := r.cfg.Load()
	idx := place(bucket, key, c.n)
	if idx == c.selfIdx {
		return c.local, true
	}
	return c.peers[idx], false
}

// ---- placement -------------------------------------------------------------

var (
	walName    = regexp.MustCompile(`^[0-9A-Fa-f]{24}$`)
	relSegment = regexp.MustCompile(`^(\d+)(?:\.(\d+))?$`)
)

// place maps an object to a slot in [0,n). Three cases:
//
//  1. WAL segment names (24 hex chars, e.g. 000000010000000000000001):
//     route by their low-64-bit ordinal mod n so consecutive segments
//     round-robin across slots.
//
//  2. Postgres relation segment files (numeric OID, optional .N suffix, e.g.
//     base/16384/1259.42): route by (hash(dir+oid) + segNum) mod n so every
//     segment of the same relation distributes evenly — a 127 GB table (127
//     × 1 GB segments) spreads exactly 31-32 segments per channel regardless
//     of n, and is recomputable on read with no location map.
//
//  3. Everything else: fnv-1a(bucket+"/"+key) mod n.
//
// All three are deterministic and stable — the read path recomputes the same
// slot. N and the ordering must stay frozen for the data's retention life.
func place(bucket, key string, n int) int {
	if n <= 1 {
		return 0
	}

	// base = last path component; dir = everything before it
	base := key
	dir := bucket
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			base = key[i+1:]
			dir = bucket + "/" + key[:i]
			break
		}
	}

	// Case 1: WAL segment (24 hex chars)
	if walName.MatchString(base) {
		if ord, err := strconv.ParseUint(base[8:], 16, 64); err == nil {
			return int(ord % uint64(n))
		}
	}

	// Case 2: Postgres relation file — OID with optional .segNum suffix.
	// Examples: "1259", "1259.1", "1259.42"
	// Hash the (dir+OID) pair to pick a base slot, then add the segment
	// number so every segment of the same relation lands on a distinct slot.
	if m := relSegment.FindStringSubmatch(base); m != nil {
		segNum := 0
		if m[2] != "" {
			segNum, _ = strconv.Atoi(m[2])
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(dir + "/" + m[1]))
		oidSlot := int(h.Sum64() % uint64(n))
		return (oidSlot + segNum) % n
	}

	// Case 3: everything else (fsm, vm, init forks, config files, etc.)
	h := fnv.New64a()
	_, _ = h.Write([]byte(bucket + "/" + key))
	return int(h.Sum64() % uint64(n))
}

// ---- byte-writes: routed; serialized by chanSem when local, time-bounded when forwarded ----

// enterWrite prepares ctx for a routed byte-write and returns a cleanup func to
// defer. A local write takes the per-channel semaphore (P=1). A forwarded write
// is bounded by forwardTimeout so a partitioned peer fails fast instead of
// hanging at the OS TCP connect timeout. This is safe for writes because the
// whole operation (including body transfer) completes within the backend call;
// it is deliberately NOT applied to reads, whose response body streams after the
// call returns (cancelling there would truncate the body).
func (r *Router) enterWrite(ctx context.Context, local bool) (context.Context, func(), error) {
	if local {
		// Acquire the per-channel semaphore, but honor context cancellation: a
		// client that has already disconnected, or whose request deadline has
		// fired, fails fast instead of blocking behind an in-flight — or wedged —
		// local write and piling goroutines onto a stuck channel. The forwarded
		// path is bounded by forwardTimeout below; this is its local equivalent.
		select {
		case r.chanSem <- struct{}{}:
			return ctx, func() { <-r.chanSem }, nil
		case <-ctx.Done():
			return ctx, func() {}, ctx.Err()
		}
	}
	if r.forwardTimeout > 0 {
		c, cancel := context.WithTimeout(ctx, r.forwardTimeout)
		return c, cancel, nil
	}
	return ctx, func() {}, nil
}

func (r *Router) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done, err := r.enterWrite(ctx, local)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	defer done()
	return be.PutObject(ctx, in)
}

func (r *Router) UploadPart(ctx context.Context, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done, err := r.enterWrite(ctx, local)
	if err != nil {
		return nil, err
	}
	defer done()
	return be.UploadPart(ctx, in)
}

func (r *Router) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done, err := r.enterWrite(ctx, local)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	defer done()
	return be.CompleteMultipartUpload(ctx, in)
}

// UploadPartCopy / CopyObject route by the DEST key; a cross-node *source* is a
// v1 limitation (the local backend reads the source locally); WAL never uses them.
func (r *Router) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done, err := r.enterWrite(ctx, local)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	defer done()
	return be.UploadPartCopy(ctx, in)
}

func (r *Router) CopyObject(ctx context.Context, in s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done, err := r.enterWrite(ctx, local)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	defer done()
	return be.CopyObject(ctx, in)
}

// ---- reads & metadata: routed, no chanSem -----------------------------------

func (r *Router) GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.GetObject(ctx, in)
}

func (r *Router) HeadObject(ctx context.Context, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.HeadObject(ctx, in)
}

func (r *Router) GetObjectAttributes(ctx context.Context, in *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.GetObjectAttributes(ctx, in)
}

func (r *Router) GetObjectAcl(ctx context.Context, in *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.GetObjectAcl(ctx, in)
}

func (r *Router) PutObjectAcl(ctx context.Context, in *s3.PutObjectAclInput) error {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.PutObjectAcl(ctx, in)
}

func (r *Router) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.DeleteObject(ctx, in)
}

func (r *Router) RestoreObject(ctx context.Context, in *s3.RestoreObjectInput) error {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.RestoreObject(ctx, in)
}

func (r *Router) CreateMultipartUpload(ctx context.Context, in s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.CreateMultipartUpload(ctx, in)
}

func (r *Router) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput) error {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.AbortMultipartUpload(ctx, in)
}

func (r *Router) ListParts(ctx context.Context, in *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	be, _ := r.pick(*in.Bucket, *in.Key)
	return be.ListParts(ctx, in)
}

func (r *Router) GetObjectTagging(ctx context.Context, bucket, object, versionId string) (map[string]string, error) {
	be, _ := r.pick(bucket, object)
	return be.GetObjectTagging(ctx, bucket, object, versionId)
}

func (r *Router) PutObjectTagging(ctx context.Context, bucket, object, versionId string, tags map[string]string) error {
	be, _ := r.pick(bucket, object)
	return be.PutObjectTagging(ctx, bucket, object, versionId, tags)
}

func (r *Router) DeleteObjectTagging(ctx context.Context, bucket, object, versionId string) error {
	be, _ := r.pick(bucket, object)
	return be.DeleteObjectTagging(ctx, bucket, object, versionId)
}

func (r *Router) PutObjectRetention(ctx context.Context, bucket, object, versionId string, retention []byte) error {
	be, _ := r.pick(bucket, object)
	return be.PutObjectRetention(ctx, bucket, object, versionId, retention)
}

func (r *Router) GetObjectRetention(ctx context.Context, bucket, object, versionId string) ([]byte, error) {
	be, _ := r.pick(bucket, object)
	return be.GetObjectRetention(ctx, bucket, object, versionId)
}

func (r *Router) PutObjectLegalHold(ctx context.Context, bucket, object, versionId string, status bool) error {
	be, _ := r.pick(bucket, object)
	return be.PutObjectLegalHold(ctx, bucket, object, versionId, status)
}

func (r *Router) GetObjectLegalHold(ctx context.Context, bucket, object, versionId string) (*bool, error) {
	be, _ := r.pick(bucket, object)
	return be.GetObjectLegalHold(ctx, bucket, object, versionId)
}

// ---- bucket ops: fanned out so one create/delete is cluster-wide ------------
//
// A client should not have to create the bucket on every node. CreateBucket
// creates it locally and, ONLY if the local create newly succeeded, propagates
// to every peer. The "fan out only on a fresh create" rule is what stops a
// broadcast storm: a fan-out request arriving at a peer that already has the
// bucket returns success without re-fanning. DeleteBucket mirrors this (fan out
// only when the local delete actually removed it). Real errors (e.g.
// BucketNotEmpty) still surface; idempotent ones (already-exists / no-such-bucket)
// are swallowed.

// forwardCtx bounds a fanned-out bucket op with forwardTimeout, mirroring the
// byte-write forward path, so a partitioned or slow peer fails fast instead of
// hanging the whole cluster-wide CreateBucket/DeleteBucket on the caller's
// (possibly unbounded) context. Returns a no-op cancel when no timeout is set.
func (r *Router) forwardCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.forwardTimeout > 0 {
		return context.WithTimeout(ctx, r.forwardTimeout)
	}
	return ctx, func() {}
}

func (r *Router) CreateBucket(ctx context.Context, in *s3.CreateBucketInput, defaultACL []byte) error {
	c := r.cfg.Load()
	err := c.local.CreateBucket(ctx, in, defaultACL)
	if isBucketExists(err) {
		return nil // already present here (likely a fan-out echo) -> do not re-fan
	}
	if err != nil {
		return err
	}
	for i, p := range c.peers {
		if i == c.selfIdx || p == nil {
			continue
		}
		pctx, cancel := r.forwardCtx(ctx)
		e := p.CreateBucket(pctx, in, defaultACL)
		cancel()
		if e != nil && !isBucketExists(e) {
			return fmt.Errorf("router: create bucket on slot %d: %w", i, e)
		}
	}
	return nil
}

func (r *Router) DeleteBucket(ctx context.Context, bucket string) error {
	c := r.cfg.Load()
	err := c.local.DeleteBucket(ctx, bucket)
	if isNoSuchBucket(err) {
		return nil // already gone here (echo) -> do not re-fan
	}
	if err != nil {
		return err // e.g. BucketNotEmpty -> surface
	}
	for i, p := range c.peers {
		if i == c.selfIdx || p == nil {
			continue
		}
		pctx, cancel := r.forwardCtx(ctx)
		e := p.DeleteBucket(pctx, bucket)
		cancel()
		if e != nil && !isNoSuchBucket(e) {
			return fmt.Errorf("router: delete bucket on slot %d: %w", i, e)
		}
	}
	return nil
}

func isBucketExists(err error) bool {
	if err == nil {
		return false
	}
	var apiErr s3err.APIError
	if errors.As(err, &apiErr) &&
		(apiErr.Code == s3err.GetAPIError(s3err.ErrBucketAlreadyExists).Code ||
			apiErr.Code == s3err.GetAPIError(s3err.ErrBucketAlreadyOwnedByYou).Code) {
		return true
	}
	s := err.Error() // forwarded peer errors arrive as SDK errors carrying the S3 code
	return strings.Contains(s, "BucketAlreadyOwnedByYou") || strings.Contains(s, "BucketAlreadyExists")
}

func isNoSuchBucket(err error) bool {
	if err == nil {
		return false
	}
	var apiErr s3err.APIError
	if errors.As(err, &apiErr) && apiErr.Code == s3err.GetAPIError(s3err.ErrNoSuchBucket).Code {
		return true
	}
	// A forwarded peer error arrives as an SDK error string carrying the S3
	// code. Match "NoSuchBucket" at a code boundary so the distinct code
	// "NoSuchBucketPolicy" (and any future NoSuchBucket* code) is not
	// misclassified as an idempotent already-gone and silently swallowed.
	return hasS3Code(err.Error(), "NoSuchBucket")
}

// hasS3Code reports whether msg contains code as a whole S3 error code, i.e. not
// immediately followed by another code character. This distinguishes "NoSuchBucket"
// from the longer, unrelated "NoSuchBucketPolicy" when matching forwarded SDK
// error strings.
func hasS3Code(msg, code string) bool {
	from := 0
	for {
		i := strings.Index(msg[from:], code)
		if i < 0 {
			return false
		}
		end := from + i + len(code)
		if end >= len(msg) || !isCodeChar(msg[end]) {
			return true
		}
		from = end
	}
}

func isCodeChar(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

// NOTE (v1 limitations, delegated to the local backend via router_delegate.go):
//   - DeleteObjects (batch) can span owners; not yet fanned out per-owner.
//   - ListObjects / ListObjectVersions / ListMultipartUploads are local-only;
//     a cross-node fan-out+merge is deferred (WAL restore is GET-by-name).
//   - Forwarded reads (GetObject/HeadObject) are not yet time-bounded (the
//     response body streams after the call; needs a connect-level timeout).
