package router

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// Router is the Otter multi-channel distribution backend. It embeds the
// node-local backend (so every verb it does not override delegates locally) and
// routes per-object verbs to the slot/owner computed from the object key:
// writes locally if this node owns the object, forwards to the owning node's
// gateway otherwise.
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
	backend.Backend // node-local backend; delegate for any verb not overridden below

	local          backend.Backend   // == embedded Backend, named for clarity at call sites
	peers          []backend.Backend // len N; peers[selfIdx] is nil (owned locally)
	selfIdx        int
	n              int
	epoch          int64
	chanSem        chan struct{} // weight-1: at most one byte-write into the local channel at a time
	forwardTimeout time.Duration // deadline for a forwarded byte-write (0 = no timeout)
}

// New builds a Router. selfIdx is this node's slot (supplied per-node as a flag).
// peers must have length m.N; peers[selfIdx] is ignored (this node's objects are
// written through local), and every other entry must be a forwarding backend to
// that slot's gateway. forwardTimeout bounds a forwarded byte-write (0 disables).
func New(local backend.Backend, peers []backend.Backend, m *OwnerMap, selfIdx int, forwardTimeout time.Duration) (*Router, error) {
	if local == nil {
		return nil, fmt.Errorf("router: nil local backend")
	}
	if selfIdx < 0 || selfIdx >= m.N {
		return nil, fmt.Errorf("router: selfIdx %d out of range [0,%d)", selfIdx, m.N)
	}
	if len(peers) != m.N {
		return nil, fmt.Errorf("router: have %d peer backends but owner map n=%d", len(peers), m.N)
	}
	for i := range peers {
		if i != selfIdx && peers[i] == nil {
			return nil, fmt.Errorf("router: peers[%d] is nil but it is not the local slot", i)
		}
	}
	return &Router{
		Backend:        local,
		local:          local,
		peers:          peers,
		selfIdx:        selfIdx,
		n:              m.N,
		epoch:          m.Epoch,
		chanSem:        make(chan struct{}, 1),
		forwardTimeout: forwardTimeout,
	}, nil
}

func (r *Router) String() string {
	return fmt.Sprintf("Otter router (n=%d, self=%d, epoch=%d)", r.n, r.selfIdx, r.epoch)
}

// pick returns the backend that owns (bucket,key) and whether it is local.
func (r *Router) pick(bucket, key string) (backend.Backend, bool) {
	idx := place(bucket, key, r.n)
	if idx == r.selfIdx {
		return r.local, true
	}
	return r.peers[idx], false
}

// ---- placement -------------------------------------------------------------

var walName = regexp.MustCompile(`^[0-9A-Fa-f]{24}$`)

// place maps an object to a slot in [0,n). WAL segment names (24 hex chars) are
// monotonic, so we place by their low-64-bit ordinal mod n: consecutive
// segments land on distinct slots (round-robin-like, and recomputable on read).
// Everything else uses fnv-1a(bucket+"/"+key) mod n. Deterministic and stable —
// the read path recomputes the same slot with no location map. N and the
// ordering must stay frozen for the data's retention life.
func place(bucket, key string, n int) int {
	if n <= 1 {
		return 0
	}
	base := key
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			base = key[i+1:]
			break
		}
	}
	if walName.MatchString(base) {
		// last 16 hex chars = (logid<<32 | seg); increments by 1 per segment
		if ord, err := strconv.ParseUint(base[8:], 16, 64); err == nil {
			return int(ord % uint64(n))
		}
	}
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
func (r *Router) enterWrite(ctx context.Context, local bool) (context.Context, func()) {
	if local {
		r.chanSem <- struct{}{}
		return ctx, func() { <-r.chanSem }
	}
	if r.forwardTimeout > 0 {
		return context.WithTimeout(ctx, r.forwardTimeout)
	}
	return ctx, func() {}
}

func (r *Router) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done := r.enterWrite(ctx, local)
	defer done()
	return be.PutObject(ctx, in)
}

func (r *Router) UploadPart(ctx context.Context, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done := r.enterWrite(ctx, local)
	defer done()
	return be.UploadPart(ctx, in)
}

func (r *Router) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done := r.enterWrite(ctx, local)
	defer done()
	return be.CompleteMultipartUpload(ctx, in)
}

// UploadPartCopy / CopyObject route by the DEST key; a cross-node *source* is a
// v1 limitation (the local backend reads the source locally); WAL never uses them.
func (r *Router) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done := r.enterWrite(ctx, local)
	defer done()
	return be.UploadPartCopy(ctx, in)
}

func (r *Router) CopyObject(ctx context.Context, in s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	be, local := r.pick(*in.Bucket, *in.Key)
	ctx, done := r.enterWrite(ctx, local)
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

func (r *Router) CreateBucket(ctx context.Context, in *s3.CreateBucketInput, defaultACL []byte) error {
	err := r.local.CreateBucket(ctx, in, defaultACL)
	if isBucketExists(err) {
		return nil // already present here (likely a fan-out echo) -> do not re-fan
	}
	if err != nil {
		return err
	}
	for i, p := range r.peers {
		if i == r.selfIdx || p == nil {
			continue
		}
		if e := p.CreateBucket(ctx, in, defaultACL); e != nil && !isBucketExists(e) {
			return fmt.Errorf("router: create bucket on slot %d: %w", i, e)
		}
	}
	return nil
}

func (r *Router) DeleteBucket(ctx context.Context, bucket string) error {
	err := r.local.DeleteBucket(ctx, bucket)
	if isNoSuchBucket(err) {
		return nil // already gone here (echo) -> do not re-fan
	}
	if err != nil {
		return err // e.g. BucketNotEmpty -> surface
	}
	for i, p := range r.peers {
		if i == r.selfIdx || p == nil {
			continue
		}
		if e := p.DeleteBucket(ctx, bucket); e != nil && !isNoSuchBucket(e) {
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
	return strings.Contains(err.Error(), "NoSuchBucket")
}

// NOTE (v1 limitations, delegated to the local backend via embedding):
//   - DeleteObjects (batch) can span owners; not yet fanned out per-owner.
//   - ListObjects / ListObjectVersions / ListMultipartUploads are local-only;
//     a cross-node fan-out+merge is deferred (WAL restore is GET-by-name).
//   - Forwarded reads (GetObject/HeadObject) are not yet time-bounded (the
//     response body streams after the call; needs a connect-level timeout).
