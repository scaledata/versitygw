package grant

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// defaultHookTimeout bounds the upsert hook (grant-driven bucket mkdir) so a
// hook that blocks on a wedged/unavailable filesystem cannot stall the JFL push
// handler (Upsert) or the startup warm loop (Warm) indefinitely.
const defaultHookTimeout = 30 * time.Second

// Source is the durable, authoritative backing for grants — CockroachDB in
// production (see crdb_source.go, build tag otter_crdb). The cache is filled from
// it lazily on a miss and warmed from it at startup. Implementations must be safe
// for concurrent use.
type Source interface {
	// Lookup returns the grant for (access,bucket) and whether it exists.
	Lookup(ctx context.Context, access, bucket string) (Grant, bool, error)
	// ActiveGrants returns every active grant, for the startup cache warm.
	ActiveGrants(ctx context.Context) ([]Grant, error)
}

// Resolver is the control-plane entry point the gateway uses. It owns the grant
// cache and, optionally, a durable Source. The Thrift push handler calls Upsert;
// the S3 hot path calls Resolve; the gateway calls Warm once before serving.
type Resolver struct {
	cache         *Cache
	source        Source            // may be nil (push-only, or tests without CRDB)
	lookupTimeout time.Duration     // bound on a synchronous CRDB read (0 = no bound)
	onUpsert      func(Grant) error // optional hook run on a freshly-applied grant (bucket mkdir)
	now           func() time.Time  // clock for lease-expiry checks; injectable for tests
}

// Option configures a Resolver.
type Option func(*Resolver)

// WithSource sets the durable CRDB backing used for lazy fallback and startup warm.
func WithSource(s Source) Option { return func(r *Resolver) { r.source = s } }

// WithLookupTimeout bounds a synchronous CRDB read on a cache miss so a slow/parked
// DB turns into a fast retryable 503 instead of hanging the S3 request.
func WithLookupTimeout(d time.Duration) Option { return func(r *Resolver) { r.lookupTimeout = d } }

// WithUpsertHook registers a callback invoked once per freshly-applied grant (via
// Upsert or Warm) — used for grant-driven bucket mkdir on this node's owned channel.
// The hook must be idempotent; the gateway's hook logs and swallows errors so a
// failed mkdir never blocks serving.
func WithUpsertHook(fn func(Grant) error) Option { return func(r *Resolver) { r.onUpsert = fn } }

// withClock overrides the lease-expiry clock (test-only).
func withClock(fn func() time.Time) Option { return func(r *Resolver) { r.now = fn } }

// NewResolver builds a Resolver with an empty cache.
func NewResolver(opts ...Option) *Resolver {
	r := &Resolver{cache: NewCache(), now: time.Now}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Cache exposes the underlying cache (diagnostics, tests, and the revoke path).
func (r *Resolver) Cache() *Cache { return r.cache }

// Resolve returns the live grant for (access,bucket) for use on the S3 hot path.
// Order: cache hit -> a bounded synchronous CRDB read on a miss -> ErrGrantMiss.
// An inactive or lease-expired grant is treated as a miss (503, retryable), never a
// hard denial: only the IAM/SigV4 layer returns 403, and only for a bad credential.
// This is the write-before-grant race from otter-design-doc.md §14.5.
func (r *Resolver) Resolve(ctx context.Context, access, bucket string) (Grant, error) {
	if g, ok := r.cache.Get(access, bucket); ok && r.usable(g) {
		return g, nil
	}
	if r.source != nil {
		lctx := ctx
		if r.lookupTimeout > 0 {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, r.lookupTimeout)
			defer cancel()
		}
		g, ok, err := r.source.Lookup(lctx, access, bucket)
		if err != nil {
			// A lookup that timed out — our lookupTimeout firing, or the caller's
			// own deadline — is a slow/parked DB, not a hard failure: map it to
			// the retryable grant-miss (503) that WithLookupTimeout promises,
			// instead of surfacing a hard 5xx.
			if errors.Is(err, context.DeadlineExceeded) {
				return Grant{}, fmt.Errorf("%w: grant lookup timed out: %v", ErrGrantMiss, err)
			}
			// Any other CRDB read failure is not a grant miss — surface it so the
			// caller returns its own 5xx rather than a misleading "retry".
			return Grant{}, err
		}
		if ok && r.usable(g) {
			// Route through r.Upsert (not r.cache.Upsert) so the mkdir hook runs:
			// the node that missed the original push is exactly the one whose
			// owned channel dir still needs creating. Epoch-guarded, so the retry
			// is a hot cache hit. Hook errors are the hook's own concern (logged
			// and swallowed there, retried on the next push/warm), as in Warm.
			_, _, _ = r.Upsert(g)
			return g, nil
		}
	}
	return Grant{}, ErrGrantMiss
}

// Upsert applies a grant pushed by JFL (the fast path). It is epoch-guarded and
// idempotent: an equal-or-lower epoch is a no-op (accepted=false). On a fresh apply
// it runs the upsert hook (grant-driven bucket mkdir); the hook's error is returned
// but the grant stays cached, so the mkdir is retried on the next push or warm.
func (r *Resolver) Upsert(g Grant) (accepted bool, current int64, err error) {
	accepted, current = r.cache.Upsert(g)
	if accepted {
		err = r.runHook(g)
	}
	return accepted, current, err
}

// runHook invokes the upsert hook bounded by defaultHookTimeout so a hook that
// blocks on a wedged filesystem cannot stall the caller. On timeout it returns
// an error; the hook goroutine is left to finish on its own (it cannot be
// cancelled — the hook takes no context), which is bounded by the number of
// simultaneously-wedged hooks.
func (r *Resolver) runHook(g Grant) error {
	if r.onUpsert == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- r.onUpsert(g) }()
	select {
	case err := <-done:
		return err
	case <-time.After(defaultHookTimeout):
		return fmt.Errorf("otter grant: upsert hook timed out after %s", defaultHookTimeout)
	}
}

// Warm loads every active grant from the Source into the cache before the gateway
// serves, so a restart doesn't 503-storm on grants that were only ever pushed. It
// runs the upsert hook per freshly-loaded grant (idempotent bucket mkdir on owned
// channels). It returns the number of grants loaded; a nil Source is a no-op. The
// returned error reports only a failed CRDB read — hook errors are the hook's own
// concern (the gateway's hook logs and continues).
func (r *Resolver) Warm(ctx context.Context) (int, error) {
	if r.source == nil {
		return 0, nil
	}
	gs, err := r.source.ActiveGrants(ctx)
	if err != nil {
		return 0, err
	}
	loaded := 0
	for _, g := range gs {
		if accepted, _ := r.cache.Upsert(g); accepted {
			loaded++
			_ = r.runHook(g) // bounded; hook errors are the hook's own concern
		}
	}
	return loaded, nil
}

// usable reports whether a grant is currently valid to serve from: active and, if
// it carries a lease, not past its expiry.
func (r *Resolver) usable(g Grant) bool {
	if !g.Active {
		return false
	}
	if g.LeaseExpiryUnixS != 0 && r.now().Unix() >= g.LeaseExpiryUnixS {
		return false
	}
	return true
}
