package grant

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// ---- test helpers ----------------------------------------------------------
//
// grantFor / walSeg live in grant_test.go (same package) and are reused here.

// fakeSource is an in-memory Source standing in for CRDB.
type fakeSource struct {
	byKey    map[cacheKey]Grant
	active   []Grant
	lookups  int   // count of Lookup calls (to prove caching stops re-reads)
	lookErr  error // if set, Lookup returns it
	activErr error // if set, ActiveGrants returns it
}

func newFakeSource() *fakeSource { return &fakeSource{byKey: map[cacheKey]Grant{}} }

func (f *fakeSource) put(g Grant) {
	f.byKey[cacheKey{g.AccessKeyID, g.Bucket}] = g
	if g.Active {
		f.active = append(f.active, g)
	}
}

func (f *fakeSource) Lookup(_ context.Context, access, bucket string) (Grant, bool, error) {
	f.lookups++
	if f.lookErr != nil {
		return Grant{}, false, f.lookErr
	}
	g, ok := f.byKey[cacheKey{access, bucket}]
	return g, ok, nil
}

func (f *fakeSource) ActiveGrants(_ context.Context) ([]Grant, error) {
	if f.activErr != nil {
		return nil, f.activErr
	}
	return f.active, nil
}

// ---- resolver: hit / lazy CRDB fallback / miss / errors --------------------

func TestResolveCacheHit(t *testing.T) {
	src := newFakeSource()
	r := NewResolver(WithSource(src))
	r.Upsert(grantFor("otter", "wal", 1, 3, PolicyKeyHash))

	g, err := r.Resolve(context.Background(), "otter", "wal")
	if err != nil {
		t.Fatalf("resolve hit: %v", err)
	}
	if g.Epoch != 1 {
		t.Fatalf("resolved epoch %d, want 1", g.Epoch)
	}
	if src.lookups != 0 {
		t.Fatalf("cache hit should not touch the source; got %d lookups", src.lookups)
	}
}

// A miss falls back to a bounded CRDB read, then caches the result so the retry is
// a hot hit (no second source read) — the write-before-grant race, §14.5.
func TestResolveLazyCRDBFallbackThenCaches(t *testing.T) {
	src := newFakeSource()
	src.put(grantFor("otter", "wal", 7, 3, PolicyOrdinalRoundRobin))
	r := NewResolver(WithSource(src), WithLookupTimeout(200*time.Millisecond))

	g, err := r.Resolve(context.Background(), "otter", "wal")
	if err != nil || g.Epoch != 7 {
		t.Fatalf("lazy fallback: g.Epoch=%d err=%v, want 7/nil", g.Epoch, err)
	}
	if _, err := r.Resolve(context.Background(), "otter", "wal"); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if src.lookups != 1 {
		t.Fatalf("expected exactly 1 source lookup (then cached); got %d", src.lookups)
	}
}

// Neither cache nor source has the grant -> ErrGrantMiss -> 503 Retry-After.
func TestResolveGrantMissMapsTo503(t *testing.T) {
	r := NewResolver(WithSource(newFakeSource()))
	_, err := r.Resolve(context.Background(), "otter", "wal")
	if !errors.Is(err, ErrGrantMiss) {
		t.Fatalf("expected ErrGrantMiss, got %v", err)
	}
	apiErr, ok := SlowDownIfMiss(err)
	if !ok {
		t.Fatalf("SlowDownIfMiss should recognize a grant miss")
	}
	if apiErr.HTTPStatusCode != http.StatusServiceUnavailable {
		t.Fatalf("grant miss mapped to HTTP %d, want 503", apiErr.HTTPStatusCode)
	}
}

// A CRDB read failure is NOT a grant miss: it must surface so the caller returns its
// own 5xx rather than a misleading "retry, it's coming".
func TestResolveSourceErrorNotMiss(t *testing.T) {
	src := newFakeSource()
	src.lookErr = errors.New("crdb unreachable")
	r := NewResolver(WithSource(src))

	_, err := r.Resolve(context.Background(), "otter", "wal")
	if err == nil || errors.Is(err, ErrGrantMiss) {
		t.Fatalf("source error should surface (not as ErrGrantMiss); got %v", err)
	}
	if _, ok := SlowDownIfMiss(err); ok {
		t.Fatalf("a raw source error must not map to 503")
	}
}

// No source configured (push-only): a miss is still ErrGrantMiss, never a panic.
func TestResolveNoSourceMiss(t *testing.T) {
	r := NewResolver()
	if _, err := r.Resolve(context.Background(), "otter", "wal"); !errors.Is(err, ErrGrantMiss) {
		t.Fatalf("push-only miss: want ErrGrantMiss, got %v", err)
	}
}

// ---- resolver: inactive / lease expiry -------------------------------------

func TestResolveInactiveTreatedAsMiss(t *testing.T) {
	src := newFakeSource()
	g := grantFor("otter", "wal", 1, 3, PolicyKeyHash)
	g.Active = false
	src.byKey[cacheKey{g.AccessKeyID, g.Bucket}] = g // present but inactive
	r := NewResolver(WithSource(src))

	if _, err := r.Resolve(context.Background(), "otter", "wal"); !errors.Is(err, ErrGrantMiss) {
		t.Fatalf("inactive grant should resolve as a miss (503), got %v", err)
	}
}

func TestResolveLeaseExpiredTreatedAsMiss(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { return now }

	src := newFakeSource()
	g := grantFor("otter", "wal", 1, 3, PolicyKeyHash)
	g.LeaseExpiryUnixS = now.Unix() - 1 // already expired
	src.put(g)
	r := NewResolver(WithSource(src), withClock(clock))

	if _, err := r.Resolve(context.Background(), "otter", "wal"); !errors.Is(err, ErrGrantMiss) {
		t.Fatalf("expired-lease grant should resolve as a miss, got %v", err)
	}

	// The same grant with a lease in the future resolves normally.
	g2 := grantFor("otter", "future", 1, 3, PolicyKeyHash)
	g2.LeaseExpiryUnixS = now.Unix() + 3600
	src.put(g2)
	if _, err := r.Resolve(context.Background(), "otter", "future"); err != nil {
		t.Fatalf("unexpired-lease grant should resolve, got %v", err)
	}
}

// ---- warm & upsert hook (grant-driven bucket mkdir) ------------------------

func TestWarmLoadsActiveAndRunsHook(t *testing.T) {
	src := newFakeSource()
	src.put(grantFor("keyA", "walA", 1, 2, PolicyKeyHash))
	src.put(grantFor("keyB", "walB", 1, 3, PolicyOrdinalRoundRobin))

	var mkdirs []string
	r := NewResolver(WithSource(src), WithUpsertHook(func(g Grant) error {
		mkdirs = append(mkdirs, g.AccessKeyID+"/"+g.Bucket)
		return nil
	}))

	loaded, err := r.Warm(context.Background())
	if err != nil || loaded != 2 {
		t.Fatalf("warm: loaded=%d err=%v, want 2/nil", loaded, err)
	}
	if len(mkdirs) != 2 {
		t.Fatalf("warm should run the mkdir hook per grant; got %v", mkdirs)
	}
	// Both grants are now hot — resolving them touches the source zero more times.
	before := src.lookups
	if _, err := r.Resolve(context.Background(), "keyA", "walA"); err != nil {
		t.Fatalf("resolve warmed grant: %v", err)
	}
	if src.lookups != before {
		t.Fatalf("resolving a warmed grant should not read the source")
	}
}

func TestWarmSourceErrorPropagates(t *testing.T) {
	src := newFakeSource()
	src.activErr = errors.New("crdb down at startup")
	r := NewResolver(WithSource(src))
	if _, err := r.Warm(context.Background()); err == nil {
		t.Fatalf("warm should surface a source error")
	}
}

func TestWarmNoSourceIsNoop(t *testing.T) {
	r := NewResolver()
	if loaded, err := r.Warm(context.Background()); err != nil || loaded != 0 {
		t.Fatalf("warm with no source: loaded=%d err=%v, want 0/nil", loaded, err)
	}
}

// The mkdir hook fires only on a freshly-applied (accepted) grant, not on an
// idempotent stale re-push — so a re-pushed grant doesn't re-mkdir needlessly.
func TestUpsertHookOnlyOnFreshEpoch(t *testing.T) {
	var hooks int
	r := NewResolver(WithUpsertHook(func(Grant) error { hooks++; return nil }))

	r.Upsert(grantFor("otter", "wal", 5, 2, PolicyKeyHash)) // fresh -> hook
	r.Upsert(grantFor("otter", "wal", 6, 2, PolicyKeyHash)) // newer -> hook
	r.Upsert(grantFor("otter", "wal", 6, 2, PolicyKeyHash)) // equal -> no hook
	r.Upsert(grantFor("otter", "wal", 3, 2, PolicyKeyHash)) // stale -> no hook

	if hooks != 2 {
		t.Fatalf("mkdir hook fired %d times, want 2 (fresh + newer only)", hooks)
	}
}

// TestResolveLazyRunsUpsertHook: a grant learned via the lazy CRDB fallback must
// run the mkdir hook — the node that missed the push is the one that still needs
// its owned channel dir created.
func TestResolveLazyRunsUpsertHook(t *testing.T) {
	src := newFakeSource()
	src.put(grantFor("otter", "wal", 1, 2, PolicyKeyHash))
	var hooked int
	r := NewResolver(WithSource(src), WithUpsertHook(func(Grant) error { hooked++; return nil }))

	if _, err := r.Resolve(context.Background(), "otter", "wal"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if hooked != 1 {
		t.Fatalf("lazy Resolve ran the upsert hook %d times, want 1", hooked)
	}
}

// blockingSource.Lookup blocks until the (bounded) context fires, standing in for
// a slow/parked CRDB.
type blockingSource struct{}

func (blockingSource) Lookup(ctx context.Context, _, _ string) (Grant, bool, error) {
	<-ctx.Done()
	return Grant{}, false, ctx.Err()
}
func (blockingSource) ActiveGrants(context.Context) ([]Grant, error) { return nil, nil }

// TestResolveLookupTimeoutIsGrantMiss: when WithLookupTimeout fires, Resolve must
// return a retryable grant-miss (503), not a hard 5xx.
func TestResolveLookupTimeoutIsGrantMiss(t *testing.T) {
	r := NewResolver(WithSource(blockingSource{}), WithLookupTimeout(20*time.Millisecond))

	_, err := r.Resolve(context.Background(), "otter", "wal")
	if !errors.Is(err, ErrGrantMiss) {
		t.Fatalf("timeout err should wrap ErrGrantMiss, got %v", err)
	}
	if _, ok := SlowDownIfMiss(err); !ok {
		t.Fatalf("lookup timeout should map to a retryable 503, got %v", err)
	}
}
