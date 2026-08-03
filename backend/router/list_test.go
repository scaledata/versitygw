package router

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// mockBackend is an in-memory backend holding a disjoint, sorted key set. It
// mirrors the real backends' pagination contract: Marker / ContinuationToken is
// an *exclusive* cursor (returns keys strictly greater), MaxKeys caps the page,
// and IsTruncated reports whether more keys remain past the page.
//
// It also models what a real *peer* is (a peer's :9002 router): a fan-out
// sub-request arrives carrying the local-only sentinel, and the peer router
// unwraps it and serves only its own channel. So the mock unwraps the sentinel
// too — the local slot receives a raw cursor, a peer slot receives a wrapped one,
// and either way the effective cursor is the raw key.
type mockBackend struct {
	backend.BackendUnsupported
	keys []string // must be sorted ascending
	err  error    // if non-nil, every list call returns it (fail-closed tests)
}

// cursorOf extracts the effective raw exclusive cursor from a marker/token,
// unwrapping the local-only fan-out sentinel a peer would receive.
func cursorOf(tok string) string {
	if cur, ok := unwrapLocalOnly(tok); ok {
		return cur
	}
	return tok
}

func newMock(keys ...string) *mockBackend {
	sort.Strings(keys)
	return &mockBackend{keys: keys}
}

// pageAfter returns up to max keys strictly greater than marker, and whether
// any matching key remains beyond the page.
func (m *mockBackend) pageAfter(marker string, max int32) (objs []s3response.Object, truncated bool) {
	for _, k := range m.keys {
		if k <= marker {
			continue
		}
		if int32(len(objs)) >= max {
			return objs, true // at least one more matching key exists
		}
		kk := k
		objs = append(objs, s3response.Object{Key: &kk})
	}
	return objs, false
}

func (m *mockBackend) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if m.err != nil {
		return s3response.ListObjectsV2Result{}, m.err
	}
	max := int32(1000)
	if in.MaxKeys != nil {
		max = *in.MaxKeys
	}
	objs, trunc := m.pageAfter(cursorOf(backend.GetStringFromPtr(in.ContinuationToken)), max)
	return s3response.ListObjectsV2Result{
		Contents:    objs,
		IsTruncated: &trunc,
		KeyCount:    int32Ptr(int32(len(objs))),
	}, nil
}

func (m *mockBackend) ListObjects(_ context.Context, in *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if m.err != nil {
		return s3response.ListObjectsResult{}, m.err
	}
	max := int32(1000)
	if in.MaxKeys != nil {
		max = *in.MaxKeys
	}
	objs, trunc := m.pageAfter(cursorOf(backend.GetStringFromPtr(in.Marker)), max)
	return s3response.ListObjectsResult{
		Contents:    objs,
		IsTruncated: &trunc,
	}, nil
}

// newTestRouter wires backends[0] as the local slot and the rest as peers,
// bypassing New (which needs an OwnerMap). backendFor(0) uses local, so
// peers[0] stays nil, exactly as in production.
func newTestRouter(backends ...backend.Backend) *Router {
	n := len(backends)
	peers := make([]backend.Backend, n)
	for i := 1; i < n; i++ {
		peers[i] = backends[i]
	}
	r := &Router{chanSem: make(chan struct{}, 1)}
	r.cfg.Store(&routerCfg{
		local:   backends[0],
		peers:   peers,
		selfIdx: 0,
		n:       n,
	})
	return r
}

func strptr(s string) *string { return &s }

// disjointBackends distributes `total` sorted keys round-robin across n mock
// backends and returns the backends plus the full sorted union.
func disjointBackends(n, total int) ([]backend.Backend, []string) {
	perBackend := make([][]string, n)
	var union []string
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("obj/%05d", i)
		perBackend[i%n] = append(perBackend[i%n], k)
		union = append(union, k)
	}
	bes := make([]backend.Backend, n)
	for i := range perBackend {
		bes[i] = newMock(perBackend[i]...)
	}
	sort.Strings(union)
	return bes, union
}

// collectAllV2 walks every page of a fanned-out ListObjectsV2, following the
// router's NextContinuationToken, and returns the concatenated keys. It asserts
// per-page invariants: a final page carries no token, a truncated page must.
func collectAllV2(t *testing.T, r *Router, maxKeys int32) []string {
	t.Helper()
	var got []string
	var token *string
	for iter := 0; ; iter++ {
		if iter > 100000 {
			t.Fatal("pagination did not terminate")
		}
		res, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            strptr("bk"),
			MaxKeys:           int32Ptr(maxKeys),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
		if int32(len(res.Contents)) > maxKeys {
			t.Fatalf("page returned %d keys > maxKeys %d", len(res.Contents), maxKeys)
		}
		for _, o := range res.Contents {
			got = append(got, *o.Key)
		}
		if !boolVal(res.IsTruncated) {
			if res.NextContinuationToken != nil {
				t.Fatal("final page carried a NextContinuationToken")
			}
			return got
		}
		if res.NextContinuationToken == nil {
			t.Fatal("truncated page carried no NextContinuationToken")
		}
		token = res.NextContinuationToken
	}
}

func assertExactUnion(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	seen := make(map[string]int, len(got))
	for _, k := range got {
		seen[k]++
	}
	for _, k := range want {
		if seen[k] == 0 {
			t.Fatalf("missing key %q", k)
		}
		if seen[k] > 1 {
			t.Fatalf("duplicate key %q (x%d)", k, seen[k])
		}
	}
	// merged output must be globally sorted
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("out of order at %d: %q >= %q", i, got[i-1], got[i])
		}
	}
}

// The whole point of the change: a single fanned-out endpoint must enumerate
// the entire union, across many page boundaries, for a range of maxKeys.
func TestListV2FullUnionAcrossPages(t *testing.T) {
	for _, maxKeys := range []int32{1, 2, 3, 7, 100, 1000} {
		for _, n := range []int{2, 3, 4} {
			bes, union := disjointBackends(n, 53) // 53 is coprime-ish to page sizes
			r := newTestRouter(bes...)
			got := collectAllV2(t, r, maxKeys)
			t.Run(fmt.Sprintf("n=%d/maxKeys=%d", n, maxKeys), func(t *testing.T) {
				assertExactUnion(t, got, union)
			})
		}
	}
}

// A page boundary that splits one backend's run mid-stream must not lose,
// duplicate, or reorder that backend's tail on the next page.
func TestListV2PageBoundarySplitsBackend(t *testing.T) {
	// Backend 0 owns a dense contiguous run; others are sparse. maxKeys=2 forces
	// the merge to stop inside backend 0's run repeatedly.
	b0 := newMock("a01", "a02", "a03", "a04", "a05")
	b1 := newMock("a02.5", "a04.5")
	b2 := newMock("z99")
	r := newTestRouter(b0, b1, b2)
	got := collectAllV2(t, r, 2)
	want := []string{"a01", "a02", "a02.5", "a03", "a04", "a04.5", "a05", "z99"}
	assertExactUnion(t, got, want)
}

// Regression for the re-fan recursion bug: a router acting as a PEER must serve
// ONLY its own channel when it receives a local-only fan-out sentinel, never
// re-fan to its own peers. Peers point at each other's :9002 routers, so a
// re-fan would recurse across the cluster. Local and peer key sets are disjoint;
// a re-fan would surface the peer keys too.
func TestListV2LocalOnlySentinelDoesNotRefan(t *testing.T) {
	localKeys := []string{"a", "c", "e"}
	peerKeys := []string{"b", "d", "f"}
	r := newTestRouter(newMock(localKeys...), newMock(peerKeys...))

	res, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            strptr("bk"),
		MaxKeys:           int32Ptr(1000),
		ContinuationToken: strptr(wrapLocalOnly("")),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got []string
	for _, o := range res.Contents {
		got = append(got, backend.GetStringFromPtr(o.Key))
	}
	assertExactUnion(t, got, localKeys) // local only, NOT localKeys ∪ peerKeys
	if boolVal(res.IsTruncated) {
		t.Fatal("local-only page should not be truncated here")
	}
	if res.NextContinuationToken != nil {
		t.Fatal("local-only page must not issue a router continuation token")
	}
}

// The v1 (Marker) equivalent of the re-fan regression test.
func TestListV1LocalOnlySentinelDoesNotRefan(t *testing.T) {
	localKeys := []string{"a", "c", "e"}
	peerKeys := []string{"b", "d", "f"}
	r := newTestRouter(newMock(localKeys...), newMock(peerKeys...))

	res, err := r.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket: strptr("bk"),
		Marker: strptr(wrapLocalOnly("")),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got []string
	for _, o := range res.Contents {
		got = append(got, backend.GetStringFromPtr(o.Key))
	}
	assertExactUnion(t, got, localKeys)
	if res.NextMarker != nil {
		t.Fatal("local-only page must not issue a router marker")
	}
}

// Empty bucket: no keys, not truncated, no token.
func TestListV2Empty(t *testing.T) {
	r := newTestRouter(newMock(), newMock(), newMock())
	res, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: strptr("bk")})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Contents) != 0 || boolVal(res.IsTruncated) || res.NextContinuationToken != nil {
		t.Fatalf("empty listing wrong: %+v", res)
	}
	if res.KeyCount != nil && *res.KeyCount != 0 {
		t.Fatalf("KeyCount=%d, want 0", *res.KeyCount)
	}
}

// MaxKeys==0 returns an empty, non-truncated page (matches backend.Walk).
func TestListV2MaxKeysZero(t *testing.T) {
	bes, _ := disjointBackends(3, 10)
	r := newTestRouter(bes...)
	res, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  strptr("bk"),
		MaxKeys: int32Ptr(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Contents) != 0 {
		t.Fatalf("maxKeys=0 returned %d keys", len(res.Contents))
	}
	if boolVal(res.IsTruncated) {
		t.Fatal("maxKeys=0 should be non-truncated (backend.Walk semantics)")
	}
}

// A continuation token minted for a different N must be rejected, not silently
// mis-applied (it would drop or duplicate keys).
func TestListV2RejectsWrongNToken(t *testing.T) {
	bes, _ := disjointBackends(4, 10)
	r := newTestRouter(bes...)
	badTok := encodeCompound([]string{"a", "b", "c"}) // N=3, router n=4
	_, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            strptr("bk"),
		ContinuationToken: &badTok,
	})
	if err == nil {
		t.Fatal("expected error for wrong-N token")
	}
	if _, ok := err.(s3err.InvalidArgumentError); !ok {
		t.Fatalf("want InvalidArgumentError, got %T: %v", err, err)
	}
}

// A continuation token the router never issued (not our prefix) is invalid for
// V2 (V2 tokens are always server-issued).
func TestListV2RejectsForeignToken(t *testing.T) {
	bes, _ := disjointBackends(3, 10)
	r := newTestRouter(bes...)
	foreign := "some-random-key"
	_, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            strptr("bk"),
		ContinuationToken: &foreign,
	})
	if _, ok := err.(s3err.InvalidArgumentError); !ok {
		t.Fatalf("want InvalidArgumentError, got %T: %v", err, err)
	}
}

// Fail closed: one backend erroring fails the whole LIST (no silent partial).
func TestListV2FailClosed(t *testing.T) {
	b0 := newMock("a", "b")
	b1 := &mockBackend{err: s3err.GetAPIError(s3err.ErrInternalError)}
	b2 := newMock("c", "d")
	r := newTestRouter(b0, b1, b2)
	_, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: strptr("bk")})
	if err == nil {
		t.Fatal("expected fail-closed error when a backend errors")
	}
}

// StartAfter seeds every backend on the first page and is echoed on the result.
func TestListV2StartAfter(t *testing.T) {
	bes, union := disjointBackends(3, 30)
	r := newTestRouter(bes...)
	// list everything > union[9]
	after := union[9]
	var got []string
	var token *string
	for {
		in := &s3.ListObjectsV2Input{Bucket: strptr("bk"), MaxKeys: int32Ptr(4), ContinuationToken: token}
		if token == nil {
			in.StartAfter = strptr(after)
		}
		res, err := r.ListObjectsV2(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range res.Contents {
			got = append(got, *o.Key)
		}
		if !boolVal(res.IsTruncated) {
			break
		}
		token = res.NextContinuationToken
	}
	assertExactUnion(t, got, union[10:])
}

// n<=1 delegates straight to the local backend (no fan-out, raw markers pass
// through unchanged).
func TestListV2SingleChannelPassthrough(t *testing.T) {
	local := newMock("k1", "k2", "k3")
	r := &Router{chanSem: make(chan struct{}, 1)}
	r.cfg.Store(&routerCfg{local: local, peers: []backend.Backend{nil}, selfIdx: 0, n: 1})
	res, err := r.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: strptr("bk")})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Contents) != 3 {
		t.Fatalf("passthrough got %d keys, want 3", len(res.Contents))
	}
}

// ---- v1 mirror --------------------------------------------------------------

func collectAllV1(t *testing.T, r *Router, maxKeys int32) []string {
	t.Helper()
	var got []string
	var marker *string
	for iter := 0; ; iter++ {
		if iter > 100000 {
			t.Fatal("pagination did not terminate")
		}
		res, err := r.ListObjects(context.Background(), &s3.ListObjectsInput{
			Bucket:  strptr("bk"),
			MaxKeys: int32Ptr(maxKeys),
			Marker:  marker,
		})
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		for _, o := range res.Contents {
			got = append(got, *o.Key)
		}
		if !boolVal(res.IsTruncated) {
			if res.NextMarker != nil {
				t.Fatal("final page carried a NextMarker")
			}
			return got
		}
		if res.NextMarker == nil {
			t.Fatal("truncated page carried no NextMarker")
		}
		marker = res.NextMarker
	}
}

func TestListV1FullUnionAcrossPages(t *testing.T) {
	for _, maxKeys := range []int32{1, 3, 8, 1000} {
		for _, n := range []int{2, 4} {
			bes, union := disjointBackends(n, 41)
			r := newTestRouter(bes...)
			got := collectAllV1(t, r, maxKeys)
			t.Run(fmt.Sprintf("n=%d/maxKeys=%d", n, maxKeys), func(t *testing.T) {
				assertExactUnion(t, got, union)
			})
		}
	}
}

// A raw (non-token) v1 Marker seeds every backend and lists the remainder.
func TestListV1RawMarkerSeed(t *testing.T) {
	bes, union := disjointBackends(3, 20)
	r := newTestRouter(bes...)
	after := union[4]
	res, err := r.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket:  strptr("bk"),
		Marker:  strptr(after),
		MaxKeys: int32Ptr(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, o := range res.Contents {
		got = append(got, *o.Key)
	}
	assertExactUnion(t, got, union[5:])
}

func TestListV1FailClosed(t *testing.T) {
	b0 := newMock("a", "b")
	b1 := &mockBackend{err: s3err.GetAPIError(s3err.ErrInternalError)}
	r := newTestRouter(b0, b1)
	_, err := r.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: strptr("bk")})
	if err == nil {
		t.Fatal("expected fail-closed error")
	}
}

// ---- token codec ------------------------------------------------------------

func TestCompoundTokenRoundTrip(t *testing.T) {
	cursors := []string{"a/b", "", "z/9", "obj/00042"}
	tok := encodeCompound(cursors)
	got, ok, err := decodeCompound(tok, len(cursors))
	if err != nil || !ok {
		t.Fatalf("decode ok=%v err=%v", ok, err)
	}
	for i := range cursors {
		if got[i] != cursors[i] {
			t.Fatalf("cursor %d: got %q want %q", i, got[i], cursors[i])
		}
	}
}

func TestCompoundTokenNotOurs(t *testing.T) {
	_, ok, err := decodeCompound("just-a-key", 4)
	if ok || err != nil {
		t.Fatalf("raw key should be not-ours with no error; ok=%v err=%v", ok, err)
	}
}

func TestCompoundTokenCorrupt(t *testing.T) {
	_, ok, err := decodeCompound(listTokenPrefix+"!!!not-base64!!!", 4)
	if !ok || err == nil {
		t.Fatalf("corrupt token should be ours-but-invalid; ok=%v err=%v", ok, err)
	}
}
