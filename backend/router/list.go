package router

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// maxListKeys is the S3 hard cap on objects returned per LIST page.
const maxListKeys int32 = 1000

// listTokenPrefix tags a router-issued pagination token so it can be told apart
// from a user-supplied v1 Marker (which is a raw object key). Bump the version
// suffix to invalidate every previously issued token.
const listTokenPrefix = "otter.list.v1."

// compoundToken carries one resume cursor per backend across a paginated,
// fanned-out LIST. It is stateless: the entire continuation state lives in the
// token the client echoes back, so the gateway keeps no server-side session.
// N is stamped in so a token minted for one channel count is rejected against a
// cluster whose count changed (placement is frozen for the data's retention
// life, but a stale/hand-crafted token must still fail loudly rather than
// silently drop or duplicate keys).
type compoundToken struct {
	N       int      `json:"n"`
	Cursors []string `json:"c"`
}

// encodeCompound serializes per-backend cursors into an opaque, URL-safe token.
func encodeCompound(cursors []string) string {
	b, _ := json.Marshal(compoundToken{N: len(cursors), Cursors: cursors})
	return listTokenPrefix + base64.RawURLEncoding.EncodeToString(b)
}

// decodeCompound reverses encodeCompound.
//
//   - ok=false, err=nil: tok is not a router-issued token at all (e.g. a raw
//     user-supplied v1 Marker). The caller decides how to treat it.
//   - ok=true, err!=nil: tok looks like our token but is malformed or was minted
//     for a different N. Fail closed.
//   - ok=true, err=nil: cursors are valid.
func decodeCompound(tok string, n int) (cursors []string, ok bool, err error) {
	if !strings.HasPrefix(tok, listTokenPrefix) {
		return nil, false, nil
	}
	raw, derr := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(tok, listTokenPrefix))
	if derr != nil {
		return nil, true, errBadToken()
	}
	var ct compoundToken
	if jerr := json.Unmarshal(raw, &ct); jerr != nil {
		return nil, true, errBadToken()
	}
	if ct.N != n || len(ct.Cursors) != n {
		return nil, true, errBadToken()
	}
	return ct.Cursors, true, nil
}

func errBadToken() error {
	return s3err.InvalidArgumentError{
		Description:  "The continuation token provided is incorrect",
		ArgumentName: "continuation-token",
	}
}

// listLocalOnlyPrefix marks a fan-out sub-request so the receiving router serves
// ONLY its own channel and never re-fans. Peers point at each other's :9002
// router (not a local backend), so without this a fanned-out LIST would make the
// peer re-fan to its peers -> unbounded recursion / connection storm. The signal
// rides in the sub-request's ContinuationToken (V2) / Marker (v1) — fields that
// s3proxy already forwards verbatim and the router already inspects — so no
// custom-header plumbing across the HTTP hop is needed. The wrapped payload is
// the raw resume cursor for that peer's channel.
const listLocalOnlyPrefix = "otter.localonly.v1."

// wrapLocalOnly encodes a per-peer cursor as a local-only fan-out token. Always
// applied to peer sub-requests (even with an empty cursor) so the peer knows to
// serve local-only rather than treating an empty token as a fresh client LIST.
func wrapLocalOnly(cursor string) string {
	return listLocalOnlyPrefix + base64.RawURLEncoding.EncodeToString([]byte(cursor))
}

// unwrapLocalOnly reports whether tok is a local-only fan-out token and, if so,
// the raw cursor it carries. A token bearing the prefix but with a garbled body
// is still treated as local-only (cursor "") — better to serve from the start
// than to risk re-fanning.
func unwrapLocalOnly(tok string) (cursor string, ok bool) {
	if !strings.HasPrefix(tok, listLocalOnlyPrefix) {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(tok, listLocalOnlyPrefix))
	if err != nil {
		return "", true
	}
	return string(raw), true
}

// backendFor returns the backend for slot i: the local backend for this node's
// own slot, the forwarding peer otherwise.
func (r *Router) backendFor(i int) backend.Backend {
	if i == r.selfIdx {
		return r.local
	}
	return r.peers[i]
}

// ListObjectsV2 fans the request out to every channel, k-way merges the
// key-sorted results, and returns one merged page. Without this override the
// embedded local backend would answer from this node's channel alone (~1/N of
// the bucket), silently truncating a single-endpoint `aws s3 cp --recursive`
// (which enumerates with ListObjectsV2, then GETs each key — and GET already
// forwards).
//
// Pagination across N backends rides in a compound continuation token carrying
// one resume cursor (the last key consumed) per backend. Each backend is
// re-listed from its own cursor via ContinuationToken — an *exclusive* marker
// in the posix/proxy backends (backend.Walk advances on `key > marker`) — so
// resuming never repeats a key. Any buffered-but-unemitted tail of a backend is
// simply re-read on the next page; correct, at the cost of re-reading up to
// maxKeys*N key names per page boundary (metadata only, no object bytes).
//
// Fail-closed: if any channel errors, the whole LIST errors. A partial listing
// would silently drop objects — the exact bug this method removes.
//
// Delimiter handling is best-effort: sub-request delimiters are honored and the
// per-channel CommonPrefixes are merged+deduped, but delimited pagination is
// NOT guaranteed correct across N backends yet (the CommonPrefix/Contents
// interleave vs. the maxKeys cut needs more care). The flat (no-delimiter)
// listing used by cp/sync restore is fully correct.
func (r *Router) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	// Local-only fan-out sub-request from a peer router: serve just this node's
	// channel and never re-fan (peers point at :9002 routers, so re-fanning would
	// recurse across the cluster).
	if tok := backend.GetStringFromPtr(in.ContinuationToken); tok != "" {
		if cursor, ok := unwrapLocalOnly(tok); ok {
			sub := *in
			sub.StartAfter = nil
			sub.ContinuationToken = nil
			if cursor != "" {
				c := cursor
				sub.ContinuationToken = &c
			}
			return r.local.ListObjectsV2(ctx, &sub)
		}
	}
	if r.n <= 1 {
		return r.local.ListObjectsV2(ctx, in)
	}

	maxKeys, err := clampMaxKeys(in.MaxKeys)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	// Seed per-backend cursors: from the compound token when resuming, else all
	// backends start at the user's StartAfter (empty => bucket start).
	cursors := make([]string, r.n)
	if tok := backend.GetStringFromPtr(in.ContinuationToken); tok != "" {
		cs, ok, derr := decodeCompound(tok, r.n)
		if derr != nil {
			return s3response.ListObjectsV2Result{}, derr
		}
		if !ok {
			// A V2 continuation token is always server-issued; anything the
			// router did not mint is invalid.
			return s3response.ListObjectsV2Result{}, errBadToken()
		}
		cursors = cs
	} else {
		seed := backend.GetStringFromPtr(in.StartAfter)
		for i := range cursors {
			cursors[i] = seed
		}
	}

	pages := make([]s3response.ListObjectsV2Result, r.n)
	if maxKeys > 0 {
		g, gctx := errgroup.WithContext(ctx)
		for i := 0; i < r.n; i++ {
			i := i
			be := r.backendFor(i)
			g.Go(func() error {
				sub := *in // shallow copy; only this goroutine's scalar ptr fields are replaced
				sub.StartAfter = nil
				sub.ContinuationToken = nil
				if i == r.selfIdx {
					// local slot: raw exclusive marker straight to the posix backend
					if cursors[i] != "" {
						c := cursors[i]
						sub.ContinuationToken = &c
					}
				} else {
					// peer slot: local-only sentinel so the peer router serves its
					// own channel and does NOT re-fan (would otherwise recurse)
					c := wrapLocalOnly(cursors[i])
					sub.ContinuationToken = &c
				}
				mk := maxKeys
				sub.MaxKeys = &mk
				res, lerr := be.ListObjectsV2(gctx, &sub)
				if lerr != nil {
					return fmt.Errorf("list slot %d: %w", i, lerr)
				}
				pages[i] = res
				return nil
			})
		}
		if werr := g.Wait(); werr != nil {
			return s3response.ListObjectsV2Result{}, werr
		}
	}

	contents := make([][]s3response.Object, r.n)
	pageTrunc := make([]bool, r.n)
	for i := range pages {
		contents[i] = pages[i].Contents
		pageTrunc[i] = boolVal(pages[i].IsTruncated)
	}
	merged, lastEmitted, truncated := kwayMerge(contents, pageTrunc, cursors, maxKeys)

	var cps []types.CommonPrefix
	if hasDelimiter(in.Delimiter) {
		all := make([]types.CommonPrefix, 0)
		for i := range pages {
			all = append(all, pages[i].CommonPrefixes...)
		}
		cps = mergeCommonPrefixes(all)
	}

	out := s3response.ListObjectsV2Result{
		Name:              in.Bucket,
		Prefix:            in.Prefix,
		StartAfter:        in.StartAfter,
		Delimiter:         in.Delimiter,
		MaxKeys:           &maxKeys,
		Contents:          merged,
		CommonPrefixes:    cps,
		KeyCount:          int32Ptr(int32(len(merged) + len(cps))),
		IsTruncated:       &truncated,
		ContinuationToken: in.ContinuationToken,
		EncodingType:      in.EncodingType,
	}
	if truncated {
		out.NextContinuationToken = backend.GetPtrFromString(encodeCompound(lastEmitted))
	}
	return out, nil
}

// ListObjects is the v1 sibling of ListObjectsV2: identical fan-out + k-way
// merge, but the resume cursor rides in Marker/NextMarker instead of the
// continuation token.
//
// v1 Marker is dual-purpose (a user-supplied start key OR a router-issued
// compound token), so a Marker that is not a router token is treated as a
// user seed applied to every backend. Correct pagination therefore requires the
// client to echo the router's NextMarker verbatim — every standard AWS SDK
// paginator does (it prefers NextMarker over the last key). A client that
// fabricates its own raw Marker from the last returned key can miss keys across
// a page where one channel truncated before another's tail; that is an inherent
// v1 limitation. The restore path (cp/sync) uses ListObjectsV2, which has no
// such ambiguity.
func (r *Router) ListObjects(ctx context.Context, in *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	// Local-only fan-out sub-request (sentinel in Marker): serve this node's
	// channel only, never re-fan. See ListObjectsV2.
	if mkr := backend.GetStringFromPtr(in.Marker); mkr != "" {
		if cursor, ok := unwrapLocalOnly(mkr); ok {
			sub := *in
			sub.Marker = nil
			if cursor != "" {
				c := cursor
				sub.Marker = &c
			}
			return r.local.ListObjects(ctx, &sub)
		}
	}
	if r.n <= 1 {
		return r.local.ListObjects(ctx, in)
	}

	maxKeys, err := clampMaxKeys(in.MaxKeys)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	cursors := make([]string, r.n)
	if mk := backend.GetStringFromPtr(in.Marker); mk != "" {
		cs, ok, derr := decodeCompound(mk, r.n)
		if derr != nil {
			return s3response.ListObjectsResult{}, derr
		}
		if ok {
			cursors = cs
		} else {
			for i := range cursors {
				cursors[i] = mk // raw user marker: seed every backend
			}
		}
	}

	pages := make([]s3response.ListObjectsResult, r.n)
	if maxKeys > 0 {
		g, gctx := errgroup.WithContext(ctx)
		for i := 0; i < r.n; i++ {
			i := i
			be := r.backendFor(i)
			g.Go(func() error {
				sub := *in
				sub.Marker = nil
				if i == r.selfIdx {
					// local slot: raw exclusive marker straight to the posix backend
					if cursors[i] != "" {
						c := cursors[i]
						sub.Marker = &c
					}
				} else {
					// peer slot: local-only sentinel so the peer serves its own
					// channel and does NOT re-fan (would otherwise recurse)
					c := wrapLocalOnly(cursors[i])
					sub.Marker = &c
				}
				mk := maxKeys
				sub.MaxKeys = &mk
				res, lerr := be.ListObjects(gctx, &sub)
				if lerr != nil {
					return fmt.Errorf("list slot %d: %w", i, lerr)
				}
				pages[i] = res
				return nil
			})
		}
		if werr := g.Wait(); werr != nil {
			return s3response.ListObjectsResult{}, werr
		}
	}

	contents := make([][]s3response.Object, r.n)
	pageTrunc := make([]bool, r.n)
	for i := range pages {
		contents[i] = pages[i].Contents
		pageTrunc[i] = boolVal(pages[i].IsTruncated)
	}
	merged, lastEmitted, truncated := kwayMerge(contents, pageTrunc, cursors, maxKeys)

	var cps []types.CommonPrefix
	if hasDelimiter(in.Delimiter) {
		all := make([]types.CommonPrefix, 0)
		for i := range pages {
			all = append(all, pages[i].CommonPrefixes...)
		}
		cps = mergeCommonPrefixes(all)
	}

	out := s3response.ListObjectsResult{
		Name:           in.Bucket,
		Prefix:         in.Prefix,
		Marker:         in.Marker,
		Delimiter:      in.Delimiter,
		MaxKeys:        &maxKeys,
		Contents:       merged,
		CommonPrefixes: cps,
		IsTruncated:    &truncated,
		EncodingType:   in.EncodingType,
	}
	if truncated {
		out.NextMarker = backend.GetPtrFromString(encodeCompound(lastEmitted))
	}
	return out, nil
}

// kwayMerge merges N key-sorted object lists into a single key-sorted run of up
// to maxKeys objects. It returns:
//
//   - merged:      the emitted objects, globally key-sorted;
//   - lastEmitted: the per-backend resume cursor — the last key consumed from
//     each backend, carried forward from the seed cursor for any backend
//     nothing was consumed from (so it re-lists from the same point next page);
//   - truncated:   whether keys remain beyond this page (a backend has a
//     buffered-but-unemitted key, or its sub-page was itself truncated).
//
// Because the merge emits in strict global key order, everything emitted is <=
// the last emitted key and everything unemitted is > some backend's cursor —
// the per-backend cursors are what make resume lossless even when one channel's
// sub-page truncated earlier than another's.
//
// Placement is disjoint, so backends never share a key. The equal-key dedup is
// a defensive guard that emits a shared key once (and would indicate a
// placement bug); it is intra-page only, which disjoint placement makes
// sufficient.
func kwayMerge(contents [][]s3response.Object, pageTrunc []bool, cursors []string, maxKeys int32) (merged []s3response.Object, lastEmitted []string, truncated bool) {
	n := len(contents)
	heads := make([]int, n)
	lastEmitted = append([]string(nil), cursors...)

	var prevKey string
	havePrev := false
	for int32(len(merged)) < maxKeys {
		minI := -1
		var minKey string
		for i := 0; i < n; i++ {
			if heads[i] >= len(contents[i]) {
				continue
			}
			k := backend.GetStringFromPtr(contents[i][heads[i]].Key)
			if minI == -1 || k < minKey {
				minI, minKey = i, k
			}
		}
		if minI == -1 {
			break // every backend's buffer drained
		}
		obj := contents[minI][heads[minI]]
		heads[minI]++
		lastEmitted[minI] = minKey
		if havePrev && minKey == prevKey {
			continue // duplicate key across backends: consume but emit once
		}
		merged = append(merged, obj)
		prevKey, havePrev = minKey, true
	}

	for i := 0; i < n; i++ {
		if heads[i] < len(contents[i]) || pageTrunc[i] {
			truncated = true
			break
		}
	}
	return merged, lastEmitted, truncated
}

// mergeCommonPrefixes concatenates, dedups (a delimiter-derived prefix can be
// produced by several channels), and key-sorts common prefixes.
func mergeCommonPrefixes(all []types.CommonPrefix) []types.CommonPrefix {
	if len(all) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(all))
	out := make([]types.CommonPrefix, 0, len(all))
	for _, cp := range all {
		k := backend.GetStringFromPtr(cp.Prefix)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, cp)
	}
	sort.Slice(out, func(a, b int) bool {
		return backend.GetStringFromPtr(out[a].Prefix) < backend.GetStringFromPtr(out[b].Prefix)
	})
	return out
}

// clampMaxKeys applies the S3 semantics: default 1000, hard-capped at 1000,
// negative rejected.
func clampMaxKeys(in *int32) (int32, error) {
	if in == nil {
		return maxListKeys, nil
	}
	if *in < 0 {
		return 0, s3err.InvalidArgumentError{
			Description:  "max-keys cannot be negative",
			ArgumentName: "max-keys",
		}
	}
	if *in < maxListKeys {
		return *in, nil
	}
	return maxListKeys, nil
}

func hasDelimiter(d *string) bool { return d != nil && *d != "" }
func int32Ptr(v int32) *int32     { return &v }
func boolVal(b *bool) bool        { return b != nil && *b }
