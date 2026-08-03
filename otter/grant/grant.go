// Package grant implements Otter's control-plane grant model: the record JFL
// pushes to each gateway that says "which client (by S3 access key) may write
// which bucket, across which ordered channel-set." A grant is the unit of both
// authorization (is this key allowed on this bucket) and routing (which of the
// client's channels owns a given object key).
//
// This is the gateway (Otter) side of the JFL <-> Otter control plane described
// in otter-design-doc.md §14 and .plans/otter-jfl-control-plane-plan.md. It is
// deliberately transport- and storage-agnostic: the Thrift push handler calls
// Resolver.Upsert, and the durable CockroachDB source is injected as a Source, so
// the whole subsystem is unit-testable without a cluster.
package grant

import (
	"hash/fnv"
	"regexp"
	"strconv"
)

// Policy selects how an object key maps to a slot within the channel-set.
type Policy int

const (
	// PolicyKeyHash spreads any key evenly by fnv-1a(bucket+"/"+key) mod N.
	// It is the fallback for arbitrary key spaces.
	PolicyKeyHash Policy = iota
	// PolicyOrdinalRoundRobin routes a monotonic key space (e.g. Postgres WAL
	// segment names) by its trailing ordinal mod N, so consecutive objects land
	// on distinct slots. Keys without a recognized ordinal fall back to KeyHash.
	PolicyOrdinalRoundRobin
)

func (p Policy) String() string {
	switch p {
	case PolicyKeyHash:
		return "KEY_HASH"
	case PolicyOrdinalRoundRobin:
		return "ORDINAL_ROUND_ROBIN"
	default:
		return "UNKNOWN"
	}
}

// Slot is one channel in the ordered channel-set: slot index i owns the objects
// that place to i. NodeID/ChannelPath identify the AF2 channel; Endpoint is that
// slot's gateway, used to forward an object this node does not own.
type Slot struct {
	Slot        int    `json:"slot"`
	NodeID      string `json:"nodeId"`
	Endpoint    string `json:"endpoint"`
	ChannelPath string `json:"channelPath"`
}

// Grant is one control-plane record, keyed by (AccessKeyID, Bucket). It mirrors
// the Thrift OtterChannelGrant (poc0-jfl-thrift/otter_control.thrift) and the CRDB
// row (otter_channel_grant). ClientID is provenance only — Otter authorizes and
// routes purely on AccessKeyID.
type Grant struct {
	ClientID         string // logical client (snappable / DB-cluster id) — provenance only
	AccessKeyID      string // the SigV4 key the client signs with — the join key
	Bucket           string // the bucket this grant authorizes
	Epoch            int64  // monotonic version, frozen per (AccessKeyID,Bucket); higher wins
	Slots            []Slot // ordered slot -> owner: the channel-set (len == N)
	Policy           Policy
	LeaseExpiryUnixS int64 // 0 = no TTL; else a revoke-by-expiry backstop
	Active           bool  // false = soft-disabled
}

// N is the channel count. len(Slots) is the single source of truth; the Thrift/CRDB
// boundary validates that the wire's explicit n matches it (see crdb_source.go).
func (g Grant) N() int { return len(g.Slots) }

var walName = regexp.MustCompile(`^[0-9A-Fa-f]{24}$`)

// Place returns the slot index in [0,N) that owns key, per the grant's policy. It
// is deterministic and stable: the read path recomputes the same slot, so there is
// no per-object location map. N and the slot ordering are frozen for the grant's
// epoch — a changed set would mis-locate existing objects (grow via a new
// epoch/bucket instead). This generalizes the router's global place() to a
// per-client channel-set (the "placeInSet" the design doc calls for).
//
// NOTE: unlike the router's global place(), this intentionally does NOT carry
// the Postgres relation-segment case (route "<oid>[.segNum]" by hash(dir+oid)+
// segNum for per-relation segment affinity). A grant has one Policy per bucket,
// so base/relation files land under KEY_HASH's plain hash spread. If per-client
// relation-segment affinity is ever needed, add it as a new Policy value rather
// than folding it into these two — do not silently diverge from the router.
func (g Grant) Place(key string) int {
	n := g.N()
	if n <= 1 {
		return 0
	}
	if g.Policy == PolicyOrdinalRoundRobin {
		if slot, ok := ordinalSlot(key, n); ok {
			return slot
		}
		// A key with no recognized ordinal falls through to the hash so the grant
		// still places it somewhere stable rather than always slot 0.
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(g.Bucket + "/" + key))
	return int(h.Sum64() % uint64(n))
}

// ordinalSlot extracts a monotonic ordinal from a key's basename and returns its
// slot. It recognizes Postgres WAL segment names (24 hex chars): the trailing 16
// hex chars are (logid<<32 | seg), which increments by 1 per segment, so
// consecutive segments round-robin across the set.
func ordinalSlot(key string, n int) (int, bool) {
	base := key
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			base = key[i+1:]
			break
		}
	}
	if walName.MatchString(base) {
		if ord, err := strconv.ParseUint(base[8:], 16, 64); err == nil {
			return int(ord % uint64(n)), true
		}
	}
	return 0, false
}
