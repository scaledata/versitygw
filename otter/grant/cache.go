package grant

import "sync"

// cacheKey is the grant identity on the hot path: (access key, bucket). ClientID
// is provenance and is deliberately NOT part of the key — Otter routes purely on
// the access key the client signs with.
type cacheKey struct {
	access string
	bucket string
}

// Cache is the in-memory grant map on the S3 hot path: every request is authorized
// and routed from here with no network round-trip. It is filled three ways — JFL
// push (Upsert), a lazy CRDB read on a miss, and a startup CRDB warm — and is NOT
// the source of truth (CRDB is), so it can always be rebuilt. Entries are versioned
// by Epoch (higher wins) and frozen per (access,bucket). Safe for concurrent use.
type Cache struct {
	mu     sync.RWMutex
	grants map[cacheKey]Grant
	// tombstones records, per key, the highest epoch at which a revoke was seen.
	// It outlives the grant-map entry so a delayed/replayed/out-of-order Upsert
	// of an equal-or-older grant cannot resurrect a revoked key after the entry
	// is gone (or when the revoke arrives before the grant was ever cached). A
	// tombstone is cleared once a strictly-newer grant legitimately re-creates
	// the key, so a revoked-then-regranted key does not retain one; only keys
	// revoked and never regranted keep a tombstone — bounded by the number of
	// permanently-decommissioned (access,bucket) pairs.
	tombstones map[cacheKey]int64
}

// NewCache returns an empty grant cache.
func NewCache() *Cache {
	return &Cache{
		grants:     make(map[cacheKey]Grant),
		tombstones: make(map[cacheKey]int64),
	}
}

// Get returns the cached grant for (access,bucket), if present.
func (c *Cache) Get(access, bucket string) (Grant, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	g, ok := c.grants[cacheKey{access, bucket}]
	return g, ok
}

// Upsert applies g under the epoch guard: it wins only if strictly newer than the
// cached grant for the same (access,bucket). It returns whether it was accepted and
// the epoch now in effect. An equal-or-lower epoch is an idempotent no-op
// (accepted=false, current=cached epoch) — this is what makes a re-pushed, replayed,
// or out-of-order grant safe.
func (c *Cache) Upsert(g Grant) (accepted bool, current int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey{g.AccessKeyID, g.Bucket}
	if cur, ok := c.grants[k]; ok && g.Epoch <= cur.Epoch {
		return false, cur.Epoch
	}
	// Refuse to resurrect a revoked key: a grant at or below the revoke epoch is
	// stale even though the map entry is gone. A strictly-newer grant is a
	// legitimate re-grant and clears the tombstone.
	if ts, ok := c.tombstones[k]; ok && g.Epoch <= ts {
		return false, ts
	}
	delete(c.tombstones, k)
	c.grants[k] = g
	return true, g.Epoch
}

// Delete removes the grant for (access,bucket) if the supplied epoch is at least
// the cached one, so a stale revoke can't drop a newer grant. It returns whether an
// entry was removed. (The revoke path; the durable CRDB delete is authoritative.)
//
// A tombstone at the revoke epoch is always recorded (even when no entry is
// present) so a later out-of-order Upsert of the revoked-or-older grant cannot
// resurrect the key. See the Cache.tombstones doc for lifetime.
func (c *Cache) Delete(access, bucket string, epoch int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey{access, bucket}
	if epoch > c.tombstones[k] {
		c.tombstones[k] = epoch
	}
	if cur, ok := c.grants[k]; ok && epoch >= cur.Epoch {
		delete(c.grants, k)
		return true
	}
	return false
}

// Snapshot returns a copy of all cached grants (for diagnostics and tests).
func (c *Cache) Snapshot() []Grant {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Grant, 0, len(c.grants))
	for _, g := range c.grants {
		out = append(out, g)
	}
	return out
}
