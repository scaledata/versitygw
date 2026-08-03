package grant

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/versity/versitygw/s3err"
)

// ---- test helpers ----------------------------------------------------------

// grantFor builds a minimal active grant with n round-robin slots.
func grantFor(access, bucket string, epoch int64, n int, policy Policy) Grant {
	slots := make([]Slot, n)
	for i := range slots {
		slots[i] = Slot{Slot: i, NodeID: fmt.Sprintf("node%d", i),
			Endpoint: fmt.Sprintf("http://10.0.0.%d:9000", i), ChannelPath: fmt.Sprintf("/sd/mount/ch_%d", i)}
	}
	return Grant{ClientID: "clientA", AccessKeyID: access, Bucket: bucket,
		Epoch: epoch, Slots: slots, Policy: policy, Active: true}
}

// walSeg builds a 24-hex PG WAL segment filename: timeline(8) logid(8) seg(8).
func walSeg(tli, logid, seg uint32) string {
	return fmt.Sprintf("%08X%08X%08X", tli, logid, seg)
}

// ---- placement (placeInSet) ------------------------------------------------

// ORDINAL_ROUND_ROBIN: consecutive WAL segments must land on distinct, rotating
// slots so a single monotonic stream spreads across all N channels of the set.
func TestPlaceOrdinalRoundRobin(t *testing.T) {
	n := 4
	g := grantFor("otter", "wal", 1, n, PolicyOrdinalRoundRobin)
	for start := uint32(0x10); start < 0x40; start += 7 {
		seen := map[int]bool{}
		for i := 0; i < n; i++ {
			seen[g.Place(walSeg(1, 0, start+uint32(i)))] = true
		}
		if len(seen) != n {
			t.Fatalf("start=%#x: %d consecutive segs hit %d distinct slots, want %d", start, n, len(seen), n)
		}
	}
}

// A non-ordinal key under ORDINAL_ROUND_ROBIN falls back to the hash (stable, in
// range) rather than always landing on slot 0.
func TestPlaceOrdinalFallsBackToHash(t *testing.T) {
	g := grantFor("otter", "bkt", 1, 4, PolicyOrdinalRoundRobin)
	a, b := g.Place("not-an-ordinal"), g.Place("not-an-ordinal")
	if a != b {
		t.Fatalf("fallback placement not deterministic: %d vs %d", a, b)
	}
	if a < 0 || a >= 4 {
		t.Fatalf("fallback slot %d out of range [0,4)", a)
	}
}

// KEY_HASH is deterministic and spreads arbitrary keys across every slot.
func TestPlaceKeyHashDeterministicAndSpreads(t *testing.T) {
	n := 4
	g := grantFor("otter", "bkt", 1, n, PolicyKeyHash)
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("randomkey-%d", i)
		if g.Place(k) != g.Place(k) {
			t.Fatalf("KEY_HASH placement of %q not deterministic", k)
		}
		seen[g.Place(k)] = true
	}
	if len(seen) != n {
		t.Fatalf("KEY_HASH reached only %d of %d slots", len(seen), n)
	}
}

// A single-channel set always routes to slot 0 regardless of policy/key.
func TestPlaceSingleSlot(t *testing.T) {
	for _, p := range []Policy{PolicyKeyHash, PolicyOrdinalRoundRobin} {
		g := grantFor("otter", "bkt", 1, 1, p)
		if s := g.Place("anything"); s != 0 {
			t.Fatalf("policy %v: single-slot set routed to %d, want 0", p, s)
		}
	}
}

// ---- cache epoch guard -----------------------------------------------------

func TestCacheEpochGuard(t *testing.T) {
	c := NewCache()

	if acc, cur := c.Upsert(grantFor("otter", "wal", 5, 3, PolicyKeyHash)); !acc || cur != 5 {
		t.Fatalf("first upsert: accepted=%v current=%d, want true/5", acc, cur)
	}
	// A newer epoch wins.
	if acc, cur := c.Upsert(grantFor("otter", "wal", 6, 2, PolicyKeyHash)); !acc || cur != 6 {
		t.Fatalf("newer epoch: accepted=%v current=%d, want true/6", acc, cur)
	}
	// An equal epoch is a no-op (idempotent re-push).
	if acc, cur := c.Upsert(grantFor("otter", "wal", 6, 2, PolicyKeyHash)); acc || cur != 6 {
		t.Fatalf("equal epoch: accepted=%v current=%d, want false/6", acc, cur)
	}
	// A stale (lower) epoch is rejected and does not clobber the cached grant.
	if acc, cur := c.Upsert(grantFor("otter", "wal", 4, 9, PolicyKeyHash)); acc || cur != 6 {
		t.Fatalf("stale epoch: accepted=%v current=%d, want false/6", acc, cur)
	}
	g, ok := c.Get("otter", "wal")
	if !ok || g.Epoch != 6 || g.N() != 2 {
		t.Fatalf("cached grant after guards: ok=%v epoch=%d n=%d, want true/6/2", ok, g.Epoch, g.N())
	}
}

func TestCacheDeleteEpochGuard(t *testing.T) {
	c := NewCache()
	c.Upsert(grantFor("otter", "wal", 6, 2, PolicyKeyHash))
	// A stale revoke must not drop a newer grant.
	if c.Delete("otter", "wal", 5) {
		t.Fatalf("stale revoke (epoch 5 < 6) should not delete")
	}
	if _, ok := c.Get("otter", "wal"); !ok {
		t.Fatalf("grant wrongly deleted by stale revoke")
	}
	// A current-or-newer revoke removes it.
	if !c.Delete("otter", "wal", 6) {
		t.Fatalf("revoke at current epoch should delete")
	}
	if _, ok := c.Get("otter", "wal"); ok {
		t.Fatalf("grant still present after valid revoke")
	}
}

// TestCacheDeleteTombstone verifies a revoke leaves a tombstone that blocks a
// delayed/replayed Upsert of the revoked-or-older grant from resurrecting it,
// while a strictly-newer re-grant clears the tombstone.
func TestCacheDeleteTombstone(t *testing.T) {
	c := NewCache()
	c.Upsert(grantFor("otter", "wal", 5, 2, PolicyKeyHash))

	if !c.Delete("otter", "wal", 6) {
		t.Fatalf("revoke at epoch 6 should delete the epoch-5 grant")
	}
	// A delayed re-delivery of the original epoch-5 grant must NOT resurrect it.
	if accepted, _ := c.Upsert(grantFor("otter", "wal", 5, 2, PolicyKeyHash)); accepted {
		t.Fatalf("stale epoch-5 Upsert resurrected a grant revoked at epoch 6")
	}
	if _, ok := c.Get("otter", "wal"); ok {
		t.Fatalf("revoked grant present after replayed stale Upsert")
	}
	// An Upsert exactly at the revoke epoch is also refused.
	if accepted, _ := c.Upsert(grantFor("otter", "wal", 6, 2, PolicyKeyHash)); accepted {
		t.Fatalf("epoch-6 Upsert (== revoke epoch) should be refused")
	}
	// A strictly-newer re-grant is accepted and clears the tombstone.
	if accepted, _ := c.Upsert(grantFor("otter", "wal", 7, 2, PolicyKeyHash)); !accepted {
		t.Fatalf("epoch-7 re-grant should be accepted")
	}
	if _, ok := c.Get("otter", "wal"); !ok {
		t.Fatalf("epoch-7 re-grant not present")
	}
}

// TestCacheDeleteTombstoneBeforeGrant covers a revoke that arrives before the
// key was ever cached (fan-out ordering): a later stale Upsert must still be
// refused, and a strictly-newer one accepted.
func TestCacheDeleteTombstoneBeforeGrant(t *testing.T) {
	c := NewCache()
	c.Delete("otter", "wal", 6) // revoke first; key never cached
	if accepted, _ := c.Upsert(grantFor("otter", "wal", 5, 2, PolicyKeyHash)); accepted {
		t.Fatalf("stale Upsert accepted despite prior revoke at epoch 6")
	}
	if accepted, _ := c.Upsert(grantFor("otter", "wal", 7, 2, PolicyKeyHash)); !accepted {
		t.Fatalf("newer Upsert (epoch 7 > revoke 6) should be accepted")
	}
}

// TestCacheConcurrent hammers the cache from many goroutines under -race to
// substantiate the documented "safe for concurrent use" claim.
func TestCacheConcurrent(t *testing.T) {
	c := NewCache()
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bucket := fmt.Sprintf("b%d", i%5)
			c.Upsert(grantFor("otter", bucket, int64(i), 2, PolicyKeyHash))
			c.Get("otter", bucket)
			c.Delete("otter", bucket, int64(i))
			c.Snapshot()
		}(i)
	}
	wg.Wait()
}

// ---- grant-miss error contract ---------------------------------------------

// A grant miss maps to exactly the S3 SlowDown/503 the design contract specifies.
func TestSlowDownCodeIs503(t *testing.T) {
	e := s3err.GetAPIError(s3err.ErrSlowDown)
	if e.HTTPStatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ErrSlowDown is HTTP %d, want 503", e.HTTPStatusCode)
	}
}
