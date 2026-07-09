package router

import (
	"fmt"
	"testing"
)

// walSeg builds a 24-hex PG WAL segment filename: timeline(8) logid(8) seg(8).
func walSeg(tli, logid, seg uint32) string {
	return fmt.Sprintf("%08X%08X%08X", tli, logid, seg)
}

// Consecutive WAL segments must land on distinct, rotating slots (round-robin-
// like) so a single monotonic stream spreads across all N channels.
func TestPlaceWALRotates(t *testing.T) {
	n := 4
	for start := uint32(0x10); start < 0x40; start += 7 { // a few starting points
		got := make([]int, n)
		for i := 0; i < n; i++ {
			name := walSeg(1, 0, start+uint32(i))
			got[i] = place("wal", name, n)
		}
		seen := map[int]bool{}
		for _, s := range got {
			seen[s] = true
		}
		if len(seen) != n {
			t.Fatalf("start=%#x: %d consecutive WAL segs hit %d distinct slots (%v), want %d",
				start, n, len(seen), got, n)
		}
	}
}

// Placement must be deterministic — the read path recomputes the same slot.
func TestPlaceDeterministic(t *testing.T) {
	keys := []string{"wal/000000010000000000000016", "objectfoo", "a/b/c.dat", walSeg(1, 2, 0xAB)}
	for _, k := range keys {
		a := place("bkt", k, 4)
		b := place("bkt", k, 4)
		if a != b {
			t.Fatalf("place(%q) not deterministic: %d vs %d", k, a, b)
		}
		if a < 0 || a >= 4 {
			t.Fatalf("place(%q)=%d out of range [0,4)", k, a)
		}
	}
}

// A WAL key routes by its basename ordinal whether or not it carries a prefix.
func TestPlaceWALPrefixIgnored(t *testing.T) {
	name := walSeg(1, 0, 0x16)
	if place("wal", name, 4) != place("wal", "some/prefix/"+name, 4) {
		t.Fatalf("WAL placement should depend only on the segment basename ordinal")
	}
}

// Non-WAL keys fall back to fnv and still spread across slots.
func TestPlaceFnvFallbackSpreads(t *testing.T) {
	n := 4
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[place("bkt", fmt.Sprintf("randomkey-%d", i), n)] = true
	}
	if len(seen) != n {
		t.Fatalf("fnv fallback only reached %d of %d slots", len(seen), n)
	}
}

func TestPlaceSingleChannel(t *testing.T) {
	if place("bkt", "anything", 1) != 0 {
		t.Fatalf("n=1 must always route to slot 0")
	}
}
