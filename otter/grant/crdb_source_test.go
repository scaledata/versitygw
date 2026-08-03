//go:build otter_crdb

package grant

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakeRow is an in-memory scanRow: it assigns vals into Scan's destination
// pointers in order, standing in for a pgx row without a live database.
type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.vals) {
		return fmt.Errorf("fakeRow: %d dest but %d vals", len(dest), len(r.vals))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = r.vals[i].(string)
		case *int:
			*p = r.vals[i].(int)
		case *int64:
			*p = r.vals[i].(int64)
		case *[]byte:
			*p = r.vals[i].([]byte)
		case *bool:
			*p = r.vals[i].(bool)
		default:
			return fmt.Errorf("fakeRow: unsupported dest type %T at %d", d, i)
		}
	}
	return nil
}

// validScanVals returns a well-formed grant row: 9 columns in scanGrant's order,
// with n (2) matching the two encoded slots and an ORDINAL_ROUND_ROBIN policy.
func validScanVals() []any {
	return []any{
		"clientA", "keyA", "wal", int64(5), 2,
		[]byte(`[{"slot":0,"nodeId":"n0","endpoint":"http://10.0.0.0:9000","channelPath":"/sd/ch0"},` +
			`{"slot":1,"nodeId":"n1","endpoint":"http://10.0.0.1:9000","channelPath":"/sd/ch1"}]`),
		"ORDINAL_ROUND_ROBIN", int64(0), true,
	}
}

func TestScanGrantValid(t *testing.T) {
	g, err := scanGrant(fakeRow{vals: validScanVals()})
	if err != nil {
		t.Fatalf("scanGrant valid row: %v", err)
	}
	if g.ClientID != "clientA" || g.AccessKeyID != "keyA" || g.Bucket != "wal" {
		t.Fatalf("scanned identity wrong: %+v", g)
	}
	if g.Epoch != 5 || g.N() != 2 || !g.Active {
		t.Fatalf("scanned epoch/n/active wrong: epoch=%d n=%d active=%v", g.Epoch, g.N(), g.Active)
	}
	if g.Policy != PolicyOrdinalRoundRobin {
		t.Fatalf("policy = %v, want ORDINAL_ROUND_ROBIN", g.Policy)
	}
	if g.Slots[1].Endpoint != "http://10.0.0.1:9000" {
		t.Fatalf("slot decode wrong: %+v", g.Slots)
	}
}

// The n column must equal the number of decoded slots — a mismatch is a corrupt
// row and must be rejected, not silently served with the wrong channel count.
func TestScanGrantNMismatch(t *testing.T) {
	vals := validScanVals()
	vals[4] = 3 // n=3 but only 2 slots encoded
	_, err := scanGrant(fakeRow{vals: vals})
	if err == nil || !strings.Contains(err.Error(), "n=3 but 2 slots") {
		t.Fatalf("expected n-mismatch error, got %v", err)
	}
}

// scanGrant must reject slots whose declared index disagrees with their array
// position: Place() indexes Slots directly, so a misordered slot silently
// misroutes every key that hashes there.
func TestScanGrantSlotIndexMismatch(t *testing.T) {
	vals := validScanVals()
	vals[5] = []byte(`[{"slot":1,"nodeId":"n1","endpoint":"http://10.0.0.1:9000","channelPath":"/sd/ch1"},` +
		`{"slot":0,"nodeId":"n0","endpoint":"http://10.0.0.0:9000","channelPath":"/sd/ch0"}]`)
	_, err := scanGrant(fakeRow{vals: vals})
	if err == nil || !strings.Contains(err.Error(), "slot at position 0 declares slot=1") {
		t.Fatalf("expected slot-index-mismatch error, got %v", err)
	}
}

func TestScanGrantBadSlotsJSON(t *testing.T) {
	vals := validScanVals()
	vals[5] = []byte("{not-json")
	_, err := scanGrant(fakeRow{vals: vals})
	if err == nil || !strings.Contains(err.Error(), "decode slots") {
		t.Fatalf("expected slots-decode error, got %v", err)
	}
}

// A row Scan error is returned as-is (the caller wraps it with context).
func TestScanGrantScanError(t *testing.T) {
	boom := errors.New("boom")
	_, err := scanGrant(fakeRow{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("expected the scan error to surface, got %v", err)
	}
}

func TestParsePolicy(t *testing.T) {
	cases := map[string]Policy{
		"ORDINAL_ROUND_ROBIN": PolicyOrdinalRoundRobin,
		"KEY_HASH":            PolicyKeyHash,
		"UNKNOWN":             PolicyKeyHash, // unrecognized defaults to KEY_HASH
		"":                    PolicyKeyHash,
	}
	for in, want := range cases {
		if got := parsePolicy(in); got != want {
			t.Fatalf("parsePolicy(%q) = %v, want %v", in, got, want)
		}
	}
}
