//go:build !otter_crdb

// This file exercises the stub Resolver only (constructed as a bare zero
// value below). The real, otter_crdb-tagged Resolver holds a live
// *pgxpool.Pool; a zero-value instance of it would nil-panic on first use,
// so this test must not compile into that build.
package noderesolver

import (
	"context"
	"testing"
)

// TestStubDataIPNotFound verifies the stub (non-CRDB build) returns not-found.
func TestStubDataIPNotFound(t *testing.T) {
	r := &Resolver{}
	ip, found, err := r.DataIP(context.Background(), "some-node-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatalf("stub should return not-found, got ip=%s", ip)
	}
	if ip != "" {
		t.Fatalf("stub should return empty ip, got %q", ip)
	}
}

// TestStubNewReturnsNil verifies New() is a no-op in stub mode.
func TestStubNewReturnsNil(t *testing.T) {
	r, err := New(context.Background(), "localhost:26257", "sd")
	if err != nil {
		t.Fatalf("stub New should not error: %v", err)
	}
	if r == nil {
		return // expected in stub mode (nil returned)
	}
	r.Close() // no-op
}
