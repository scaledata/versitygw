//go:build !otter_crdb

package grant

import (
	"context"
	"strings"
	"testing"
)

// Without the otter_crdb tag, NewCRDBSource must fail clearly (no live DB support)
// rather than pretend to connect. Push-only operation still works; only the lazy
// fallback / warm are unavailable.
func TestNewCRDBSourceStubErrors(t *testing.T) {
	s, err := NewCRDBSource(context.Background(), CRDBDSN("localhost:26257", "otter"))
	if err == nil {
		t.Fatal("stub NewCRDBSource should return an error, got nil")
	}
	if s != nil {
		t.Fatalf("stub NewCRDBSource should return a nil Source, got %v", s)
	}
	if !strings.Contains(err.Error(), "without CRDB support") {
		t.Fatalf("error should explain the missing build tag, got %v", err)
	}
}
