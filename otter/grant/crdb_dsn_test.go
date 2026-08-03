package grant

import "testing"

// wantDSN is the exact read-only DSN CRDBDSN must produce. Asserting the literal
// pins the on-node cert layout and mTLS mode the proven read path depends on.
const wantDSN = "postgresql://rk_reader@localhost:26257/otter?sslmode=verify-full" +
	"&sslrootcert=/var/lib/rubrik/certs/cockroachdb/ca.crt" +
	"&sslcert=/var/lib/rubrik/certs/cockroachdb/client.rk_reader.crt" +
	"&sslkey=/var/lib/rubrik/certs/cockroachdb/client.rk_reader.key"

func TestCRDBDSN(t *testing.T) {
	if got := CRDBDSN("localhost:26257", "otter"); got != wantDSN {
		t.Fatalf("CRDBDSN mismatch:\n got %q\nwant %q", got, wantDSN)
	}
}
