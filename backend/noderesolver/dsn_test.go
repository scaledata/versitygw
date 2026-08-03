//go:build otter_crdb

package noderesolver

import "testing"

func TestCrdbDSN(t *testing.T) {
	got := crdbDSN("localhost:26257", "sd")
	want := "postgresql://rk_reader@localhost:26257/sd?sslmode=verify-full" +
		"&sslrootcert=/var/lib/rubrik/certs/cockroachdb/ca.crt" +
		"&sslcert=/var/lib/rubrik/certs/cockroachdb/client.rk_reader.crt" +
		"&sslkey=/var/lib/rubrik/certs/cockroachdb/client.rk_reader.key"
	if got != want {
		t.Fatalf("crdbDSN mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}
