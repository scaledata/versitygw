package grant

// The CRDB DSN is built the same way in every build — it needs no pgx — so it
// lives in this untagged file rather than being duplicated across the
// otter_crdb / !otter_crdb source split. Callers reference these in both builds.

// DefaultCRDBCertDir is where CDM keeps the CockroachDB client certs on a node.
const DefaultCRDBCertDir = "/var/lib/rubrik/certs/cockroachdb"

// CRDBDSN builds the standard read-only DSN for the gateway: the rk_reader role
// over verify-full mTLS using the on-node cert paths. host is usually
// "localhost:26257" (the pod runs hostNetwork); db is the grants database.
func CRDBDSN(host, db string) string {
	return "postgresql://rk_reader@" + host + "/" + db + "?sslmode=verify-full" +
		"&sslrootcert=" + DefaultCRDBCertDir + "/ca.crt" +
		"&sslcert=" + DefaultCRDBCertDir + "/client.rk_reader.crt" +
		"&sslkey=" + DefaultCRDBCertDir + "/client.rk_reader.key"
}
