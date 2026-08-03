//go:build otter_crdb

// Package noderesolver resolves CDM node IDs to data IP addresses by querying
// the sd.node table in CockroachDB. It is compiled only with -tags otter_crdb.
package noderesolver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultCRDBCertDir is where CDM keeps the CockroachDB client certs on a node.
const defaultCRDBCertDir = "/var/lib/rubrik/certs/cockroachdb"

// crdbOpTimeout bounds each CRDB operation (ping, lookup) so an unreachable or
// slow CockroachDB cannot hang the caller indefinitely when it passes an
// undeadlined context (e.g. context.Background()). This resolver sits on the S3
// forward-routing path, so it must fail fast rather than stall.
const crdbOpTimeout = 10 * time.Second

// localClusterID is the sentinel cluster_id CDM uses in sd.node for the local
// cluster's own nodes (as opposed to remote/replication-target clusters).
const localClusterID = "cluster"

// crdbDSN builds the rk_reader mTLS DSN for host/db. Kept local (rather than
// importing otter/grant's CRDBDSN) so this package has no dependency on the
// grant control-plane package — noderesolver only needs peer-IP resolution,
// which the data-plane router requires regardless of how grants arrive.
func crdbDSN(host, db string) string {
	return "postgresql://rk_reader@" + host + "/" + db + "?sslmode=verify-full" +
		"&sslrootcert=" + defaultCRDBCertDir + "/ca.crt" +
		"&sslcert=" + defaultCRDBCertDir + "/client.rk_reader.crt" +
		"&sslkey=" + defaultCRDBCertDir + "/client.rk_reader.key"
}

// Resolver looks up data IPs for CDM node IDs from sd.node.
type Resolver struct {
	pool *pgxpool.Pool
}

// New opens a pooled CRDB connection using the same cert-based DSN convention
// as the grant control plane. host is "localhost:26257" on a CDM node; db is
// typically "sd".
func New(ctx context.Context, host, db string) (*Resolver, error) {
	dsn := crdbDSN(host, db)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("noderesolver: connect CRDB: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, crdbOpTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("noderesolver: ping CRDB: %w", err)
	}
	return &Resolver{pool: pool}, nil
}

// Close releases the connection pool. Nil-safe so a caller that ignored a New()
// error and holds a nil *Resolver does not panic (matching the stub build).
func (r *Resolver) Close() {
	if r == nil || r.pool == nil {
		return
	}
	r.pool.Close()
}

// DataIP returns the data_ip_address for nodeId from sd.node (the local cluster).
// Returns ("", false, nil) when the node is not found.
func (r *Resolver) DataIP(ctx context.Context, nodeId string) (string, bool, error) {
	if r == nil || r.pool == nil {
		return "", false, fmt.Errorf("noderesolver: nil resolver")
	}
	qctx, cancel := context.WithTimeout(ctx, crdbOpTimeout)
	defer cancel()
	var ip string
	err := r.pool.QueryRow(qctx,
		`SELECT data_ip_address::text FROM sd.node WHERE cluster_id = $1 AND node_id = $2`,
		localClusterID, nodeId,
	).Scan(&ip)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("noderesolver: lookup %s: %w", nodeId, err)
	}
	return ip, true, nil
}
