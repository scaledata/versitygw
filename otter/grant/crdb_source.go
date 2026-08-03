//go:build otter_crdb

// This file is compiled only with -tags otter_crdb, so the default binary and all
// unit tests stay free of the pgx dependency. Build on a Brik node with:
//
//	go build -tags otter_crdb ./cmd/versitygw
//
// The read path (rk_reader role, verify-full mTLS, on-node certs) is exactly the
// one proven end-to-end in otter-jfl-crdb-comms-poc.md.

package grant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// crdbPingTimeout bounds the initial connectivity check so an unreachable CRDB or
// missing/invalid on-node certs fail startup fast instead of hanging when the
// caller passes an undeadlined context.
const crdbPingTimeout = 10 * time.Second

// CRDBSource reads grants from CockroachDB — the durable source of truth for the
// control plane. It uses the read-only rk_reader role over cluster mTLS.
type CRDBSource struct {
	pool *pgxpool.Pool
}

var _ Source = (*CRDBSource)(nil)

// NewCRDBSource opens a pooled, verified connection to CRDB using dsn (see CRDBDSN).
func NewCRDBSource(ctx context.Context, dsn string) (Source, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("otter grant: connect CRDB: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, crdbPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("otter grant: ping CRDB: %w", err)
	}
	return &CRDBSource{pool: pool}, nil
}

// Close releases the connection pool.
func (s *CRDBSource) Close() { s.pool.Close() }

const grantCols = `client_id, access_key_id, bucket, epoch, n, slots, policy, lease_expiry, active`

// Lookup returns the grant for (access,bucket). CRDB's primary key is
// (client_id,bucket) per the plan, so this (access_key_id,bucket) lookup relies on
// a unique secondary index on (access_key_id,bucket) — see the schema in
// .plans/otter-jfl-control-plane-plan.md §3.
func (s *CRDBSource) Lookup(ctx context.Context, access, bucket string) (Grant, bool, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+grantCols+` FROM otter_channel_grant WHERE access_key_id=$1 AND bucket=$2`,
		access, bucket)
	g, err := scanGrant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Grant{}, false, nil
	}
	if err != nil {
		return Grant{}, false, fmt.Errorf("otter grant: lookup (%s,%s): %w", access, bucket, err)
	}
	return g, true, nil
}

// ActiveGrants returns every active grant, for the startup cache warm.
func (s *CRDBSource) ActiveGrants(ctx context.Context) ([]Grant, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+grantCols+` FROM otter_channel_grant WHERE active = true`)
	if err != nil {
		return nil, fmt.Errorf("otter grant: active grants: %w", err)
	}
	defer rows.Close()
	var out []Grant
	for rows.Next() {
		g, err := scanGrant(rows)
		if err != nil {
			// Skip a single corrupt/unparseable row rather than failing the
			// entire warm: one bad grant must not 503-storm every other client
			// on this node at startup. The bad row stays absent (lazy Resolve
			// will retry it) and the operator sees the log line.
			fmt.Fprintf(os.Stderr, "warn: otter grant: skipping unparseable active grant: %v\n", err)
			continue
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// scanRow is the subset of pgx.Row / pgx.Rows that scanGrant needs.
type scanRow interface {
	Scan(dest ...any) error
}

func scanGrant(row scanRow) (Grant, error) {
	var (
		g         Grant
		n         int
		slotsJSON []byte
		policy    string
	)
	if err := row.Scan(&g.ClientID, &g.AccessKeyID, &g.Bucket, &g.Epoch, &n,
		&slotsJSON, &policy, &g.LeaseExpiryUnixS, &g.Active); err != nil {
		return Grant{}, err
	}
	if err := json.Unmarshal(slotsJSON, &g.Slots); err != nil {
		return Grant{}, fmt.Errorf("decode slots for (%s,%s): %w", g.AccessKeyID, g.Bucket, err)
	}
	if n != len(g.Slots) {
		return Grant{}, fmt.Errorf("grant (%s,%s): n=%d but %d slots", g.AccessKeyID, g.Bucket, n, len(g.Slots))
	}
	// Slots are "ordered slot -> owner": Place(key) returns a bare index into
	// Slots, so a slot whose own index disagrees with its array position would
	// silently route to the wrong node/channel. Reject it alongside the n check.
	for i := range g.Slots {
		if g.Slots[i].Slot != i {
			return Grant{}, fmt.Errorf("grant (%s,%s): slot at position %d declares slot=%d — slots must be ordered by index",
				g.AccessKeyID, g.Bucket, i, g.Slots[i].Slot)
		}
	}
	g.Policy = parsePolicy(policy)
	return g, nil
}

// parsePolicy maps the CRDB policy string to a Policy (defaulting to KEY_HASH).
// Keep this in sync with the Policy enum: a policy value present in the enum but
// missing a case here is silently downgraded to KEY_HASH, which mis-routes every
// object under a grant carrying it.
func parsePolicy(s string) Policy {
	if s == PolicyOrdinalRoundRobin.String() {
		return PolicyOrdinalRoundRobin
	}
	return PolicyKeyHash
}
