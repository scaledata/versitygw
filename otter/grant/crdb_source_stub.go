//go:build !otter_crdb

// This file provides the CRDB constructor seam in builds WITHOUT the otter_crdb
// tag, so callers (cmd/versitygw/otter.go) compile everywhere while the pgx
// dependency and the live DB code are pulled in only for the on-cluster build
// (go build -tags otter_crdb). The default binary and every unit test avoid pgx.

package grant

import (
	"context"
	"errors"
)

// NewCRDBSource returns an error: this binary was built without CRDB support.
// Rebuild with -tags otter_crdb on the cluster to enable the durable grant source.
// (Push-only operation still works — the resolver just has no lazy fallback/warm.)
func NewCRDBSource(_ context.Context, _ string) (Source, error) {
	return nil, errors.New("otter grant: built without CRDB support (rebuild with -tags otter_crdb)")
}
