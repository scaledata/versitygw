//go:build !otter_crdb

// Package noderesolver provides a no-op stub when built without -tags otter_crdb.
package noderesolver

import "context"

// Resolver is a no-op stub; real implementation requires -tags otter_crdb.
type Resolver struct{}

// New returns nil — CRDB is unavailable without the otter_crdb build tag.
func New(_ context.Context, _, _ string) (*Resolver, error) { return nil, nil }

// Close is a no-op.
func (r *Resolver) Close() {}

// DataIP always returns not-found without CRDB.
func (r *Resolver) DataIP(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}
