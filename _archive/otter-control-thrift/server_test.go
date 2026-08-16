package control

import (
	"context"
	"strings"
	"testing"
	"time"

	thrift "github.com/apache/thrift/lib/go/thrift"

	"github.com/versity/versitygw/otter/control/ottercontrol"
	"github.com/versity/versitygw/otter/grant"
)

// dialClient connects a real generated OtterControl client to addr over the same
// plain-TBinaryProtocol/buffered wire the server serves (and that PoC0's Scala
// SafeThrift client handshaked against).
func dialClient(t *testing.T, addr string) (*ottercontrol.OtterControlClient, func()) {
	t.Helper()
	proto := thrift.NewTBinaryProtocolFactoryConf(nil)
	trans, err := thrift.NewTBufferedTransportFactory(8192).GetTransport(thrift.NewTSocketConf(addr, nil))
	if err != nil {
		t.Fatalf("client transport: %v", err)
	}
	if err := trans.Open(); err != nil {
		t.Fatalf("client open %s: %v", addr, err)
	}
	return ottercontrol.NewOtterControlClientFactory(trans, proto), func() { _ = trans.Close() }
}

func sampleGrant(epoch int64) *ottercontrol.OtterChannelGrant {
	slots := []*ottercontrol.ChannelSlot{
		{Slot: 0, NodeID: "B", Endpoint: "http://10.0.0.1:9000", ChannelPath: "/sd/mount/ch_0"},
		{Slot: 1, NodeID: "C", Endpoint: "http://10.0.0.2:9000", ChannelPath: "/sd/mount/ch_1"},
		{Slot: 2, NodeID: "D", Endpoint: "http://10.0.0.3:9000", ChannelPath: "/sd/mount/ch_2"},
	}
	return &ottercontrol.OtterChannelGrant{
		ClientID: "clientA", AccessKeyID: "otter", Bucket: "wal", Epoch: epoch,
		N: int32(len(slots)), Slots: slots,
		Policy: ottercontrol.PlacementPolicy_ORDINAL_ROUND_ROBIN, Active: true,
	}
}

// End-to-end over the real Thrift wire: ping, then the full grant lifecycle
// (fresh upsert -> stale reject -> newer upsert -> stale revoke reject -> revoke),
// asserting the server's grant.Resolver cache reflects each step and the
// grant-driven mkdir hook fires only on a freshly-applied epoch.
func TestServerGrantLifecycleOverWire(t *testing.T) {
	var mkdirs []string
	resolver := grant.NewResolver(grant.WithUpsertHook(func(g grant.Grant) error {
		mkdirs = append(mkdirs, g.AccessKeyID+"/"+g.Bucket)
		return nil
	}))

	srv, err := NewServer(NewHandler(resolver), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Stop() }()

	c, closec := dialClient(t, srv.Addr().String())
	defer closec()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ping
	pr, err := c.Ping(ctx, "tester")
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if !strings.Contains(pr.Message, "hello tester") {
		t.Fatalf("ping message = %q", pr.Message)
	}

	// fresh upsert (epoch 5) -> accepted; cached; hook fired once
	up, err := c.UpsertChannelGrant(ctx, sampleGrant(5))
	if err != nil {
		t.Fatalf("upsert fresh: %v", err)
	}
	if !up.Accepted || up.CurrentEpoch != 5 {
		t.Fatalf("upsert fresh: accepted=%v current=%d, want true/5 (detail=%q)", up.Accepted, up.CurrentEpoch, up.Detail)
	}
	if g, ok := resolver.Cache().Get("otter", "wal"); !ok || g.Epoch != 5 || g.Policy != grant.PolicyOrdinalRoundRobin || g.N() != 3 {
		t.Fatalf("cache after fresh upsert: ok=%v grant=%+v", ok, g)
	}
	if len(mkdirs) != 1 || mkdirs[0] != "otter/wal" {
		t.Fatalf("mkdir hook fired %v, want [otter/wal]", mkdirs)
	}

	// stale upsert (epoch 4) -> rejected, reports the winning epoch, no re-mkdir
	up, err = c.UpsertChannelGrant(ctx, sampleGrant(4))
	if err != nil {
		t.Fatalf("upsert stale: %v", err)
	}
	if up.Accepted || up.CurrentEpoch != 5 || !strings.Contains(up.Detail, "stale") {
		t.Fatalf("upsert stale: accepted=%v current=%d detail=%q, want false/5/stale", up.Accepted, up.CurrentEpoch, up.Detail)
	}
	if len(mkdirs) != 1 {
		t.Fatalf("stale upsert must not re-run the mkdir hook; got %v", mkdirs)
	}

	// newer upsert (epoch 6) -> accepted
	up, err = c.UpsertChannelGrant(ctx, sampleGrant(6))
	if err != nil {
		t.Fatalf("upsert newer: %v", err)
	}
	if !up.Accepted || up.CurrentEpoch != 6 {
		t.Fatalf("upsert newer: accepted=%v current=%d, want true/6", up.Accepted, up.CurrentEpoch)
	}

	// stale revoke (epoch 5 < 6) -> no-op; grant still cached
	rv, err := c.RevokeChannelGrant(ctx, &ottercontrol.RevokeRequest{AccessKeyID: "otter", Bucket: "wal", Epoch: 5})
	if err != nil {
		t.Fatalf("revoke stale: %v", err)
	}
	if rv.Revoked {
		t.Fatalf("stale revoke (epoch 5 < 6) should be a no-op, got revoked=true")
	}
	if _, ok := resolver.Cache().Get("otter", "wal"); !ok {
		t.Fatalf("grant wrongly dropped by a stale revoke")
	}

	// current revoke (epoch 6) -> removed
	rv, err = c.RevokeChannelGrant(ctx, &ottercontrol.RevokeRequest{AccessKeyID: "otter", Bucket: "wal", Epoch: 6})
	if err != nil {
		t.Fatalf("revoke current: %v", err)
	}
	if !rv.Revoked {
		t.Fatalf("revoke at current epoch should remove the grant (detail=%q)", rv.Detail)
	}
	if _, ok := resolver.Cache().Get("otter", "wal"); ok {
		t.Fatalf("grant still cached after a valid revoke")
	}
}

// A malformed grant (n != len(slots)) is rejected with a clear detail, not an
// RPC-level exception.
func TestServerRejectsMalformedGrant(t *testing.T) {
	resolver := grant.NewResolver()
	srv, err := NewServer(NewHandler(resolver), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Stop() }()

	c, closec := dialClient(t, srv.Addr().String())
	defer closec()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bad := sampleGrant(1)
	bad.N = 99 // lie about the channel count
	up, err := c.UpsertChannelGrant(ctx, bad)
	if err != nil {
		t.Fatalf("upsert malformed returned an RPC error (want a typed reject): %v", err)
	}
	if up.Accepted || !strings.Contains(up.Detail, "n=99") {
		t.Fatalf("malformed grant: accepted=%v detail=%q, want false + n-mismatch detail", up.Accepted, up.Detail)
	}
	if _, ok := resolver.Cache().Get("otter", "wal"); ok {
		t.Fatalf("malformed grant must not be cached")
	}
}
