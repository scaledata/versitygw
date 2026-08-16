package controlgrpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/versity/versitygw/otter/controlgrpc/ottercontrolpb"
	"github.com/versity/versitygw/otter/grant"
)

// startServer stands up a plaintext gRPC server on a random loopback port and
// returns a connected client + cleanup.
func startServer(t *testing.T, resolver *grant.Resolver) (pb.OtterControlServiceClient, func()) {
	t.Helper()
	srv, err := NewServer(NewHandler(resolver), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go func() { _ = srv.Serve() }()
	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial %s: %v", srv.Addr().String(), err)
	}
	return pb.NewOtterControlServiceClient(conn), func() { _ = conn.Close(); srv.Stop() }
}

func sampleGrant(epoch int64) *pb.OtterChannelGrant {
	slots := []*pb.ChannelSlot{
		{Slot: 0, NodeId: "B", Endpoint: "http://10.0.0.1:9000", ChannelPath: "/sd/mount/ch_0"},
		{Slot: 1, NodeId: "C", Endpoint: "http://10.0.0.2:9000", ChannelPath: "/sd/mount/ch_1"},
		{Slot: 2, NodeId: "D", Endpoint: "http://10.0.0.3:9000", ChannelPath: "/sd/mount/ch_2"},
	}
	return &pb.OtterChannelGrant{
		ClientId: "clientA", AccessKeyId: "otter", Bucket: "wal", Epoch: epoch,
		N: int32(len(slots)), Slots: slots,
		Policy: pb.PlacementPolicy_ORDINAL_ROUND_ROBIN, Active: true,
	}
}

// End-to-end over the real gRPC wire: ping + full grant lifecycle (fresh upsert ->
// stale-epoch reject -> newer upsert -> stale-revoke no-op -> revoke), asserting the
// resolver cache reflects each step and the grant-driven mkdir hook fires only on a
// freshly-applied epoch. This is the gRPC analog of the Thrift server_test.
func TestGrpcServerGrantLifecycle(t *testing.T) {
	var mkdirs []string
	resolver := grant.NewResolver(grant.WithUpsertHook(func(g grant.Grant) error {
		mkdirs = append(mkdirs, g.AccessKeyID+"/"+g.Bucket)
		return nil
	}))
	c, cleanup := startServer(t, resolver)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ping
	pr, err := c.Ping(ctx, &pb.PingRequest{Caller: "tester"})
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if !strings.Contains(pr.GetMessage(), "hello tester") {
		t.Fatalf("ping message = %q", pr.GetMessage())
	}

	// fresh upsert (epoch 5) -> accepted; cached; hook fired once
	up, err := c.UpsertChannelGrant(ctx, &pb.UpsertChannelGrantRequest{Grant: sampleGrant(5)})
	if err != nil {
		t.Fatalf("upsert fresh: %v", err)
	}
	if !up.GetAccepted() || up.GetCurrentEpoch() != 5 {
		t.Fatalf("upsert fresh: accepted=%v current=%d detail=%q", up.GetAccepted(), up.GetCurrentEpoch(), up.GetDetail())
	}
	if g, ok := resolver.Cache().Get("otter", "wal"); !ok || g.Epoch != 5 || g.Policy != grant.PolicyOrdinalRoundRobin || g.N() != 3 {
		t.Fatalf("cache after fresh upsert: ok=%v grant=%+v", ok, g)
	}
	if len(mkdirs) != 1 || mkdirs[0] != "otter/wal" {
		t.Fatalf("mkdir hook fired %v, want [otter/wal]", mkdirs)
	}

	// stale upsert (epoch 4) -> rejected, reports the winning epoch, no re-mkdir
	up, err = c.UpsertChannelGrant(ctx, &pb.UpsertChannelGrantRequest{Grant: sampleGrant(4)})
	if err != nil {
		t.Fatalf("upsert stale: %v", err)
	}
	if up.GetAccepted() || up.GetCurrentEpoch() != 5 || !strings.Contains(up.GetDetail(), "stale") {
		t.Fatalf("upsert stale: accepted=%v current=%d detail=%q", up.GetAccepted(), up.GetCurrentEpoch(), up.GetDetail())
	}
	if len(mkdirs) != 1 {
		t.Fatalf("stale upsert must not re-run the mkdir hook; got %v", mkdirs)
	}

	// newer upsert (epoch 6) -> accepted
	up, err = c.UpsertChannelGrant(ctx, &pb.UpsertChannelGrantRequest{Grant: sampleGrant(6)})
	if err != nil {
		t.Fatalf("upsert newer: %v", err)
	}
	if !up.GetAccepted() || up.GetCurrentEpoch() != 6 {
		t.Fatalf("upsert newer: accepted=%v current=%d", up.GetAccepted(), up.GetCurrentEpoch())
	}

	// stale revoke (epoch 5 < 6) -> no-op; grant still cached
	rv, err := c.RevokeChannelGrant(ctx, &pb.RevokeChannelGrantRequest{AccessKeyId: "otter", Bucket: "wal", Epoch: 5})
	if err != nil {
		t.Fatalf("revoke stale: %v", err)
	}
	if rv.GetRevoked() {
		t.Fatalf("stale revoke (epoch 5 < 6) should be a no-op")
	}
	if _, ok := resolver.Cache().Get("otter", "wal"); !ok {
		t.Fatalf("grant wrongly dropped by a stale revoke")
	}

	// current revoke (epoch 6) -> removed
	rv, err = c.RevokeChannelGrant(ctx, &pb.RevokeChannelGrantRequest{AccessKeyId: "otter", Bucket: "wal", Epoch: 6})
	if err != nil {
		t.Fatalf("revoke current: %v", err)
	}
	if !rv.GetRevoked() {
		t.Fatalf("revoke at current epoch should remove the grant (detail=%q)", rv.GetDetail())
	}
	if _, ok := resolver.Cache().Get("otter", "wal"); ok {
		t.Fatalf("grant still cached after a valid revoke")
	}
}

// A malformed grant (n != len(slots)) is a typed reject, not a gRPC error.
func TestGrpcServerRejectsMalformedGrant(t *testing.T) {
	resolver := grant.NewResolver()
	c, cleanup := startServer(t, resolver)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bad := sampleGrant(1)
	bad.N = 99 // lie about the channel count
	up, err := c.UpsertChannelGrant(ctx, &pb.UpsertChannelGrantRequest{Grant: bad})
	if err != nil {
		t.Fatalf("upsert malformed returned a gRPC error (want a typed reject): %v", err)
	}
	if up.GetAccepted() || !strings.Contains(up.GetDetail(), "n=99") {
		t.Fatalf("malformed grant: accepted=%v detail=%q", up.GetAccepted(), up.GetDetail())
	}
	if _, ok := resolver.Cache().Get("otter", "wal"); ok {
		t.Fatalf("malformed grant must not be cached")
	}
}
