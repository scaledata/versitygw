// Package controlgrpc implements the OtterControlService gRPC server — the gRPC
// transport for the JFL -> Otter control-plane grant push, on top of the same
// grant.Resolver the Thrift PoC used. This is the receiver (vgw) side; it is the
// gRPC analog of otter/control (Thrift), built to validate the gRPC contract +
// resolver wiring on the laptop. The Handler logic is transport-identical to the
// Thrift Handler, so it ports unchanged to a native src/go/src/rubrik server.
//
// mTLS (on-cluster) is cluster-cert mutual TLS via credentials.NewTLS — the
// mechanism proven in PoC0 and used by rubrik/util/auth.GetServerTLSConfig.
package controlgrpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/versity/versitygw/otter/controlgrpc/ottercontrolpb"
	"github.com/versity/versitygw/otter/grant"
)

// Handler implements pb.OtterControlServiceServer against a grant.Resolver. It
// holds no state of its own — the same bridge logic as the Thrift Handler.
type Handler struct {
	pb.UnimplementedOtterControlServiceServer
	resolver *grant.Resolver
	now      func() time.Time
}

var _ pb.OtterControlServiceServer = (*Handler)(nil)

// NewHandler builds a Handler bound to r.
func NewHandler(r *grant.Resolver) *Handler {
	return &Handler{resolver: r, now: time.Now}
}

// Ping is the liveness / mTLS-handshake probe.
func (h *Handler) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("otter-control(grpc): ping from %q", req.GetCaller())
	return &pb.PingResponse{
		Message:      "otter-control alive; hello " + req.GetCaller(),
		ServerUnixMs: h.now().UnixMilli(),
	}, nil
}

// UpsertChannelGrant applies a pushed grant: epoch-guarded cache write + the
// grant-driven bucket-mkdir hook. Idempotent; a malformed grant is a typed reject
// (accepted=false), not a gRPC error.
func (h *Handler) UpsertChannelGrant(_ context.Context, req *pb.UpsertChannelGrantRequest) (*pb.UpsertChannelGrantResponse, error) {
	gr, err := fromProtoGrant(req.GetGrant())
	if err != nil {
		log.Printf("otter-control(grpc): upsert REJECTED (malformed): %v", err)
		return &pb.UpsertChannelGrantResponse{Accepted: false, Detail: err.Error()}, nil
	}
	accepted, current, hookErr := h.resolver.Upsert(gr)
	detail := "stored"
	switch {
	case !accepted:
		detail = fmt.Sprintf("stale epoch %d <= current %d", gr.Epoch, current)
	case hookErr != nil:
		detail = "stored (bucket-ensure warning: " + hookErr.Error() + ")"
	}
	log.Printf("otter-control(grpc): upsert client=%s access=%s bucket=%s epoch=%d n=%d -> accepted=%v current=%d detail=%q",
		gr.ClientID, gr.AccessKeyID, gr.Bucket, gr.Epoch, gr.N(), accepted, current, detail)
	return &pb.UpsertChannelGrantResponse{Accepted: accepted, CurrentEpoch: current, Detail: detail}, nil
}

// RevokeChannelGrant drops a grant from the hot cache, epoch-guarded.
func (h *Handler) RevokeChannelGrant(_ context.Context, req *pb.RevokeChannelGrantRequest) (*pb.RevokeChannelGrantResponse, error) {
	revoked := h.resolver.Cache().Delete(req.GetAccessKeyId(), req.GetBucket(), req.GetEpoch())
	detail := "revoked"
	if !revoked {
		detail = "no-op (absent or stale epoch)"
	}
	log.Printf("otter-control(grpc): revoke access=%s bucket=%s epoch=%d -> revoked=%v",
		req.GetAccessKeyId(), req.GetBucket(), req.GetEpoch(), revoked)
	return &pb.RevokeChannelGrantResponse{Revoked: revoked, Detail: detail}, nil
}

// ---- proto <-> grant mapping -----------------------------------------------

func fromProtoGrant(g *pb.OtterChannelGrant) (grant.Grant, error) {
	if g == nil {
		return grant.Grant{}, fmt.Errorf("nil grant")
	}
	if int(g.GetN()) != len(g.GetSlots()) {
		return grant.Grant{}, fmt.Errorf("grant (%s,%s): n=%d but %d slots", g.GetAccessKeyId(), g.GetBucket(), g.GetN(), len(g.GetSlots()))
	}
	slots := make([]grant.Slot, len(g.GetSlots()))
	for i, s := range g.GetSlots() {
		if s == nil {
			return grant.Grant{}, fmt.Errorf("grant (%s,%s): nil slot at %d", g.GetAccessKeyId(), g.GetBucket(), i)
		}
		slots[i] = grant.Slot{
			Slot:        int(s.GetSlot()),
			NodeID:      s.GetNodeId(),
			Endpoint:    s.GetEndpoint(),
			ChannelPath: s.GetChannelPath(),
		}
	}
	return grant.Grant{
		ClientID:         g.GetClientId(),
		AccessKeyID:      g.GetAccessKeyId(),
		Bucket:           g.GetBucket(),
		Epoch:            g.GetEpoch(),
		Slots:            slots,
		Policy:           fromProtoPolicy(g.GetPolicy()),
		LeaseExpiryUnixS: g.GetLeaseExpiryUnixS(),
		Active:           g.GetActive(),
	}, nil
}

func fromProtoPolicy(p pb.PlacementPolicy) grant.Policy {
	if p == pb.PlacementPolicy_ORDINAL_ROUND_ROBIN {
		return grant.PolicyOrdinalRoundRobin
	}
	return grant.PolicyKeyHash
}

// ---- embedded server -------------------------------------------------------

// Server is the embedded OtterControlService gRPC server (the JFL push endpoint).
type Server struct {
	grpc *grpc.Server
	lis  net.Listener
}

// NewServer binds a gRPC server for h on addr. If tlsCfg != nil it terminates
// mutual TLS (cluster identity); otherwise plaintext (dev / loopback tests). The
// listener is bound here (Addr is valid immediately); call Serve to accept.
func NewServer(h *Handler, addr string, tlsCfg *tls.Config) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("otter control(grpc): listen %s: %w", addr, err)
	}
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	s := grpc.NewServer(opts...)
	pb.RegisterOtterControlServiceServer(s, h)
	return &Server{grpc: s, lis: lis}, nil
}

// Addr returns the bound listen address (useful with ":0" in tests).
func (s *Server) Addr() net.Addr { return s.lis.Addr() }

// Serve blocks accepting connections until Stop.
func (s *Server) Serve() error { return s.grpc.Serve(s.lis) }

// Stop gracefully stops the server.
func (s *Server) Stop() { s.grpc.GracefulStop() }
