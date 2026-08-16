// Package control implements the OtterControl Thrift service — the JFL -> Otter
// control-plane "grant push" endpoint — on top of a grant.Resolver. This is the
// gateway (server) side; JFL is the client. The transport (cluster mutual TLS,
// cipher interop with a real Scala SafeThrift client) is the one proven in PoC0
// (otter-jfl-crdb-comms-poc.md / .plans/otter-jfl-control-plane-plan.md §8).
package control

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	thrift "github.com/apache/thrift/lib/go/thrift"

	"github.com/versity/versitygw/otter/control/ottercontrol"
	"github.com/versity/versitygw/otter/grant"
)

// Handler implements ottercontrol.OtterControl against a grant.Resolver: an
// upsert becomes Resolver.Upsert (epoch-guarded cache write + grant-driven bucket
// mkdir), a revoke becomes a cache delete. It holds no state of its own.
type Handler struct {
	resolver *grant.Resolver
	now      func() time.Time // injectable clock (tests)
}

var _ ottercontrol.OtterControl = (*Handler)(nil)

// NewHandler builds a Handler bound to r.
func NewHandler(r *grant.Resolver) *Handler {
	return &Handler{resolver: r, now: time.Now}
}

// Ping is the liveness / mTLS-handshake probe.
func (h *Handler) Ping(_ context.Context, caller string) (*ottercontrol.PingResponse, error) {
	log.Printf("otter-control: ping from %q", caller)
	return &ottercontrol.PingResponse{
		Message:      "otter-control alive; hello " + caller,
		ServerUnixMs: h.now().UnixMilli(),
	}, nil
}

// UpsertChannelGrant applies a pushed grant. It is idempotent and epoch-guarded: an
// equal-or-lower epoch is a no-op reporting the winning epoch (accepted=false). A
// malformed grant is rejected with detail, not an RPC-level exception, so JFL sees a
// clear typed answer rather than a transport error.
func (h *Handler) UpsertChannelGrant(_ context.Context, g *ottercontrol.OtterChannelGrant) (*ottercontrol.UpsertGrantResponse, error) {
	gr, err := fromThriftGrant(g)
	if err != nil {
		log.Printf("otter-control: upsert REJECTED (malformed): %v", err)
		return &ottercontrol.UpsertGrantResponse{Accepted: false, Detail: err.Error()}, nil
	}
	accepted, current, hookErr := h.resolver.Upsert(gr)
	detail := "stored"
	switch {
	case !accepted:
		detail = fmt.Sprintf("stale epoch %d <= current %d", gr.Epoch, current)
	case hookErr != nil:
		// The grant is cached; only the idempotent bucket-mkdir warned. Surface it
		// but keep accepted=true — the mkdir retries on the next push/warm.
		detail = "stored (bucket-ensure warning: " + hookErr.Error() + ")"
	}
	log.Printf("otter-control: upsert client=%s access=%s bucket=%s epoch=%d n=%d -> accepted=%v current=%d detail=%q",
		gr.ClientID, gr.AccessKeyID, gr.Bucket, gr.Epoch, gr.N(), accepted, current, detail)
	return &ottercontrol.UpsertGrantResponse{Accepted: accepted, CurrentEpoch: current, Detail: detail}, nil
}

// RevokeChannelGrant drops a grant from the hot cache, guarded by epoch so a stale
// revoke can't remove a newer grant. The durable CRDB delete is authoritative; this
// just stops the gateway serving the grant immediately.
func (h *Handler) RevokeChannelGrant(_ context.Context, req *ottercontrol.RevokeRequest) (*ottercontrol.RevokeGrantResponse, error) {
	revoked := h.resolver.Cache().Delete(req.AccessKeyID, req.Bucket, req.Epoch)
	detail := "revoked"
	if !revoked {
		detail = "no-op (absent or stale epoch)"
	}
	log.Printf("otter-control: revoke access=%s bucket=%s epoch=%d -> revoked=%v", req.AccessKeyID, req.Bucket, req.Epoch, revoked)
	return &ottercontrol.RevokeGrantResponse{Revoked: revoked, Detail: detail}, nil
}

// ---- Thrift <-> grant type mapping -----------------------------------------

func fromThriftGrant(g *ottercontrol.OtterChannelGrant) (grant.Grant, error) {
	if g == nil {
		return grant.Grant{}, fmt.Errorf("nil grant")
	}
	if int(g.N) != len(g.Slots) {
		return grant.Grant{}, fmt.Errorf("grant (%s,%s): n=%d but %d slots", g.AccessKeyID, g.Bucket, g.N, len(g.Slots))
	}
	slots := make([]grant.Slot, len(g.Slots))
	for i, s := range g.Slots {
		if s == nil {
			return grant.Grant{}, fmt.Errorf("grant (%s,%s): nil slot at %d", g.AccessKeyID, g.Bucket, i)
		}
		slots[i] = grant.Slot{
			Slot:        int(s.Slot),
			NodeID:      s.NodeID,
			Endpoint:    s.Endpoint,
			ChannelPath: s.ChannelPath,
		}
	}
	return grant.Grant{
		ClientID:         g.ClientID,
		AccessKeyID:      g.AccessKeyID,
		Bucket:           g.Bucket,
		Epoch:            g.Epoch,
		Slots:            slots,
		Policy:           fromThriftPolicy(g.Policy),
		LeaseExpiryUnixS: g.LeaseExpiryUnixS,
		Active:           g.Active,
	}, nil
}

func fromThriftPolicy(p ottercontrol.PlacementPolicy) grant.Policy {
	if p == ottercontrol.PlacementPolicy_ORDINAL_ROUND_ROBIN {
		return grant.PolicyOrdinalRoundRobin
	}
	return grant.PolicyKeyHash
}

// ---- embedded server -------------------------------------------------------

// controlSocket is the common surface of the plaintext (*TServerSocket) and mTLS
// (*TSSLServerSocket) server transports: TServerTransport (which includes Listen)
// plus the bound Addr.
type controlSocket interface {
	thrift.TServerTransport
	Addr() net.Addr
}

// Server is the embedded OtterControl Thrift server (the JFL push endpoint), bound
// to its own control port (9200 in the design) separate from S3 data on 9000.
type Server struct {
	srv  *thrift.TSimpleServer
	sock controlSocket
}

// NewServer builds and binds a server for h on addr. If tlsCfg != nil it terminates
// mutual TLS (cluster identity); otherwise plaintext (dev / loopback tests). The
// listener is bound here (Addr is valid immediately); call Serve to accept.
func NewServer(h *Handler, addr string, tlsCfg *tls.Config) (*Server, error) {
	var (
		sock controlSocket
		err  error
	)
	if tlsCfg != nil {
		sock, err = thrift.NewTSSLServerSocket(addr, tlsCfg)
	} else {
		sock, err = thrift.NewTServerSocket(addr)
	}
	if err != nil {
		return nil, fmt.Errorf("otter control: server socket %s: %w", addr, err)
	}
	if err := sock.Listen(); err != nil {
		return nil, fmt.Errorf("otter control: listen %s: %w", addr, err)
	}
	// Plain TBinaryProtocol over a buffered transport — matches the PoC0 wire the
	// Scala SafeThrift client handshaked against (buffered server <-> unbuffered
	// client is wire-compatible: buffered, not framed).
	srv := thrift.NewTSimpleServer4(
		ottercontrol.NewOtterControlProcessor(h),
		sock,
		thrift.NewTBufferedTransportFactory(8192),
		thrift.NewTBinaryProtocolFactoryConf(nil),
	)
	return &Server{srv: srv, sock: sock}, nil
}

// ServerTLSConfig builds a mutual-TLS config for the control server using the
// cluster identity: it presents certFile/keyFile and requires+verifies a client
// cert chaining to caFile. On a Brik node all three are the cluster cert material
// (/var/lib/rubrik/certs/cluster.crt + cluster.pem — self-signed, its own CA), the
// identity PoC0 handshaked with the real Scala SafeThrift client. CA-based mutual
// auth, no hostname/SAN pin — matching CDM's ThriftSslSocketFactory.
func ServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("otter control: load keypair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("otter control: read CA %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("otter control: no certs parsed from %q", caFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert, // mutual TLS (setNeedClientAuth(true))
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Addr returns the bound listen address (useful with ":0" in tests).
func (s *Server) Addr() net.Addr { return s.sock.Addr() }

// Serve blocks accepting connections until Stop.
func (s *Server) Serve() error { return s.srv.Serve() }

// Stop gracefully stops the server.
func (s *Server) Stop() error { return s.srv.Stop() }
