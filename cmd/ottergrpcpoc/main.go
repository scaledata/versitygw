// PoC driver: on-cluster gRPC OtterControlService over cluster mutual TLS.
//
//	server: ottergrpcpoc --mode server --addr 127.0.0.1:9300 --gwroot /tmp/otter-grpc-gwroot --tls
//	client: ottergrpcpoc --mode client --addr 127.0.0.1:9300 --tls
//
// --tls uses the node's cluster cert (/var/lib/rubrik/certs/cluster.{crt,pem}) on
// BOTH ends (mutual TLS). Run under sudo so the 0640 cluster key is readable. This
// is the gRPC analog of poc0-jfl-thrift/main.go; it exercises the real gRPC wire +
// the grant.Resolver + the grant-driven mkdir on a real node.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/versity/versitygw/otter/controlgrpc"
	pb "github.com/versity/versitygw/otter/controlgrpc/ottercontrolpb"
	"github.com/versity/versitygw/otter/grant"
)

// tlsConfig builds a cluster-mTLS config. Server requires+verifies the client cert
// against the cluster CA. Client presents the cluster cert and verifies the server
// chains to the cluster CA but skips hostname matching (the cluster cert is
// self-signed, CN=<uuid>.cluster.rubrik.local, not the dialed host) — mirrors
// CDM's CA-based, no-SAN-pin behavior (and PoC0's Go client).
func tlsConfig(cert, key, ca string, server bool) *tls.Config {
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		log.Fatalf("load keypair: %v", err)
	}
	caPEM, err := os.ReadFile(ca)
	if err != nil {
		log.Fatalf("read ca: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		log.Fatalf("no certs parsed from %s", ca)
	}
	c := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	if server {
		c.ClientAuth = tls.RequireAndVerifyClientCert
		c.ClientCAs = pool
	} else {
		c.RootCAs = pool
		c.InsecureSkipVerify = true
		c.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("no server cert presented")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			inter := x509.NewCertPool()
			for _, r := range rawCerts[1:] {
				if cc, e := x509.ParseCertificate(r); e == nil {
					inter.AddCert(cc)
				}
			}
			_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter})
			return err
		}
	}
	return c
}

func grantMsg(epoch int64) *pb.OtterChannelGrant {
	return &pb.OtterChannelGrant{
		ClientId: "clientA", AccessKeyId: "otter", Bucket: "jflpush-grpc", Epoch: epoch, N: 2,
		Slots: []*pb.ChannelSlot{
			{Slot: 0, NodeId: "A", Endpoint: "http://10.27.124.49:9000", ChannelPath: "/sd/mount/otter-mc/channel_0"},
			{Slot: 1, NodeId: "B", Endpoint: "http://10.27.125.224:9000", ChannelPath: "/sd/mount/otter-mc/channel_1"},
		},
		Policy: pb.PlacementPolicy_ORDINAL_ROUND_ROBIN, Active: true,
	}
}

func main() {
	mode := flag.String("mode", "server", "server|client")
	addr := flag.String("addr", "127.0.0.1:9300", "host:port")
	gwroot := flag.String("gwroot", "/tmp/otter-grpc-gwroot", "server: gateway root for grant-driven mkdir")
	useTLS := flag.Bool("tls", false, "cluster mutual TLS")
	cert := flag.String("cert", "/var/lib/rubrik/certs/cluster.crt", "cert file")
	key := flag.String("key", "/var/lib/rubrik/certs/cluster.pem", "key file")
	ca := flag.String("ca", "/var/lib/rubrik/certs/cluster.crt", "CA file")
	caller := flag.String("caller", "grpc-poc", "client: ping caller id")
	flag.Parse()

	if *mode == "server" {
		if err := os.MkdirAll(*gwroot, 0o755); err != nil {
			log.Fatalf("mkdir gwroot: %v", err)
		}
		resolver := grant.NewResolver(grant.WithUpsertHook(func(g grant.Grant) error {
			dir := filepath.Join(*gwroot, g.Bucket)
			if e := os.MkdirAll(dir, 0o755); e != nil {
				fmt.Fprintf(os.Stderr, "warn: grant-driven mkdir %q: %v\n", dir, e)
				return e
			}
			return nil
		}))
		var cfg *tls.Config
		if *useTLS {
			cfg = tlsConfig(*cert, *key, *ca, true)
		}
		srv, err := controlgrpc.NewServer(controlgrpc.NewHandler(resolver), *addr, cfg)
		if err != nil {
			log.Fatalf("server: %v", err)
		}
		log.Printf("OtterControlService(gRPC) listening on %s (tls=%v) gwroot=%s", *addr, *useTLS, *gwroot)
		if err := srv.Serve(); err != nil {
			log.Fatalf("serve: %v", err)
		}
		return
	}

	// client
	var creds credentials.TransportCredentials
	if *useTLS {
		creds = credentials.NewTLS(tlsConfig(*cert, *key, *ca, false))
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()
	c := pb.NewOtterControlServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pr, err := c.Ping(ctx, &pb.PingRequest{Caller: *caller})
	if err != nil {
		log.Fatalf("ping FAILED: %v", err)
	}
	fmt.Printf("PING OK: %s (server_unix_ms=%d)\n", pr.GetMessage(), pr.GetServerUnixMs())

	epoch := time.Now().Unix()
	u1, err := c.UpsertChannelGrant(ctx, &pb.UpsertChannelGrantRequest{Grant: grantMsg(epoch)})
	if err != nil {
		log.Fatalf("upsert fresh FAILED: %v", err)
	}
	fmt.Printf("UPSERT fresh (epoch %d): accepted=%v current_epoch=%d detail=%q\n", epoch, u1.GetAccepted(), u1.GetCurrentEpoch(), u1.GetDetail())

	u2, err := c.UpsertChannelGrant(ctx, &pb.UpsertChannelGrantRequest{Grant: grantMsg(epoch - 5)})
	if err != nil {
		log.Fatalf("upsert stale FAILED: %v", err)
	}
	fmt.Printf("UPSERT stale (epoch %d): accepted=%v current_epoch=%d detail=%q\n", epoch-5, u2.GetAccepted(), u2.GetCurrentEpoch(), u2.GetDetail())

	r1, err := c.RevokeChannelGrant(ctx, &pb.RevokeChannelGrantRequest{AccessKeyId: "otter", Bucket: "jflpush-grpc", Epoch: epoch})
	if err != nil {
		log.Fatalf("revoke FAILED: %v", err)
	}
	fmt.Printf("REVOKE (epoch %d): revoked=%v detail=%q\n", epoch, r1.GetRevoked(), r1.GetDetail())

	r2, err := c.RevokeChannelGrant(ctx, &pb.RevokeChannelGrantRequest{AccessKeyId: "otter", Bucket: "jflpush-grpc", Epoch: epoch})
	if err != nil {
		log.Fatalf("revoke-again FAILED: %v", err)
	}
	fmt.Printf("REVOKE again: revoked=%v detail=%q\n", r2.GetRevoked(), r2.GetDetail())
	fmt.Println("=== done ===")
}
