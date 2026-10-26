package quictransport

import (
	"context"
	"io"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/registry"
	"github.com/mrschneider/afep/internal/transport/grpcsvc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSelfSignedCertIsValidForLoopback(t *testing.T) {
	cert, err := SelfSigned()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509leaf(cert)
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("loopback SAN: %v", err)
	}
	if err := leaf.VerifyHostname("localhost"); err != nil {
		t.Fatalf("localhost SAN: %v", err)
	}
	if leaf.NotAfter.Before(time.Now().Add(300 * 24 * time.Hour)) {
		t.Fatal("cert should live ~1 year")
	}
}

func TestStreamEchoOverQUIC(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(c, buf); err != nil {
			return
		}
		_, _ = c.Write(buf) // echo
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo = %q", buf)
	}
}

func TestGRPCOverQUIC(t *testing.T) {
	// identity under test
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	self, err := identity.NewCard("local/quic", "quic", kp, func(c *afepv1.AgentCard) {
		c.Capabilities = []*afepv1.Capability{{Name: "demo.echo", Version: "0.1"}}
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New()
	if err := reg.Put(self); err != nil {
		t.Fatal(err)
	}
	svc, err := grpcsvc.New(self, reg, kp)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	afepv1.RegisterAgentTransportServiceServer(srv, svc)
	go func() { _ = srv.Serve(ln) }() // net.Listener: one gRPC conn per QUIC stream
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///afep-quic",
		grpc.WithContextDialer(DialFunc(ln.Addr().String())),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := afepv1.NewAgentTransportServiceClient(conn)
	resp, err := client.GetCard(ctx, &afepv1.GetCardRequest{AgentId: "local/quic"})
	if err != nil {
		t.Fatalf("gRPC over QUIC failed: %v", err)
	}
	if resp.GetCard().GetId() != "local/quic" {
		t.Fatalf("card = %q", resp.GetCard().GetId())
	}
	if err := identity.VerifyCard(resp.GetCard()); err != nil {
		t.Fatalf("card verify: %v", err)
	}

	// second RPC reuses the same QUIC connection (multiplexed streams)
	if _, err := client.GetCard(ctx, &afepv1.GetCardRequest{AgentId: "local/quic"}); err != nil {
		t.Fatalf("second RPC over QUIC failed: %v", err)
	}
}
