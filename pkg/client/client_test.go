package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/registry"
	"github.com/mrschneider/afep/internal/transport/grpcsvc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type node struct {
	kp   *identity.KeyPair
	card *afepv1.AgentCard
	lis  *bufconn.Listener
}

func newNode(t *testing.T, id, capability string, score uint64, signer *identity.KeyPair) *node {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	// signer != nil simulates a stale card: same id, different key
	signFor := kp
	if signer != nil {
		signFor = signer
	}
	card, err := identity.NewCard(id, id, signFor, func(c *afepv1.AgentCard) {
		c.Capabilities = []*afepv1.Capability{{Name: capability, Version: "0.1"}}
		c.Endpoint = &afepv1.Endpoint{GrpcAddress: "bufnet-" + id}
	})
	if err != nil {
		t.Fatal(err)
	}
	serveKp := kp
	n := &node{kp: serveKp, card: card, lis: bufconn.Listen(1 << 20)}

	reg := registry.New()
	if err := reg.Put(card); err != nil {
		t.Fatal(err)
	}
	svc, err := grpcsvc.New(card, reg, serveKp, grpcsvc.WithBidScore(score))
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	afepv1.RegisterAgentTransportServiceServer(srv, svc)
	go func() { _ = srv.Serve(n.lis) }()
	t.Cleanup(srv.Stop)
	return n
}

// dialer routes bufnet-<id> targets to the right in-memory listener.
func dialer(nodes ...*node) DialFunc {
	byAddr := map[string]*bufconn.Listener{}
	for _, n := range nodes {
		byAddr[n.card.GetEndpoint().GetGrpcAddress()] = n.lis
	}
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		lis, ok := byAddr[target]
		if !ok {
			return nil, errors.New("no route to " + target)
		}
		return grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}

func probeReq() *afepv1.ProbeRequest {
	return &afepv1.ProbeRequest{
		Capability:   "vision.tag",
		DeadlineUnix: time.Now().Add(time.Minute).Unix(),
	}
}

func TestProbeAllAggregatesAndRanks(t *testing.T) {
	a := newNode(t, "node-a", "vision.tag", 50, nil)
	b := newNode(t, "node-b", "vision.tag", 100, nil)
	other := newNode(t, "node-c", "llm.infer", 999, nil) // different capability: not probed

	res, err := ProbeAll(context.Background(), dialer(a, b, other),
		[]*afepv1.AgentCard{a.card, b.card, other.card}, probeReq())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Bids) != 2 {
		t.Fatalf("bids = %d, want 2 (rejected=%v unreachable=%v)", len(res.Bids), res.Rejected, res.Unreachable)
	}
	if res.Bids[0].GetAgentId() != "node-b" || res.Bids[1].GetAgentId() != "node-a" {
		t.Fatalf("rank order = %s, %s — want node-b (100) then node-a (50)",
			res.Bids[0].GetAgentId(), res.Bids[1].GetAgentId())
	}
	// both signatures verified against their own cards (implicit in Bids)
	if len(res.Unreachable) != 0 || len(res.Rejected) != 0 {
		t.Fatalf("unexpected failures: %+v", res)
	}
}

func TestProbeAllRecordsUnreachable(t *testing.T) {
	a := newNode(t, "node-a", "vision.tag", 10, nil)
	dead := newNode(t, "node-dead", "vision.tag", 10, nil)
	dead.lis.Close() // listener gone: dial target has no route after removal

	res, err := ProbeAll(context.Background(), dialer(a),
		[]*afepv1.AgentCard{a.card, dead.card}, probeReq())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Bids) != 1 {
		t.Fatalf("bids = %d, want 1", len(res.Bids))
	}
	if len(res.Unreachable) != 1 || res.Unreachable[0] != "node-dead" {
		t.Fatalf("unreachable = %v", res.Unreachable)
	}
}

func TestProbeAllRejectsStaleCardSignature(t *testing.T) {
	// honest node serves with its real key; requester holds a stale card
	// with the same id but a different public key -> signature must fail.
	realKp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	stale := newNode(t, "node-stale", "vision.tag", 10, realKp)

	res, err := ProbeAll(context.Background(), dialer(stale),
		[]*afepv1.AgentCard{stale.card}, probeReq())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Bids) != 0 {
		t.Fatalf("stale-card bid must not be accepted, got %d", len(res.Bids))
	}
	if len(res.Rejected) != 1 {
		t.Fatalf("rejected = %v", res.Rejected)
	}
}
