// Package client aggregates and verifies bids across AFEP nodes: direct
// gRPC fan-out over the registry, signature checks against each node's
// card, deterministic ranking of the survivors.
package client

import (
	"context"
	"fmt"
	"io"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/rank"
	"google.golang.org/grpc"
)

// DialFunc opens a connection to a card's gRPC address. Tests inject
// in-memory dialers; production code passes a grpc.WithContextDialer setup.
type DialFunc func(ctx context.Context, target string) (*grpc.ClientConn, error)

// Result carries the outcome of a ProbeAll sweep.
type Result struct {
	// Bids are signature-verified against their cards, rank-sorted.
	Bids []*afepv1.Bid
	// Unreachable lists agent ids whose node could not be dialed or errored.
	Unreachable []string
	// Rejected lists "agent: reason" for bids that failed verification.
	Rejected []string
}

// ProbeAll probes every card advertising req.Capability, verifies each bid
// against the card it arrived from and returns the survivors ranked
// (score desc, id asc). Soft failures are recorded in the result; only a
// context error aborts the sweep.
func ProbeAll(ctx context.Context, dial DialFunc, cards []*afepv1.AgentCard, req *afepv1.ProbeRequest) (*Result, error) {
	res := &Result{}
	for _, card := range cards {
		if !advertises(card, req.GetCapability()) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return res, err
		}
		probeOne(ctx, dial, card, req, res)
	}
	rank.Sort(res.Bids)
	return res, nil
}

func probeOne(ctx context.Context, dial DialFunc, card *afepv1.AgentCard, req *afepv1.ProbeRequest, res *Result) {
	conn, err := dial(ctx, card.GetEndpoint().GetGrpcAddress())
	if err != nil {
		res.Unreachable = append(res.Unreachable, card.GetId())
		return
	}
	defer conn.Close()

	stream, err := afepv1.NewAgentTransportServiceClient(conn).Probe(ctx, req)
	if err != nil {
		res.Unreachable = append(res.Unreachable, card.GetId())
		return
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			res.Unreachable = append(res.Unreachable, card.GetId())
			return
		}
		for _, bid := range msg.GetBids() {
			if bid.GetAgentId() != card.GetId() {
				res.Rejected = append(res.Rejected,
					fmt.Sprintf("%s: node %s answered with a bid for %q", card.GetId(), card.GetId(), bid.GetAgentId()))
				continue
			}
			if err := identity.VerifyBid(bid, card); err != nil {
				res.Rejected = append(res.Rejected, fmt.Sprintf("%s: %v", bid.GetAgentId(), err))
				continue
			}
			res.Bids = append(res.Bids, bid)
		}
	}
}

func advertises(card *afepv1.AgentCard, capability string) bool {
	for _, c := range card.GetCapabilities() {
		if c.GetName() == capability {
			return true
		}
	}
	return false
}
