package identity

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

// bidBody is the deterministic serialization covered by a bid signature:
// the bid with the signature field cleared.
func bidBody(bid *afepv1.Bid) ([]byte, error) {
	b := proto.Clone(bid).(*afepv1.Bid)
	b.Signature = nil
	return proto.MarshalOptions{Deterministic: true}.Marshal(b)
}

// SignBid signs bid in place with kp; the caller must have stamped
// agent_id, score, rationale and deadline first.
func SignBid(bid *afepv1.Bid, kp *KeyPair) error {
	if bid == nil {
		return errors.New("afep/identity: nil bid")
	}
	if bid.GetAgentId() == "" {
		return errors.New("afep/identity: bid has no agent id")
	}
	body, err := bidBody(bid)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	bid.Signature = ed25519.Sign(kp.Private, body)
	return nil
}

// VerifyBid checks the bid signature against the signing card: the card id
// must equal the bidding agent and the key must verify the body.
func VerifyBid(bid *afepv1.Bid, card *afepv1.AgentCard) error {
	if bid == nil || card == nil {
		return errors.New("afep/identity: nil bid or card")
	}
	if bid.GetAgentId() == "" || bid.GetAgentId() != card.GetId() {
		return fmt.Errorf("afep/identity: bid agent %q does not match card %q", bid.GetAgentId(), card.GetId())
	}
	if len(bid.GetSignature()) == 0 {
		return errors.New("afep/identity: bid is unsigned")
	}
	body, err := bidBody(bid)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	if err := VerifyCard(card); err != nil {
		return fmt.Errorf("afep/identity: card: %w", err)
	}
	if !ed25519.Verify(card.GetPublicKey(), body, bid.GetSignature()) {
		return errors.New("afep/identity: bad bid signature")
	}
	return nil
}
