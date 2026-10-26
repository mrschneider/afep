package identity

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func TestBidRoundtrip(t *testing.T) {
	kp, _ := Generate()
	card, err := NewCard("tenant/provider", "prov", kp, nil)
	if err != nil {
		t.Fatal(err)
	}
	bid := &afepv1.Bid{AgentId: "tenant/provider", Score: 100, Rationale: "capable", ProposedDeadlineUnix: 1793000000}
	if err := SignBid(bid, kp); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBid(bid, card); err != nil {
		t.Fatalf("fresh bid must verify: %v", err)
	}
}

func TestBidTamperFails(t *testing.T) {
	kp, _ := Generate()
	card, _ := NewCard("tenant/provider", "prov", kp, nil)
	bid := &afepv1.Bid{AgentId: "tenant/provider", Score: 100}
	if err := SignBid(bid, kp); err != nil {
		t.Fatal(err)
	}
	bid.Score = 999
	if err := VerifyBid(bid, card); err == nil {
		t.Fatal("tampered score must fail")
	}
}

func TestBidForeignCardFails(t *testing.T) {
	a, _ := Generate()
	b, _ := Generate()
	cardB, _ := NewCard("tenant/provider", "prov", b, nil)
	bid := &afepv1.Bid{AgentId: "tenant/provider", Score: 1}
	if err := SignBid(bid, a); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBid(bid, cardB); err == nil {
		t.Fatal("bid signed by foreign key must fail")
	}
}

func TestUnsignedBidFails(t *testing.T) {
	kp, _ := Generate()
	card, _ := NewCard("tenant/provider", "prov", kp, nil)
	if err := VerifyBid(&afepv1.Bid{AgentId: "tenant/provider"}, card); err == nil {
		t.Fatal("unsigned bid must fail")
	}
}

func TestBidIDMismatchFails(t *testing.T) {
	kp, _ := Generate()
	card, _ := NewCard("tenant/real", "r", kp, nil)
	bid := &afepv1.Bid{AgentId: "tenant/other", Score: 1}
	_ = SignBid(bid, kp)
	if err := VerifyBid(bid, card); err == nil {
		t.Fatal("id mismatch must fail")
	}
}
