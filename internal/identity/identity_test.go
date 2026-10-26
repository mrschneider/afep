package identity

import (
	"bytes"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func TestCardRoundtrip(t *testing.T) {
	kp, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := NewCard("tenant/agent", "tester", kp, func(c *afepv1.AgentCard) {
		c.Capabilities = []*afepv1.Capability{{Name: "vision.tag", Version: "0.1"}}
		c.SecurityClearances = []string{"tenant:acme"}
		c.Endpoint = &afepv1.Endpoint{GrpcAddress: "127.0.0.1:7700"}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCard(card); err != nil {
		t.Fatalf("fresh card must verify: %v", err)
	}
}

func TestTamperedCardFails(t *testing.T) {
	kp, _ := Generate()
	card, err := NewCard("tenant/agent", "tester", kp, nil)
	if err != nil {
		t.Fatal(err)
	}
	card.DisplayName = "mallory"
	if err := VerifyCard(card); err == nil {
		t.Fatal("tampered card must not verify")
	}
}

func TestSeedRoundtrip(t *testing.T) {
	kp, _ := Generate()
	restored, err := FromSeed(kp.Private.Seed())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.Public, kp.Public) {
		t.Fatal("restored public key mismatch")
	}
}

func TestSignKeyMismatch(t *testing.T) {
	a, _ := Generate()
	b, _ := Generate()
	card, err := NewCard("tenant/agent", "t", a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := SignCard(card, b); err == nil {
		t.Fatal("signing with a foreign key must fail")
	}
}

func TestBadSeedLength(t *testing.T) {
	if _, err := FromSeed([]byte("short")); err == nil {
		t.Fatal("short seed must fail")
	}
}
