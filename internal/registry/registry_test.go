package registry

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
)

func mustCard(t *testing.T, id, capability string) *afepv1.AgentCard {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := identity.NewCard(id, id, kp, func(c *afepv1.AgentCard) {
		if capability != "" {
			c.Capabilities = []*afepv1.Capability{{Name: capability, Version: "0.1"}}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return card
}

func TestPutGet(t *testing.T) {
	r := New()
	card := mustCard(t, "a/one", "vision.tag")
	if err := r.Put(card); err != nil {
		t.Fatal(err)
	}
	got, ok := r.Get("a/one")
	if !ok || got.GetId() != "a/one" {
		t.Fatalf("get: ok=%v id=%q", ok, got.GetId())
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatal("unknown id must miss")
	}
}

func TestPutRejectsUnsigned(t *testing.T) {
	r := New()
	card := mustCard(t, "a/one", "")
	card.Signature = nil
	if err := r.Put(card); err == nil {
		t.Fatal("unsigned card must be rejected")
	}
}

func TestByCapability(t *testing.T) {
	r := New()
	_ = r.Put(mustCard(t, "a/vision", "vision.tag"))
	_ = r.Put(mustCard(t, "b/llm", "llm.infer"))
	_ = r.Put(mustCard(t, "c/none", ""))

	got := r.ByCapability("vision.tag")
	if len(got) != 1 || got[0].GetId() != "a/vision" {
		t.Fatalf("bycapability = %v", got)
	}
	if got := r.ByCapability("nope"); len(got) != 0 {
		t.Fatalf("unknown capability must be empty, got %d", len(got))
	}
}
