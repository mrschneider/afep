package clearance

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func card(grants ...string) *afepv1.AgentCard {
	return &afepv1.AgentCard{Id: "a/x", SecurityClearances: grants}
}

func TestHolds(t *testing.T) {
	exact := card("tenant:acme")
	if !Holds(exact, "tenant:acme") {
		t.Fatal("exact grant must hold")
	}
	if Holds(exact, "tenant:other") {
		t.Fatal("foreign grant must not hold")
	}
	wild := card("tenant:*")
	if !Holds(wild, "tenant:anything") {
		t.Fatal("tenant:* must hold any tenant grant")
	}
	if Holds(wild, "domain:edge") {
		t.Fatal("tenant:* must not hold domain grants")
	}
	glob := card("*")
	if !glob_holds(glob) {
		t.Fatal("* must hold everything")
	}
	if Holds(card(), "tenant:acme") {
		t.Fatal("no grants must hold nothing")
	}
}

func glob_holds(c *afepv1.AgentCard) bool { return Holds(c, "tenant:acme") && Holds(c, "domain:x") }

func TestCanServe(t *testing.T) {
	if err := CanServe(card(), ""); err != nil {
		t.Fatalf("empty tenant must be open: %v", err)
	}
	if err := CanServe(card("tenant:acme"), "acme"); err != nil {
		t.Fatalf("granted tenant must pass: %v", err)
	}
	if err := CanServe(card("tenant:acme"), "other"); err == nil {
		t.Fatal("foreign tenant must be denied")
	}
	if err := CanServe(card(), "acme"); err == nil {
		t.Fatal("grantless card must be denied")
	}
}
