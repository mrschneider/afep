package identity

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func issuerAndEnv(t *testing.T) (*KeyPair, *afepv1.AgentCard, *afepv1.TaskEnvelope) {
	t.Helper()
	kp, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := NewCard("acme/issuer", "issuer", kp, nil)
	if err != nil {
		t.Fatal(err)
	}
	env := &afepv1.TaskEnvelope{
		TaskId:     "task-1",
		RootTaskId: "task-1",
		Tenant:     "acme",
		State:      afepv1.TaskState_TASK_STATE_PROBE,
		Capability: "vision.tag",
		Hops:       1,
	}
	return kp, card, env
}

func TestEnvelopeRoundtrip(t *testing.T) {
	kp, card, env := issuerAndEnv(t)
	if err := SignEnvelope(env, kp); err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnvelope(env, card); err != nil {
		t.Fatalf("fresh envelope must verify: %v", err)
	}
}

func TestEnvelopeTamperFails(t *testing.T) {
	kp, card, env := issuerAndEnv(t)
	if err := SignEnvelope(env, kp); err != nil {
		t.Fatal(err)
	}
	env.Capability = "llm.infer" // request essence is signed
	if err := VerifyEnvelope(env, card); err == nil {
		t.Fatal("tampered capability must fail")
	}
}

// The lifecycle tail is relay-mutable by design: routers stamp it without
// the issuer key, so those mutations must NOT invalidate the signature.
func TestEnvelopeRelayTailMutable(t *testing.T) {
	kp, card, env := issuerAndEnv(t)
	if err := SignEnvelope(env, kp); err != nil {
		t.Fatal(err)
	}
	env.State = afepv1.TaskState_TASK_STATE_ESCALATED
	env.Hops = 2
	env.AssigneeAgent = "other/agent"
	env.UpdatedAtUnix = 1793999999
	env.Escalation = &afepv1.Escalation{FromDomain: "beta", ToDomain: "alpha", ReasonCode: "NO_BIDS"}
	if err := VerifyEnvelope(env, card); err != nil {
		t.Fatalf("relay tail must stay valid: %v", err)
	}
}

func TestEnvelopeUnsignedRejected(t *testing.T) {
	_, card, env := issuerAndEnv(t)
	if err := VerifyEnvelope(env, card); err == nil {
		t.Fatal("unsigned envelope must fail")
	}
}

func TestEnvelopeForeignIssuerFails(t *testing.T) {
	kp, _, env := issuerAndEnv(t)
	foreign, _ := Generate()
	other, _ := NewCard("acme/other", "other", foreign, nil)
	if err := SignEnvelope(env, kp); err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnvelope(env, other); err == nil {
		t.Fatal("foreign issuer must fail")
	}
}

func TestEnvelopeNeedsTaskID(t *testing.T) {
	kp, _ := Generate()
	if err := SignEnvelope(&afepv1.TaskEnvelope{}, kp); err == nil {
		t.Fatal("id-less envelope must fail")
	}
}

// TestEnvelopeVerifyCacheHit re-verifying the same (core, key, sig) tuple
// must return the cached result without re-running ed25519: a second
// verification of an identical envelope is correct, and verification after
// the envelope's RELAY-MUTABLE tail changes (same signed core) must still
// verify — that is the redelivery case the cache exists for.
func TestEnvelopeVerifyCacheHit(t *testing.T) {
	kp, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := NewCard("t/cache", "cache", kp, nil)
	if err != nil {
		t.Fatal(err)
	}
	env := &afepv1.TaskEnvelope{TaskId: "e1", Tenant: "t", IssuerAgent: "t/cache"}
	if err := SignEnvelope(env, kp); err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnvelope(env, card); err != nil {
		t.Fatal(err)
	}
	// Relay-mutable tail changes: the signed core is untouched, so the
	// cached signature result must apply and verification must pass.
	env.Hops = 3
	env.State = afepv1.TaskState_TASK_STATE_ESCALATED
	if err := VerifyEnvelope(env, card); err != nil {
		t.Fatalf("tail-mutated envelope must verify via cache: %v", err)
	}
	// Tampered core: new digest, must be re-verified and REJECTED (and
	// the poisoned result cached, so repeated checks stay cheap).
	env.Payload = append([]byte(nil), env.Payload...)
	env.Payload = append(env.Payload, 'x')
	if err := VerifyEnvelope(env, card); err == nil {
		t.Fatal("tampered core must not verify")
	}
	if err := VerifyEnvelope(env, card); err == nil {
		t.Fatal("tampered core must not verify (cached)")
	}
}

// TestEnvelopeVerifyCacheKeyedByIssuerKey: the same signed core presented
// under a DIFFERENT issuer key (re-issued card, same id) must NOT ride the
// first key's cached result.
func TestEnvelopeVerifyCacheKeyedByIssuerKey(t *testing.T) {
	kp1, _ := Generate()
	kp2, _ := Generate()
	card1, _ := NewCard("t/same-id", "one", kp1, nil)
	env := &afepv1.TaskEnvelope{TaskId: "e2", Tenant: "t", IssuerAgent: "t/same-id"}
	if err := SignEnvelope(env, kp1); err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnvelope(env, card1); err != nil {
		t.Fatal(err)
	}
	// Re-issued card: same id, new key. The cached result of key1 must
	// not apply; verification against key2 must fail.
	card2, _ := NewCard("t/same-id", "two", kp2, nil)
	if err := VerifyEnvelope(env, card2); err == nil {
		t.Fatal("re-issued issuer key must not inherit the cached verify")
	}
}
