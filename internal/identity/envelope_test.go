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
	env.Hops = 2 // relay mutation after signing
	if err := VerifyEnvelope(env, card); err == nil {
		t.Fatal("tampered hops must fail")
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
