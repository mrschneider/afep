package identity

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

// envelopeBody is the deterministic serialization covered by an envelope
// signature. The signed core is the request essence — ids, tenant, issuer,
// capability, payload and deadline. The lifecycle tail (state, assignee,
// bids, updated_at, escalation, hops) is relay-mutable: routers stamp it
// without the issuer key, so it is protected by hop-qualified dedup keys
// and the audit chain rather than by this signature. Callers sign after
// building the request, not after each hop.
func envelopeBody(env *afepv1.TaskEnvelope) ([]byte, error) {
	e := proto.Clone(env).(*afepv1.TaskEnvelope)
	e.Signature = nil
	e.State = afepv1.TaskState_TASK_STATE_UNSPECIFIED
	e.AssigneeAgent = ""
	e.Bids = nil
	e.UpdatedAtUnix = 0
	e.Escalation = nil
	e.Hops = 0
	return proto.MarshalOptions{Deterministic: true}.Marshal(e)
}

// SignEnvelope signs env in place with kp (the issuer's key).
func SignEnvelope(env *afepv1.TaskEnvelope, kp *KeyPair) error {
	if env == nil {
		return errors.New("afep/identity: nil envelope")
	}
	if env.GetTaskId() == "" {
		return errors.New("afep/identity: envelope has no task id")
	}
	body, err := envelopeBody(env)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	env.Signature = ed25519.Sign(kp.Private, body)
	return nil
}

// VerifyEnvelope checks the envelope signature against the issuer card.
// An unsigned envelope is rejected.
func VerifyEnvelope(env *afepv1.TaskEnvelope, issuer *afepv1.AgentCard) error {
	if env == nil || issuer == nil {
		return errors.New("afep/identity: nil envelope or issuer")
	}
	if env.GetTaskId() == "" {
		return errors.New("afep/identity: envelope has no task id")
	}
	if len(env.GetSignature()) == 0 {
		return errors.New("afep/identity: envelope is unsigned")
	}
	if err := verifyCardCached(issuer); err != nil {
		return fmt.Errorf("afep/identity: issuer card: %w", err)
	}
	body, err := envelopeBody(env)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	if !ed25519.Verify(issuer.GetPublicKey(), body, env.GetSignature()) {
		return errors.New("afep/identity: bad envelope signature")
	}
	return nil
}
