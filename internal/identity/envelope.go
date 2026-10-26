package identity

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

// envelopeBody is the deterministic serialization covered by an envelope
// signature: the envelope with the signature field cleared. Callers must
// sign only after the final state transition (state, timestamps and hops
// are all covered).
func envelopeBody(env *afepv1.TaskEnvelope) ([]byte, error) {
	e := proto.Clone(env).(*afepv1.TaskEnvelope)
	e.Signature = nil
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
	if err := VerifyCard(issuer); err != nil {
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
