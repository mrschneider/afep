// Package identity issues and verifies self-signed AFEP AgentCards.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

// KeyPair is an ed25519 identity.
type KeyPair struct {
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
}

// Generate creates a fresh ed25519 key pair.
func Generate() (*KeyPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("afep/identity: generate: %w", err)
	}
	return &KeyPair{Public: pub, Private: priv}, nil
}

// FromSeed restores a key pair from a 32-byte seed.
func FromSeed(seed []byte) (*KeyPair, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("afep/identity: seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return &KeyPair{Public: priv.Public().(ed25519.PublicKey), Private: priv}, nil
}

// NewCard builds a self-signed AgentCard. mutate (optional) fills endpoint,
// capabilities and clearances before signing.
func NewCard(id, displayName string, kp *KeyPair, mutate func(*afepv1.AgentCard)) (*afepv1.AgentCard, error) {
	if id == "" {
		return nil, errors.New("afep/identity: empty card id")
	}
	card := &afepv1.AgentCard{
		Id:           id,
		DisplayName:  displayName,
		PublicKey:    append([]byte(nil), kp.Public...),
		IssuedAtUnix: time.Now().Unix(),
	}
	if mutate != nil {
		mutate(card)
	}
	if err := SignCard(card, kp); err != nil {
		return nil, err
	}
	return card, nil
}

// cardBody is the deterministic serialization covered by the signature:
// the card with the signature field cleared.
func cardBody(card *afepv1.AgentCard) ([]byte, error) {
	c := proto.Clone(card).(*afepv1.AgentCard)
	c.Signature = nil
	return proto.MarshalOptions{Deterministic: true}.Marshal(c)
}

// SignCard signs card in place with kp. The card public key must match kp.
func SignCard(card *afepv1.AgentCard, kp *KeyPair) error {
	if card == nil {
		return errors.New("afep/identity: nil card")
	}
	if !bytes.Equal(card.PublicKey, kp.Public) {
		return errors.New("afep/identity: card public key does not match key pair")
	}
	body, err := cardBody(card)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	card.Signature = ed25519.Sign(kp.Private, body)
	return nil
}

// VerifyCard checks the card structure and its self-signature.
func VerifyCard(card *afepv1.AgentCard) error {
	if card == nil {
		return errors.New("afep/identity: nil card")
	}
	if card.GetId() == "" {
		return errors.New("afep/identity: card has no id")
	}
	if len(card.GetPublicKey()) != ed25519.PublicKeySize {
		return fmt.Errorf("afep/identity: public key must be %d bytes, got %d", ed25519.PublicKeySize, len(card.GetPublicKey()))
	}
	if len(card.GetSignature()) != ed25519.SignatureSize {
		return fmt.Errorf("afep/identity: signature must be %d bytes, got %d", ed25519.SignatureSize, len(card.GetSignature()))
	}
	body, err := cardBody(card)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	if !ed25519.Verify(card.GetPublicKey(), body, card.GetSignature()) {
		return errors.New("afep/identity: bad card signature")
	}
	return nil
}
