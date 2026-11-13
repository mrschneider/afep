// Package identity issues and verifies self-signed AFEP AgentCards.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
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
// the card with the signature field cleared. It builds a bare AgentCard
// sharing the (immutable) nested messages instead of deep-cloning them:
// proto.Marshal is read-only, so sharing Endpoint, Capabilities and
// Metadata is safe. This cuts the clone cost from O(card size) to a single
// struct allocation.
func cardBody(card *afepv1.AgentCard) ([]byte, error) {
	c := &afepv1.AgentCard{
		Id:                 card.Id,
		DisplayName:        card.DisplayName,
		PublicKey:          card.PublicKey,
		Endpoint:           card.Endpoint,
		Capabilities:       card.Capabilities,
		SecurityClearances: card.SecurityClearances,
		Metadata:           card.Metadata,
		IssuedAtUnix:       card.IssuedAtUnix,
		ExpiresAtUnix:      card.ExpiresAtUnix,
	}
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

// cardVerifyCache memoizes VerifyCard results keyed by the card's canonical
// bytes. AgentCards are immutable self-signed artifacts: once a card's
// bytes verify, they verify forever. The per-envelope verify path re-checks
// the issuer card on every hop, so caching the (rarely-changing) card check
// removes ~half of VerifyEnvelope's cost. Keyed on the full marshaled card
// (not the id) so a re-issued card under the same id is re-verified.
//
// Bounded LRU: a federation has far fewer than cardCacheCap distinct cards,
// so the cache rarely evicts. On overflow only the least-recently-used card
// is dropped (not the whole cache), preserving the hit rate of warm entries
// instead of a total drop that would thundering-reverify the federation.
const cardCacheCap = 4096

var cardCache = newLRUCache[[32]byte, error](cardCacheCap)

// cardDigestCache memoizes cardCacheKey per card POINTER. AgentCards are
// immutable self-signed artifacts that are shared across goroutines (the
// bus mirrors the KV bucket into one cardCache entry; the mesh re-uses the
// same pointer for every envelope from that issuer), so the digest of a
// card's bytes is stable for the pointer's whole life. Keying on the
// pointer — not the bytes — turns the per-envelope warm-cache verify path
// from "deterministic marshal + sha256 of the card" into a single map
// lookup: the envelope path then pays only envelopeCacheKey + LRU lookups.
//
// The map grows one entry per distinct card object (a few dozen to a few
// thousand in a federation — the same bound that keeps cardCache warm);
// each entry is a pointer + [32]byte, and the objects it pins are the
// cards already alive in the bus cache / issuer resolver, so the extra
// retention is bounded by the federation's card count, not its traffic.
var cardDigestCache sync.Map // *afepv1.AgentCard -> cardDigest

type cardDigest struct {
	key [32]byte
	ok  bool
}

func cardCacheKey(card *afepv1.AgentCard) ([32]byte, bool) {
	if d, ok := cardDigestCache.Load(card); ok {
		cd := d.(cardDigest)
		return cd.key, cd.ok
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(card)
	if err != nil {
		cardDigestCache.Store(card, cardDigest{ok: false})
		return [32]byte{}, false // unmarshalable: never cache, always verify
	}
	cd := cardDigest{key: sha256.Sum256(body), ok: true}
	cardDigestCache.Store(card, cd)
	return cd.key, cd.ok
}

// verifyCardCached is VerifyCard with a memoization layer. The cache stores
// the full result (including the error) so a poisoned card is not re-verified
// on every hop either.
func verifyCardCached(card *afepv1.AgentCard) error {
	key, ok := cardCacheKey(card)
	if ok {
		if v, hit := cardCache.get(key); hit {
			return v
		}
	}
	err := VerifyCard(card)
	if ok {
		cardCache.put(key, err)
	}
	return err
}
