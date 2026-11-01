package identity

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"

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
	// The card check runs BEFORE the cache: it has its own memoization
	// keyed on the card bytes (a re-published card re-verifies). Only the
	// ed25519 result is cached here, so a card-lookup failure is never
	// frozen by this layer.
	if err := verifyCardCached(issuer); err != nil {
		return fmt.Errorf("afep/identity: issuer card: %w", err)
	}
	// Cheap fingerprint first: a cache hit avoids rebuilding the signed
	// core entirely (no clone, no deterministic marshal, no sha256 over
	// the body). The key is a projection, so a hit is only ever a
	// shortcut to a previously computed ed25519 result for the same
	// (core, key, signature) triple.
	key := envelopeCacheKey(env, issuer.GetPublicKey(), env.GetSignature())
	envelopeCacheMu.Lock()
	if v, ok := envelopeCache[key]; ok {
		envelopeCacheMu.Unlock()
		return v
	}
	envelopeCacheMu.Unlock()
	body, err := envelopeBody(env)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	verr := error(nil)
	if !ed25519.Verify(issuer.GetPublicKey(), body, env.GetSignature()) {
		verr = errors.New("afep/identity: bad envelope signature")
	}
	envelopeCacheMu.Lock()
	if len(envelopeCache) >= envelopeCacheCap {
		envelopeCache = make(map[[32]byte]error)
	}
	envelopeCache[key] = verr
	envelopeCacheMu.Unlock()
	return verr
}

// envelopeCache memoizes the signature check, keyed by the digest of
// (signed core fields || issuer public key || envelope signature). The
// signed core is immutable after signing — relays only mutate the tail
// that sits OUTSIDE the signature — so a core that verified against a key
// verifies forever, and Nak redeliveries, re-probes and escalations back
// to the same node stop paying the ~100 µs ed25519 verify. Keyed on the
// core AND the public key (not just the issuer id) so a re-issued card
// under the same id is re-verified; a tampered core or signature changes
// the digest, so a poisoned envelope cannot ride a valid one's cached
// result.
//
// The key is computed directly from the core fields (length-prefixed
// concatenation into one sha256) instead of cloning + deterministic
// marshaling the core: the digest is a CACHE key only, so it must merely
// be a faithful, collision-resistant fingerprint of exactly the fields
// envelopeBody signs — which the length-prefixed projection is. The
// ed25519 verify itself always runs on a miss; a key collision can only
// cause a spurious re-verify, never a bad accept.
//
// Bounded like the card cache: a node sees far fewer distinct cores than
// envelopeCacheCap; on overflow the whole cache is dropped and repopulated
// lazily.
const envelopeCacheCap = 4096

var (
	envelopeCacheMu sync.Mutex
	envelopeCache   = make(map[[32]byte]error)
)

// appendLP writes len(s) as a u32le prefix followed by s. The length
// prefix makes the field concatenation unambiguous (no cross-field
// boundary collisions).
func appendLP(b []byte, s []byte) []byte {
	var p [4]byte
	binary.LittleEndian.PutUint32(p[:], uint32(len(s)))
	return append(append(b, p[:]...), s...)
}

// appendLPS is the string variant of appendLP.
func appendLPS(b []byte, s string) []byte {
	var p [4]byte
	binary.LittleEndian.PutUint32(p[:], uint32(len(s)))
	return append(append(b, p[:]...), s...)
}

// envelopeCacheKey fingerprints (core fields, public key, signature).
func envelopeCacheKey(env *afepv1.TaskEnvelope, pub, sig []byte) [32]byte {
	var buf []byte
	buf = appendLPS(buf, env.TaskId)
	buf = appendLPS(buf, env.RootTaskId)
	buf = appendLPS(buf, env.Tenant)
	buf = appendLPS(buf, env.IssuerAgent)
	buf = appendLPS(buf, env.Capability)
	buf = appendLPS(buf, env.PayloadType)
	buf = appendLP(buf, env.Payload)
	d := env.GetDeadline()
	buf = appendLP(buf, strconv.AppendInt(nil, d.GetStartUnix(), 10))
	buf = appendLP(buf, strconv.AppendInt(nil, d.GetExpiresUnix(), 10))
	buf = appendLP(buf, pub)
	buf = appendLP(buf, sig)
	return sha256.Sum256(buf)
}
