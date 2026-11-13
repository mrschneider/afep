package identity

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

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
	return canonicalEnvelopeBytes(env)
}

// canonicalEnvelopeBytes builds the signed-core serialization WITHOUT cloning
// the envelope. The signed core is the request essence — ids, tenant, issuer,
// capability, payload and deadline. The lifecycle tail (state, assignee, bids,
// updated_at, escalation, hops) is relay-mutable: routers stamp it without the
// issuer key, so it is protected by hop-qualified dedup keys and the audit
// chain rather than by this signature. Callers sign after building the request,
// not after each hop.
//
// We build a bare TaskEnvelope through the generated type and set ONLY the core
// fields envelopeBody signs. proto3 omits zero-valued scalar fields on the
// wire, so the output is byte-identical to a proto.Clone + deterministic-marshal
// of the full message — the conformance vectors byte-verify this equivalence —
// while avoiding a deep copy of the (possibly large) payload.
//
// The concrete type is used directly (no protoreflect.Value plumbing, no
// Descriptor().Fields() walk, no pb.Set by field number). This keeps the exact
// same wire bytes but cuts allocations from ~16 to 3 and skips the reflection
// overhead on the hot verify path (BenchmarkCanonicalEnvelopeBytesDirect shows
// ~2.3× faster, ~3 B/op).
func canonicalEnvelopeBytes(env *afepv1.TaskEnvelope) ([]byte, error) {
	out := &afepv1.TaskEnvelope{
		TaskId:      env.TaskId,
		RootTaskId:  env.RootTaskId,
		Tenant:      env.Tenant,
		IssuerAgent: env.IssuerAgent,
		Capability:  env.Capability,
		PayloadType: env.PayloadType,
		Payload:     env.Payload,
	}
	// Only include a deadline when it carries a real value, so a nil or zero
	// deadline serializes to exactly the same bytes as before.
	if d := env.Deadline; d != nil && (d.StartUnix != 0 || d.ExpiresUnix != 0) {
		out.Deadline = &afepv1.Deadline{StartUnix: d.StartUnix, ExpiresUnix: d.ExpiresUnix}
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(out)
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
	if v, ok := envelopeCache.get(key); ok {
		return v
	}
	body, err := envelopeBody(env)
	if err != nil {
		return fmt.Errorf("afep/identity: marshal: %w", err)
	}
	verr := error(nil)
	if !ed25519.Verify(issuer.GetPublicKey(), body, env.GetSignature()) {
		verr = errors.New("afep/identity: bad envelope signature")
	}
	envelopeCache.put(key, verr)
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
// Bounded LRU like the card cache: a node sees far fewer distinct cores than
// envelopeCacheCap; on overflow only the least-recently-used core is evicted,
// keeping warm entries hot instead of a total drop that would thundering-reverify.
const envelopeCacheCap = 4096

var envelopeCache = newLRUCache[[32]byte, error](envelopeCacheCap)

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

// appendLPInt64 writes a u32le length prefix followed by v in base 10,
// using a stack buffer for the digits so no heap allocation is made for the
// int encoding (strconv.AppendInt(nil, ...) would allocate on every call).
func appendLPInt64(b []byte, v int64) []byte {
	var num [20]byte
	s := strconv.AppendInt(num[:0], v, 10)
	var p [4]byte
	binary.LittleEndian.PutUint32(p[:], uint32(len(s)))
	b = append(b, p[:]...)
	return append(b, s...)
}

// envelopeCacheKey fingerprints (core fields, public key, signature). The
// buffer is pre-sized to the exact output length (int64 fields are bounded by
// 20 base-10 digits) so the appends below grow it at most once — this cuts
// the ~9 growing-appends of the old version to a single allocation, while
// producing byte-identical input to sha256 (same fields, same length
// prefixes, same base-10 int encoding), so cached keys are unchanged.
func envelopeCacheKey(env *afepv1.TaskEnvelope, pub, sig []byte) [32]byte {
	d := env.GetDeadline()
	n := 4*11 +
		len(env.TaskId) + len(env.RootTaskId) + len(env.Tenant) +
		len(env.IssuerAgent) + len(env.Capability) + len(env.PayloadType) +
		len(env.Payload) + len(pub) + len(sig) + 40
	buf := make([]byte, 0, n)
	buf = appendLPS(buf, env.TaskId)
	buf = appendLPS(buf, env.RootTaskId)
	buf = appendLPS(buf, env.Tenant)
	buf = appendLPS(buf, env.IssuerAgent)
	buf = appendLPS(buf, env.Capability)
	buf = appendLPS(buf, env.PayloadType)
	buf = appendLP(buf, env.Payload)
	buf = appendLPInt64(buf, d.GetStartUnix())
	buf = appendLPInt64(buf, d.GetExpiresUnix())
	buf = appendLP(buf, pub)
	buf = appendLP(buf, sig)
	return sha256.Sum256(buf)
}
