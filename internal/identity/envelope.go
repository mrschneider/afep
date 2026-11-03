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
	"google.golang.org/protobuf/reflect/protoreflect"
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
// the envelope, replacing the previous proto.Clone + deterministic-marshal
// path. The deterministic marshal allocates a whole copy of the message just
// to zero a handful of fields — for the hot verify path that clone is a
// meaningful per-envelope cost (benchmarks show ~1 µs/op of the ~4 µs
// VerifyEnvelope).
//
// The signed core is the request essence — ids, tenant, issuer, capability,
// payload and deadline. The lifecycle tail (state, assignee, bids, updated_at,
// escalation, hops) is relay-mutable: routers stamp it without the issuer key,
// so it is protected by hop-qualified dedup keys and the audit chain rather
// than by this signature. Callers sign after building the request, not after
// each hop.
//
// Instead of the old proto.Clone + set-tail-fields-zero path (which deep-copies
// the whole message just to zero those fields), we build a bare TaskEnvelope
// through the proto reflection API and set ONLY the core fields envelopeBody
// signs. proto3 omits zero-valued scalar fields on the wire, so the output is
// byte-identical to the old clone-and-zero approach — the conformance vectors
// byte-verify this equivalence — while avoiding a deep copy of the (possibly
// large) payload.
func canonicalEnvelopeBytes(env *afepv1.TaskEnvelope) ([]byte, error) {
	// Build the bare TaskEnvelope directly through the generated type, setting
	// only the core fields envelopeBody signs. Using the concrete type (rather
	// than the protoreflect.Message interface proto.Message reflection returns)
	// keeps the reflection writes while producing a proto.Message we can marshal.
	m := &afepv1.TaskEnvelope{}
	pb := m.ProtoReflect()
	fd := pb.Descriptor().Fields()
	set := func(n protoreflect.FieldNumber, v protoreflect.Value) {
		pb.Set(fd.ByNumber(n), v)
	}

	if env.GetTaskId() != "" {
		set(1, protoreflect.ValueOfString(env.GetTaskId()))
	}
	if env.GetRootTaskId() != "" {
		set(2, protoreflect.ValueOfString(env.GetRootTaskId()))
	}
	if env.GetTenant() != "" {
		set(3, protoreflect.ValueOfString(env.GetTenant()))
	}
	if env.GetIssuerAgent() != "" {
		set(4, protoreflect.ValueOfString(env.GetIssuerAgent()))
	}
	if env.GetCapability() != "" {
		set(7, protoreflect.ValueOfString(env.GetCapability()))
	}
	if env.GetPayloadType() != "" {
		set(8, protoreflect.ValueOfString(env.GetPayloadType()))
	}
	if len(env.GetPayload()) != 0 {
		set(9, protoreflect.ValueOfBytes(env.GetPayload()))
	}
	if d := env.GetDeadline(); d != nil {
		if d.GetStartUnix() != 0 || d.GetExpiresUnix() != 0 {
			sub := afepv1.Deadline{StartUnix: d.GetStartUnix(), ExpiresUnix: d.GetExpiresUnix()}
			set(11, protoreflect.ValueOfMessage(sub.ProtoReflect()))
		}
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
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
