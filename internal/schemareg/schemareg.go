// Package schemareg validates TaskEnvelope payloads against a registry of
// media types — the schema boundary every ingress checks.
package schemareg

import (
	"encoding/json"
	"fmt"
	"sync"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

// MediaTypeEcho is the built-in demo payload type.
const MediaTypeEcho = "application/x-afep-echo+proto"

// MediaTypeJSON validates bodies as (any) JSON documents.
const MediaTypeJSON = "application/json"

// Registry maps media types to payload message factories.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]func() proto.Message
}

// New returns a registry with the built-in types registered.
func New() *Registry {
	r := &Registry{factories: make(map[string]func() proto.Message)}
	_ = r.Register(MediaTypeEcho, func() proto.Message { return &afepv1.EchoPayload{} })
	return r
}

// Register adds a media type. Factories return a fresh message per call.
func (r *Registry) Register(mediaType string, factory func() proto.Message) error {
	if mediaType == "" {
		return fmt.Errorf("afep/schemareg: empty media type")
	}
	if factory == nil {
		return fmt.Errorf("afep/schemareg: nil factory for %q", mediaType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.factories[mediaType]; dup {
		return fmt.Errorf("afep/schemareg: duplicate media type %q", mediaType)
	}
	r.factories[mediaType] = factory
	return nil
}

// Get resolves a registered factory.
func (r *Registry) Get(mediaType string) (func() proto.Message, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[mediaType]
	return f, ok
}

// Validate checks a payload_type/payload pair:
//   - both empty: ok (probe-only envelopes);
//   - type registered, body empty: ok (declaration without payload);
//   - unknown type or body that fails JSON/proto decode: error.
func (r *Registry) Validate(mediaType string, payload []byte) error {
	if mediaType == "" {
		if len(payload) > 0 {
			return fmt.Errorf("afep/schemareg: payload bytes without payload_type")
		}
		return nil
	}
	if mediaType == MediaTypeJSON {
		if len(payload) == 0 {
			return nil
		}
		if !json.Valid(payload) {
			return fmt.Errorf("afep/schemareg: invalid JSON body")
		}
		return nil
	}
	factory, ok := r.Get(mediaType)
	if !ok {
		return fmt.Errorf("afep/schemareg: unregistered payload_type %q", mediaType)
	}
	if len(payload) == 0 {
		return nil
	}
	msg := factory()
	if err := proto.Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("afep/schemareg: body does not match %s: %w", mediaType, err)
	}
	if unknown := msg.ProtoReflect().GetUnknown(); len(unknown) > 0 {
		return fmt.Errorf("afep/schemareg: body carries fields outside schema %s", mediaType)
	}
	return nil
}

// ValidateEnvelope is the ingress check for TaskEnvelopes.
func (r *Registry) ValidateEnvelope(env *afepv1.TaskEnvelope) error {
	if env == nil {
		return fmt.Errorf("afep/schemareg: nil envelope")
	}
	return r.Validate(env.GetPayloadType(), env.GetPayload())
}
