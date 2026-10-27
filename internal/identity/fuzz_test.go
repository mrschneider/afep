package identity

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/proto"
)

// FuzzEnvelopeBytes drives the signed-core builder and envelope
// verification with arbitrary protobuf input: neither may panic.
func FuzzEnvelopeBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x03, 'a', 'b', 'c'}) // task_id = "abc"
	f.Fuzz(func(t *testing.T, data []byte) {
		env := &afepv1.TaskEnvelope{}
		if err := proto.Unmarshal(data, env); err != nil {
			return
		}
		if _, err := envelopeBody(env); err != nil {
			return
		}
		card := &afepv1.AgentCard{PublicKey: make([]byte, 32)}
		_ = VerifyEnvelope(env, card) // error paths only; no panics
	})
}
