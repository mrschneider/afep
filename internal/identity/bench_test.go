package identity

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// Benchmarks pin the signature-path costs (signed-core rebuild per
// sign/verify) so regressions are measurable without a load test.
func benchEnvelope() *afepv1.TaskEnvelope {
	return &afepv1.TaskEnvelope{
		TaskId:      "bench-task",
		RootTaskId:  "bench-task",
		Tenant:      "tenant:bench",
		IssuerAgent: "tenant/bench",
		Capability:  "bench.cap",
		Payload:     []byte("benchmark payload"),
		Deadline:    &afepv1.Deadline{StartUnix: 1700000000, ExpiresUnix: 1700003600},
	}
}

func BenchmarkSignEnvelope(b *testing.B) {
	kp, err := Generate()
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnvelope()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := SignEnvelope(env, kp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerifyEnvelope(b *testing.B) {
	kp, err := Generate()
	if err != nil {
		b.Fatal(err)
	}
	card, err := NewCard("tenant/bench", "bench", kp, nil)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnvelope()
	if err := SignEnvelope(env, kp); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := VerifyEnvelope(env, card); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnvelopeBody(b *testing.B) {
	env := benchEnvelope()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := envelopeBody(env); err != nil {
			b.Fatal(err)
		}
	}
}
