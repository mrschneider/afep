package identity

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func profEnv() *afepv1.TaskEnvelope {
	return &afepv1.TaskEnvelope{
		TaskId: "bench-task", RootTaskId: "bench-task", Tenant: "tenant:bench",
		IssuerAgent: "tenant/bench", Capability: "bench.cap",
		Payload:     []byte("benchmark payload data"),
		PayloadType: "application/x-afep-bench",
		Deadline:    &afepv1.Deadline{StartUnix: 1700000000, ExpiresUnix: 1700003600},
		Hops:        3, State: afepv1.TaskState_TASK_STATE_PROBE,
	}
}

func BenchmarkCanonicalProfile(b *testing.B) {
	env := profEnv()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = canonicalEnvelopeBytes(env)
	}
}

func BenchmarkCacheKeyProfile(b *testing.B) {
	kp, _ := Generate()
	env := profEnv()
	_ = SignEnvelope(env, kp)
	pub := kp.Public
	sig := env.Signature
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = envelopeCacheKey(env, pub, sig)
	}
}
