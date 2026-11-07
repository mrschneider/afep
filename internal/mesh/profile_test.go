package mesh

import (
	"context"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/schemareg"
	"google.golang.org/protobuf/proto"
)

func profHandler(t *testing.B) (*Handler, *afepv1.TaskEnvelope, *identity.KeyPair) {
	kp, _ := identity.Generate()
	self, _ := identity.NewCard("tenant/bench", "bench", kp, func(c *afepv1.AgentCard) {
		c.Capabilities = append(c.Capabilities, &afepv1.Capability{Name: "bench.cap", Version: "1"})
		c.SecurityClearances = []string{"tenant:*"}
	})
	reg := schemareg.New()
	_ = reg.Register("application/x-afep-bench", func() proto.Message { return &afepv1.TaskEnvelope{} })
	h, _ := New(Config{
		Domain: "bench", Self: self, Key: kp, Bids: benchBids{}, Relay: benchRelay{},
		Schemas: reg, Issuers: CardResolverFunc(func(context.Context, string) (*afepv1.AgentCard, bool, error) { return self, true, nil }),
	})
	env := &afepv1.TaskEnvelope{
		TaskId: "bench-task", RootTaskId: "bench-task", Tenant: "tenant:bench",
		IssuerAgent: "tenant/bench", State: afepv1.TaskState_TASK_STATE_PROBE,
		Capability: "bench.cap", PayloadType: "application/x-afep-bench",
	}
	_ = identity.SignEnvelope(env, kp)
	return h, env, kp
}

func BenchmarkOnEnvelopeProfile(b *testing.B) {
	h, env, _ := profHandler(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.OnEnvelope(ctx, env)
	}
}

func BenchmarkSignBidProfile(b *testing.B) {
	_, _, kp := profHandler(b)
	bid := &afepv1.Bid{AgentId: "tenant/bench", Score: 100, TaskId: "t", Rationale: "x", ProposedDeadlineUnix: 1700003600}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = identity.SignBid(bid, kp)
	}
}
