package mesh

import (
	"context"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/schemareg"
	"google.golang.org/protobuf/proto"
)

type benchBids struct{}

func (benchBids) PublishBid(domain, agent string, bid *afepv1.Bid) error { return nil }

type benchRelay struct{}

func (benchRelay) PublishEnvelope(ctx context.Context, domain, agent string, env *afepv1.TaskEnvelope) error {
	return nil
}

// BenchmarkOnEnvelope measures the full inbound path: schema gate,
// issuer verification (clone + deterministic marshal + ed25519), serve
// check and bid publication.
func BenchmarkOnEnvelope(b *testing.B) {
	kp, err := identity.Generate()
	if err != nil {
		b.Fatal(err)
	}
	self, err := identity.NewCard("tenant/bench", "bench", kp, func(c *afepv1.AgentCard) {
		c.Capabilities = append(c.Capabilities, &afepv1.Capability{Name: "bench.cap", Version: "1"})
		c.SecurityClearances = []string{"tenant:*"}
	})
	if err != nil {
		b.Fatal(err)
	}
	reg := schemareg.New()
	if err := reg.Register("application/x-afep-bench", func() proto.Message {
		return &afepv1.TaskEnvelope{}
	}); err != nil {
		b.Fatal(err)
	}
	h, err := New(Config{
		Domain:  "bench",
		Self:    self,
		Key:     kp,
		Bids:    benchBids{},
		Relay:   benchRelay{},
		Schemas: reg,
		Issuers: CardResolverFunc(func(context.Context, string) (*afepv1.AgentCard, bool, error) {
			return self, true, nil
		}),
	})
	if err != nil {
		b.Fatal(err)
	}
	env := &afepv1.TaskEnvelope{
		TaskId:      "bench-task",
		RootTaskId:  "bench-task",
		Tenant:      "tenant:bench",
		IssuerAgent: "tenant/bench",
		State:       afepv1.TaskState_TASK_STATE_PROBE,
		Capability:  "bench.cap",
		PayloadType: "application/x-afep-bench",
	}
	if err := identity.SignEnvelope(env, kp); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.OnEnvelope(ctx, env); err != nil {
			b.Fatal(err)
		}
	}
}
