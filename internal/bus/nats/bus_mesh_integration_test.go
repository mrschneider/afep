package natsbus

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/mesh"
	"github.com/mrschneider/afep/internal/router"
)

// TestIntegrationMeshBidAndEscalation drives the full mesh loop against a
// real broker: a PROBE lands on the provider domain's durable, the
// provider publishes a signed bid, and an unservable probe escalates back
// to the requester domain's relay. Skipped unless AFEP_NATS is set.
func TestIntegrationMeshBidAndEscalation(t *testing.T) {
	url := os.Getenv("AFEP_NATS")
	if url == "" {
		t.Skip("AFEP_NATS not set")
	}
	run := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// provider node in domain beta
	provBus, err := Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer provBus.Close()
	kp, _ := identity.Generate()
	provCard, err := identity.NewCard("beta/vision", "vision", kp, func(c *afepv1.AgentCard) {
		c.Capabilities = []*afepv1.Capability{{Name: "vision.tag", Version: "0.1"}}
		c.SecurityClearances = []string{"tenant:*"}
	})
	if err != nil {
		t.Fatal(err)
	}
	provRT, err := router.New("beta", []string{"alpha"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	provMesh, err := mesh.New(mesh.Config{
		Domain: "beta", Self: provCard, Key: kp, Score: 7,
		Router: provRT, Bids: provBus, Relay: provBus,
		Issuers: mesh.CardResolverFunc(provBus.GetCard),
	})
	if err != nil {
		t.Fatal(err)
	}
	provDurable := fmt.Sprintf("e2e-prov-%d", run)
	if _, err := provBus.SubscribeEnvelopes(ctx, "beta", provDurable, func(env *afepv1.TaskEnvelope) error {
		err := provMesh.OnEnvelope(ctx, env)
		t.Logf("provider got task=%s state=%s cap=%s -> err=%v", env.GetTaskId(), env.GetState(), env.GetCapability(), err)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// requester node in domain alpha: identity published to the KV
	// directory so the provider can verify its envelopes
	reqBus, err := Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer reqBus.Close()
	reqKP, _ := identity.Generate()
	reqCard, err := identity.NewCard("alpha/requester", "requester", reqKP, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reqBus.PutCard(ctx, reqCard); err != nil {
		t.Fatal(err)
	}
	bids := make(chan *afepv1.Bid, 4)
	bidSub, err := reqBus.SubscribeBids("beta", func(_, _ string, bid *afepv1.Bid) {
		t.Logf("requester bid inbox: task=%s from=%s score=%d", bid.GetTaskId(), bid.GetAgentId(), bid.GetScore())
		bids <- bid
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bidSub.Unsubscribe()
	escDurable := fmt.Sprintf("e2e-req-%d", run)
	escalations := make(chan *afepv1.TaskEnvelope, 4)
	if _, err := reqBus.SubscribeEnvelopes(ctx, "alpha", escDurable, func(env *afepv1.TaskEnvelope) error {
		t.Logf("requester inbox: task=%s state=%s hops=%d", env.GetTaskId(), env.GetState(), env.GetHops())
		escalations <- env
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond) // let durables bind

	// 1) servable probe -> signed bid back
	probe := &afepv1.TaskEnvelope{
		TaskId:      fmt.Sprintf("e2e-bid-%d", run),
		RootTaskId:  fmt.Sprintf("e2e-bid-%d", run),
		State:       afepv1.TaskState_TASK_STATE_PROBE,
		Capability:  "vision.tag",
		Tenant:      "acme",
		IssuerAgent: "alpha/requester",
	}
	if err := identity.SignEnvelope(probe, reqKP); err != nil {
		t.Fatal(err)
	}
	if err := reqBus.PublishEnvelope(ctx, "beta", "probe", probe); err != nil {
		t.Fatal(err)
	}
	select {
	case bid := <-bids:
		if bid.GetTaskId() != probe.GetTaskId() || bid.GetScore() != 7 {
			t.Fatalf("bid = %+v", bid)
		}
		if err := identity.VerifyBid(bid, provCard); err != nil {
			t.Fatalf("mesh bid must verify: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no bid delivered")
	}

	// 2) unservable probe -> escalation relayed to alpha
	probe2 := &afepv1.TaskEnvelope{
		TaskId:      fmt.Sprintf("e2e-esc-%d", run),
		RootTaskId:  fmt.Sprintf("e2e-esc-%d", run),
		State:       afepv1.TaskState_TASK_STATE_PROBE,
		Capability:  "llm.absent",
		IssuerAgent: "alpha/requester",
	}
	if err := identity.SignEnvelope(probe2, reqKP); err != nil {
		t.Fatal(err)
	}
	if err := reqBus.PublishEnvelope(ctx, "beta", "probe", probe2); err != nil {
		t.Fatal(err)
	}
	select {
	case env := <-escalations:
		if env.GetTaskId() != probe2.GetTaskId() {
			t.Fatalf("escalated task = %s", env.GetTaskId())
		}
		if env.GetState() != afepv1.TaskState_TASK_STATE_ESCALATED || env.GetHops() != 1 {
			t.Fatalf("state=%s hops=%d", env.GetState(), env.GetHops())
		}
		if env.GetEscalation().GetFromDomain() != "beta" || env.GetEscalation().GetToDomain() != "alpha" {
			t.Fatalf("escalation = %+v", env.GetEscalation())
		}
		if env.GetRootTaskId() != probe2.GetTaskId() {
			t.Fatal("root chain broken")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no escalation delivered")
	}
}
