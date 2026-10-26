package natsbus

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
)

// TestIntegrationRoundtrip exercises Dial, KV cards, publish and the durable
// subscriber against a real broker. Skipped unless AFEP_NATS is set.
func TestIntegrationRoundtrip(t *testing.T) {
	url := os.Getenv("AFEP_NATS")
	if url == "" {
		t.Skip("AFEP_NATS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bus, err := Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()

	// KV card roundtrip.
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := identity.NewCard("local/integ", "integ", kp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.PutCard(ctx, card); err != nil {
		t.Fatal(err)
	}
	got, ok, err := bus.GetCard(ctx, "local/integ")
	if err != nil || !ok {
		t.Fatalf("getcard: ok=%v err=%v", ok, err)
	}
	if err := identity.VerifyCard(got); err != nil {
		t.Fatalf("kv card must verify: %v", err)
	}
	if _, ok, _ := bus.GetCard(ctx, "missing/agent"); ok {
		t.Fatal("unknown card must miss")
	}

	// Durable subscriber + at-least-once delivery with dedup by task id.
	taskID := fmt.Sprintf("integ-%d", time.Now().UnixNano())
	env := &afepv1.TaskEnvelope{
		TaskId:      taskID,
		RootTaskId:  taskID,
		State:       afepv1.TaskState_TASK_STATE_PROBE,
		Capability:  "demo.echo",
		Tenant:      "acme",
		IssuerAgent: "local/integ",
	}
	delivered := make(chan *afepv1.TaskEnvelope, 1)
	cc, err := bus.SubscribeEnvelopes(ctx, "local", "integ-durable", func(e *afepv1.TaskEnvelope) error {
		delivered <- e
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	// Give the consumer a moment to bind the filter.
	time.Sleep(300 * time.Millisecond)

	if err := bus.PublishEnvelope(ctx, "local", "integ-node", env); err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-delivered:
		if e.GetTaskId() != taskID {
			t.Fatalf("task id = %q, want %q", e.GetTaskId(), taskID)
		}
		if e.GetCapability() != "demo.echo" {
			t.Fatalf("capability = %q", e.GetCapability())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("envelope not delivered within 10s")
	}
}
