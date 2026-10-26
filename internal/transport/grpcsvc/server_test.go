package grpcsvc

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func mustCard(t *testing.T, id, capability string) *afepv1.AgentCard {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := identity.NewCard(id, id, kp, func(c *afepv1.AgentCard) {
		if capability != "" {
			c.Capabilities = []*afepv1.Capability{{Name: capability, Version: "0.1"}}
		}
		c.Endpoint = &afepv1.Endpoint{GrpcAddress: "bufnet"}
	})
	if err != nil {
		t.Fatal(err)
	}
	return card
}

// newClient starts an in-memory server with one capability provider.
func newClient(t *testing.T) afepv1.AgentTransportServiceClient {
	t.Helper()
	self := mustCard(t, "local/server", "demo.echo")
	provider := mustCard(t, "local/vision", "vision.tag")

	reg := registry.New()
	for _, c := range []*afepv1.AgentCard{self, provider} {
		if err := reg.Put(c); err != nil {
			t.Fatal(err)
		}
	}

	srv := grpc.NewServer()
	afepv1.RegisterAgentTransportServiceServer(srv, New(self, reg))
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return afepv1.NewAgentTransportServiceClient(conn)
}

func TestGetCard(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	resp, err := c.GetCard(ctx, &afepv1.GetCardRequest{AgentId: "local/server"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetCard().GetId() != "local/server" {
		t.Fatalf("card id = %q", resp.GetCard().GetId())
	}
	if err := identity.VerifyCard(resp.GetCard()); err != nil {
		t.Fatalf("served card must verify: %v", err)
	}
	if _, err := c.GetCard(ctx, &afepv1.GetCardRequest{AgentId: "missing/agent"}); err == nil {
		t.Fatal("unknown agent must 404")
	}
}

func TestProbeCommitFlow(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	stream, err := c.Probe(ctx, &afepv1.ProbeRequest{
		Tenant:       "acme",
		Capability:   "vision.tag",
		DeadlineUnix: time.Now().Add(time.Minute).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	taskID := first.GetTaskId()
	if taskID == "" {
		t.Fatal("probe stream must carry a task id")
	}
	bids := len(first.GetBids())
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		bids += len(msg.GetBids())
	}
	if bids != 1 {
		t.Fatalf("bids = %d, want 1 (the vision provider)", bids)
	}

	commit, err := c.Commit(ctx, &afepv1.CommitRequest{TaskId: taskID, WinnerAgent: "local/vision"})
	if err != nil {
		t.Fatal(err)
	}
	if commit.GetEnvelope().GetState() != afepv1.TaskState_TASK_STATE_COMMIT {
		t.Fatalf("state = %s, want COMMIT", commit.GetEnvelope().GetState())
	}
	if commit.GetEnvelope().GetAssigneeAgent() != "local/vision" {
		t.Fatalf("assignee = %q", commit.GetEnvelope().GetAssigneeAgent())
	}

	if _, err := c.Commit(ctx, &afepv1.CommitRequest{TaskId: "nope", WinnerAgent: "local/vision"}); err == nil {
		t.Fatal("unknown task must 404")
	}
	if _, err := c.Commit(ctx, &afepv1.CommitRequest{TaskId: taskID, WinnerAgent: "ghost/agent"}); err == nil {
		t.Fatal("unregistered winner must 404")
	}
}

func TestProbeWithoutProvidersStillYieldsTaskID(t *testing.T) {
	c := newClient(t)
	stream, err := c.Probe(context.Background(), &afepv1.ProbeRequest{Capability: "no.such.cap"})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetTaskId() == "" {
		t.Fatal("empty probe must still expose the task id")
	}
	if len(msg.GetBids()) != 0 {
		t.Fatalf("unexpected bids: %v", msg.GetBids())
	}
}

func TestTaskStreamEcho(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	st, err := c.TaskStream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Envelope frame: gets assigned an id and lands in PROBE.
	if err := st.Send(&afepv1.TaskStreamRequest{Frame: &afepv1.TaskStreamRequest_Envelope{
		Envelope: &afepv1.TaskEnvelope{Capability: "demo.echo", Payload: []byte("hi")},
	}}); err != nil {
		t.Fatal(err)
	}
	in, err := st.Recv()
	if err != nil {
		t.Fatal(err)
	}
	env := in.GetEnvelope()
	if env == nil || env.GetTaskId() == "" || env.GetState() != afepv1.TaskState_TASK_STATE_PROBE {
		t.Fatalf("envelope frame = %v", env)
	}

	// Chunk frame: echoed back on the same connection.
	chunk := &afepv1.Chunk{TaskId: env.GetTaskId(), Data: []byte("partial-output"), Last: true}
	if err := st.Send(&afepv1.TaskStreamRequest{Frame: &afepv1.TaskStreamRequest_Chunk{Chunk: chunk}}); err != nil {
		t.Fatal(err)
	}
	echo, err := st.Recv()
	if err != nil {
		t.Fatal(err)
	}
	got := echo.GetChunk()
	if got == nil || string(got.GetData()) != string(chunk.Data) || !got.GetLast() {
		t.Fatalf("chunk echo = %v", got)
	}

	if err := st.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := st.Recv(); err == io.EOF {
			return
		} else if err != nil {
			t.Fatalf("close handshake: %v", err)
		}
	}
}
