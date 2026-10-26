package grpcsvc

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/registry"
	"github.com/mrschneider/afep/internal/taskstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// cardSpec describes a test identity.
type cardSpec struct {
	kp   *identity.KeyPair
	card *afepv1.AgentCard
}

func mustCard(t *testing.T, id, capability string, grants ...string) cardSpec {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	card, err := identity.NewCard(id, id, kp, func(c *afepv1.AgentCard) {
		if capability != "" {
			c.Capabilities = []*afepv1.Capability{{Name: capability, Version: "0.1"}}
		}
		if len(grants) > 0 {
			c.SecurityClearances = grants
		}
		c.Endpoint = &afepv1.Endpoint{GrpcAddress: "bufnet"}
	})
	if err != nil {
		t.Fatal(err)
	}
	return cardSpec{kp: kp, card: card}
}

// startService serves one identity over bufconn and returns a client.
func startService(t *testing.T, self cardSpec, reg *registry.Registry, opts ...Option) afepv1.AgentTransportServiceClient {
	t.Helper()
	if reg == nil {
		reg = registry.New()
	}
	if err := reg.Put(self.card); err != nil {
		t.Fatal(err)
	}
	svc, err := New(self.card, reg, self.kp, opts...)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	afepv1.RegisterAgentTransportServiceServer(srv, svc)
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
	self := mustCard(t, "local/server", "demo.echo")
	c := startService(t, self, nil)
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

func TestProbeReturnsSignedSelfBidThenCommit(t *testing.T) {
	self := mustCard(t, "local/vision", "vision.tag", "tenant:acme")
	c := startService(t, self, nil)
	ctx := context.Background()

	stream, err := c.Probe(ctx, &afepv1.ProbeRequest{
		Tenant:       "acme",
		Capability:   "vision.tag",
		DeadlineUnix: time.Now().Add(time.Minute).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("probe must end after one message, got %v", err)
	}
	if msg.GetTaskId() == "" {
		t.Fatal("probe must carry a task id")
	}
	if len(msg.GetBids()) != 1 {
		t.Fatalf("bids = %d, want 1", len(msg.GetBids()))
	}
	bid := msg.GetBids()[0]
	if err := identity.VerifyBid(bid, self.card); err != nil {
		t.Fatalf("self-bid must verify against the card: %v", err)
	}
	if bid.GetTaskId() != msg.GetTaskId() {
		t.Fatalf("bid task id = %q, want %q", bid.GetTaskId(), msg.GetTaskId())
	}
	if bid.GetScore() != 100 {
		t.Fatalf("default score = %d, want 100", bid.GetScore())
	}

	commit, err := c.Commit(ctx, &afepv1.CommitRequest{TaskId: msg.GetTaskId(), WinnerAgent: "local/vision"})
	if err != nil {
		t.Fatal(err)
	}
	if commit.GetEnvelope().GetState() != afepv1.TaskState_TASK_STATE_COMMIT {
		t.Fatalf("state = %s", commit.GetEnvelope().GetState())
	}
	if err := identity.VerifyEnvelope(commit.GetEnvelope(), self.card); err != nil {
		t.Fatalf("emitted envelope must carry issuer signature: %v", err)
	}
	if _, err := c.Commit(ctx, &afepv1.CommitRequest{TaskId: "nope", WinnerAgent: "local/vision"}); err == nil {
		t.Fatal("unknown task must 404")
	}
}

func TestProbeClearanceDeniedYieldsNoBids(t *testing.T) {
	self := mustCard(t, "local/vision", "vision.tag", "tenant:acme")
	c := startService(t, self, nil)

	stream, err := c.Probe(context.Background(), &afepv1.ProbeRequest{Tenant: "other", Capability: "vision.tag"})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetTaskId() == "" {
		t.Fatal("task id must still be delivered")
	}
	if len(msg.GetBids()) != 0 {
		t.Fatalf("clearance-denied probe must yield 0 bids, got %d", len(msg.GetBids()))
	}
}

func TestProbeCapabilityNotHeldYieldsNoBids(t *testing.T) {
	self := mustCard(t, "local/vision", "vision.tag")
	c := startService(t, self, nil)

	stream, err := c.Probe(context.Background(), &afepv1.ProbeRequest{Capability: "llm.infer"})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(msg.GetBids()) != 0 {
		t.Fatalf("bids = %d, want 0", len(msg.GetBids()))
	}
}

func TestProbeUnregisteredPayloadTypeRejected(t *testing.T) {
	self := mustCard(t, "local/vision", "vision.tag")
	c := startService(t, self, nil)

	stream, err := c.Probe(context.Background(), &afepv1.ProbeRequest{
		Capability:  "vision.tag",
		PayloadType: "application/x-nonsense",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestTaskStreamRejectsBadPayload(t *testing.T) {
	self := mustCard(t, "local/server", "demo.echo")
	c := startService(t, self, nil)
	st, err := c.TaskStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// declared echo schema, body truncated mid-field: must not be admitted
	bad := &afepv1.TaskEnvelope{
		PayloadType: "application/x-afep-echo+proto",
		Payload:     []byte{0x0A, 0x05, 0x61, 0x62},
	}
	if err := st.Send(&afepv1.TaskStreamRequest{Frame: &afepv1.TaskStreamRequest_Envelope{Envelope: bad}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for bad payload, got %v", err)
	}
}

func TestTaskStreamRejectsUnregisteredPayloadType(t *testing.T) {
	self := mustCard(t, "local/server", "demo.echo")
	c := startService(t, self, nil)
	st, err := c.TaskStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	env := &afepv1.TaskEnvelope{PayloadType: "application/x-nope", Payload: []byte("x")}
	if err := st.Send(&afepv1.TaskStreamRequest{Frame: &afepv1.TaskStreamRequest_Envelope{Envelope: env}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for unregistered type, got %v", err)
	}
}

func TestTaskStreamEcho(t *testing.T) {
	self := mustCard(t, "local/server", "demo.echo")
	c := startService(t, self, nil)
	ctx := context.Background()
	st, err := c.TaskStream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Send(&afepv1.TaskStreamRequest{Frame: &afepv1.TaskStreamRequest_Envelope{
		Envelope: &afepv1.TaskEnvelope{Capability: "demo.echo"},
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

func TestTaskStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	self := mustCard(t, "local/vision", "vision.tag", "tenant:acme")
	c := startService(t, self, nil, WithTaskStore(mustStore(t, path)))

	stream, err := c.Probe(context.Background(), &afepv1.ProbeRequest{Tenant: "acme", Capability: "vision.tag"})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	taskID := msg.GetTaskId()
	if _, err := c.Commit(context.Background(), &afepv1.CommitRequest{TaskId: taskID, WinnerAgent: "local/vision"}); err != nil {
		t.Fatal(err)
	}

	// simulate restart: reopen the file independently of the server
	st2, err := taskstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	env, ok := st2.Get(taskID)
	if !ok {
		t.Fatal("committed task must be persisted")
	}
	if env.GetState() != afepv1.TaskState_TASK_STATE_COMMIT {
		t.Fatalf("persisted state = %s", env.GetState())
	}
}

func mustStore(t *testing.T, path string) *taskstore.Store {
	t.Helper()
	st, err := taskstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestNewRequiresIdentity(t *testing.T) {
	self := mustCard(t, "local/x", "demo.echo")
	reg := registry.New()
	if _, err := New(self.card, reg, nil); err == nil {
		t.Fatal("nil key pair must fail")
	}
	if _, err := New(nil, reg, self.kp); err == nil {
		t.Fatal("nil card must fail")
	}
}
