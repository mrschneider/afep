package mesh

import (
	"context"
	"errors"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/router"
)

type fakeBids struct {
	domain string
	agent  string
	bid    *afepv1.Bid
	calls  int
}

func (f *fakeBids) PublishBid(domain, agent string, bid *afepv1.Bid) error {
	f.calls++
	f.domain, f.agent, f.bid = domain, agent, bid
	return nil
}

type fakeRelay struct {
	domain string
	agent  string
	env    *afepv1.TaskEnvelope
	calls  int
}

func (f *fakeRelay) PublishEnvelope(_ context.Context, domain, agent string, env *afepv1.TaskEnvelope) error {
	f.calls++
	f.domain, f.agent, f.env = domain, agent, env
	return nil
}

// staticResolver is a test CardResolver over a fixed directory.
type staticResolver map[string]*afepv1.AgentCard

func (r staticResolver) Card(_ context.Context, id string) (*afepv1.AgentCard, bool, error) {
	c, ok := r[id]
	return c, ok, nil
}

type fixture struct {
	self       *afepv1.AgentCard
	kp         *identity.KeyPair
	issuerKP   *identity.KeyPair
	issuerCard *afepv1.AgentCard
	bids       *fakeBids
	relay      *fakeRelay
	rt         *router.Router
	h          *Handler
}

func newFixture(t *testing.T, capability, grant string, peers []string) *fixture {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	self, err := identity.NewCard("tenant/node1", "node1", kp, func(c *afepv1.AgentCard) {
		if capability != "" {
			c.Capabilities = []*afepv1.Capability{{Name: capability, Version: "0.1"}}
		}
		if grant != "" {
			c.SecurityClearances = []string{grant}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	issuerKP, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	issuerCard, err := identity.NewCard("alpha/requester", "requester", issuerKP, nil)
	if err != nil {
		t.Fatal(err)
	}
	var rt *router.Router
	if peers != nil {
		rt, err = router.New("alpha", peers, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	f := &fixture{
		self: self, kp: kp, issuerKP: issuerKP, issuerCard: issuerCard,
		bids: &fakeBids{}, relay: &fakeRelay{}, rt: rt,
	}
	f.h, err = New(Config{
		Domain:  "alpha",
		Self:    self,
		Key:     kp,
		Router:  rt,
		Bids:    f.bids,
		Relay:   f.relay,
		Issuers: staticResolver{"alpha/requester": issuerCard},
		Score:   42,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// envFor builds a fresh PROBE envelope from the requester identity and
// signs its core (the shape a well-behaved peer publishes).
func (f *fixture) envFor(capability, tenant string) *afepv1.TaskEnvelope {
	env := &afepv1.TaskEnvelope{
		TaskId:      "task-1",
		RootTaskId:  "task-1",
		Tenant:      tenant,
		IssuerAgent: "alpha/requester",
		State:       afepv1.TaskState_TASK_STATE_PROBE,
		Capability:  capability,
		Deadline:    &afepv1.Deadline{ExpiresUnix: 1793000000},
	}
	if err := identity.SignEnvelope(env, f.issuerKP); err != nil {
		panic(err)
	}
	return env
}

func TestServePublishesSignedBid(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", nil)
	env := f.envFor("vision.tag", "acme")
	if err := f.h.OnEnvelope(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if f.bids.calls != 1 {
		t.Fatalf("bids published = %d", f.bids.calls)
	}
	if f.bids.domain != "alpha" || f.bids.agent != "tenant-node1" {
		t.Fatalf("bid subject = afep.%s.%s.bids", f.bids.domain, f.bids.agent)
	}
	bid := f.bids.bid
	if bid.GetTaskId() != "task-1" || bid.GetScore() != 42 {
		t.Fatalf("bid = %+v", bid)
	}
	if err := identity.VerifyBid(bid, f.self); err != nil {
		t.Fatalf("bid must verify: %v", err)
	}
	if f.relay.calls != 0 {
		t.Fatal("serving must not escalate")
	}
}

func TestNoServeWithoutClearance(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", nil)
	env := f.envFor("vision.tag", "other")
	if err := f.h.OnEnvelope(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if f.bids.calls != 0 {
		t.Fatal("cleared-out tenant must receive no bid")
	}
}

func TestEscalatesWhenCannotServe(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", []string{"beta"})
	env := f.envFor("llm.infer", "acme")
	if err := f.h.OnEnvelope(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if f.bids.calls != 0 {
		t.Fatal("cannot serve, no bid")
	}
	if f.relay.calls != 1 {
		t.Fatalf("relays = %d", f.relay.calls)
	}
	if f.relay.domain != "beta" || f.relay.agent != router.RelayAgent {
		t.Fatalf("relay target = %s.%s", f.relay.domain, f.relay.agent)
	}
	got := f.relay.env
	if got.GetState() != afepv1.TaskState_TASK_STATE_ESCALATED || got.GetHops() != 1 {
		t.Fatalf("relayed env = state %s hops %d", got.GetState(), got.GetHops())
	}
	if got.GetRootTaskId() != "task-1" {
		t.Fatalf("root = %q", got.GetRootTaskId())
	}
	if got.GetEscalation().GetFromDomain() != "alpha" {
		t.Fatalf("escalation = %+v", got.GetEscalation())
	}
	// relay tail changed after issuance: signature must still verify
	if err := identity.VerifyEnvelope(got, f.issuerCard); err != nil {
		t.Fatalf("relayed envelope must keep verifying: %v", err)
	}
}

func TestRelayedEnvelopeIsServedNotRelayedAgain(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:*", []string{"beta"})
	env := f.envFor("vision.tag", "acme")
	env.State = afepv1.TaskState_TASK_STATE_ESCALATED
	env.Hops = 1
	env.Escalation = &afepv1.Escalation{FromDomain: "beta", ToDomain: "alpha"}
	if err := f.h.OnEnvelope(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if f.bids.calls != 1 || f.relay.calls != 0 {
		t.Fatalf("bids=%d relay=%d — must serve only", f.bids.calls, f.relay.calls)
	}
}

func TestHopLimitDropsInsteadOfLooping(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", []string{"beta"})
	env := f.envFor("llm.infer", "acme") // cannot serve -> escalation path
	env.State = afepv1.TaskState_TASK_STATE_ESCALATED
	env.Hops = 5 // router default max
	env.Escalation = &afepv1.Escalation{FromDomain: "gamma", ToDomain: "alpha"}
	if err := f.h.OnEnvelope(context.Background(), env); err != nil {
		t.Fatalf("hop exhaustion must ack-drop, got %v", err)
	}
	if f.relay.calls != 0 || f.bids.calls != 0 {
		t.Fatal("nothing may be published at hop limit")
	}
}

func TestSchemaViolationReturnsRejected(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", nil)
	env := f.envFor("vision.tag", "acme")
	env.PayloadType = "application/x-afep-echo+proto"
	env.Payload = []byte{0x0A, 0x05, 0x61} // truncated mid-field
	err := f.h.OnEnvelope(context.Background(), env)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("want ErrRejected, got %v", err)
	}
	if f.bids.calls != 0 || f.relay.calls != 0 {
		t.Fatal("rejected envelopes must not fan out")
	}
	if err := f.h.OnEnvelope(context.Background(), nil); !errors.Is(err, ErrRejected) {
		t.Fatalf("nil envelope: want ErrRejected, got %v", err)
	}
}

func TestUnsignedEnvelopeRejected(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", nil)
	env := f.envFor("vision.tag", "acme")
	env.Signature = nil // issuer present, signature stripped
	err := f.h.OnEnvelope(context.Background(), env)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("unsigned: want ErrRejected, got %v", err)
	}

	noIssuer := f.envFor("vision.tag", "acme")
	noIssuer.IssuerAgent = ""
	if err := f.h.OnEnvelope(context.Background(), noIssuer); !errors.Is(err, ErrRejected) {
		t.Fatalf("no issuer: want ErrRejected, got %v", err)
	}
	if f.bids.calls != 0 || f.relay.calls != 0 {
		t.Fatal("untrusted envelopes must not fan out")
	}
}

func TestUnknownIssuerRejected(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", nil)
	stranger, _ := identity.Generate()
	env := f.envFor("vision.tag", "acme")
	env.IssuerAgent = "ghost/issuer"
	if err := identity.SignEnvelope(env, stranger); err != nil {
		t.Fatal(err)
	}
	err := f.h.OnEnvelope(context.Background(), env)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("unknown issuer: want ErrRejected, got %v", err)
	}
	if f.bids.calls != 0 {
		t.Fatal("unknown issuer must not receive a bid")
	}
}

func TestForeignSignatureRejected(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", nil)
	foreign, _ := identity.Generate()
	env := f.envFor("vision.tag", "acme")
	if err := identity.SignEnvelope(env, foreign); err != nil {
		t.Fatal(err) // re-signed with the wrong key: resolver card won't match
	}
	err := f.h.OnEnvelope(context.Background(), env)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("foreign signature: want ErrRejected, got %v", err)
	}
}

func TestNonProbeStatesAck(t *testing.T) {
	f := newFixture(t, "vision.tag", "tenant:acme", []string{"beta"})
	for _, st := range []afepv1.TaskState{
		afepv1.TaskState_TASK_STATE_RUNNING,
		afepv1.TaskState_TASK_STATE_DONE,
		afepv1.TaskState_TASK_STATE_FAILED,
		afepv1.TaskState_TASK_STATE_COMMIT,
		afepv1.TaskState_TASK_STATE_BID,
	} {
		env := f.envFor("vision.tag", "acme")
		env.State = st
		if err := f.h.OnEnvelope(context.Background(), env); err != nil {
			t.Fatalf("state %s: %v", st, err)
		}
	}
	if f.bids.calls != 0 || f.relay.calls != 0 {
		t.Fatal("non-probe states must be acked silently")
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty config must fail")
	}
	kp, _ := identity.Generate()
	self, _ := identity.NewCard("a/x", "x", kp, nil)
	if _, err := New(Config{Domain: "d", Self: self, Key: kp, Bids: &fakeBids{}, Router: mustRouter(t)}); err == nil {
		t.Fatal("router without relay publisher must fail")
	}
	if _, err := New(Config{Domain: "d", Self: self, Key: kp, Bids: &fakeBids{}}); err == nil {
		t.Fatal("missing issuer resolver must fail")
	}
	issuer, _ := identity.NewCard("a/y", "y", kp, nil)
	if _, err := New(Config{
		Domain: "d", Self: self, Key: kp, Bids: &fakeBids{},
		Issuers: staticResolver{"a/y": issuer},
	}); err != nil {
		t.Fatalf("complete config must be accepted: %v", err)
	}
}

func mustRouter(t *testing.T) *router.Router {
	t.Helper()
	r, err := router.New("alpha", []string{"beta"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
