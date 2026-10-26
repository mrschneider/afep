package router

import (
	"context"
	"errors"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

type fakePub struct {
	domain string
	agent  string
	env    *afepv1.TaskEnvelope
	err    error
	calls  int
}

func (f *fakePub) PublishEnvelope(_ context.Context, domain, agent string, env *afepv1.TaskEnvelope) error {
	f.calls++
	f.domain, f.agent, f.env = domain, agent, env
	return f.err
}

func probeEnv() *afepv1.TaskEnvelope {
	return &afepv1.TaskEnvelope{
		TaskId:     "t1",
		RootTaskId: "t1",
		State:      afepv1.TaskState_TASK_STATE_PROBE,
		Capability: "vision.tag",
	}
}

func TestEscalateRoutesToFirstPeer(t *testing.T) {
	r, err := New("alpha", []string{"beta", "gamma"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	pub := &fakePub{}
	env := probeEnv()
	if err := r.Escalate(context.Background(), pub, env, ""); err != nil {
		t.Fatal(err)
	}
	if pub.domain != "beta" || pub.agent != RelayAgent {
		t.Fatalf("published to %s.%s, want beta.%s", pub.domain, pub.agent, RelayAgent)
	}
	if env.GetState() != afepv1.TaskState_TASK_STATE_ESCALATED {
		t.Fatalf("state = %s", env.GetState())
	}
	if env.GetHops() != 1 {
		t.Fatalf("hops = %d", env.GetHops())
	}
	if env.GetRootTaskId() != "t1" || env.GetTaskId() != "t1" {
		t.Fatalf("audit chain broken: %+v", env)
	}
	esc := env.GetEscalation()
	if esc.GetFromDomain() != "alpha" || esc.GetToDomain() != "beta" || esc.GetReasonCode() != "NO_BIDS" {
		t.Fatalf("escalation = %+v", esc)
	}
}

func TestNextPeerSkipsArrivalDomain(t *testing.T) {
	r, _ := New("alpha", []string{"beta", "gamma"}, 0)
	env := probeEnv()
	env.Escalation = &afepv1.Escalation{FromDomain: "beta", ToDomain: "alpha"}
	to, ok := r.NextPeer(env)
	if !ok || to != "gamma" {
		t.Fatalf("next = %q ok=%v, want gamma", to, ok)
	}
}

func TestChainExhausted(t *testing.T) {
	r, _ := New("alpha", []string{"beta"}, 0)
	env := probeEnv()
	env.Escalation = &afepv1.Escalation{FromDomain: "beta"}
	if _, ok := r.NextPeer(env); ok {
		t.Fatal("no peer should remain")
	}
	pub := &fakePub{}
	if err := r.Escalate(context.Background(), pub, env, ""); !errors.Is(err, ErrNoPeer) {
		t.Fatalf("want ErrNoPeer, got %v", err)
	}
	if pub.calls != 0 {
		t.Fatal("nothing must be published")
	}
}

func TestHopLimitStopsLoop(t *testing.T) {
	r, _ := New("alpha", []string{"beta"}, 3)
	env := probeEnv()
	env.Hops = 3
	pub := &fakePub{}
	err := r.EscalateTo(context.Background(), pub, env, "beta", "NO_BIDS")
	if !errors.Is(err, ErrHopLimit) {
		t.Fatalf("want ErrHopLimit, got %v", err)
	}
	if pub.calls != 0 || env.GetState() != afepv1.TaskState_TASK_STATE_PROBE {
		t.Fatal("hop-limit must not publish nor mutate state")
	}
}

func TestIllegalStateRefused(t *testing.T) {
	r, _ := New("alpha", []string{"beta"}, 0)
	env := probeEnv()
	env.State = afepv1.TaskState_TASK_STATE_DONE
	pub := &fakePub{}
	if err := r.EscalateTo(context.Background(), pub, env, "beta", ""); err == nil {
		t.Fatal("DONE envelope must not escalate")
	}
	if pub.calls != 0 {
		t.Fatal("nothing must be published")
	}
}

func TestTargetValidation(t *testing.T) {
	r, _ := New("alpha", []string{"beta"}, 0)
	pub := &fakePub{}
	if err := r.EscalateTo(context.Background(), pub, probeEnv(), "alpha", ""); err == nil {
		t.Fatal("self-target must fail")
	}
	if err := r.EscalateTo(context.Background(), pub, probeEnv(), "", ""); err == nil {
		t.Fatal("empty target must fail")
	}
}

func TestNewFiltersPeers(t *testing.T) {
	r, err := New("alpha", []string{"", "alpha", "beta", "beta", "gamma"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	peers := r.Peers()
	if len(peers) != 2 || peers[0] != "beta" || peers[1] != "gamma" {
		t.Fatalf("peers = %v", peers)
	}
	if _, err := New("", nil, 0); err == nil {
		t.Fatal("empty domain must fail")
	}
	if r.maxHops != DefaultMaxHops {
		t.Fatalf("maxHops = %d", r.maxHops)
	}
}

func TestPublishErrorPropagates(t *testing.T) {
	r, _ := New("alpha", []string{"beta"}, 0)
	pub := &fakePub{err: errors.New("broker down")}
	if err := r.EscalateTo(context.Background(), pub, probeEnv(), "beta", ""); err == nil {
		t.Fatal("publish failure must surface")
	}
}
