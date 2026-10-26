// Package router relays envelopes across administrative domains: the
// ESCALATED hop of the task lifecycle, bounded so domains cannot ping-pong.
package router

import (
	"context"
	"errors"
	"fmt"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/lifecycle"
)

// DefaultMaxHops bounds cross-domain relays when none is configured.
const DefaultMaxHops = 5

// RelayAgent is the subject segment every domain consumer listens on:
// publishing to afep.<domain>.relay.envelopes reaches all durable
// consumers filtered on afep.<domain>.*.envelopes.
const RelayAgent = "relay"

// ErrHopLimit marks envelopes that exhausted their relay budget.
var ErrHopLimit = errors.New("afep/router: hop limit")

// ErrNoPeer marks envelopes whose escalation chain visited every peer.
var ErrNoPeer = errors.New("afep/router: no peer left")

// Publisher is the outbound envelope path (implemented by natsbus.Bus).
type Publisher interface {
	PublishEnvelope(ctx context.Context, domain, agent string, env *afepv1.TaskEnvelope) error
}

// Router escalates envelopes from the local domain to a peer list.
type Router struct {
	from    string
	peers   []string
	maxHops uint32
}

// New builds a router; empty peer entries, duplicates and the local domain
// are filtered out. maxHops 0 selects DefaultMaxHops.
func New(from string, peers []string, maxHops uint32) (*Router, error) {
	if from == "" {
		return nil, errors.New("afep/router: empty local domain")
	}
	if maxHops == 0 {
		maxHops = DefaultMaxHops
	}
	seen := map[string]bool{from: true}
	clean := make([]string, 0, len(peers))
	for _, p := range peers {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		clean = append(clean, p)
	}
	return &Router{from: from, peers: clean, maxHops: maxHops}, nil
}

// Peers returns the escalation candidates in order.
func (r *Router) Peers() []string {
	return append([]string(nil), r.peers...)
}

// From reports the local domain.
func (r *Router) From() string { return r.from }

// NextPeer picks the next domain: the first peer that is neither the local
// domain nor the domain the envelope arrived from.
func (r *Router) NextPeer(env *afepv1.TaskEnvelope) (string, bool) {
	arrivedFrom := ""
	if esc := env.GetEscalation(); esc != nil {
		arrivedFrom = esc.GetFromDomain()
	}
	for _, p := range r.peers {
		if p == arrivedFrom {
			continue
		}
		return p, true
	}
	return "", false
}

// Escalate routes the envelope to the next eligible peer domain.
func (r *Router) Escalate(ctx context.Context, pub Publisher, env *afepv1.TaskEnvelope, reason string) error {
	to, ok := r.NextPeer(env)
	if !ok {
		return fmt.Errorf("%w: task %s has no eligible peer", ErrNoPeer, env.GetTaskId())
	}
	return r.EscalateTo(ctx, pub, env, to, reason)
}

// EscalateTo forces a specific target domain: apply the ESCALATED
// transition, stamp the audit chain, bump hops and publish to the relay
// subject. Both mutations happen only if the whole path will run.
func (r *Router) EscalateTo(ctx context.Context, pub Publisher, env *afepv1.TaskEnvelope, to, reason string) error {
	if env == nil {
		return errors.New("afep/router: nil envelope")
	}
	if to == "" || to == r.from {
		return fmt.Errorf("afep/router: invalid target domain %q", to)
	}
	if env.GetHops() >= r.maxHops {
		return fmt.Errorf("%w: task %s reached %d hops", ErrHopLimit, env.GetTaskId(), env.GetHops())
	}
	if reason == "" {
		reason = "NO_BIDS"
	}
	if err := lifecycle.Apply(env, afepv1.TaskState_TASK_STATE_ESCALATED); err != nil {
		return fmt.Errorf("afep/router: %w", err)
	}
	env.Hops = env.GetHops() + 1
	env.Escalation = &afepv1.Escalation{
		FromDomain: r.from,
		ToDomain:   to,
		ReasonCode: reason,
		Detail:     fmt.Sprintf("hops=%d", env.GetHops()),
	}
	if err := pub.PublishEnvelope(ctx, to, RelayAgent, env); err != nil {
		return fmt.Errorf("afep/router: publish: %w", err)
	}
	return nil
}
