// Package mesh binds bus deliveries to AFEP policy: serve signed bids for
// envelopes this node can handle, escalate the rest to peer domains.
package mesh

import (
	"context"
	"errors"
	"fmt"
	"strings"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/clearance"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/router"
	"github.com/mrschneider/afep/internal/schemareg"
)

// ErrRejected marks envelopes that failed the schema boundary: consumers
// should Term them (poison) instead of redelivering forever.
var ErrRejected = errors.New("afep/mesh: envelope rejected")

// BidPublisher is the outbound bid path (natsbus.Bus.PublishBid).
type BidPublisher interface {
	PublishBid(domain, agent string, bid *afepv1.Bid) error
}

// CardResolver resolves an issuer's AgentCard (KV directory in
// production, static maps in tests). ok=false means unknown issuer.
type CardResolver interface {
	Card(ctx context.Context, agentID string) (*afepv1.AgentCard, bool, error)
}

// CardResolverFunc adapts a plain function (e.g. natsbus.Bus.GetCard).
type CardResolverFunc func(ctx context.Context, agentID string) (*afepv1.AgentCard, bool, error)

// Card implements CardResolver.
func (f CardResolverFunc) Card(ctx context.Context, agentID string) (*afepv1.AgentCard, bool, error) {
	return f(ctx, agentID)
}

// Config wires a mesh handler. Domain, Self, Key and Bids are mandatory;
// Router is optional (no escalation without it).
type Config struct {
	Domain  string
	Self    *afepv1.AgentCard
	Key     *identity.KeyPair
	Score   uint64 // 0 selects the default
	Schemas *schemareg.Registry
	Router  *router.Router
	Bids    BidPublisher
	Relay   router.Publisher
	Issuers CardResolver
}

// Handler processes one inbound envelope per call.
type Handler struct {
	domain  string
	self    *afepv1.AgentCard
	kp      *identity.KeyPair
	score   uint64
	schemas *schemareg.Registry
	router  *router.Router
	bids    BidPublisher
	relay   router.Publisher
	issuers CardResolver
}

const defaultScore = 100

// New validates the wiring and returns a handler.
func New(cfg Config) (*Handler, error) {
	if cfg.Domain == "" || cfg.Self == nil || cfg.Key == nil || cfg.Bids == nil {
		return nil, errors.New("afep/mesh: domain, self card, key and bid publisher are required")
	}
	if cfg.Router != nil && cfg.Relay == nil {
		return nil, errors.New("afep/mesh: router requires a relay publisher")
	}
	if cfg.Issuers == nil {
		return nil, errors.New("afep/mesh: issuer resolver is required")
	}
	if cfg.Schemas == nil {
		cfg.Schemas = schemareg.New()
	}
	if cfg.Score == 0 {
		cfg.Score = defaultScore
	}
	return &Handler{
		domain:  cfg.Domain,
		self:    cfg.Self,
		kp:      cfg.Key,
		score:   cfg.Score,
		schemas: cfg.Schemas,
		router:  cfg.Router,
		bids:    cfg.Bids,
		relay:   cfg.Relay,
		issuers: cfg.Issuers,
	}, nil
}

// OnEnvelope handles one inbound envelope:
//
//   - schema violations return ErrRejected (Term-worthy);
//   - PROBE (fresh or relayed as ESCALATED): publish a signed bid when this
//     node holds the capability and clearance, else escalate to the next
//     peer domain;
//   - hop-limit / exhausted chain: acknowledged and dropped;
//   - any other state: acknowledged unchanged.
func (h *Handler) OnEnvelope(ctx context.Context, env *afepv1.TaskEnvelope) error {
	if env == nil {
		return fmt.Errorf("%w: nil envelope", ErrRejected)
	}
	if err := h.schemas.ValidateEnvelope(env); err != nil {
		return fmt.Errorf("%w: %v", ErrRejected, err)
	}
	if err := h.verifyIssuer(ctx, env); err != nil {
		return err // ErrRejected (Term) or transient lookup error (Nak)
	}
	switch env.GetState() {
	case afepv1.TaskState_TASK_STATE_PROBE, afepv1.TaskState_TASK_STATE_ESCALATED:
	default:
		return nil
	}
	if h.canServe(env) {
		return h.publishBid(env)
	}
	if h.router != nil {
		if err := h.router.Escalate(ctx, h.relay, env, "NO_BIDS"); err != nil {
			if errors.Is(err, router.ErrHopLimit) || errors.Is(err, router.ErrNoPeer) {
				return nil // chain dead: ack, do not loop
			}
			return err
		}
	}
	return nil
}

// verifyIssuer enforces the trust chain at the federation boundary: the
// issuer must resolve to a card whose key verifies the envelope's signed
// core. Unknown or unverifiable envelopes are poison (ErrRejected).
func (h *Handler) verifyIssuer(ctx context.Context, env *afepv1.TaskEnvelope) error {
	issuer := env.GetIssuerAgent()
	if issuer == "" {
		return fmt.Errorf("%w: envelope has no issuer", ErrRejected)
	}
	card, ok, err := h.issuers.Card(ctx, issuer)
	if err != nil {
		return fmt.Errorf("afep/mesh: issuer lookup: %w", err) // transient: Nak
	}
	if !ok {
		return fmt.Errorf("%w: unknown issuer %q", ErrRejected, issuer)
	}
	if err := identity.VerifyEnvelope(env, card); err != nil {
		return fmt.Errorf("%w: issuer signature: %v", ErrRejected, err)
	}
	return nil
}

func (h *Handler) canServe(env *afepv1.TaskEnvelope) bool {
	if !hasCapability(h.self, env.GetCapability()) {
		return false
	}
	return clearance.CanServe(h.self, env.GetTenant()) == nil
}

func (h *Handler) publishBid(env *afepv1.TaskEnvelope) error {
	bid := &afepv1.Bid{
		AgentId:              h.self.GetId(),
		Score:                h.score,
		TaskId:               env.GetTaskId(),
		Rationale:            "mesh: capability registered and clearance held",
		ProposedDeadlineUnix: env.GetDeadline().GetExpiresUnix(),
	}
	if err := identity.SignBid(bid, h.kp); err != nil {
		return fmt.Errorf("afep/mesh: sign: %w", err)
	}
	if err := h.bids.PublishBid(h.domain, subjectAgent(h.self.GetId()), bid); err != nil {
		return fmt.Errorf("afep/mesh: publish: %w", err)
	}
	return nil
}

// subjectAgent maps a card id ("tenant/agent") onto one subject segment.
func subjectAgent(id string) string {
	return strings.NewReplacer("/", "-", " ", "-", "\\", "-").Replace(id)
}

func hasCapability(card *afepv1.AgentCard, name string) bool {
	for _, c := range card.GetCapabilities() {
		if c.GetName() == name {
			return true
		}
	}
	return false
}
