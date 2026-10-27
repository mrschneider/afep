// Package grpcsvc exposes the AFEP AgentTransportService.
package grpcsvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/clearance"
	"github.com/mrschneider/afep/internal/identity"
	"github.com/mrschneider/afep/internal/lifecycle"
	"github.com/mrschneider/afep/internal/registry"
	"github.com/mrschneider/afep/internal/schemareg"
	"github.com/mrschneider/afep/internal/taskstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// defaultBidScore is used when no WithBidScore option is given.
const defaultBidScore = 100

// Server implements AgentTransportService: signed self-bids, clearance
// admission, schema-validated envelopes and an optional durable task store.
type Server struct {
	afepv1.UnimplementedAgentTransportServiceServer

	self    *afepv1.AgentCard
	reg     *registry.Registry
	kp      *identity.KeyPair
	schemas *schemareg.Registry
	store   *taskstore.Store
	score   uint64

	mu    sync.RWMutex
	tasks map[string]*afepv1.TaskEnvelope
}

// Option customizes the service.
type Option func(*Server)

// WithBidScore sets the score stamped on self-bids (ranking input).
func WithBidScore(n uint64) Option {
	return func(s *Server) { s.score = n }
}

// WithSchemaRegistry replaces the payload validation registry.
func WithSchemaRegistry(r *schemareg.Registry) Option {
	return func(s *Server) { s.schemas = r }
}

// WithTaskStore persists state transitions across restarts.
func WithTaskStore(st *taskstore.Store) Option {
	return func(s *Server) { s.store = st }
}

// New wires the service: the node's card, directory and signing key are
// mandatory — a node can only bid for identities whose key it holds.
func New(self *afepv1.AgentCard, reg *registry.Registry, kp *identity.KeyPair, opts ...Option) (*Server, error) {
	if self == nil || reg == nil || kp == nil {
		return nil, fmt.Errorf("afep/grpcsvc: self card, registry and key pair are required")
	}
	s := &Server{
		self:    self,
		reg:     reg,
		kp:      kp,
		schemas: schemareg.New(),
		score:   defaultBidScore,
		tasks:   make(map[string]*afepv1.TaskEnvelope),
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// GetCard serves the self card or any registered agent card.
func (s *Server) GetCard(ctx context.Context, req *afepv1.GetCardRequest) (*afepv1.GetCardResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.GetAgentId() == "" || req.GetAgentId() == s.self.GetId() {
		return &afepv1.GetCardResponse{Card: s.self}, nil
	}
	card, ok := s.reg.Get(req.GetAgentId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "agent %q not registered", req.GetAgentId())
	}
	return &afepv1.GetCardResponse{Card: card}, nil
}

// Probe answers with at most one bid: this node's own, ed25519-signed, and
// only when it advertises the capability and holds the tenant clearance.
// A zero-bid response still carries the task id so the caller can escalate.
func (s *Server) Probe(req *afepv1.ProbeRequest, stream afepv1.AgentTransportService_ProbeServer) error {
	if err := s.schemas.Validate(req.GetPayloadType(), nil); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	env := &afepv1.TaskEnvelope{
		TaskId:        newID(),
		Tenant:        req.GetTenant(),
		IssuerAgent:   s.self.GetId(),
		State:         afepv1.TaskState_TASK_STATE_PROBE,
		Capability:    req.GetCapability(),
		PayloadType:   req.GetPayloadType(),
		Deadline:      &afepv1.Deadline{StartUnix: time.Now().Unix(), ExpiresUnix: req.GetDeadlineUnix()},
		UpdatedAtUnix: time.Now().Unix(),
	}
	env.RootTaskId = env.GetTaskId()
	if err := s.save(env); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	empty := func() error {
		return stream.Send(&afepv1.ProbeResponse{TaskId: env.GetTaskId()})
	}

	if err := clearance.CanServe(s.self, req.GetTenant()); err != nil {
		return empty() // admission denied: no bid, caller escalates
	}
	if !hasCapability(s.self, req.GetCapability()) {
		return empty() // capability not held: no bid, caller escalates
	}

	bid := &afepv1.Bid{
		AgentId:              s.self.GetId(),
		TaskId:               env.GetTaskId(),
		Score:                s.score,
		Rationale:            "self: capability registered and clearance held",
		ProposedDeadlineUnix: req.GetDeadlineUnix(),
	}
	if err := identity.SignBid(bid, s.kp); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if err := stream.Send(&afepv1.ProbeResponse{TaskId: env.GetTaskId(), Bids: []*afepv1.Bid{bid}}); err != nil {
		return err
	}
	if err := lifecycle.Apply(env, afepv1.TaskState_TASK_STATE_BID); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return s.save(env)
}

// Commit picks the winning bid of a probe. The winner must be known (self
// or registry) and must hold the envelope's tenant clearance.
func (s *Server) Commit(ctx context.Context, req *afepv1.CommitRequest) (*afepv1.CommitResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	env, ok := s.load(req.GetTaskId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetTaskId())
	}
	if req.GetWinnerAgent() == "" {
		return nil, status.Error(codes.InvalidArgument, "winner_agent is required")
	}

	var winner *afepv1.AgentCard
	switch {
	case req.GetWinnerAgent() == s.self.GetId():
		winner = s.self
	default:
		c, ok := s.reg.Get(req.GetWinnerAgent())
		if !ok {
			return nil, status.Errorf(codes.NotFound, "winner %q not registered", req.GetWinnerAgent())
		}
		winner = c
	}
	if err := clearance.CanServe(winner, env.GetTenant()); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	env.AssigneeAgent = req.GetWinnerAgent()
	if err := lifecycle.Apply(env, afepv1.TaskState_TASK_STATE_COMMIT); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := s.save(env); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := proto.Clone(env).(*afepv1.TaskEnvelope)
	if err := identity.SignEnvelope(out, s.kp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &afepv1.CommitResponse{Envelope: out}, nil
}

// TaskStream is the full-duplex loop: every envelope frame crosses the
// schema boundary before it is admitted; chunks relay as-is.
func (s *Server) TaskStream(stream afepv1.AgentTransportService_TaskStreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch frame := in.GetFrame().(type) {
		case *afepv1.TaskStreamRequest_Envelope:
			env := frame.Envelope
			if err := s.schemas.ValidateEnvelope(env); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			if err := s.verifyIngressEnvelope(env); err != nil {
				return err
			}
			if env.GetTaskId() == "" {
				env.TaskId = newID()
			}
			if env.GetRootTaskId() == "" {
				env.RootTaskId = env.GetTaskId()
			}
			if env.GetState() == afepv1.TaskState_TASK_STATE_UNSPECIFIED {
				if err := lifecycle.Apply(env, afepv1.TaskState_TASK_STATE_PROBE); err != nil {
					return status.Error(codes.Internal, err.Error())
				}
			}
			env.UpdatedAtUnix = time.Now().Unix()
			if err := s.save(env); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			out := proto.Clone(env).(*afepv1.TaskEnvelope)
			if err := identity.SignEnvelope(out, s.kp); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			if err := stream.Send(&afepv1.TaskStreamResponse{Frame: &afepv1.TaskStreamResponse_Envelope{Envelope: out}}); err != nil {
				return err
			}
		case *afepv1.TaskStreamRequest_Chunk:
			if err := stream.Send(&afepv1.TaskStreamResponse{Frame: &afepv1.TaskStreamResponse_Chunk{Chunk: frame.Chunk}}); err != nil {
				return err
			}
		default:
			return status.Error(codes.InvalidArgument, "unknown frame type")
		}
	}
}

// verifyIngressEnvelope enforces issuer signatures on envelopes entering
// via TaskStream: the issuer must be this node or a locally registered
// card, and its key must verify the signed core.
func (s *Server) verifyIngressEnvelope(env *afepv1.TaskEnvelope) error {
	issuer := env.GetIssuerAgent()
	if issuer == "" {
		return status.Error(codes.InvalidArgument, "envelope has no issuer")
	}
	var card *afepv1.AgentCard
	if issuer == s.self.GetId() {
		card = s.self
	} else {
		c, ok := s.reg.Get(issuer)
		if !ok {
			return status.Errorf(codes.NotFound, "issuer %q not registered", issuer)
		}
		card = c
	}
	if err := identity.VerifyEnvelope(env, card); err != nil {
		return status.Error(codes.PermissionDenied, "issuer signature: "+err.Error())
	}
	return nil
}

// save records the envelope in memory and, when configured, flushes it to
// the durable task store.
func (s *Server) save(env *afepv1.TaskEnvelope) error {
	s.mu.Lock()
	// server-owned copy: RPC request objects must never alias the
	// map (the store clones again at its own boundary in Put).
	s.tasks[env.GetTaskId()] = proto.Clone(env).(*afepv1.TaskEnvelope)
	s.mu.Unlock()
	if s.store != nil {
		return s.store.Put(env)
	}
	return nil
}

func (s *Server) load(id string) (*afepv1.TaskEnvelope, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	env, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	// callers own the value: Commit mutates the result in place, so it
	// must be a private copy or it races concurrent saves/flushes.
	return proto.Clone(env).(*afepv1.TaskEnvelope), true
}

func hasCapability(card *afepv1.AgentCard, name string) bool {
	for _, c := range card.GetCapabilities() {
		if c.GetName() == name {
			return true
		}
	}
	return false
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
