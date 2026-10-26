// Package grpcsvc exposes the AFEP AgentTransport service.
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
	"github.com/mrschneider/afep/internal/lifecycle"
	"github.com/mrschneider/afep/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server is the reference implementation: card lookup, probe/commit over a
// shared task table, and an echoing bi-directional stream.
type Server struct {
	afepv1.UnimplementedAgentTransportServiceServer

	self *afepv1.AgentCard
	reg  *registry.Registry

	mu    sync.RWMutex
	tasks map[string]*afepv1.TaskEnvelope
}

// New wires the service to a card and a registry.
func New(self *afepv1.AgentCard, reg *registry.Registry) *Server {
	return &Server{self: self, reg: reg, tasks: make(map[string]*afepv1.TaskEnvelope)}
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

// Probe streams bids from every registered provider of the capability.
// The first message always carries the task id, even with zero bids, so the
// caller can escalate an unanswered probe.
func (s *Server) Probe(req *afepv1.ProbeRequest, stream afepv1.AgentTransportService_ProbeServer) error {
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
	s.save(env)

	sent := false
	for _, p := range s.reg.ByCapability(req.GetCapability()) {
		bid := &afepv1.Bid{
			AgentId:              p.GetId(),
			Rationale:            "scaffold: capability registered",
			ProposedDeadlineUnix: req.GetDeadlineUnix(),
		}
		if err := stream.Send(&afepv1.ProbeResponse{TaskId: env.GetTaskId(), Bids: []*afepv1.Bid{bid}}); err != nil {
			return err
		}
		sent = true
	}
	if sent {
		if err := lifecycle.Apply(env, afepv1.TaskState_TASK_STATE_BID); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	} else if err := stream.Send(&afepv1.ProbeResponse{TaskId: env.GetTaskId()}); err != nil {
		return err
	}
	return nil
}

// Commit picks the winning bid of a probe and commits the assignee.
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
	if _, ok := s.reg.Get(req.GetWinnerAgent()); !ok {
		return nil, status.Errorf(codes.NotFound, "winner %q not registered", req.GetWinnerAgent())
	}
	env.AssigneeAgent = req.GetWinnerAgent()
	if err := lifecycle.Apply(env, afepv1.TaskState_TASK_STATE_COMMIT); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	s.save(env)
	return &afepv1.CommitResponse{Envelope: proto.Clone(env).(*afepv1.TaskEnvelope)}, nil
}

// TaskStream is the full-duplex loop: envelopes flow one way, chunks and
// telemetry the other, multiplexed on one connection.
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
			s.save(env)
			out := proto.Clone(env).(*afepv1.TaskEnvelope)
			if err := stream.Send(&afepv1.TaskStreamResponse{Frame: &afepv1.TaskStreamResponse_Envelope{Envelope: out}}); err != nil {
				return err
			}
		case *afepv1.TaskStreamRequest_Chunk:
			// Scaffold relay: echo partial output back on the same stream.
			if err := stream.Send(&afepv1.TaskStreamResponse{Frame: &afepv1.TaskStreamResponse_Chunk{Chunk: frame.Chunk}}); err != nil {
				return err
			}
		default:
			return status.Error(codes.InvalidArgument, "unknown frame type")
		}
	}
}

func (s *Server) save(env *afepv1.TaskEnvelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[env.GetTaskId()] = env
}

func (s *Server) load(id string) (*afepv1.TaskEnvelope, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	env, ok := s.tasks[id]
	return env, ok
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
