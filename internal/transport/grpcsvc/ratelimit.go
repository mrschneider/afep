package grpcsvc

import (
	"context"
	"sync"
	"time"

	"github.com/mrschneider/afep/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Limiter is a global token bucket for ingress admission: unary RPCs cost
// one token, every stream message costs one token. Bounded work before
// signatures and schema validation are even attempted.
type Limiter struct {
	mu     sync.Mutex
	tokens float64
	rate   float64 // tokens per second
	burst  float64
	last   time.Time
}

// NewLimiter allows reqPerSec sustained requests with burst capacity.
func NewLimiter(reqPerSec, burst float64) *Limiter {
	return &Limiter{tokens: burst, rate: reqPerSec, burst: burst, last: time.Now()}
}

func (l *Limiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * l.rate
	l.last = now
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

// Unary returns a unary interceptor enforcing the bucket.
func (l *Limiter) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !l.allow() {
			metrics.RateLimited.Inc()
			return nil, status.Error(codes.ResourceExhausted, "ingress rate limit")
		}
		return handler(ctx, req)
	}
}

// rateStream charges one token per decoded message: streaming RPCs are
// long-lived, so counting stream opens alone would leave TaskStream
// unbounded.
type rateStream struct {
	grpc.ServerStream
	l *Limiter
}

func (s rateStream) RecvMsg(m any) error {
	if !s.l.allow() {
		metrics.RateLimited.Inc()
		return status.Error(codes.ResourceExhausted, "ingress rate limit")
	}
	return s.ServerStream.RecvMsg(m)
}

// Stream returns a stream interceptor enforcing the bucket per message.
func (l *Limiter) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, rateStream{ServerStream: ss, l: l})
	}
}
