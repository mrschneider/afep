// Package grpcsvc: handler panic containment for the daemon's gRPC
// servers. Without these interceptors a panic in any handler takes the
// whole process down (Go panics kill the runtime, not just the goroutine).
package grpcsvc

import (
	"context"
	"log"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryUnary converts handler panics into codes.Internal responses.
func RecoveryUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("grpc: panic in %s: %v\n%s", info.FullMethod, r, debug.Stack())
			err = status.Errorf(codes.Internal, "panic: %v", r)
		}
	}()
	return handler(ctx, req)
}

// RecoveryStream converts stream handler panics into codes.Internal.
func RecoveryStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("grpc: panic in %s: %v\n%s", info.FullMethod, r, debug.Stack())
			err = status.Errorf(codes.Internal, "panic: %v", r)
		}
	}()
	return handler(srv, ss)
}
