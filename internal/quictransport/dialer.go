package quictransport

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// Dial opens a QUIC connection to addr and returns the first bidirectional
// stream as a net.Conn — the client side of one gRPC connection.
func Dial(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := quic.DialAddr(ctx, addr, ClientTLS(), &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  60 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("afep/quic: dial %s: %w", addr, err)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "stream")
		return nil, fmt.Errorf("afep/quic: stream %s: %w", addr, err)
	}
	return &streamConn{s: stream, conn: conn}, nil
}

// DialFunc returns a grpc dialer that always targets serverAddr — plug it
// into grpc.WithContextDialer to run gRPC over QUIC.
func DialFunc(serverAddr string) func(ctx context.Context, _ string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		return Dial(ctx, serverAddr)
	}
}
