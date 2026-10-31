package quictransport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// quicConfig is the transport tuning shared by every pooled connection: a
// keepalive well below the idle timeout so a half-dead peer is detected and
// the connection recycled before gRPC gives up.
var quicConfig = &quic.Config{
	KeepAlivePeriod: 15 * time.Second,
	MaxIdleTimeout:  60 * time.Second,
}

// connPool reuses a single QUIC connection to one peer: the first RPC pays
// the TLS 1.3 handshake, every subsequent RPC opens a multiplexed stream
// over the same connection. A dead connection (idle timeout, peer reset) is
// detected via the connection context and redialed transparently.
type connPool struct {
	mu     sync.Mutex
	addr   string
	conf   *tls.Config
	conn   *quic.Conn
	closed bool
}

func newConnPool(addr string, conf *tls.Config) *connPool {
	return &connPool{addr: addr, conf: conf}
}

// get returns a fresh bidirectional stream (as a net.Conn) over the pooled
// connection, dialing if necessary. It never hands out a dead connection:
// if the current connection is closed it is evicted and redialed once.
func (p *connPool) get(ctx context.Context) (net.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		c, err := p.aliveConn(ctx)
		if err != nil {
			// Dial failed: nothing to evict, surface the real error.
			return nil, err
		}
		stream, err := c.OpenStreamSync(ctx)
		if err == nil {
			return &streamConn{s: stream, conn: c}, nil
		}
		// The connection failed to open a stream: treat it as stale,
		// evict it and redial on the next attempt.
		lastErr = fmt.Errorf("afep/quic: stream %s: %w", p.addr, err)
		p.evict(c)
	}
	return nil, lastErr
}

// aliveConn returns the live pooled connection, dialing under the lock when
// there is none. Dialing under the lock serializes the first concurrent RPCs
// to the same peer on a single handshake — a deliberate trade that avoids
// racing duplicate connections and keeps the pool to exactly one conn/peer.
func (p *connPool) aliveConn(ctx context.Context) (*quic.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("afep/quic: pool to %s is closed", p.addr)
	}
	if p.conn != nil {
		// The connection context is canceled when the QUIC connection
		// closes (idle timeout, peer reset, explicit close).
		if p.conn.Context().Err() == nil {
			return p.conn, nil
		}
		_ = p.conn.CloseWithError(0, "stale")
		p.conn = nil
	}
	c, err := quic.DialAddr(ctx, p.addr, p.conf, quicConfig)
	if err != nil {
		return nil, fmt.Errorf("afep/quic: dial %s: %w", p.addr, err)
	}
	p.conn = c
	return c, nil
}

// evict drops a specific connection from the pool if it is still the current
// one, so the next get() redials.
func (p *connPool) evict(c *quic.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == c {
		_ = c.CloseWithError(0, "stale")
		p.conn = nil
	}
}

// close shuts down the pooled connection.
func (p *connPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.conn != nil {
		_ = p.conn.CloseWithError(0, "pool closed")
		p.conn = nil
	}
}

// Dialer is a reusable gRPC-over-QUIC dialer that multiplexes streams over a
// single pooled connection per peer. Use it with grpc.WithContextDialer.
type Dialer struct {
	pool *connPool
}

// NewDialer builds a Dialer targeting serverAddr with an explicit client TLS
// config (use ClientTLSFromCA to pin instead of the dev skip-verify default).
func NewDialer(serverAddr string, conf *tls.Config) *Dialer {
	if conf == nil {
		conf = ClientTLS()
	}
	return &Dialer{pool: newConnPool(serverAddr, conf)}
}

// DialContext implements the grpc dialer signature: it always targets the
// dialer's serverAddr and returns a multiplexed stream over the pooled
// QUIC connection.
func (d *Dialer) DialContext(ctx context.Context, _ string) (net.Conn, error) {
	return d.pool.get(ctx)
}

// Close releases the pooled connection. Safe to call more than once.
func (d *Dialer) Close() {
	d.pool.close()
}

// sharedPools backs the convenience DialFunc: one pool per server address,
// all using the default ClientTLS() config.
var (
	sharedMu    sync.Mutex
	sharedPools = make(map[string]*connPool)
)

// getSharedPool returns the process-wide pool for serverAddr, creating it
// (with the default ClientTLS() config) on first use.
func getSharedPool(serverAddr string) *connPool {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	p, ok := sharedPools[serverAddr]
	if !ok {
		p = newConnPool(serverAddr, ClientTLS())
		sharedPools[serverAddr] = p
	}
	return p
}
