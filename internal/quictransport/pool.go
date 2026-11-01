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
//
// Concurrent first-contact RPCs share ONE dial: the dialer that claims the
// in-flight state publishes the result (connection or error) on the shared
// channel, and every other caller waits for it instead of racing duplicate
// handshakes — one RTT per peer per epoch, not one per concurrent RPC.
type connPool struct {
	mu     sync.Mutex
	addr   string
	conf   *tls.Config
	conn   *quic.Conn
	// dialing is non-nil while a dial is in flight; its channel carries
	// the outcome exactly once to every waiter.
	dialing *dialShared
	closed  bool
}

// dialShared is the in-flight dial outcome shared by concurrent callers.
// The claimant writes conn/err and then closes done; the close
// establishes happens-before, so ANY NUMBER of waiters can read
// conn/err safely after <-done (a channel value would only reach one
// reader).
type dialShared struct {
	done chan struct{}
	conn *quic.Conn
	err  error
}

// closed reports whether the outcome has been published.
func (d *dialShared) closed() bool {
	select {
	case <-d.done:
		return true
	default:
		return false
	}
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

// aliveConn returns the live pooled connection. When there is none it
// dials — and concurrent callers SHARE that dial: the first caller claims
// the in-flight state and publishes the outcome on a shared channel, every
// other caller (including ones that arrive mid-handshake) waits for the
// same result. This keeps the pool to exactly one conn/peer while making
// the first fan-out cost ONE handshake, not one per concurrent RPC.
//
// Cancellation is per-waiter: a caller whose context expires is released
// (it gets ctx.Err()) without canceling the shared dial for the rest.
func (p *connPool) aliveConn(ctx context.Context) (*quic.Conn, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("afep/quic: pool to %s is closed", p.addr)
		}
		if p.conn != nil {
			// The connection context is canceled when the QUIC connection
			// closes (idle timeout, peer reset, explicit close).
			if p.conn.Context().Err() == nil {
				c := p.conn
				p.mu.Unlock()
				return c, nil
			}
			_ = p.conn.CloseWithError(0, "stale")
			p.conn = nil
		}
		if d := p.dialing; d != nil {
			// A dial is already in flight: wait for ITS outcome.
			p.mu.Unlock()
			res, ok := waitForDial(ctx, d)
			if !ok {
				return nil, res.err
			}
			return res.conn, res.err
		}
		// Claim the dial. The claimant ALWAYS publishes the outcome
		// before returning — even when its own context expires
		// mid-handshake — or the other waiters would block forever.
		d := &dialShared{done: make(chan struct{})}
		p.dialing = d
		p.mu.Unlock()

		c, err := quic.DialAddr(ctx, p.addr, p.conf, quicConfig)
		if err != nil {
			err = fmt.Errorf("afep/quic: dial %s: %w", p.addr, err)
		} else {
			p.mu.Lock()
			p.conn = c
			p.dialing = nil
			p.mu.Unlock()
		}
		d.conn, d.err = c, err
		close(d.done) // wakes every waiter; writes above are visible
		p.mu.Lock()
		if p.dialing == d {
			p.dialing = nil
		}
		p.mu.Unlock()
		return c, err
	}
}

// dialOutcome is the published result of a shared dial.
type dialOutcome struct {
	conn *quic.Conn
	err  error
}

// waitForDial blocks until the shared dial publishes its outcome or the
// caller's context is done (whichever first). A canceled caller is simply
// released with its own error: the dial keeps running for the other
// waiters.
func waitForDial(ctx context.Context, d *dialShared) (dialOutcome, bool) {
	select {
	case <-ctx.Done():
		return dialOutcome{err: ctx.Err()}, false
	case <-d.done:
		return dialOutcome{conn: d.conn, err: d.err}, true
	}
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
