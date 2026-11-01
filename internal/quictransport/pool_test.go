package quictransport

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// firstConnPtr returns the underlying *quic.Conn of a pooled stream so tests
// can assert connection identity (reuse vs. redial).
func firstConnPtr(t *testing.T, c net.Conn) *quic.Conn {
	t.Helper()
	sc, ok := c.(*streamConn)
	if !ok {
		t.Fatalf("pooled conn is %T, want *streamConn", c)
	}
	return sc.conn
}

// drainListener accepts every stream and reads it to EOF so the server side
// never blocks on a half-closed stream.
func drainListener(t *testing.T, ln *Listener) {
	t.Helper()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			buf := make([]byte, 64)
			for {
				if _, err := c.Read(buf); err != nil {
					return
				}
			}
		}(c)
	}
}

func TestPoolReusesConnection(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go drainListener(t, ln)

	p := newConnPool(ln.Addr().String(), ClientTLS())
	defer p.close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstConn := firstConnPtr(t, first)
	first.Close()

	for i := 0; i < 5; i++ {
		c, err := p.get(ctx)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if got := firstConnPtr(t, c); got != firstConn {
			t.Fatalf("get %d: new connection, expected reuse of the pooled one", i)
		}
		c.Close()
	}
}

func TestPoolRedialsAfterPeerClose(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go drainListener(t, ln)

	p := newConnPool(ln.Addr().String(), ClientTLS())
	defer p.close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c1, err := p.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldConn := firstConnPtr(t, c1)
	c1.Close()

	// Simulate the peer dropping the connection (reset / idle timeout):
	// close the pooled conn out from under the pool.
	oldConn.CloseWithError(0, "peer gone")

	// The next get must detect the dead conn and transparently redial.
	c2, err := p.get(ctx)
	if err != nil {
		t.Fatalf("redial after peer close: %v", err)
	}
	defer c2.Close()
	if firstConnPtr(t, c2) == oldConn {
		t.Fatal("expected a fresh connection after redial, got the old one")
	}
}

func TestPoolClosedRefusesDial(t *testing.T) {
	p := newConnPool("127.0.0.1:1", ClientTLS()) // port 1: nothing listens
	p.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := p.get(ctx); err == nil {
		t.Fatal("expected an error from a closed pool, got nil")
	}
}

func TestPoolConcurrentGets(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go drainListener(t, ln)

	p := newConnPool(ln.Addr().String(), ClientTLS())
	defer p.close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.get(ctx)
			if err != nil {
				errs <- err
				return
			}
			c.Close()
			errs <- nil
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent get: %v", err)
		}
	}
	// Exactly one live connection must remain pooled.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil || p.conn.Context().Err() != nil {
		t.Fatal("expected one live pooled connection after concurrent gets")
	}
}

// TestPoolSharesFirstDial verifies the shared-dial path: N concurrent
// first-contact gets to a cold pool must all succeed, all ride the SAME
// connection, and the pool ends with exactly one conn (one handshake,
// not N).
func TestPoolSharesFirstDial(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go drainListener(t, ln)

	p := newConnPool(ln.Addr().String(), ClientTLS())
	defer p.close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const n = 24
	conns := make(chan *quic.Conn, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.get(ctx)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			conns <- firstConnPtr(t, c)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent first get: %v", err)
	}
	var first *quic.Conn
	for i := 0; i < n; i++ {
		c := <-conns
		if i == 0 {
			first = c
		} else if c != first {
			t.Fatalf("get %d got a different connection: the first dial must be shared", i)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != first {
		t.Fatal("pool must hold the shared connection")
	}
}

// TestPoolSharedDialCancellation verifies a waiter whose context expires is
// released with its own error while the shared dial is left untouched for
// the surviving callers (deterministic: the in-flight dial is simulated
// directly, no timing dependency).
func TestPoolSharedDialCancellation(t *testing.T) {
	p := newConnPool("127.0.0.1:1", ClientTLS()) // nothing listens; unused
	defer p.close()

	// Simulate a claimed dial that has not published yet.
	d := &dialShared{done: make(chan struct{})}
	p.mu.Lock()
	p.dialing = d
	p.mu.Unlock()

	// An expired-context waiter must get its OWN error, without
	// disturbing the in-flight dial…
	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	if _, err := p.aliveConn(short); err == nil {
		t.Fatal("expired-context waiter must get an error")
	}
	p.mu.Lock()
	if p.dialing != d || d.closed() {
		t.Fatal("waiter cancellation must not disturb the in-flight dial")
	}
	p.mu.Unlock()

	// …and the surviving waiter must receive the dial's outcome.
	d.conn, d.err = (*quic.Conn)(nil), nil // stand-in outcome (success)
	close(d.done)
	if c, err := p.aliveConn(context.Background()); err != nil {
		t.Fatalf("surviving waiter: %v", err)
	} else if c != nil {
		t.Fatal("surviving waiter must receive the published (nil) outcome, not a live conn")
	}
}
