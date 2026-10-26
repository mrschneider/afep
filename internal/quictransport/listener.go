package quictransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Listener wraps a QUIC listener as net.Listener: every accepted QUIC
// stream surfaces as one net.Conn (one gRPC connection each), multiplexed
// over the same QUIC connection.
type Listener struct {
	inner *quic.Listener
	ch    chan net.Conn
	done  context.Context
	stop  context.CancelFunc
	once  sync.Once
}

var _ net.Listener = (*Listener)(nil)

// Listen opens a QUIC listener on addr with an ephemeral self-signed cert.
func Listen(addr string) (*Listener, error) {
	cert, err := SelfSigned()
	if err != nil {
		return nil, err
	}
	return ListenTLS(addr, ServerTLS(cert))
}

// ListenTLS opens a QUIC listener with a caller-provided TLS config.
func ListenTLS(addr string, tlsConf *tls.Config) (*Listener, error) {
	inner, err := quic.ListenAddr(addr, tlsConf, &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  60 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("afep/quic: listen: %w", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	l := &Listener{inner: inner, ch: make(chan net.Conn, 64), done: ctx, stop: stop}
	go l.acceptConns()
	return l, nil
}

func (l *Listener) acceptConns() {
	for {
		conn, err := l.inner.Accept(l.done)
		if err != nil {
			return // listener closed
		}
		go l.acceptStreams(conn)
	}
}

func (l *Listener) acceptStreams(conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(l.done)
		if err != nil {
			return // connection closed or listener stopped
		}
		select {
		case l.ch <- &streamConn{s: stream, conn: conn}:
		case <-l.done.Done():
			stream.CancelRead(0)
			stream.CancelWrite(0)
			return
		}
	}
}

// Accept implements net.Listener.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done.Done():
		return nil, errors.New("afep/quic: listener closed")
	}
}

// Close implements net.Listener.
func (l *Listener) Close() error {
	var err error
	l.once.Do(func() {
		l.stop()
		err = l.inner.Close()
	})
	return err
}

// Addr implements net.Listener.
func (l *Listener) Addr() net.Addr { return l.inner.Addr() }
