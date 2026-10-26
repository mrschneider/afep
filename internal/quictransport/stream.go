package quictransport

import (
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// streamConn adapts a QUIC stream to net.Conn so grpc-go can run its HTTP/2
// framing directly over it.
type streamConn struct {
	s    *quic.Stream
	conn *quic.Conn
}

var _ net.Conn = (*streamConn)(nil)

func (c *streamConn) Read(p []byte) (int, error)  { return c.s.Read(p) }
func (c *streamConn) Write(p []byte) (int, error) { return c.s.Write(p) }

// Close shuts the write side and cancels the read side of the stream; the
// underlying QUIC connection stays open for the next RPC (multiplexing).
func (c *streamConn) Close() error {
	c.s.CancelRead(0)
	return c.s.Close()
}

func (c *streamConn) LocalAddr() net.Addr  { return c.conn.LocalAddr() }
func (c *streamConn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }
func (c *streamConn) SetDeadline(t time.Time) error {
	if err := c.s.SetReadDeadline(t); err != nil {
		return err
	}
	return c.s.SetWriteDeadline(t)
}
func (c *streamConn) SetReadDeadline(t time.Time) error  { return c.s.SetReadDeadline(t) }
func (c *streamConn) SetWriteDeadline(t time.Time) error { return c.s.SetWriteDeadline(t) }
