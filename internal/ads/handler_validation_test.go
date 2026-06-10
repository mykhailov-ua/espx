package ads

import (
	"bytes"
	"net"
	"testing"

	"espx/internal/config"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
)

type mockValidationConn struct {
	gnet.Conn
	buf     []byte
	written []byte
	ctx     any
	addr    net.Addr
}

func (m *mockValidationConn) Context() any     { return m.ctx }
func (m *mockValidationConn) SetContext(v any) { m.ctx = v }

func (m *mockValidationConn) Write(b []byte) (int, error) {
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockValidationConn) InboundBuffered() int {
	return len(m.buf)
}

func (m *mockValidationConn) Peek(n int) ([]byte, error) {
	if n > len(m.buf) {
		return m.buf, nil
	}
	return m.buf[:n], nil
}

func (m *mockValidationConn) Discard(n int) (int, error) {
	if n > len(m.buf) {
		n = len(m.buf)
	}
	m.buf = m.buf[n:]
	return n, nil
}

func (m *mockValidationConn) RemoteAddr() net.Addr {
	if m.addr != nil {
		return m.addr
	}
	return staticRemoteAddr
}

func TestAdsPacketHandler_Validation(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 100,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud-stream")

	t.Run("POST /unknown -> 404 Not Found", func(t *testing.T) {
		conn := &mockValidationConn{
			buf: []byte("POST /unknown HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 5\r\n\r\nhello"),
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.None, act)
		assert.True(t, bytes.Equal(conn.written, respNotFound), "expected response: %q, got: %q", string(respNotFound), string(conn.written))
	})

	t.Run("GET /track -> 405 Method Not Allowed", func(t *testing.T) {
		conn := &mockValidationConn{
			buf: []byte("GET /track HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n"),
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.None, act)
		assert.True(t, bytes.Equal(conn.written, respMethodNotAllowed), "expected response: %q, got: %q", string(respMethodNotAllowed), string(conn.written))
	})

	t.Run("DELETE /track -> 405 Method Not Allowed", func(t *testing.T) {
		conn := &mockValidationConn{
			buf: []byte("DELETE /track HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n"),
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.None, act)
		assert.True(t, bytes.Equal(conn.written, respMethodNotAllowed), "expected response: %q, got: %q", string(respMethodNotAllowed), string(conn.written))
	})

	t.Run("POST /track too large -> 413 Payload Too Large", func(t *testing.T) {
		body := make([]byte, 105)
		req := []byte("POST /track HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 105\r\n\r\n")
		req = append(req, body...)
		conn := &mockValidationConn{
			buf: req,
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.Close, act)
		assert.True(t, bytes.Equal(conn.written, respPayloadTooLarge), "expected response: %q, got: %q", string(respPayloadTooLarge), string(conn.written))
	})

	t.Run("POST /track missing Content-Length -> 400 Bad Request", func(t *testing.T) {
		conn := &mockValidationConn{
			buf: []byte("POST /track HTTP/1.1\r\nContent-Type: application/json\r\n\r\nhello"),
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.Close, act)
		assert.True(t, bytes.Equal(conn.written, respBadRequestClose), "expected response: %q, got: %q", string(respBadRequestClose), string(conn.written))
	})

	t.Run("GET /health -> 200 OK when healthy", func(t *testing.T) {
		handler.healthy.Store(1)
		conn := &mockValidationConn{
			buf: []byte("GET /health HTTP/1.1\r\nContent-Length: 0\r\n\r\n"),
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.None, act)
		assert.True(t, bytes.Equal(conn.written, respHealth), "expected response: %q, got: %q", string(respHealth), string(conn.written))
	})

	t.Run("GET /health -> 503 Service Unavailable when unhealthy", func(t *testing.T) {
		handler.healthy.Store(0)
		conn := &mockValidationConn{
			buf: []byte("GET /health HTTP/1.1\r\nContent-Length: 0\r\n\r\n"),
		}
		act := handler.OnTraffic(conn)
		assert.Equal(t, gnet.None, act)
		assert.True(t, bytes.Equal(conn.written, respHealthUnavailable), "expected response: %q, got: %q", string(respHealthUnavailable), string(conn.written))
	})
}

func TestTrustedProxies(t *testing.T) {
	trusted := []string{"1.1.1.1", "10.0.0.0/8"}

	t.Run("IP is trusted proxy -> extract X-Forwarded-For", func(t *testing.T) {
		req := parsedHTTPRequest{
			ClientIP: []byte("2.2.2.2"),
		}
		addr := &net.TCPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 1234}
		ctx := &connContext{}
		conn := &mockValidationConn{ctx: ctx, addr: addr}
		ip := extractClientIPGnet(ctx, &req, conn, trusted)
		assert.Equal(t, "2.2.2.2", ip)
	})

	t.Run("IP in trusted CIDR -> extract X-Forwarded-For", func(t *testing.T) {
		req := parsedHTTPRequest{
			ClientIP: []byte("2.2.2.2"),
		}
		addr := &net.TCPAddr{IP: net.IPv4(10, 0, 0, 5), Port: 1234}
		ctx := &connContext{}
		conn := &mockValidationConn{ctx: ctx, addr: addr}
		ip := extractClientIPGnet(ctx, &req, conn, trusted)
		assert.Equal(t, "2.2.2.2", ip)
	})

	t.Run("IP is NOT trusted proxy -> ignore X-Forwarded-For", func(t *testing.T) {
		req := parsedHTTPRequest{
			ClientIP: []byte("2.2.2.2"),
		}
		addr := &net.TCPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 1234}
		ctx := &connContext{}
		conn := &mockValidationConn{ctx: ctx, addr: addr}
		ip := extractClientIPGnet(ctx, &req, conn, trusted)
		assert.Equal(t, "8.8.8.8", ip)
	})

	t.Run("TrustedProxies is empty -> ignore X-Forwarded-For", func(t *testing.T) {
		req := parsedHTTPRequest{
			ClientIP: []byte("2.2.2.2"),
		}
		addr := &net.TCPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 1234}
		ctx := &connContext{}
		conn := &mockValidationConn{ctx: ctx, addr: addr}
		ip := extractClientIPGnet(ctx, &req, conn, nil)
		assert.Equal(t, "1.1.1.1", ip)
	})
}
