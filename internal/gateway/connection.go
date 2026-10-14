package gateway

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"

	"chat-platform/internal/metrics"
)

const readLimitBytes = 32 * 1024

// Connection is a single client WebSocket. Outbound frames go through the
// buffered send channel and are written by a dedicated goroutine, so slow
// readers can never block the fan-out path — they get disconnected instead.
type Connection struct {
	ID     string
	UserID string

	ws   *websocket.Conn
	send chan []byte
	log  *slog.Logger

	closeOnce sync.Once
	cancel    context.CancelFunc
}

func newConnection(id, userID string, ws *websocket.Conn, buffer int, cancel context.CancelFunc, log *slog.Logger) *Connection {
	if buffer <= 0 {
		buffer = 32
	}
	ws.SetReadLimit(readLimitBytes)
	return &Connection{
		ID:     id,
		UserID: userID,
		ws:     ws,
		send:   make(chan []byte, buffer),
		cancel: cancel,
		log:    log,
	}
}

// enqueue queues a frame for delivery. If the send buffer is full the consumer
// is too slow: drop the frame and tear the connection down so it resyncs on
// reconnect via the after_seq history API.
func (c *Connection) enqueue(payload []byte) {
	select {
	case c.send <- payload:
	default:
		metrics.FramesDropped.Inc()
		c.log.Warn("dropping frame, send buffer full", "conn", c.ID, "user", c.UserID)
		c.close(websocket.StatusPolicyViolation, "send buffer full")
	}
}

// writeLoop drains the send channel to the socket until the context is
// cancelled or a write fails.
func (c *Connection) writeLoop(ctx context.Context, writeTimeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-c.send:
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.ws.Write(wctx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				c.close(websocket.StatusInternalError, "write failed")
				return
			}
		}
	}
}

// pingLoop keeps the connection alive and detects half-open sockets. Ping
// blocks until the pong is received or the context times out.
func (c *Connection) pingLoop(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, interval)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				c.close(websocket.StatusPolicyViolation, "ping timeout")
				return
			}
		}
	}
}

// readLoop reads text frames and hands them to handle until the socket closes.
func (c *Connection) readLoop(ctx context.Context, handle func(context.Context, []byte)) {
	for {
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		handle(ctx, data)
	}
}

// close tears the connection down exactly once and cancels its context so all
// of its goroutines unwind.
func (c *Connection) close(code websocket.StatusCode, reason string) {
	c.closeOnce.Do(func() {
		_ = c.ws.Close(code, reason)
		if c.cancel != nil {
			c.cancel()
		}
	})
}
