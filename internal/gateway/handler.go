package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"chat-platform/internal/auth"
	"chat-platform/internal/chat"
	"chat-platform/internal/command"
	"chat-platform/internal/config"
	"chat-platform/internal/events"
	kafkax "chat-platform/internal/kafka"
	"chat-platform/internal/metrics"
	"chat-platform/internal/session"
)

// Gateway is the WebSocket + HTTP edge.
//
// Reads (list conversations, history) still call the chat service directly.
// Writes (message.send / edit / delete / read) are published to the
// chat.commands Kafka topic; the authoritative result comes back on
// chat.events, which ConsumeEvents turns into client frames.
type Gateway struct {
	hub      *Hub
	chat     *chat.Service
	sessions *session.Registry
	verifier *auth.Verifier
	issuer   *auth.Issuer
	commands *kafkax.Producer
	cfg      config.Config
	log      *slog.Logger
}

// New builds a Gateway. The JWT verifier and issuer are derived from cfg.
func New(
	hub *Hub,
	chatSvc *chat.Service,
	sessions *session.Registry,
	commands *kafkax.Producer,
	cfg config.Config,
	log *slog.Logger,
) *Gateway {
	return &Gateway{
		hub:      hub,
		chat:     chatSvc,
		sessions: sessions,
		verifier: auth.NewVerifier(cfg.JWTSecret, cfg.JWTIssuer),
		issuer:   auth.NewIssuer(cfg.JWTSecret, cfg.JWTIssuer),
		commands: commands,
		cfg:      cfg,
		log:      log,
	}
}

// HandleWS upgrades the request and runs the connection until it closes.
func (g *Gateway) HandleWS(w http.ResponseWriter, r *http.Request) {
	claims, ok := claimsFrom(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing claims")
		return
	}
	userID := claims.UserID()

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Browsers send an Origin on the handshake; in dev we accept any.
		InsecureSkipVerify: g.cfg.DevMode,
	})
	if err != nil {
		g.log.Warn("websocket accept failed", "err", err)
		return
	}

	connID := uuid.NewString()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	conn := newConnection(connID, userID, ws, g.cfg.SendBuffer, cancel, g.log)

	g.hub.Add(conn)
	metrics.WSConnections.Inc()
	if err := g.sessions.Register(ctx, userID, connID); err != nil {
		g.log.Warn("session register failed", "err", err, "user", userID, "conn", connID)
	}
	g.log.Info("ws connected", "user", userID, "conn", connID, "total", g.hub.ConnectionCount())

	defer func() {
		g.hub.Remove(conn)
		metrics.WSConnections.Dec()
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := g.sessions.Remove(rmCtx, userID, connID); err != nil {
			g.log.Warn("session remove failed", "err", err, "user", userID, "conn", connID)
		}
		rmCancel()
		_ = ws.Close(websocket.StatusNormalClosure, "bye")
		g.log.Info("ws disconnected", "user", userID, "conn", connID, "total", g.hub.ConnectionCount())
	}()

	conn.enqueue(mustJSON(helloFrame{
		Type:         frameHello,
		ConnectionID: connID,
		UserID:       userID,
		GatewayID:    g.cfg.GatewayID,
	}))

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); conn.writeLoop(ctx, g.cfg.WriteTimeout) }()
	go func() { defer wg.Done(); conn.pingLoop(ctx, g.cfg.HeartbeatInterval) }()
	go func() { defer wg.Done(); g.heartbeatLoop(ctx, userID, connID) }()

	conn.readLoop(ctx, g.handleFrame(conn))
	cancel()
	wg.Wait()
}

func (g *Gateway) heartbeatLoop(ctx context.Context, userID, connID string) {
	t := time.NewTicker(g.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			hbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := g.sessions.Heartbeat(hbCtx, userID, connID); err != nil {
				g.log.Warn("session heartbeat failed", "err", err, "user", userID, "conn", connID)
			}
			cancel()
		}
	}
}

func (g *Gateway) handleFrame(conn *Connection) func(context.Context, []byte) {
	return func(ctx context.Context, raw []byte) {
		var f inboundFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			conn.enqueue(mustJSON(errorFrame{Type: frameError, Code: "bad_frame", Message: "invalid JSON"}))
			return
		}
		switch f.Type {
		case frameMessageSend:
			g.publishCommand(ctx, conn, f, command.SendMessage)
		case frameMessageEdit:
			g.publishCommand(ctx, conn, f, command.EditMessage)
		case frameMessageDelete:
			g.publishCommand(ctx, conn, f, command.DeleteMessage)
		case frameMessageRead:
			g.publishCommand(ctx, conn, f, command.MarkRead)
		case framePing:
			conn.enqueue(mustJSON(map[string]string{"type": framePong}))
		default:
			conn.enqueue(mustJSON(errorFrame{
				Type:      frameError,
				RequestID: f.RequestID,
				Code:      "unknown_type",
				Message:   "unsupported frame type: " + f.Type,
			}))
		}
	}
}

// publishCommand validates the frame cheaply, publishes a command keyed by
// conversation id, and returns an "accepted" frame. Authoritative validation
// happens in the chat service and comes back as an "error" frame.
func (g *Gateway) publishCommand(ctx context.Context, conn *Connection, f inboundFrame, t command.Type) {
	if f.ConversationID == "" {
		conn.enqueue(mustJSON(errorFrame{Type: frameError, RequestID: f.RequestID, Code: "validation", Message: "conversation_id is required"}))
		return
	}
	if t == command.SendMessage && f.ClientMessageID == "" {
		conn.enqueue(mustJSON(errorFrame{Type: frameError, RequestID: f.RequestID, Code: "validation", Message: "client_message_id is required"}))
		return
	}

	cmd := command.Command{
		Type:            t,
		CommandID:       uuid.NewString(),
		GatewayID:       g.cfg.GatewayID,
		ConversationID:  f.ConversationID,
		UserID:          conn.UserID,
		RequestID:       f.RequestID,
		ClientMessageID: f.ClientMessageID,
		MessageKind:     f.Kind,
		Body:            f.Body,
		Seq:             f.Seq,
	}
	if err := g.commands.Publish(ctx, cmd.ConversationID, cmd); err != nil {
		g.log.Error("publish command failed", "err", err, "type", t, "conversation", f.ConversationID)
		conn.enqueue(mustJSON(errorFrame{Type: frameError, RequestID: f.RequestID, Code: "internal", Message: "could not enqueue command"}))
		return
	}
	metrics.CommandsPublished.Inc()
	conn.enqueue(mustJSON(acceptedFrame{Type: frameAccepted, RequestID: f.RequestID, CommandID: cmd.CommandID}))
}

// ConsumeEvents reads chat.events and delivers frames to locally-held
// connections until ctx is cancelled.
func (g *Gateway) ConsumeEvents(ctx context.Context, r *kafka.Reader) {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			g.log.Error("event read failed", "err", err)
			continue
		}
		var e events.Event
		if err := json.Unmarshal(m.Value, &e); err != nil {
			g.log.Error("dropping unparseable event", "err", err)
			continue
		}
		g.dispatchEvent(e)
	}
}

func (g *Gateway) dispatchEvent(e events.Event) {
	var frame any
	switch e.Type {
	case events.MessageCreated, events.MessageUpdated, events.MessageDeleted:
		if e.Message == nil {
			g.log.Warn("event missing message", "type", e.Type, "conversation", e.ConversationID)
			return
		}
		frame = messageEventFrame{Type: e.Type, ConversationID: e.ConversationID, Duplicate: e.Duplicate, Message: *e.Message}
	case events.MessageRead:
		frame = readReceiptFrame{Type: frameReadReceipt, ConversationID: e.ConversationID, UserID: e.OriginUserID, Seq: e.Seq}
	case events.CommandRejected:
		code, msg := "internal", "command rejected"
		if e.Error != nil {
			code, msg = e.Error.Code, e.Error.Message
		}
		frame = errorFrame{Type: frameError, RequestID: e.RequestID, Code: code, Message: msg}
	default:
		g.log.Warn("unknown event type", "type", e.Type)
		return
	}
	g.hub.SendToUsers(e.Recipients, mustJSON(frame))
	metrics.EventsDelivered.Inc()
}


func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Frames are all plain structs; a marshal error is a programming bug.
		panic("gateway: marshal frame: " + err.Error())
	}
	return b
}
