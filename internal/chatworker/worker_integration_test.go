//go:build integration

package chatworker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"chat-platform/internal/chat"
	"chat-platform/internal/chatworker"
	"chat-platform/internal/command"
	"chat-platform/internal/events"
	"chat-platform/internal/postgres"
)

type capturePublisher struct {
	mu     sync.Mutex
	events []events.Event
}

func (c *capturePublisher) Publish(_ context.Context, _ string, value any) error {
	b, _ := json.Marshal(value)
	var e events.Event
	_ = json.Unmarshal(b, &e)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return nil
}

func (c *capturePublisher) last() events.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.events[len(c.events)-1]
}

func newWorker(t *testing.T) (*chatworker.Worker, *capturePublisher, *chat.Service, context.Context) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://chat:chat@localhost:5432/chat?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)

	svc := chat.NewService(pool, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	pub := &capturePublisher{}
	return chatworker.New(svc, pub, slog.New(slog.NewTextHandler(os.Stderr, nil))), pub, svc, ctx
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestWorkerSendEmitsCreatedForAllMembers(t *testing.T) {
	w, pub, svc, ctx := newWorker(t)
	u1, u2 := uuid.NewString(), uuid.NewString()
	conv, err := svc.CreateConversation(ctx, chat.CreateConversationParams{
		Type: chat.ConversationGroup, CreatorID: u1, MemberIDs: []string{u2},
	})
	if err != nil {
		t.Fatalf("create conversation: %v", err)
	}

	cmd := command.Command{
		Type: command.SendMessage, CommandID: uuid.NewString(), GatewayID: "g1",
		ConversationID: conv.ID, UserID: u1, RequestID: "r1",
		ClientMessageID: uuid.NewString(), MessageKind: "text", Body: "hi",
	}
	if err := w.Handle(ctx, mustJSON(t, cmd)); err != nil {
		t.Fatalf("handle send: %v", err)
	}

	e := pub.last()
	if e.Type != events.MessageCreated || e.Message == nil || e.Message.Seq != 1 {
		t.Fatalf("event = %+v, want message.created at seq 1", e)
	}
	if e.RequestID != "r1" || e.OriginUserID != u1 {
		t.Fatalf("event origin fields wrong: %+v", e)
	}
	got := map[string]bool{}
	for _, r := range e.Recipients {
		got[r] = true
	}
	if !got[u1] || !got[u2] || len(e.Recipients) != 2 {
		t.Fatalf("recipients = %v, want {%s,%s}", e.Recipients, u1, u2)
	}

	// Idempotent retry -> duplicate, recipients only the sender.
	if err := w.Handle(ctx, mustJSON(t, cmd)); err != nil {
		t.Fatalf("handle retry: %v", err)
	}
	e = pub.last()
	if !e.Duplicate || len(e.Recipients) != 1 || e.Recipients[0] != u1 {
		t.Fatalf("retry event = %+v, want duplicate to sender only", e)
	}
}

func TestWorkerRejectsInvalidCommand(t *testing.T) {
	w, pub, _, ctx := newWorker(t)

	cmd := command.Command{
		Type: command.SendMessage, CommandID: uuid.NewString(),
		ConversationID: "not-a-uuid", UserID: uuid.NewString(), RequestID: "r2",
		ClientMessageID: uuid.NewString(), Body: "x",
	}
	if err := w.Handle(ctx, mustJSON(t, cmd)); err != nil {
		t.Fatalf("handle should not return transient error for a domain rejection: %v", err)
	}
	e := pub.last()
	if e.Type != events.CommandRejected || e.Error == nil || e.Error.Code != "validation" {
		t.Fatalf("event = %+v, want command.rejected/validation", e)
	}
	if len(e.Recipients) != 1 {
		t.Fatalf("rejection recipients = %v, want just the origin user", e.Recipients)
	}
}
