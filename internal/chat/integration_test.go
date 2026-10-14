//go:build integration

// Integration tests for the chat service. They need a real PostgreSQL with the
// 0001_init migration applied.
//
//	make up && make test-integration
//
// Override the DSN with DATABASE_URL if you are not using deployments/docker-compose.yml.
package chat_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"chat-platform/internal/chat"
	"chat-platform/internal/postgres"
)

func newService(t *testing.T) (*chat.Service, context.Context) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://chat:chat@localhost:5432/chat?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres (%s): %v", dsn, err)
	}
	t.Cleanup(pool.Close)

	return chat.NewService(pool, slog.New(slog.NewTextHandler(os.Stderr, nil))), ctx
}

func TestSequenceAndIdempotency(t *testing.T) {
	svc, ctx := newService(t)

	u1 := uuid.NewString()
	u2 := uuid.NewString()

	conv, err := svc.CreateConversation(ctx, chat.CreateConversationParams{
		Type:      chat.ConversationGroup,
		CreatorID: u1,
		MemberIDs: []string{u2},
	})
	if err != nil {
		t.Fatalf("create conversation: %v", err)
	}
	if conv.NextSeq != 1 {
		t.Fatalf("fresh conversation next_seq = %d, want 1", conv.NextSeq)
	}

	first, err := svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID: conv.ID, SenderID: u1, ClientMessageID: uuid.NewString(), Body: "one",
	})
	if err != nil {
		t.Fatalf("send first: %v", err)
	}
	if first.Message.Seq != 1 || first.Duplicate {
		t.Fatalf("first message seq=%d duplicate=%v, want 1/false", first.Message.Seq, first.Duplicate)
	}

	second, err := svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID: conv.ID, SenderID: u2, ClientMessageID: uuid.NewString(), Body: "two",
	})
	if err != nil {
		t.Fatalf("send second: %v", err)
	}
	if second.Message.Seq != 2 {
		t.Fatalf("second message seq=%d, want 2", second.Message.Seq)
	}

	// Idempotent retry: same client_message_id -> same row, no new seq.
	retryID := uuid.NewString()
	a, err := svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID: conv.ID, SenderID: u1, ClientMessageID: retryID, Body: "three",
	})
	if err != nil {
		t.Fatalf("send three: %v", err)
	}
	b, err := svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID: conv.ID, SenderID: u1, ClientMessageID: retryID, Body: "three (retry)",
	})
	if err != nil {
		t.Fatalf("retry three: %v", err)
	}
	if !b.Duplicate {
		t.Fatalf("retry not flagged as duplicate")
	}
	if a.Message.ID != b.Message.ID || a.Message.Seq != b.Message.Seq {
		t.Fatalf("retry returned different row: %+v vs %+v", a.Message, b.Message)
	}

	// Non-member cannot send.
	if _, err := svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID: conv.ID, SenderID: uuid.NewString(), ClientMessageID: uuid.NewString(), Body: "x",
	}); err == nil {
		t.Fatalf("expected ErrNotMember for non-member send")
	}
}

func TestConcurrentSendsHaveNoSeqGaps(t *testing.T) {
	svc, ctx := newService(t)

	u1 := uuid.NewString()
	conv, err := svc.CreateConversation(ctx, chat.CreateConversationParams{
		Type:      chat.ConversationGroup,
		CreatorID: u1,
	})
	if err != nil {
		t.Fatalf("create conversation: %v", err)
	}

	const n = 25
	seqs := make([]int64, n)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := svc.SendMessage(ctx, chat.SendMessageParams{
				ConversationID: conv.ID, SenderID: u1, ClientMessageID: uuid.NewString(), Body: "concurrent",
			})
			if err != nil {
				errs <- err
				return
			}
			seqs[i] = res.Message.Seq
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent send: %v", err)
	}

	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i := 1; i < len(seqs); i++ {
		if seqs[i] == seqs[i-1] {
			t.Fatalf("duplicate seq %d", seqs[i])
		}
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("gap in seqs: %d then %d", seqs[i-1], seqs[i])
		}
	}
}

func TestEditAndDelete(t *testing.T) {
	svc, ctx := newService(t)

	u1 := uuid.NewString()
	u2 := uuid.NewString()
	conv, err := svc.CreateConversation(ctx, chat.CreateConversationParams{
		Type:      chat.ConversationGroup,
		CreatorID: u1,
		MemberIDs: []string{u2},
	})
	if err != nil {
		t.Fatalf("create conversation: %v", err)
	}

	sent, err := svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID: conv.ID, SenderID: u1, ClientMessageID: uuid.NewString(), Body: "original",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	seq := sent.Message.Seq

	// A non-sender cannot edit.
	if _, err := svc.EditMessage(ctx, chat.EditMessageParams{
		ConversationID: conv.ID, EditorID: u2, Seq: seq, Body: "hacked",
	}); !errors.Is(err, chat.ErrNotSender) {
		t.Fatalf("edit by non-sender: err = %v, want ErrNotSender", err)
	}

	edited, err := svc.EditMessage(ctx, chat.EditMessageParams{
		ConversationID: conv.ID, EditorID: u1, Seq: seq, Body: "edited",
	})
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if edited.Body != "edited" || edited.EditedAt == nil {
		t.Fatalf("edited message = %+v, want body 'edited' and non-nil EditedAt", edited)
	}

	// Delete, then verify the tombstone and idempotent re-delete.
	del, err := svc.DeleteMessage(ctx, chat.DeleteMessageParams{
		ConversationID: conv.ID, RequesterID: u1, Seq: seq,
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if del.Body != "" || del.DeletedAt == nil {
		t.Fatalf("tombstone = %+v, want empty body and non-nil DeletedAt", del)
	}
	if _, err := svc.DeleteMessage(ctx, chat.DeleteMessageParams{
		ConversationID: conv.ID, RequesterID: u1, Seq: seq,
	}); err != nil {
		t.Fatalf("re-delete should be a no-op, got %v", err)
	}

	// Editing a deleted message fails.
	if _, err := svc.EditMessage(ctx, chat.EditMessageParams{
		ConversationID: conv.ID, EditorID: u1, Seq: seq, Body: "resurrect",
	}); !errors.Is(err, chat.ErrMessageNotFound) {
		t.Fatalf("edit after delete: err = %v, want ErrMessageNotFound", err)
	}

	// History still returns the tombstone at its seq.
	msgs, err := svc.ListMessages(ctx, chat.ListMessagesParams{
		ConversationID: conv.ID, RequesterID: u2, AfterSeq: ptr(int64(0)),
	})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Seq != seq || msgs[0].DeletedAt == nil {
		t.Fatalf("history = %+v, want one tombstone at seq %d", msgs, seq)
	}
}

func ptr[T any](v T) *T { return &v }

func TestMarkReadIsMonotonic(t *testing.T) {
	svc, ctx := newService(t)

	u1 := uuid.NewString()
	u2 := uuid.NewString()
	conv, err := svc.CreateConversation(ctx, chat.CreateConversationParams{
		Type:      chat.ConversationGroup,
		CreatorID: u1,
		MemberIDs: []string{u2},
	})
	if err != nil {
		t.Fatalf("create conversation: %v", err)
	}

	if got, err := svc.MarkRead(ctx, conv.ID, u2, 5); err != nil || got != 5 {
		t.Fatalf("MarkRead(5) = %d, %v; want 5, nil", got, err)
	}
	if got, err := svc.MarkRead(ctx, conv.ID, u2, 2); err != nil || got != 5 {
		t.Fatalf("MarkRead(2) = %d, %v; want 5, nil (must not move backwards)", got, err)
	}
	if _, err := svc.MarkRead(ctx, conv.ID, uuid.NewString(), 1); err == nil {
		t.Fatalf("expected ErrNotMember marking read for non-member")
	}
}
