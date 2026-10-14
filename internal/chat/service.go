// Package chat owns conversations, membership, message persistence and the
// per-conversation sequence numbers that give the client a strong ordering
// boundary.
//
// SQL lives in db/queries/*.sql and is compiled to the chatdb package by sqlc
// (`make sqlc`). This layer adds transactions, validation, domain types and
// the transactional-outbox writes.
//
// Phase 1/2 run this in-process inside cmd/gateway. In Phase 3 the gateway
// stops calling these methods directly and publishes commands to Kafka, where
// a standalone chat service consumes them and runs exactly this logic. The
// outbox rows are what the Phase 4 publisher drains into Kafka.
package chat

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"chat-platform/internal/chat/chatdb"
)

// Domain errors. Callers use errors.Is to map these to transport-level codes.
var (
	ErrValidation           = errors.New("chat: validation failed")
	ErrNotMember            = errors.New("chat: user is not a member of the conversation")
	ErrConversationNotFound = errors.New("chat: conversation not found")
	ErrMessageNotFound      = errors.New("chat: message not found")
	ErrNotSender            = errors.New("chat: only the original sender may modify this message")
)

// ConversationType distinguishes 1:1 and group conversations.
type ConversationType string

const (
	ConversationDirect ConversationType = "direct"
	ConversationGroup  ConversationType = "group"
)

// MessageKind is the payload discriminator for a message row.
type MessageKind string

const (
	MessageText   MessageKind = "text"
	MessageImage  MessageKind = "image"
	MessageSystem MessageKind = "system"
)

const maxTextBodyLen = 8000

// Outbox event names (also the WebSocket frame "type" for the broadcast).
const (
	eventMessageCreated = "message.created"
	eventMessageUpdated = "message.updated"
	eventMessageDeleted = "message.deleted"
)

// Conversation is a chat thread with its own monotonic sequence counter.
type Conversation struct {
	ID        string           `json:"id"`
	Type      ConversationType `json:"type"`
	NextSeq   int64            `json:"next_seq"`
	CreatedAt time.Time        `json:"created_at"`
}

// ConversationSummary is a Conversation plus the requesting user's read state.
type ConversationSummary struct {
	Conversation
	LastReadSeq int64 `json:"last_read_seq"`
	Unread      int64 `json:"unread"`
}

// Message is a persisted message row. A soft-deleted message has DeletedAt set
// and an empty Body.
type Message struct {
	ID              string     `json:"id"`
	ConversationID  string     `json:"conversation_id"`
	Seq             int64      `json:"seq"`
	SenderID        string     `json:"sender_id"`
	ClientMessageID string     `json:"client_message_id"`
	Kind            string     `json:"kind"`
	Body            string     `json:"body"`
	MediaKey        *string    `json:"media_key,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	EditedAt        *time.Time `json:"edited_at,omitempty"`
	DeletedAt       *time.Time `json:"deleted_at,omitempty"`
}

// Service is the chat domain API backed by PostgreSQL.
type Service struct {
	pool *pgxpool.Pool
	q    *chatdb.Queries
	log  *slog.Logger
}

// NewService wires the chat service to a pgx pool.
func NewService(pool *pgxpool.Pool, log *slog.Logger) *Service {
	return &Service{pool: pool, q: chatdb.New(pool), log: log}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func validUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

func clampLimit(n, def, max int) int {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// validationErr wraps a human-readable reason as ErrValidation.
func validationErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

// IsTerminal reports whether err is a domain rejection that will never succeed
// on retry (bad input, not a member, gone). Anything else — a dropped
// connection, a lock timeout — is transient and worth retrying.
func IsTerminal(err error) bool {
	switch {
	case errors.Is(err, ErrValidation),
		errors.Is(err, ErrNotMember),
		errors.Is(err, ErrConversationNotFound),
		errors.Is(err, ErrMessageNotFound),
		errors.Is(err, ErrNotSender):
		return true
	default:
		return false
	}
}

// ErrorCode maps a domain error to a stable short code for the transport layer.
func ErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrValidation):
		return "validation"
	case errors.Is(err, ErrNotMember), errors.Is(err, ErrNotSender):
		return "forbidden"
	case errors.Is(err, ErrConversationNotFound), errors.Is(err, ErrMessageNotFound):
		return "not_found"
	default:
		return "internal"
	}
}

// --- chatdb -> domain mapping -------------------------------------------------

func tsPtr(t pgtype.Timestamptz) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time
	return &v
}

func toConversation(c chatdb.Conversation) Conversation {
	return Conversation{
		ID:        c.ID,
		Type:      ConversationType(c.Type),
		NextSeq:   c.NextSeq,
		CreatedAt: c.CreatedAt.Time,
	}
}

func toMessage(m chatdb.Message) Message {
	return Message{
		ID:              m.ID,
		ConversationID:  m.ConversationID,
		Seq:             m.Seq,
		SenderID:        m.SenderID,
		ClientMessageID: m.ClientMessageID,
		Kind:            m.Kind,
		Body:            m.Body,
		MediaKey:        m.MediaKey,
		CreatedAt:       m.CreatedAt.Time,
		EditedAt:        tsPtr(m.EditedAt),
		DeletedAt:       tsPtr(m.DeletedAt),
	}
}

// outboxEvent is the payload written to the outbox for every message.* event.
// Phase 4's publisher reads these rows and produces them onto chat.events.
type outboxEvent struct {
	Event        string  `json:"event"`
	Conversation string  `json:"conversation_id"`
	Message      Message `json:"message"`
}
