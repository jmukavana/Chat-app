package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"chat-platform/internal/chat/chatdb"
)

// SendMessageParams is the input to SendMessage.
type SendMessageParams struct {
	ConversationID  string
	SenderID        string
	ClientMessageID string // client-generated idempotency key (uuid)
	Kind            string // defaults to "text"
	Body            string
}

// SendMessageResult carries the persisted message and whether it was resolved
// through idempotency (the client_message_id already existed).
type SendMessageResult struct {
	Message   Message
	Duplicate bool
}

func validateSend(p *SendMessageParams) error {
	if p.Kind == "" {
		p.Kind = string(MessageText)
	}
	if !validUUID(p.ConversationID) {
		return validationErr("conversation_id must be a uuid")
	}
	if !validUUID(p.SenderID) {
		return validationErr("sender_id must be a uuid")
	}
	if !validUUID(p.ClientMessageID) {
		return validationErr("client_message_id must be a uuid")
	}
	switch MessageKind(p.Kind) {
	case MessageText:
		if p.Body == "" {
			return validationErr("body is required for text messages")
		}
		if utf8.RuneCountInString(p.Body) > maxTextBodyLen {
			return validationErr("body exceeds %d characters", maxTextBodyLen)
		}
	case MessageImage, MessageSystem:
		// body optional; media handled in a later phase
	default:
		return validationErr("unknown message kind %q", p.Kind)
	}
	return nil
}

// SendMessage persists a message with a server-assigned, gap-free sequence
// number, in one transaction:
//
//  1. idempotency fast-path on (conversation_id, client_message_id)
//  2. SELECT ... FOR UPDATE on the conversation row to serialise seq assignment
//  3. membership check for the sender
//  4. INSERT the message at next_seq
//  5. bump conversations.next_seq
//  6. INSERT the transactional-outbox row for the future Kafka bridge
//
// A unique-violation race on step 4 (a concurrent retry of the same
// client_message_id) is caught and resolved to the winning row.
func (s *Service) SendMessage(ctx context.Context, p SendMessageParams) (SendMessageResult, error) {
	if err := validateSend(&p); err != nil {
		return SendMessageResult{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SendMessageResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	// 1. Idempotency fast-path.
	if existing, err := qtx.GetMessageByClientID(ctx, chatdb.GetMessageByClientIDParams{
		ConversationID: p.ConversationID, ClientMessageID: p.ClientMessageID,
	}); err == nil {
		if cErr := tx.Commit(ctx); cErr != nil {
			return SendMessageResult{}, fmt.Errorf("commit (idempotent): %w", cErr)
		}
		return SendMessageResult{Message: toMessage(existing), Duplicate: true}, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return SendMessageResult{}, fmt.Errorf("idempotency check: %w", err)
	}

	// 2. Lock the conversation row and read the next sequence number.
	nextSeq, err := qtx.LockConversationForUpdate(ctx, p.ConversationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return SendMessageResult{}, ErrConversationNotFound
	}
	if err != nil {
		return SendMessageResult{}, fmt.Errorf("lock conversation: %w", err)
	}

	// 3. Membership check.
	isMember, err := qtx.IsConversationMember(ctx, chatdb.IsConversationMemberParams{
		ConversationID: p.ConversationID, UserID: p.SenderID,
	})
	if err != nil {
		return SendMessageResult{}, fmt.Errorf("membership check: %w", err)
	}
	if !isMember {
		return SendMessageResult{}, ErrNotMember
	}

	// 4. Insert the message at next_seq.
	m, err := qtx.InsertMessage(ctx, chatdb.InsertMessageParams{
		ConversationID:  p.ConversationID,
		Seq:             nextSeq,
		SenderID:        p.SenderID,
		ClientMessageID: p.ClientMessageID,
		Kind:            p.Kind,
		Body:            p.Body,
	})
	if err != nil {
		if isUniqueViolation(err) {
			// Concurrent retry won the race between our fast-path and here.
			_ = tx.Rollback(ctx)
			return s.resolveDuplicate(ctx, p.ConversationID, p.ClientMessageID)
		}
		return SendMessageResult{}, fmt.Errorf("insert message: %w", err)
	}

	// 5. Advance the sequence.
	if err := qtx.AdvanceConversationSeq(ctx, p.ConversationID); err != nil {
		return SendMessageResult{}, fmt.Errorf("advance next_seq: %w", err)
	}

	// 6. Transactional outbox row.
	if err := writeOutbox(ctx, qtx, eventMessageCreated, toMessage(m)); err != nil {
		return SendMessageResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return SendMessageResult{}, fmt.Errorf("commit: %w", err)
	}
	return SendMessageResult{Message: toMessage(m)}, nil
}

func (s *Service) resolveDuplicate(ctx context.Context, conversationID, clientMessageID string) (SendMessageResult, error) {
	m, err := s.q.GetMessageByClientID(ctx, chatdb.GetMessageByClientIDParams{
		ConversationID: conversationID, ClientMessageID: clientMessageID,
	})
	if err != nil {
		return SendMessageResult{}, fmt.Errorf("resolve duplicate: %w", err)
	}
	return SendMessageResult{Message: toMessage(m), Duplicate: true}, nil
}

// EditMessageParams identifies a message by its server seq and carries the new
// body. Only the original sender may edit, and only text messages that have
// not been deleted.
type EditMessageParams struct {
	ConversationID string
	EditorID       string
	Seq            int64
	Body           string
}

// EditMessage replaces the body of a message and stamps edited_at.
func (s *Service) EditMessage(ctx context.Context, p EditMessageParams) (Message, error) {
	if !validUUID(p.ConversationID) {
		return Message{}, validationErr("conversation_id must be a uuid")
	}
	if !validUUID(p.EditorID) {
		return Message{}, validationErr("editor_id must be a uuid")
	}
	if p.Seq <= 0 {
		return Message{}, validationErr("seq must be > 0")
	}
	if p.Body == "" {
		return Message{}, validationErr("body is required")
	}
	if utf8.RuneCountInString(p.Body) > maxTextBodyLen {
		return Message{}, validationErr("body exceeds %d characters", maxTextBodyLen)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	cur, err := qtx.GetMessageBySeq(ctx, chatdb.GetMessageBySeqParams{
		ConversationID: p.ConversationID, Seq: p.Seq,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrMessageNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("get message: %w", err)
	}
	if cur.SenderID != p.EditorID {
		return Message{}, ErrNotSender
	}
	if cur.DeletedAt.Valid {
		return Message{}, ErrMessageNotFound
	}
	if MessageKind(cur.Kind) != MessageText {
		return Message{}, validationErr("only text messages can be edited")
	}

	updated, err := qtx.EditMessageBySeq(ctx, chatdb.EditMessageBySeqParams{
		Body: p.Body, ConversationID: p.ConversationID, Seq: p.Seq,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrMessageNotFound // deleted between the read and the update
	}
	if err != nil {
		return Message{}, fmt.Errorf("edit message: %w", err)
	}
	if err := writeOutbox(ctx, qtx, eventMessageUpdated, toMessage(updated)); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit: %w", err)
	}
	return toMessage(updated), nil
}

// DeleteMessageParams identifies a message to soft-delete by its server seq.
type DeleteMessageParams struct {
	ConversationID string
	RequesterID    string
	Seq            int64
}

// DeleteMessage soft-deletes a message: deleted_at is stamped and the body is
// cleared. Deleting an already-deleted message is a no-op that returns the
// existing tombstone. Only the original sender may delete.
func (s *Service) DeleteMessage(ctx context.Context, p DeleteMessageParams) (Message, error) {
	if !validUUID(p.ConversationID) {
		return Message{}, validationErr("conversation_id must be a uuid")
	}
	if !validUUID(p.RequesterID) {
		return Message{}, validationErr("requester_id must be a uuid")
	}
	if p.Seq <= 0 {
		return Message{}, validationErr("seq must be > 0")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	cur, err := qtx.GetMessageBySeq(ctx, chatdb.GetMessageBySeqParams{
		ConversationID: p.ConversationID, Seq: p.Seq,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrMessageNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("get message: %w", err)
	}
	if cur.SenderID != p.RequesterID {
		return Message{}, ErrNotSender
	}
	if cur.DeletedAt.Valid {
		return toMessage(cur), nil // already deleted; idempotent
	}

	del, err := qtx.SoftDeleteMessageBySeq(ctx, chatdb.SoftDeleteMessageBySeqParams{
		ConversationID: p.ConversationID, Seq: p.Seq,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, ErrMessageNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("delete message: %w", err)
	}
	if err := writeOutbox(ctx, qtx, eventMessageDeleted, toMessage(del)); err != nil {
		return Message{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Message{}, fmt.Errorf("commit: %w", err)
	}
	return toMessage(del), nil
}

func writeOutbox(ctx context.Context, q *chatdb.Queries, event string, m Message) error {
	payload, err := json.Marshal(outboxEvent{
		Event:        event,
		Conversation: m.ConversationID,
		Message:      m,
	})
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	if err := q.InsertOutbox(ctx, chatdb.InsertOutboxParams{
		AggregateType: "conversation",
		AggregateID:   m.ConversationID,
		EventType:     event,
		Payload:       payload,
	}); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// ListMessagesParams selects a page of a conversation's history.
//
// Exactly one of AfterSeq / BeforeSeq is normally set:
//   - AfterSeq:  reconnect sync — messages with seq > AfterSeq, ascending
//   - BeforeSeq: scroll-back    — messages with seq < BeforeSeq, ascending
//   - neither:   the latest page, ascending
type ListMessagesParams struct {
	ConversationID string
	RequesterID    string
	AfterSeq       *int64
	BeforeSeq      *int64
	Limit          int
}

// ListMessages returns a page of history in ascending seq order. The requester
// must be a member of the conversation. Soft-deleted messages are returned as
// tombstones (empty body, deleted_at set) so clients can render them.
func (s *Service) ListMessages(ctx context.Context, p ListMessagesParams) ([]Message, error) {
	if !validUUID(p.ConversationID) {
		return nil, validationErr("conversation_id must be a uuid")
	}
	member, err := s.IsMember(ctx, p.ConversationID, p.RequesterID)
	if err != nil {
		return nil, err
	}
	if !member {
		return nil, ErrNotMember
	}

	limit := int32(clampLimit(p.Limit, 100, 200))

	var (
		rows    []chatdb.Message
		reverse bool
	)
	switch {
	case p.AfterSeq != nil:
		rows, err = s.q.ListMessagesAfter(ctx, chatdb.ListMessagesAfterParams{
			ConversationID: p.ConversationID, Seq: *p.AfterSeq, Limit: limit,
		})
	case p.BeforeSeq != nil:
		reverse = true
		rows, err = s.q.ListMessagesBefore(ctx, chatdb.ListMessagesBeforeParams{
			ConversationID: p.ConversationID, Seq: *p.BeforeSeq, Limit: limit,
		})
	default:
		reverse = true
		rows, err = s.q.ListMessagesLatest(ctx, chatdb.ListMessagesLatestParams{
			ConversationID: p.ConversationID, Limit: limit,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}

	out := make([]Message, 0, len(rows))
	for _, r := range rows {
		out = append(out, toMessage(r))
	}
	if reverse {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out, nil
}
