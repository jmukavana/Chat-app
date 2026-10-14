package chat

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"chat-platform/internal/chat/chatdb"
)

// ListMemberIDs returns the user ids of every member of a conversation. The
// gateway uses this to decide who a message.* event fans out to.
func (s *Service) ListMemberIDs(ctx context.Context, conversationID string) ([]string, error) {
	ids, err := s.q.ListMemberIDs(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list member ids: %w", err)
	}
	return ids, nil
}

// IsMember reports whether userID belongs to conversationID.
func (s *Service) IsMember(ctx context.Context, conversationID, userID string) (bool, error) {
	ok, err := s.q.IsConversationMember(ctx, chatdb.IsConversationMemberParams{
		ConversationID: conversationID,
		UserID:         userID,
	})
	if err != nil {
		return false, fmt.Errorf("is member: %w", err)
	}
	return ok, nil
}

// MarkRead advances the caller's read cursor to seq. It never moves the cursor
// backwards: a delayed read for an older seq is a no-op. Returns the resulting
// last_read_seq. Returns ErrNotMember if the user is not in the conversation.
func (s *Service) MarkRead(ctx context.Context, conversationID, userID string, seq int64) (int64, error) {
	if seq < 0 {
		return 0, validationErr("seq must be >= 0")
	}
	lastRead, err := s.q.MarkRead(ctx, chatdb.MarkReadParams{
		Seq:            seq,
		ConversationID: conversationID,
		UserID:         userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotMember
	}
	if err != nil {
		return 0, fmt.Errorf("mark read: %w", err)
	}
	return lastRead, nil
}
