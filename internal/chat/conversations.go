package chat

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"chat-platform/internal/chat/chatdb"
)

// CreateConversationParams describes a new conversation. CreatorID is always
// added as a member with the "owner" role; MemberIDs are added as "member".
type CreateConversationParams struct {
	Type      ConversationType
	CreatorID string
	MemberIDs []string
}

// CreateConversation creates the conversation and its membership rows in a
// single transaction.
func (s *Service) CreateConversation(ctx context.Context, p CreateConversationParams) (Conversation, error) {
	if p.Type != ConversationDirect && p.Type != ConversationGroup {
		return Conversation{}, validationErr("type must be %q or %q", ConversationDirect, ConversationGroup)
	}
	if !validUUID(p.CreatorID) {
		return Conversation{}, validationErr("creator_id must be a uuid")
	}

	members := map[string]struct{}{p.CreatorID: {}}
	for _, id := range p.MemberIDs {
		if !validUUID(id) {
			return Conversation{}, validationErr("member id %q is not a uuid", id)
		}
		members[id] = struct{}{}
	}
	if p.Type == ConversationDirect && len(members) != 2 {
		return Conversation{}, validationErr("direct conversation requires exactly two distinct members")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Conversation{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	c, err := qtx.InsertConversation(ctx, string(p.Type))
	if err != nil {
		return Conversation{}, fmt.Errorf("insert conversation: %w", err)
	}

	for userID := range members {
		role := "member"
		if userID == p.CreatorID {
			role = "owner"
		}
		if err := qtx.InsertMember(ctx, chatdb.InsertMemberParams{
			ConversationID: c.ID,
			UserID:         userID,
			Role:           role,
		}); err != nil {
			return Conversation{}, fmt.Errorf("insert member %s: %w", userID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Conversation{}, fmt.Errorf("commit: %w", err)
	}
	return toConversation(c), nil
}

// GetConversation returns a conversation by id.
func (s *Service) GetConversation(ctx context.Context, id string) (Conversation, error) {
	c, err := s.q.GetConversation(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Conversation{}, ErrConversationNotFound
	}
	if err != nil {
		return Conversation{}, fmt.Errorf("get conversation: %w", err)
	}
	return toConversation(c), nil
}

// ListConversations returns every conversation the user belongs to, newest
// first, with the user's read state and a cheap unread count derived from the
// sequence counter (highest assigned seq is next_seq-1).
func (s *Service) ListConversations(ctx context.Context, userID string) ([]ConversationSummary, error) {
	if !validUUID(userID) {
		return nil, validationErr("user_id must be a uuid")
	}
	rows, err := s.q.ListConversationsForUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}
	out := make([]ConversationSummary, 0, len(rows))
	for _, r := range rows {
		sum := ConversationSummary{
			Conversation: Conversation{
				ID:        r.ID,
				Type:      ConversationType(r.Type),
				NextSeq:   r.NextSeq,
				CreatedAt: r.CreatedAt.Time,
			},
			LastReadSeq: r.LastReadSeq,
		}
		if unread := (r.NextSeq - 1) - r.LastReadSeq; unread > 0 {
			sum.Unread = unread
		}
		out = append(out, sum)
	}
	return out, nil
}
