// Package events defines the envelope the chat service publishes to
// chat.events after applying a command. Every gateway consumes this topic and
// delivers frames to the connections it holds for Recipients.
package events

import "chat-platform/internal/chat"

// Event types.
const (
	MessageCreated  = "message.created"
	MessageUpdated  = "message.updated"
	MessageDeleted  = "message.deleted"
	MessageRead     = "message.read"
	CommandRejected = "command.rejected"
)

// Error is the reason a command was rejected (terminal, not retryable).
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Event is the result of applying one command.
type Event struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	RequestID      string `json:"request_id,omitempty"`
	OriginUserID   string `json:"origin_user_id"`
	OriginGateway  string `json:"origin_gateway"`

	// Recipients are the user ids that should receive a frame for this event.
	// For broadcasts it is the conversation membership; for a rejection or a
	// duplicate send it is just the origin user.
	Recipients []string `json:"recipients"`

	Message   *chat.Message `json:"message,omitempty"`   // created / updated / deleted
	Seq       int64         `json:"seq,omitempty"`       // read: the resulting last_read_seq
	Duplicate bool          `json:"duplicate,omitempty"` // created: resolved via idempotency
	Error     *Error        `json:"error,omitempty"`     // command.rejected
}
