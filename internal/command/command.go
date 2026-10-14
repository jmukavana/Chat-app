// Package command defines the envelope the gateway publishes to chat.commands.
//
// The gateway is the only producer of commands, so the chat service trusts
// UserID: it is the authenticated subject from the caller's JWT, not something
// the client sent.
package command

// Type is the command discriminator; the values match the client WebSocket
// frame "type" fields.
type Type string

const (
	SendMessage   Type = "message.send"
	EditMessage   Type = "message.edit"
	DeleteMessage Type = "message.delete"
	MarkRead      Type = "message.read"
)

// Command is one action to apply to a conversation. Fields not relevant to a
// given Type are left zero.
type Command struct {
	Type      Type   `json:"type"`
	CommandID string `json:"command_id"` // uuid, for tracing
	GatewayID string `json:"gateway_id"` // origin gateway

	ConversationID string `json:"conversation_id"`
	UserID         string `json:"user_id"`             // authenticated caller
	RequestID      string `json:"request_id,omitempty"` // client's per-request id, echoed in events

	ClientMessageID string `json:"client_message_id,omitempty"` // send: idempotency key
	MessageKind     string `json:"message_kind,omitempty"`      // send: "text" (default) etc
	Body            string `json:"body,omitempty"`              // send / edit
	Seq             int64  `json:"seq,omitempty"`              // edit / delete / read
}
