package gateway

import "chat-platform/internal/chat"

// WebSocket wire protocol.
//
// Every frame is a JSON object with a "type" field. The client generates
// request_id (per WebSocket request) and client_message_id (per user message,
// also the idempotency key). The server owns the message id and seq.
//
// Since Phase 3 the write frames are asynchronous: the gateway publishes a
// command to Kafka and immediately returns an "accepted" frame; the
// authoritative result arrives later as a message.* broadcast or an "error"
// frame carrying the same request_id.
//
// See docs/protocol.md for the full contract.
const (
	// client -> server
	frameMessageSend   = "message.send"
	frameMessageEdit   = "message.edit"
	frameMessageDelete = "message.delete"
	frameMessageRead   = "message.read"
	framePing          = "ping"

	// server -> client
	frameHello          = "hello"
	frameAccepted       = "accepted"
	frameMessageCreated = "message.created"
	frameMessageUpdated = "message.updated"
	frameMessageDeleted = "message.deleted"
	frameReadReceipt    = "message.read"
	framePong           = "pong"
	frameError          = "error"
)

// inboundFrame is the union of every client->server frame. Unused fields for a
// given type stay zero.
//
//	message.send    -> conversation_id, client_message_id, kind, body
//	message.edit    -> conversation_id, seq, body
//	message.delete  -> conversation_id, seq
//	message.read    -> conversation_id, seq
type inboundFrame struct {
	Type            string `json:"type"`
	RequestID       string `json:"request_id"`
	ConversationID  string `json:"conversation_id"`
	ClientMessageID string `json:"client_message_id"`
	Kind            string `json:"kind"`
	Body            string `json:"body"`
	Seq             int64  `json:"seq"`
}

type helloFrame struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connection_id"`
	UserID       string `json:"user_id"`
	GatewayID    string `json:"gateway_id"`
}

// acceptedFrame acknowledges that a write command was published to Kafka. It
// is not a confirmation that the command succeeded.
type acceptedFrame struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	CommandID string `json:"command_id"`
}

// messageEventFrame is the canonical broadcast for message.created /
// message.updated / message.deleted, sent to every member of the conversation
// (including the sender's other connections). For message.deleted the message
// is a tombstone: empty body, deleted_at set.
type messageEventFrame struct {
	Type           string       `json:"type"`
	ConversationID string       `json:"conversation_id"`
	Duplicate      bool         `json:"duplicate,omitempty"`
	Message        chat.Message `json:"message"`
}

type readReceiptFrame struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversation_id"`
	UserID         string `json:"user_id"`
	Seq            int64  `json:"seq"`
}

// errorFrame reports a failure. When it carries a request_id it may be
// synchronous (bad frame) or asynchronous (command.rejected from the chat
// service).
type errorFrame struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}
