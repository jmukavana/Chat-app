// Package chatworker applies commands consumed from chat.commands and publishes
// the resulting events to chat.events. It is the glue between Kafka and the
// transport-agnostic chat domain in internal/chat, and runs inside cmd/chat.
package chatworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"chat-platform/internal/chat"
	"chat-platform/internal/command"
	"chat-platform/internal/events"
)

// Publisher writes an event keyed by a partition key (the conversation id).
type Publisher interface {
	Publish(ctx context.Context, key string, value any) error
}

// Worker handles one command at a time.
type Worker struct {
	svc *chat.Service
	pub Publisher
	log *slog.Logger
}

// New builds a Worker.
func New(svc *chat.Service, pub Publisher, log *slog.Logger) *Worker {
	return &Worker{svc: svc, pub: pub, log: log}
}

// Handle decodes and applies one command. It returns an error only for
// transient failures the caller should retry; domain rejections are published
// as command.rejected events and return nil.
func (w *Worker) Handle(ctx context.Context, raw []byte) error {
	var cmd command.Command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		w.log.Error("dropping unparseable command", "err", err)
		return nil // poison message: never retryable
	}

	switch cmd.Type {
	case command.SendMessage:
		return w.handleSend(ctx, cmd)
	case command.EditMessage:
		return w.handleEdit(ctx, cmd)
	case command.DeleteMessage:
		return w.handleDelete(ctx, cmd)
	case command.MarkRead:
		return w.handleRead(ctx, cmd)
	default:
		w.log.Warn("dropping unknown command type", "type", cmd.Type, "command_id", cmd.CommandID)
		return nil
	}
}

func (w *Worker) handleSend(ctx context.Context, cmd command.Command) error {
	res, err := w.svc.SendMessage(ctx, chat.SendMessageParams{
		ConversationID:  cmd.ConversationID,
		SenderID:        cmd.UserID,
		ClientMessageID: cmd.ClientMessageID,
		Kind:            cmd.MessageKind,
		Body:            cmd.Body,
	})
	if err != nil {
		return w.settle(ctx, cmd, err)
	}

	recipients := []string{cmd.UserID}
	if !res.Duplicate {
		recipients, err = w.members(ctx, cmd.ConversationID)
		if err != nil {
			return err
		}
	}
	msg := res.Message
	return w.pub.Publish(ctx, cmd.ConversationID, events.Event{
		Type:           events.MessageCreated,
		ConversationID: cmd.ConversationID,
		RequestID:      cmd.RequestID,
		OriginUserID:   cmd.UserID,
		OriginGateway:  cmd.GatewayID,
		Recipients:     recipients,
		Message:        &msg,
		Duplicate:      res.Duplicate,
	})
}

func (w *Worker) handleEdit(ctx context.Context, cmd command.Command) error {
	msg, err := w.svc.EditMessage(ctx, chat.EditMessageParams{
		ConversationID: cmd.ConversationID,
		EditorID:       cmd.UserID,
		Seq:            cmd.Seq,
		Body:           cmd.Body,
	})
	if err != nil {
		return w.settle(ctx, cmd, err)
	}
	return w.broadcast(ctx, cmd, events.MessageUpdated, &msg, 0)
}

func (w *Worker) handleDelete(ctx context.Context, cmd command.Command) error {
	msg, err := w.svc.DeleteMessage(ctx, chat.DeleteMessageParams{
		ConversationID: cmd.ConversationID,
		RequesterID:    cmd.UserID,
		Seq:            cmd.Seq,
	})
	if err != nil {
		return w.settle(ctx, cmd, err)
	}
	return w.broadcast(ctx, cmd, events.MessageDeleted, &msg, 0)
}

func (w *Worker) handleRead(ctx context.Context, cmd command.Command) error {
	lastRead, err := w.svc.MarkRead(ctx, cmd.ConversationID, cmd.UserID, cmd.Seq)
	if err != nil {
		return w.settle(ctx, cmd, err)
	}
	return w.broadcast(ctx, cmd, events.MessageRead, nil, lastRead)
}

// broadcast publishes an event to the whole conversation membership.
func (w *Worker) broadcast(ctx context.Context, cmd command.Command, typ string, msg *chat.Message, seq int64) error {
	recipients, err := w.members(ctx, cmd.ConversationID)
	if err != nil {
		return err
	}
	return w.pub.Publish(ctx, cmd.ConversationID, events.Event{
		Type:           typ,
		ConversationID: cmd.ConversationID,
		RequestID:      cmd.RequestID,
		OriginUserID:   cmd.UserID,
		OriginGateway:  cmd.GatewayID,
		Recipients:     recipients,
		Message:        msg,
		Seq:            seq,
	})
}

// settle turns a domain error into a rejection event (returning nil), or
// propagates a transient error for retry.
func (w *Worker) settle(ctx context.Context, cmd command.Command, cause error) error {
	if !chat.IsTerminal(cause) {
		return fmt.Errorf("apply %s: %w", cmd.Type, cause)
	}
	w.log.Info("command rejected", "type", cmd.Type, "command_id", cmd.CommandID, "err", cause)
	return w.pub.Publish(ctx, cmd.ConversationID, events.Event{
		Type:           events.CommandRejected,
		ConversationID: cmd.ConversationID,
		RequestID:      cmd.RequestID,
		OriginUserID:   cmd.UserID,
		OriginGateway:  cmd.GatewayID,
		Recipients:     []string{cmd.UserID},
		Error:          &events.Error{Code: chat.ErrorCode(cause), Message: cause.Error()},
	})
}

func (w *Worker) members(ctx context.Context, conversationID string) ([]string, error) {
	ids, err := w.svc.ListMemberIDs(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	if len(ids) == 0 {
		// Should not happen for a persisted message, but never publish an
		// event nobody will receive without saying so.
		return nil, errors.New("conversation has no members")
	}
	return ids, nil
}
