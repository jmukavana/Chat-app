package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"chat-platform/internal/chat"
	"chat-platform/internal/command"
	"chat-platform/internal/metrics"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Health is an unauthenticated liveness probe.
func (g *Gateway) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "gateway_id": g.cfg.GatewayID})
}

type createConversationRequest struct {
	Type      string   `json:"type"`
	MemberIDs []string `json:"member_ids"`
}

// CreateConversation handles POST /v1/conversations.
func (g *Gateway) CreateConversation(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())

	var req createConversationRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	conv, err := g.chat.CreateConversation(r.Context(), chat.CreateConversationParams{
		Type:      chat.ConversationType(req.Type),
		CreatorID: claims.UserID(),
		MemberIDs: req.MemberIDs,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, conv)
}

// ListConversations handles GET /v1/conversations.
func (g *Gateway) ListConversations(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())

	list, err := g.chat.ListConversations(r.Context(), claims.UserID())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"conversations": list})
}

// ListMessages handles GET /v1/conversations/{conversationID}/messages.
//
//	?after_seq=N   reconnect sync: messages with seq > N (ascending)
//	?before_seq=N  scroll-back:    messages with seq < N (ascending)
//	?limit=N       1..200, default 100
func (g *Gateway) ListMessages(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())
	conversationID := chi.URLParam(r, "conversationID")

	params := chat.ListMessagesParams{
		ConversationID: conversationID,
		RequesterID:    claims.UserID(),
	}

	q := r.URL.Query()
	if after, ok, err := queryInt64(q, "after_seq"); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "after_seq must be an integer")
		return
	} else if ok {
		params.AfterSeq = &after
	}
	if before, ok, err := queryInt64(q, "before_seq"); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "before_seq must be an integer")
		return
	} else if ok {
		params.BeforeSeq = &before
	}
	if limit, ok, err := queryInt64(q, "limit"); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "limit must be an integer")
		return
	} else if ok {
		params.Limit = int(limit)
	}

	msgs, err := g.chat.ListMessages(r.Context(), params)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"conversation_id": conversationID,
		"messages":        msgs,
	})
}

type editMessageRequest struct {
	Body string `json:"body"`
}

// publishWrite publishes a command derived from a REST mutation and returns
// 202 Accepted. Like the WebSocket write path, the authoritative result is
// applied by the chat service and broadcast on chat.events.
func (g *Gateway) publishWrite(w http.ResponseWriter, r *http.Request, cmd command.Command) {
	cmd.CommandID = uuid.NewString()
	cmd.GatewayID = g.cfg.GatewayID
	if err := g.commands.Publish(r.Context(), cmd.ConversationID, cmd); err != nil {
		g.log.Error("publish command failed", "err", err, "type", cmd.Type)
		writeJSONError(w, http.StatusInternalServerError, "internal", "could not enqueue command")
		return
	}
	metrics.CommandsPublished.Inc()
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "command_id": cmd.CommandID})
}

// EditMessage handles PATCH /v1/conversations/{conversationID}/messages/{seq}.
func (g *Gateway) EditMessage(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())
	seq, err := pathSeq(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "seq must be a positive integer")
		return
	}
	var req editMessageRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	g.publishWrite(w, r, command.Command{
		Type:           command.EditMessage,
		ConversationID: chi.URLParam(r, "conversationID"),
		UserID:         claims.UserID(),
		Seq:            seq,
		Body:           req.Body,
	})
}

// DeleteMessage handles DELETE /v1/conversations/{conversationID}/messages/{seq}.
func (g *Gateway) DeleteMessage(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())
	seq, err := pathSeq(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "seq must be a positive integer")
		return
	}
	g.publishWrite(w, r, command.Command{
		Type:           command.DeleteMessage,
		ConversationID: chi.URLParam(r, "conversationID"),
		UserID:         claims.UserID(),
		Seq:            seq,
	})
}

type markReadRequest struct {
	Seq int64 `json:"seq"`
}

// MarkRead handles POST /v1/conversations/{conversationID}/read.
func (g *Gateway) MarkRead(w http.ResponseWriter, r *http.Request) {
	claims, _ := claimsFrom(r.Context())
	var req markReadRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	g.publishWrite(w, r, command.Command{
		Type:           command.MarkRead,
		ConversationID: chi.URLParam(r, "conversationID"),
		UserID:         claims.UserID(),
		Seq:            req.Seq,
	})
}

type devTokenRequest struct {
	UserID     string `json:"user_id"`
	TTLSeconds int    `json:"ttl_seconds"`
}

// DevToken handles POST /v1/dev/token. It is mounted only when DEV_MODE=true
// and exists purely so there is a way to obtain a credential without a real
// identity provider. If user_id is omitted a fresh uuid is generated.
func (g *Gateway) DevToken(w http.ResponseWriter, r *http.Request) {
	var req devTokenRequest
	if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.UserID == "" {
		req.UserID = uuid.NewString()
	}
	if _, err := uuid.Parse(req.UserID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "user_id must be a uuid")
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	token, err := g.issuer.Mint(req.UserID, ttl)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to mint token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"user_id":    req.UserID,
		"expires_in": int(ttl.Seconds()),
	})
}

// --- helpers ---------------------------------------------------------------

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

func pathSeq(r *http.Request) (int64, error) {
	n, err := strconv.ParseInt(chi.URLParam(r, "seq"), 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("invalid seq")
	}
	return n, nil
}

func queryInt64(q map[string][]string, key string) (int64, bool, error) {
	vs, ok := q[key]
	if !ok || len(vs) == 0 || vs[0] == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(vs[0], 10, 64)
	if err != nil {
		return 0, false, err
	}
	return n, true, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": msg},
	})
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, chat.ErrValidation):
		writeJSONError(w, http.StatusBadRequest, "validation", err.Error())
	case errors.Is(err, chat.ErrNotMember), errors.Is(err, chat.ErrNotSender):
		writeJSONError(w, http.StatusForbidden, "forbidden", err.Error())
	case errors.Is(err, chat.ErrConversationNotFound), errors.Is(err, chat.ErrMessageNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", err.Error())
	default:
		writeJSONError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
