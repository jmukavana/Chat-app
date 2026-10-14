package gateway

import (
	"log/slog"
	"sync"
)

// Hub is the in-process registry of live connections on this gateway, indexed
// by user id so a message can be fanned out to every device a user has open.
//
// Phase 1 uses a single RWMutex-guarded map. When connection counts make that
// a contention point, the map can be sharded by a hash of the user id without
// changing this API.
type Hub struct {
	log   *slog.Logger
	mu    sync.RWMutex
	users map[string]map[string]*Connection // userID -> connID -> conn
}

// NewHub builds an empty Hub.
func NewHub(log *slog.Logger) *Hub {
	return &Hub{log: log, users: make(map[string]map[string]*Connection)}
}

// Add registers a connection.
func (h *Hub) Add(c *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	conns := h.users[c.UserID]
	if conns == nil {
		conns = make(map[string]*Connection)
		h.users[c.UserID] = conns
	}
	conns[c.ID] = c
}

// Remove deregisters a connection. Safe to call more than once.
func (h *Hub) Remove(c *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	conns := h.users[c.UserID]
	if conns == nil {
		return
	}
	delete(conns, c.ID)
	if len(conns) == 0 {
		delete(h.users, c.UserID)
	}
}

// SendToUser enqueues payload on every connection the user has open on this
// gateway. Returns the number of connections the frame was queued to.
func (h *Hub) SendToUser(userID string, payload []byte) int {
	h.mu.RLock()
	conns := make([]*Connection, 0, len(h.users[userID]))
	for _, c := range h.users[userID] {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		c.enqueue(payload)
	}
	return len(conns)
}

// SendToUsers enqueues payload for each distinct user id. Returns the total
// number of connections reached.
func (h *Hub) SendToUsers(userIDs []string, payload []byte) int {
	seen := make(map[string]struct{}, len(userIDs))
	total := 0
	for _, id := range userIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		total += h.SendToUser(id, payload)
	}
	return total
}

// ConnectionCount returns the number of live connections on this gateway.
func (h *Hub) ConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, conns := range h.users {
		n += len(conns)
	}
	return n
}
