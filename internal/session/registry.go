// Package session is the Redis-backed registry of live WebSocket connections
// and user presence. Everything here is ephemeral: entries carry a TTL and are
// refreshed by the connection heartbeat, so a gateway crash self-heals when
// its keys expire — no cleanup job required.
package session

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	sessionKeyPrefix  = "chat:session:"  // chat:session:{userID}:{connID} -> JSON, TTL
	presenceKeyPrefix = "chat:presence:" // chat:presence:{userID}         -> "1",  TTL
)

// Entry is the JSON value stored at a session key.
type Entry struct {
	GatewayID    string    `json:"gateway_id"`
	ConnectionID string    `json:"connection_id"`
	UserID       string    `json:"user_id"`
	ConnectedAt  time.Time `json:"connected_at"`
}

// Registry reads and writes session/presence state in Redis.
type Registry struct {
	rdb       *redis.Client
	ttl       time.Duration
	gatewayID string
}

// NewRegistry builds a Registry. ttl is the session/presence expiry; the
// caller's heartbeat must refresh well inside it.
func NewRegistry(rdb *redis.Client, ttl time.Duration, gatewayID string) *Registry {
	return &Registry{rdb: rdb, ttl: ttl, gatewayID: gatewayID}
}

func sessionKey(userID, connID string) string {
	return sessionKeyPrefix + userID + ":" + connID
}

func presenceKey(userID string) string {
	return presenceKeyPrefix + userID
}

// Register records a new connection and marks the user present.
func (r *Registry) Register(ctx context.Context, userID, connID string) error {
	entry := Entry{
		GatewayID:    r.gatewayID,
		ConnectionID: connID,
		UserID:       userID,
		ConnectedAt:  time.Now().UTC(),
	}
	blob, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal session entry: %w", err)
	}
	pipe := r.rdb.TxPipeline()
	pipe.Set(ctx, sessionKey(userID, connID), blob, r.ttl)
	pipe.Set(ctx, presenceKey(userID), "1", r.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("register session: %w", err)
	}
	return nil
}

// Heartbeat extends the TTL on the connection and presence keys.
func (r *Registry) Heartbeat(ctx context.Context, userID, connID string) error {
	pipe := r.rdb.TxPipeline()
	pipe.Expire(ctx, sessionKey(userID, connID), r.ttl)
	pipe.Expire(ctx, presenceKey(userID), r.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("heartbeat session: %w", err)
	}
	return nil
}

// Remove deletes the connection key. Presence is left to expire on its own so
// a user's other connections keep them present without extra bookkeeping.
func (r *Registry) Remove(ctx context.Context, userID, connID string) error {
	if err := r.rdb.Del(ctx, sessionKey(userID, connID)).Err(); err != nil {
		return fmt.Errorf("remove session: %w", err)
	}
	return nil
}

// Present reports whether the user currently has any live presence key.
func (r *Registry) Present(ctx context.Context, userID string) (bool, error) {
	n, err := r.rdb.Exists(ctx, presenceKey(userID)).Result()
	if err != nil {
		return false, fmt.Errorf("presence check: %w", err)
	}
	return n > 0, nil
}

// Sessions returns every live session entry for a user across all gateways.
// Phase 1 has a single gateway so this is mostly informational; Phase 5's
// fanout service uses the same lookup to route events to the right gateway.
func (r *Registry) Sessions(ctx context.Context, userID string) ([]Entry, error) {
	var (
		cursor  uint64
		match   = sessionKeyPrefix + userID + ":*"
		entries []Entry
	)
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, match, 64).Result()
		if err != nil {
			return nil, fmt.Errorf("scan sessions: %w", err)
		}
		for _, k := range keys {
			blob, err := r.rdb.Get(ctx, k).Bytes()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("get session %s: %w", k, err)
			}
			var e Entry
			if err := json.Unmarshal(blob, &e); err != nil {
				return nil, fmt.Errorf("unmarshal session %s: %w", k, err)
			}
			entries = append(entries, e)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return entries, nil
}
