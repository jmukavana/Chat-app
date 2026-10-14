// Package config loads runtime configuration from environment variables.
//
// Phase 1 is a single binary (cmd/gateway) that owns the WebSocket layer,
// the HTTP sync API and the chat persistence logic in-process. Later phases
// split these out and add Kafka; the config surface is expected to grow.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved configuration for the gateway process.
type Config struct {
	HTTPAddr string

	DatabaseURL string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	JWTSecret string
	JWTIssuer string

	KafkaBrokers    []string
	KafkaPartitions int

	SessionTTL        time.Duration
	HeartbeatInterval time.Duration
	WriteTimeout      time.Duration
	SendBuffer        int

	GatewayID string
	DevMode   bool
}

// Load reads configuration from the environment, applying defaults suitable
// for local development against the bundled docker-compose stack.
func Load() (Config, error) {
	c := Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		DatabaseURL:       env("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable"),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     env("REDIS_PASSWORD", ""),
		RedisDB:           envInt("REDIS_DB", 0),
		JWTSecret:         env("JWT_SECRET", "dev-secret-change-me"),
		JWTIssuer:         env("JWT_ISSUER", "chat-platform"),
		KafkaBrokers:      envList("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaPartitions:   envInt("KAFKA_PARTITIONS", 12),
		SessionTTL:        envDuration("SESSION_TTL", 30*time.Second),
		HeartbeatInterval: envDuration("HEARTBEAT_INTERVAL", 10*time.Second),
		WriteTimeout:      envDuration("WRITE_TIMEOUT", 10*time.Second),
		SendBuffer:        envInt("SEND_BUFFER", 64),
		GatewayID:         env("GATEWAY_ID", hostname()),
		DevMode:           envBool("DEV_MODE", true),
	}

	if c.JWTSecret == "" {
		return Config{}, fmt.Errorf("JWT_SECRET must not be empty")
	}
	if c.HeartbeatInterval >= c.SessionTTL {
		return Config{}, fmt.Errorf("HEARTBEAT_INTERVAL (%s) must be shorter than SESSION_TTL (%s)", c.HeartbeatInterval, c.SessionTTL)
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envList(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "gateway-local"
	}
	return h
}
