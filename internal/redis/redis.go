// Package redisx constructs the go-redis client used for ephemeral state:
// session registry, presence and (later) typing indicators.
//
// The package is named redisx rather than redis to avoid shadowing the
// upstream github.com/redis/go-redis/v9 package, whose package name is redis.
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Options is the subset of connection settings the gateway needs.
type Options struct {
	Addr     string
	Password string
	DB       int
}

// NewClient dials Redis and verifies connectivity with a ping. The caller
// owns the returned client and must Close it on shutdown.
func NewClient(ctx context.Context, opts Options) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         opts.Addr,
		Password:     opts.Password,
		DB:           opts.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}
