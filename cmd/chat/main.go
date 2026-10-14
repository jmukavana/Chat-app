// Command chat is the chat service: it consumes write commands from the
// chat.commands Kafka topic, applies them through the internal/chat domain
// logic against PostgreSQL, and publishes the resulting events to chat.events.
//
// Run one or more replicas; they share the "chat-service" consumer group so
// commands for a conversation (keyed by conversation_id) are handled in order
// by exactly one replica.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chat-platform/internal/chat"
	"chat-platform/internal/chatworker"
	"chat-platform/internal/config"
	kafkax "chat-platform/internal/kafka"
	"chat-platform/internal/postgres"
)

const (
	commandGroup = "chat-service"
	maxAttempts  = 3
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	log.Info("connected to postgres")

	if err := kafkax.EnsureTopics(ctx, cfg.KafkaBrokers,
		kafkax.TopicSpec{Name: kafkax.TopicCommands, Partitions: cfg.KafkaPartitions},
		kafkax.TopicSpec{Name: kafkax.TopicEvents, Partitions: cfg.KafkaPartitions},
	); err != nil {
		return err
	}

	events := kafkax.NewProducer(cfg.KafkaBrokers, kafkax.TopicEvents)
	defer func() { _ = events.Close() }()

	reader := kafkax.NewReader(cfg.KafkaBrokers, kafkax.TopicCommands, commandGroup)
	defer func() { _ = reader.Close() }()

	worker := chatworker.New(chat.NewService(pool, log), events, log)
	log.Info("chat service started", "brokers", cfg.KafkaBrokers, "group", commandGroup)

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("shutdown signal received")
				return nil
			}
			log.Error("fetch command failed", "err", err)
			continue
		}

		if err := handleWithRetry(ctx, worker, m.Value, log); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error("giving up on command after retries; committing to avoid a stuck partition", "err", err)
		}
		if err := reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			log.Error("commit offset failed", "err", err)
		}
	}
}

// handleWithRetry retries transient failures with linear backoff. Domain
// rejections are not errors here — the worker publishes command.rejected and
// returns nil.
func handleWithRetry(ctx context.Context, w *chatworker.Worker, raw []byte, log *slog.Logger) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = w.Handle(ctx, raw); err == nil {
			return nil
		}
		log.Warn("command handling failed; will retry", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
		}
	}
	return err
}
