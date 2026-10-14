// Command gateway is the WebSocket + HTTP edge. It serves clients, publishes
// write commands to the chat.commands Kafka topic, and consumes chat.events to
// fan results out to the connections it holds. Reads (conversation list,
// history) still hit PostgreSQL directly. Presence/session state is in Redis.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"chat-platform/internal/chat"
	"chat-platform/internal/config"
	"chat-platform/internal/gateway"
	kafkax "chat-platform/internal/kafka"
	"chat-platform/internal/postgres"
	redisx "chat-platform/internal/redis"
	"chat-platform/internal/session"
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

	rdb, err := redisx.NewClient(ctx, redisx.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	log.Info("connected to redis")

	if err := kafkax.EnsureTopics(ctx, cfg.KafkaBrokers,
		kafkax.TopicSpec{Name: kafkax.TopicCommands, Partitions: cfg.KafkaPartitions},
		kafkax.TopicSpec{Name: kafkax.TopicEvents, Partitions: cfg.KafkaPartitions},
	); err != nil {
		return err
	}
	commands := kafkax.NewProducer(cfg.KafkaBrokers, kafkax.TopicCommands)
	defer func() { _ = commands.Close() }()

	// A unique group per process: every gateway must see every event so it can
	// deliver to whichever users it holds.
	eventGroup := "gateway-" + cfg.GatewayID + "-" + uuid.NewString()
	eventsReader := kafkax.NewReader(cfg.KafkaBrokers, kafkax.TopicEvents, eventGroup)
	defer func() { _ = eventsReader.Close() }()
	log.Info("connected to kafka", "brokers", cfg.KafkaBrokers, "event_group", eventGroup)

	chatSvc := chat.NewService(pool, log)
	hub := gateway.NewHub(log)
	sessions := session.NewRegistry(rdb, cfg.SessionTTL, cfg.GatewayID)

	gw := gateway.New(hub, chatSvc, sessions, commands, cfg, log)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gw.ConsumeEvents(ctx, eventsReader)
	}()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", gw.Health)
	r.Handle("/metrics", promhttp.Handler())

	if cfg.DevMode {
		r.Post("/v1/dev/token", gw.DevToken)
		log.Warn("DEV_MODE enabled: POST /v1/dev/token will mint credentials")
	}

	r.Group(func(pr chi.Router) {
		pr.Use(gw.Authenticate)

		pr.Get("/ws", gw.HandleWS)

		pr.Route("/v1/conversations", func(cr chi.Router) {
			cr.Post("/", gw.CreateConversation)
			cr.Get("/", gw.ListConversations)
			cr.Get("/{conversationID}/messages", gw.ListMessages)
			cr.Patch("/{conversationID}/messages/{seq}", gw.EditMessage)
			cr.Delete("/{conversationID}/messages/{seq}", gw.DeleteMessage)
			cr.Post("/{conversationID}/read", gw.MarkRead)
		})
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: it would kill long-lived WebSocket connections.
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr, "gateway_id", cfg.GatewayID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	stop() // stop catching signals; unblock ConsumeEvents via ctx
	_ = eventsReader.Close()
	wg.Wait()
	return err
}
