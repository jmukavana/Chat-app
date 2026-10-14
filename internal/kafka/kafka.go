// Package kafkax wraps segmentio/kafka-go with the few helpers this project
// needs: a JSON producer keyed for per-conversation ordering, a consumer-group
// reader, and idempotent topic creation.
//
// The package is named kafkax, not kafka, to avoid shadowing the upstream
// github.com/segmentio/kafka-go package (package name kafka).
package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Topic names. Commands flow gateway -> chat service; events flow chat service
// -> every gateway. Both are keyed by conversation_id so a conversation's
// messages always land on one partition and stay ordered.
const (
	TopicCommands = "chat.commands"
	TopicEvents   = "chat.events"
)

// Producer publishes JSON values keyed by a partition key.
type Producer struct {
	w *kafka.Writer
}

// NewProducer builds a synchronous, acks=all producer for one topic.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{w: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // partition by message key
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchTimeout:           10 * time.Millisecond,
	}}
}

// Publish marshals value to JSON and writes it under key. It blocks until the
// broker acknowledges (RequireAll).
func (p *Producer) Publish(ctx context.Context, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal kafka value: %w", err)
	}
	if err := p.w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: b}); err != nil {
		return fmt.Errorf("write to %s: %w", p.w.Topic, err)
	}
	return nil
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error { return p.w.Close() }

// NewReader builds a consumer-group reader that starts at the tail of the topic
// when the group has no committed offset.
//
//   - chat service replicas share one groupID so partitions are distributed
//     and each command is handled once.
//   - each gateway uses a unique groupID so every gateway sees every event and
//     can fan out to whichever users it holds.
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
	})
}

// TopicSpec is a topic to ensure exists.
type TopicSpec struct {
	Name       string
	Partitions int
}

// EnsureTopics creates the given topics (replication factor 1) if they do not
// already exist. Safe to call on every process start.
func EnsureTopics(ctx context.Context, brokers []string, specs ...TopicSpec) error {
	if len(brokers) == 0 {
		return errors.New("kafkax: no brokers configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}
	cc, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer cc.Close()

	configs := make([]kafka.TopicConfig, len(specs))
	for i, s := range specs {
		configs[i] = kafka.TopicConfig{Topic: s.Name, NumPartitions: s.Partitions, ReplicationFactor: 1}
	}
	if err := cc.CreateTopics(configs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}
