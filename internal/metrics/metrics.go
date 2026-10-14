// Package metrics declares the Prometheus collectors exposed at /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// WSConnections tracks currently open WebSocket connections.
	WSConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chat_ws_connections",
		Help: "Number of open WebSocket connections on this gateway.",
	})

	// CommandsPublished counts write commands published to chat.commands.
	CommandsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_commands_published_total",
		Help: "Write commands published to the chat.commands topic.",
	})

	// EventsDelivered counts chat.events frames handed to the connection hub.
	EventsDelivered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_events_delivered_total",
		Help: "chat.events frames dispatched to local connections.",
	})

	// FramesDropped counts outbound frames dropped because a connection's
	// send buffer was full (slow consumer).
	FramesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_ws_frames_dropped_total",
		Help: "Outbound WebSocket frames dropped due to a full send buffer.",
	})
)
