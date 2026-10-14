// Command fanout will be the standalone fanout service introduced in Phase 5:
// it consumes message.created events from the chat.events Kafka topic, resolves
// which users are online via the Redis session registry, and routes events to
// the gateways holding those connections. Until then the gateway fans out
// in-process and this binary is an intentional no-op.
package main

import (
	"log/slog"
	"os"
)

func main() {
	slog.New(slog.NewJSONHandler(os.Stdout, nil)).
		Warn("cmd/fanout is a Phase 5 placeholder; run cmd/gateway for Phase 1")
}
