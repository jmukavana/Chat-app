// Package fanout is a Phase 5 placeholder.
//
// It will consume message.created events from the chat.events Kafka topic,
// resolve which members are online via the Redis session registry, and route
// each event to the gateways that hold those connections. In Phase 1 the
// gateway does this fan-out in-process (see internal/gateway).
package fanout
