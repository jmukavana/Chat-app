# chat-platform

A Go chat backend built in phases. **Phases 1–3 are implemented.** Two services:

- **`cmd/gateway`** — WebSocket edge + HTTP read API. Publishes write commands to
  Kafka `chat.commands`; consumes `chat.events` and fans results out to the
  connections it holds. Reads (conversation list, history) hit PostgreSQL
  directly. Session/presence in Redis.
- **`cmd/chat`** — consumes `chat.commands`, applies each via the `internal/chat`
  domain logic against PostgreSQL, publishes results to `chat.events`. Scale
  horizontally: replicas share the `chat-service` consumer group and a
  conversation's commands (keyed by `conversation_id`) stay ordered on one
  partition.

Writes are asynchronous: the client gets an immediate `accepted` frame, then the
authoritative `message.*` broadcast or an `error` frame with the same
`request_id`.

## What Phase 1 gives you

- WebSocket gateway with a connection hub (multi-device per user), buffered
  writes, server pings, heartbeat, and disconnect cleanup.
- JWT auth on both `/ws` and the REST API.
- Conversations, membership, and messages in PostgreSQL.
- **Server-assigned, gap-free sequence numbers** per conversation
  (`SELECT ... FOR UPDATE` on the conversation row).
- **Idempotent sends** via a `(conversation_id, client_message_id)` unique
  constraint — retries return the original message.
- **Transactional outbox** row written in the same transaction as each message
  (the Phase 4 publisher will drain it to Kafka; today it is written, not read).
- Reconnect sync + scroll-back history API (`after_seq` / `before_seq`).
- **Edit / delete** (`message.edit` / `message.delete` frames, `PATCH` /
  `DELETE` REST): sender-only, text-only edits, soft delete with tombstones,
  idempotent re-delete, `message.updated` / `message.deleted` broadcasts.
- Redis session registry + presence with TTL/heartbeat (no cleanup job).
- Read receipts (`last_read_seq`, monotonic) and cheap unread counts.
- Prometheus metrics at `/metrics`, structured `slog` logs, graceful shutdown.

## Layout

```
cmd/gateway      WebSocket + HTTP edge; command producer + event consumer
cmd/chat         chat service: command consumer -> domain logic -> event producer
cmd/fanout       Phase 5 placeholder (smart event router)
internal/config  env configuration
internal/auth    JWT verify + issue
internal/postgres pgx pool
internal/redis   go-redis client (package redisx)
internal/session Redis session/presence registry
internal/chat    conversations, members, messages, sequencing, idempotency, outbox, edit/delete
internal/chat/chatdb  sqlc-generated queries (do not edit; `make sqlc`)
internal/command chat.commands envelope (gateway -> chat service)
internal/events  chat.events envelope (chat service -> gateways)
internal/kafka   segmentio/kafka-go wrappers (package kafkax): producer, reader, EnsureTopics
internal/chatworker  applies commands, emits events (runs in cmd/chat)
internal/gateway WebSocket + HTTP handlers, connection hub, wire protocol
internal/metrics Prometheus collectors
db/migrations    0001_init.up.sql / .down.sql
db/queries       *.sql — sqlc source
sqlc.yaml        sqlc config
docs/protocol.md the client contract
```

## Run it

```bash
cp .env.example .env      # required — both compose and the services read it

# 1. infra: Postgres (auto-applies 0001_init on first boot), Redis, Kafka (KRaft)
make up

# if you started Postgres some other way, apply the migration yourself:
# make migrate

# 2. the two services (separate terminals)
make run-chat    # or: go run ./cmd/chat
make run         # or: go run ./cmd/gateway
```

### Ports

`.env` drives the host ports for both compose and the services, and ships them
shifted off the defaults (`POSTGRES_PORT=5433`, `REDIS_PORT=6380`,
`KAFKA_PORT=29092`) so the stack coexists with anything already bound to
5432 / 6379 / 9092. If one of the shifted ports is also taken, change the
`*_PORT` and its matching URL (`DATABASE_URL` / `REDIS_ADDR` / `KAFKA_BROKERS`)
together.

`make up` passes `--project-directory .` so `docker compose` reads the repo-root
`.env`. If you invoke `docker compose` / `podman compose` directly, run it from
the repo root (or pass `--project-directory .`) so it does the same.

### Smoke test (needs both services running, DEV_MODE=true)

```bash
go run ./scripts/smoke
```

It mints two dev tokens, creates a conversation, connects both over WebSocket,
and drives send / idempotent-retry / edit / delete — showing the `accepted`
frame followed by the async `message.*` broadcasts round-tripping through Kafka.

## Tests

```bash
make test              # unit tests, no infra
make up && make test-integration   # sequencing, idempotency, edit/delete, worker events vs real Postgres
```

## sqlc

SQL lives in `db/queries/*.sql`. `make sqlc` runs `sqlc generate` (v1.31+) and
rewrites `internal/chat/chatdb/` — generated, checked in, never hand-edited.
The `internal/chat` layer adds transactions, validation, domain types and the
outbox writes on top of the generated `Queries`.

## Known gaps (closed in later phases)

- **Dual write.** The chat service commits to Postgres and then publishes to
  `chat.events` in two steps; a crash between them loses the event. The message
  still persists and its outbox row is written — Phase 4 adds the publisher that
  drains the outbox, making event emission exactly-once-ish and deleting this
  gap.
- **Every gateway consumes every event** (unique consumer group per process)
  and filters by recipient. Phase 5's fanout service does targeted routing via
  the Redis session registry instead.
- Command retry in `cmd/chat` is bounded (3 attempts) and then commits to avoid
  a stuck partition — no dead-letter topic yet.

## Phase plan

| Phase | Adds                                                            |
| ----- | ------------------------------------------------------------- |
| 1 ✅  | single server: WebSocket + Postgres + Redis                   |
| 2 ✅  | `sqlc`, documented protocol, edit/delete message types        |
| 3 ✅  | Kafka `chat.commands` / `chat.events`, split `cmd/chat`       |
| 4     | outbox publisher → Kafka (removes the chat-service dual write) |
| 5     | split `cmd/fanout`, gateway↔fanout gRPC                       |
| 6     | presence events, typing indicators                            |
| 7     | media service + object storage                                |
