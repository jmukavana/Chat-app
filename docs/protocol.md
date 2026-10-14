# WebSocket protocol

This is the contract between clients (web, Android, iOS) and the gateway.

## Transport

- Endpoint: `GET /ws`
- Auth: `Authorization: Bearer <jwt>`, or `?access_token=<jwt>` for browsers
  that cannot set handshake headers.
- Every frame is a single JSON object with a `type` field. Text frames only.
- The server sends WebSocket pings on `HEARTBEAT_INTERVAL`; clients must let the
  library answer with pong. Clients may also send `{"type":"ping"}` and get
  `{"type":"pong"}`.

## Write model (async since Phase 3)

`message.send` / `message.edit` / `message.delete` / `message.read` are
**commands**. The gateway publishes them to Kafka and immediately replies with
`{"type":"accepted","request_id":"…","command_id":"…"}` — this only means the
command was queued. The chat service applies it and the authoritative outcome
comes back as a broadcast (`message.created` / `message.updated` /
`message.deleted` / `message.read`) or an `error` frame carrying the same
`request_id`. Order and correlation:

```
client --message.send--> gateway --> Kafka(chat.commands) --> chat service
client <--accepted------- gateway                                  |
                                                                   v
client <--message.created-- gateway <-- Kafka(chat.events) <--------+
```

## Identifiers

| Field               | Owner  | Purpose                                                    |
| ------------------- | ------ | --------------------------------------------------------- |
| `request_id`        | client | correlate a command with its `accepted` / broadcast / `error` |
| `client_message_id` | client | idempotency key for a user message (uuid)                  |
| `message.id`        | server | identity of the persisted message (uuid)                   |
| `message.seq`       | server | per-conversation ordering; strictly increasing            |

Never collapse these into one id.

## Client → server

### `message.send`

```json
{
  "type": "message.send",
  "request_id": "req-123",
  "conversation_id": "8f9b...",
  "client_message_id": "01J9...",
  "kind": "text",
  "body": "Hello"
}
```

`kind` defaults to `text`. Retrying with the same `client_message_id` is safe:
the chat service resolves it to the original message (a `message.created` with
`"duplicate": true`, delivered only to the sender) and does not create a new
one.

### `message.edit`

```json
{
  "type": "message.edit",
  "request_id": "req-124",
  "conversation_id": "8f9b...",
  "seq": 42,
  "body": "Hello (edited)"
}
```

Only the original sender may edit, only `text` messages, and not after delete.
On success the chat service broadcasts `message.updated`; on failure an `error`
frame with the same `request_id`.

### `message.delete`

```json
{ "type": "message.delete", "request_id": "req-125", "conversation_id": "8f9b...", "seq": 42 }
```

Only the original sender may delete. Soft delete: the row stays at its `seq`
with an empty `body` and `deleted_at` set. Deleting twice is a no-op. On
success the chat service broadcasts `message.deleted`.

### `message.read`

```json
{ "type": "message.read", "conversation_id": "8f9b...", "seq": 42 }
```

Advances the caller's read cursor to `seq`. Never moves it backwards.

### `ping`

```json
{ "type": "ping" }
```

## Server → client

### `hello` (on connect)

```json
{ "type": "hello", "connection_id": "…", "user_id": "…", "gateway_id": "gateway-local" }
```

### `accepted` (to the sender only, synchronous)

```json
{ "type": "accepted", "request_id": "req-123", "command_id": "…" }
```

The command was published. Not a success confirmation.

### `message.created` / `message.updated` / `message.deleted` (to every member, including the sender's other devices)

```json
{ "type": "message.created", "conversation_id": "…", "message": { … } }
{ "type": "message.updated", "conversation_id": "…", "message": { …, "edited_at": "…" } }
{ "type": "message.deleted", "conversation_id": "…", "message": { …, "body": "", "deleted_at": "…" } }
```

`message.created` also carries `"duplicate": true` when it resolved an
idempotent retry (sent to the origin sender only). The sending device
reconciles its optimistic row by `client_message_id` (for `created`) or `seq`
(for `updated` / `deleted`).

### `message.read` (read receipt, to every member)

```json
{ "type": "message.read", "conversation_id": "…", "user_id": "…", "seq": 42 }
```

### `error`

```json
{ "type": "error", "request_id": "req-123", "code": "forbidden", "message": "…" }
```

| `code`       | meaning                                  |
| ------------ | ---------------------------------------- |
| `bad_frame`  | frame was not valid JSON                 |
| `unknown_type` | unsupported `type`                     |
| `validation` | field failed validation                  |
| `forbidden`  | caller is not a member of the conversation |
| `not_found`  | conversation does not exist              |
| `internal`   | server error                            |

## Delivery semantics

At-least-once transport + idempotent writes + per-conversation `seq`. Clients
deduplicate by `message.id` and detect gaps by `seq`. On reconnect, call
`GET /v1/conversations/{id}/messages?after_seq=<last seq seen>` to catch up.

## HTTP sync API

| Method & path                                        | Purpose                    |
| ---------------------------------------------------- | -------------------------- |
| `POST   /v1/conversations`                           | create a conversation      |
| `GET    /v1/conversations`                           | list mine + unread counts  |
| `GET    /v1/conversations/{id}/messages?after_seq=&before_seq=&limit=` | reconnect sync / scroll-back |
| `PATCH  /v1/conversations/{id}/messages/{seq}`       | edit a message (`{"body":"…"}`) |
| `DELETE /v1/conversations/{id}/messages/{seq}`       | soft-delete a message      |
| `POST   /v1/conversations/{id}/read`                 | advance read cursor        |
| `POST   /v1/dev/token`  (DEV_MODE only)              | mint a JWT for testing     |

The `PATCH` / `DELETE` endpoints also broadcast `message.updated` /
`message.deleted` to members connected to this gateway.

## Reserved (later phases)

These frame types are named now so clients can plan for them; they are not yet
implemented: `typing.start`, `typing.stop`, `presence.changed`,
`conversation.created`.
