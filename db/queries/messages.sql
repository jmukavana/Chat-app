-- name: GetMessageByClientID :one
SELECT * FROM messages
WHERE conversation_id = $1 AND client_message_id = $2;

-- name: GetMessageBySeq :one
SELECT * FROM messages
WHERE conversation_id = $1 AND seq = $2;

-- name: InsertMessage :one
INSERT INTO messages (id, conversation_id, seq, sender_id, client_message_id, kind, body)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: EditMessageBySeq :one
UPDATE messages
SET body = @body, edited_at = now()
WHERE conversation_id = @conversation_id
  AND seq = @seq
  AND deleted_at IS NULL
RETURNING *;

-- name: SoftDeleteMessageBySeq :one
UPDATE messages
SET deleted_at = now(), body = '', media_key = NULL
WHERE conversation_id = @conversation_id
  AND seq = @seq
  AND deleted_at IS NULL
RETURNING *;

-- name: ListMessagesAfter :many
SELECT * FROM messages
WHERE conversation_id = $1 AND seq > $2
ORDER BY seq ASC
LIMIT $3;

-- name: ListMessagesBefore :many
SELECT * FROM messages
WHERE conversation_id = $1 AND seq < $2
ORDER BY seq DESC
LIMIT $3;

-- name: ListMessagesLatest :many
SELECT * FROM messages
WHERE conversation_id = $1
ORDER BY seq DESC
LIMIT $2;
