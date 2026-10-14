-- name: InsertConversation :one
INSERT INTO conversations (id, type)
VALUES (gen_random_uuid(), $1)
RETURNING *;

-- name: GetConversation :one
SELECT * FROM conversations
WHERE id = $1;

-- name: LockConversationForUpdate :one
SELECT next_seq FROM conversations
WHERE id = $1
FOR UPDATE;

-- name: AdvanceConversationSeq :exec
UPDATE conversations
SET next_seq = next_seq + 1
WHERE id = $1;

-- name: ListConversationsForUser :many
SELECT c.id, c.type, c.next_seq, c.created_at, m.last_read_seq
FROM conversations c
JOIN conversation_members m ON m.conversation_id = c.id
WHERE m.user_id = $1
ORDER BY c.created_at DESC;
