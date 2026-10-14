-- name: InsertMember :exec
INSERT INTO conversation_members (conversation_id, user_id, role)
VALUES ($1, $2, $3);

-- name: ListMemberIDs :many
SELECT user_id FROM conversation_members
WHERE conversation_id = $1;

-- name: IsConversationMember :one
SELECT EXISTS (
    SELECT 1 FROM conversation_members
    WHERE conversation_id = $1 AND user_id = $2
);

-- name: MarkRead :one
UPDATE conversation_members
SET last_read_seq = GREATEST(last_read_seq, @seq::bigint)
WHERE conversation_id = @conversation_id AND user_id = @user_id
RETURNING last_read_seq;
