-- name: InsertOutbox :exec
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
VALUES (gen_random_uuid(), $1, $2, $3, $4);
