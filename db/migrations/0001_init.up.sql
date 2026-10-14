-- 0001_init — conversations, membership, messages, transactional outbox.
--
-- File names follow the golang-migrate convention (NNNN_name.up.sql /
-- .down.sql) so `migrate` can run them, but they are plain SQL and also work
-- with `psql -f`. Written idempotently (IF NOT EXISTS) so `make up` can run it
-- unconditionally.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- gen_random_uuid() on PG < 13

-- A chat thread. next_seq is the sequence number the *next* message in this
-- conversation will receive; it is advanced under a row lock so sequence
-- numbers are strictly increasing and gap-free.
CREATE TABLE IF NOT EXISTS conversations (
    id         UUID PRIMARY KEY,
    type       VARCHAR(20)  NOT NULL CHECK (type IN ('direct', 'group')),
    next_seq   BIGINT       NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Membership plus each member's read cursor. last_read_seq only ever moves
-- forward (enforced in application code with GREATEST).
CREATE TABLE IF NOT EXISTS conversation_members (
    conversation_id UUID        NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    user_id         UUID        NOT NULL,
    role            VARCHAR(30) NOT NULL DEFAULT 'member',
    last_read_seq   BIGINT      NOT NULL DEFAULT 0,
    joined_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (conversation_id, user_id)
);

CREATE INDEX IF NOT EXISTS conversation_members_user_idx
    ON conversation_members (user_id);

-- One message. The PK (conversation_id, seq) is also the primary read path
-- (history queries filter by conversation and range over seq). The
-- (conversation_id, client_message_id) unique constraint is what makes
-- message.send idempotent under client retries.
CREATE TABLE IF NOT EXISTS messages (
    id                UUID        NOT NULL,
    conversation_id   UUID        NOT NULL REFERENCES conversations (id) ON DELETE CASCADE,
    seq               BIGINT      NOT NULL,
    sender_id         UUID        NOT NULL,
    client_message_id UUID        NOT NULL,
    kind              VARCHAR(30) NOT NULL DEFAULT 'text',
    body              TEXT        NOT NULL DEFAULT '',
    media_key         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    edited_at         TIMESTAMPTZ,
    deleted_at        TIMESTAMPTZ,
    PRIMARY KEY (conversation_id, seq),
    UNIQUE (id),
    UNIQUE (conversation_id, client_message_id)
);

-- Transactional outbox. message.send writes a row here in the same
-- transaction as the message insert. Phase 4 adds a publisher that drains
-- unpublished rows onto Kafka; Phase 1 simply never reads this table.
CREATE TABLE IF NOT EXISTS outbox (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(50)  NOT NULL,
    aggregate_id   UUID         NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    payload        JSONB        NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;

COMMIT;
