BEGIN;

ALTER TABLE bot_daily_job_runs
    ADD COLUMN IF NOT EXISTS attempt_id TEXT,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP;

UPDATE bot_daily_job_runs
SET attempt_id = COALESCE(attempt_id, 'legacy-' || id::text),
    claimed_at = COALESCE(claimed_at, created_at),
    heartbeat_at = COALESCE(heartbeat_at, claimed_at, created_at)
WHERE attempt_id IS NULL
   OR claimed_at IS NULL
   OR heartbeat_at IS NULL;

ALTER TABLE bot_daily_job_runs
    ALTER COLUMN attempt_id SET NOT NULL,
    ALTER COLUMN claimed_at SET NOT NULL,
    ALTER COLUMN heartbeat_at SET NOT NULL;

CREATE TABLE IF NOT EXISTS bot_notification_deliveries (
    id SERIAL PRIMARY KEY,
    notification_kind TEXT NOT NULL,
    notification_key TEXT NOT NULL,
    chat_id BIGINT NOT NULL,
    message_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'started',
    attempt_id TEXT NOT NULL,
    claimed_at TIMESTAMP NOT NULL,
    delivered_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_bot_notification_deliveries_identity
        UNIQUE (notification_kind, notification_key, chat_id, message_type)
);

CREATE INDEX IF NOT EXISTS ix_bot_notification_deliveries_status_claimed
    ON bot_notification_deliveries (status, claimed_at);

COMMIT;
