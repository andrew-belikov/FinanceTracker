BEGIN;

CREATE TABLE IF NOT EXISTS payout_calendar_events (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    figi TEXT NOT NULL,
    instrument_uid TEXT NULL,
    ticker TEXT NULL,
    name TEXT NULL,
    instrument_type TEXT NULL,
    event_type TEXT NOT NULL,
    event_uid TEXT NOT NULL,
    payment_date DATE NOT NULL,
    record_date DATE NULL,
    last_buy_date DATE NULL,
    amount_per_unit NUMERIC(18, 9) NULL,
    quantity NUMERIC(18, 6) NOT NULL,
    expected_amount NUMERIC(18, 2) NULL,
    currency TEXT NULL,
    source_event_type TEXT NULL,
    fetched_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_payout_calendar_event_source'
    ) THEN
        ALTER TABLE payout_calendar_events
            ADD CONSTRAINT uq_payout_calendar_event_source
            UNIQUE (account_id, figi, event_type, event_uid);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_payout_calendar_events_payment_date
    ON payout_calendar_events (payment_date);

CREATE INDEX IF NOT EXISTS ix_payout_calendar_events_account_payment
    ON payout_calendar_events (account_id, payment_date);

COMMIT;
