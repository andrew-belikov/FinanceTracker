BEGIN;

ALTER TABLE payout_calendar_events
    ADD COLUMN IF NOT EXISTS coupon_start_date DATE NULL,
    ADD COLUMN IF NOT EXISTS coupon_end_date DATE NULL,
    ADD COLUMN IF NOT EXISTS coupon_period_days INTEGER NULL;

COMMIT;
