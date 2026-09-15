-- Align legacy ORM-created columns with the explicit PostgreSQL schema manifest.
-- VARCHAR -> TEXT and NUMERIC -> NUMERIC(p, s) preserve existing values.
BEGIN;

ALTER TABLE public.instruments
    ALTER COLUMN figi TYPE TEXT,
    ALTER COLUMN ticker TYPE TEXT,
    ALTER COLUMN name TYPE TEXT,
    ALTER COLUMN class_code TYPE TEXT,
    ALTER COLUMN instrument_type TYPE TEXT;

ALTER TABLE public.portfolio_snapshots
    ALTER COLUMN account_id TYPE TEXT,
    ALTER COLUMN currency TYPE TEXT,
    ALTER COLUMN account_name TYPE TEXT;

ALTER TABLE public.portfolio_positions
    ALTER COLUMN figi TYPE TEXT,
    ALTER COLUMN ticker TYPE TEXT,
    ALTER COLUMN name TYPE TEXT,
    ALTER COLUMN instrument_type TYPE TEXT,
    ALTER COLUMN currency TYPE TEXT;

ALTER TABLE public.asset_aliases
    ALTER COLUMN id TYPE BIGINT,
    ALTER COLUMN asset_uid TYPE TEXT,
    ALTER COLUMN instrument_uid TYPE TEXT,
    ALTER COLUMN figi TYPE TEXT,
    ALTER COLUMN ticker TYPE TEXT,
    ALTER COLUMN name TYPE TEXT;
ALTER SEQUENCE IF EXISTS public.asset_aliases_id_seq AS BIGINT;

ALTER TABLE public.rebalance_targets
    ALTER COLUMN account_id TYPE TEXT,
    ALTER COLUMN asset_class TYPE TEXT;

ALTER TABLE public.invest_notifications
    ALTER COLUMN account_id TYPE TEXT,
    ALTER COLUMN operation_id TYPE TEXT;

ALTER TABLE public.payout_calendar_events
    ALTER COLUMN account_id TYPE TEXT,
    ALTER COLUMN figi TYPE TEXT,
    ALTER COLUMN event_type TYPE TEXT,
    ALTER COLUMN event_uid TYPE TEXT,
    ALTER COLUMN instrument_uid TYPE TEXT,
    ALTER COLUMN ticker TYPE TEXT,
    ALTER COLUMN name TYPE TEXT,
    ALTER COLUMN instrument_type TYPE TEXT,
    ALTER COLUMN currency TYPE TEXT,
    ALTER COLUMN source_event_type TYPE TEXT;

COMMIT;
