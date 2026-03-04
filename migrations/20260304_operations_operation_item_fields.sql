BEGIN;

ALTER TABLE public.operations
    ADD COLUMN IF NOT EXISTS cursor TEXT,
    ADD COLUMN IF NOT EXISTS broker_account_id TEXT,
    ADD COLUMN IF NOT EXISTS parent_operation_id TEXT,
    ADD COLUMN IF NOT EXISTS name TEXT,
    ADD COLUMN IF NOT EXISTS state TEXT,
    ADD COLUMN IF NOT EXISTS instrument_type TEXT,
    ADD COLUMN IF NOT EXISTS instrument_kind TEXT,
    ADD COLUMN IF NOT EXISTS position_uid TEXT,
    ADD COLUMN IF NOT EXISTS asset_uid TEXT,
    ADD COLUMN IF NOT EXISTS price NUMERIC,
    ADD COLUMN IF NOT EXISTS commission NUMERIC,
    ADD COLUMN IF NOT EXISTS yield NUMERIC,
    ADD COLUMN IF NOT EXISTS yield_relative NUMERIC,
    ADD COLUMN IF NOT EXISTS accrued_int NUMERIC,
    ADD COLUMN IF NOT EXISTS quantity BIGINT,
    ADD COLUMN IF NOT EXISTS quantity_rest BIGINT,
    ADD COLUMN IF NOT EXISTS quantity_done BIGINT,
    ADD COLUMN IF NOT EXISTS cancel_date_time TIMESTAMP WITHOUT TIME ZONE,
    ADD COLUMN IF NOT EXISTS cancel_reason TEXT;

-- Для идемпотентного upsert по идентификатору операции.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_operations_operation_id'
    ) THEN
        ALTER TABLE public.operations
            ADD CONSTRAINT uq_operations_operation_id UNIQUE (operation_id);
    END IF;
END;
$$;

COMMIT;
