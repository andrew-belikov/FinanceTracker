BEGIN;

ALTER TABLE operations
    ADD COLUMN IF NOT EXISTS cashflow_category TEXT;

CREATE INDEX IF NOT EXISTS ix_operations_cashflow_category
    ON operations (account_id, cashflow_category, date);

COMMIT;
