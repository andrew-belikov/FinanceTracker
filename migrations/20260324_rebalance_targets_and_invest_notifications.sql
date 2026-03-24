BEGIN;

CREATE TABLE IF NOT EXISTS rebalance_targets (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    target_weight_pct NUMERIC(9, 4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_rebalance_targets_account_class'
    ) THEN
        ALTER TABLE rebalance_targets
            ADD CONSTRAINT uq_rebalance_targets_account_class
            UNIQUE (account_id, asset_class);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS invest_notifications (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_invest_notifications_account_operation'
    ) THEN
        ALTER TABLE invest_notifications
            ADD CONSTRAINT uq_invest_notifications_account_operation
            UNIQUE (account_id, operation_id);
    END IF;
END;
$$;

COMMIT;
