BEGIN;

-- Rollback к legacy-схеме deposits.
-- Ограничение: таблица operations и накопленные в ней данные не удаляются.

DROP VIEW IF EXISTS public.deposits;

DO $$
BEGIN
    IF to_regclass('public.deposits_legacy') IS NOT NULL THEN
        ALTER TABLE public.deposits_legacy RENAME TO deposits;
    ELSE
        CREATE TABLE IF NOT EXISTS public.deposits (
            id BIGSERIAL PRIMARY KEY,
            account_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            amount NUMERIC(18, 2) NOT NULL,
            currency TEXT NOT NULL,
            description TEXT,
            source TEXT,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
        );

        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'uq_deposits_account_operation'
        ) THEN
            ALTER TABLE public.deposits
                ADD CONSTRAINT uq_deposits_account_operation
                UNIQUE (account_id, operation_id);
        END IF;
    END IF;
END;
$$;

COMMIT;
