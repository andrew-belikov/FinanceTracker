BEGIN;

-- 1) Единая таблица операций.
CREATE TABLE IF NOT EXISTS operations (
    id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation_type TEXT NOT NULL DEFAULT 'OPERATION_TYPE_INPUT',
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    currency TEXT NOT NULL,
    description TEXT,
    source TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- 2) Уникальность операций на уровне счёта.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_operations_account_operation'
    ) THEN
        ALTER TABLE operations
            ADD CONSTRAINT uq_operations_account_operation
            UNIQUE (account_id, operation_id);
    END IF;
END;
$$;

-- 3) Сохранить старую таблицу deposits (если она ещё таблица, а не view).
DO $$
BEGIN
    IF to_regclass('public.deposits') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'public'
             AND c.relname = 'deposits'
             AND c.relkind = 'r'
       )
       AND to_regclass('public.deposits_legacy') IS NULL THEN
        ALTER TABLE public.deposits RENAME TO deposits_legacy;
    END IF;
END;
$$;

-- 4) Совместимость со старым SQL-кодом: deposits теперь view.
-- Без прямого backfill из deposits_legacy: tracker заполняет operations из API-истории.
DROP VIEW IF EXISTS public.deposits;

DO $$
BEGIN
    IF to_regclass('public.deposits_legacy') IS NOT NULL THEN
        EXECUTE $view$
            CREATE VIEW public.deposits AS
            SELECT
                o.id,
                o.account_id,
                o.operation_id,
                o.date,
                o.amount,
                o.currency,
                o.description,
                o.source,
                o.created_at
            FROM public.operations o
            WHERE o.operation_type = 'OPERATION_TYPE_INPUT'
            UNION ALL
            SELECT
                d.id,
                d.account_id,
                d.operation_id,
                d.date,
                d.amount,
                d.currency,
                d.description,
                d.source,
                d.created_at
            FROM public.deposits_legacy d
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.operations o
                WHERE o.account_id = d.account_id
                  AND o.operation_id = d.operation_id
                  AND o.operation_type = 'OPERATION_TYPE_INPUT'
            )
        $view$;
    ELSE
        EXECUTE $view$
            CREATE VIEW public.deposits AS
            SELECT
                o.id,
                o.account_id,
                o.operation_id,
                o.date,
                o.amount,
                o.currency,
                o.description,
                o.source,
                o.created_at
            FROM public.operations o
            WHERE o.operation_type = 'OPERATION_TYPE_INPUT'
        $view$;
    END IF;
END;
$$;

COMMIT;
