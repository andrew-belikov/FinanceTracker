-- Legacy TIMESTAMP WITHOUT TIME ZONE values have always represented UTC.
-- Convert them without applying the PostgreSQL session timezone.
BEGIN;

-- PostgreSQL does not allow an underlying column type change while a view
-- depends on it. The compatibility view is recreated after both possible
-- sources have been migrated.
DROP VIEW IF EXISTS public.deposits;

DO $$
BEGIN
    IF to_regclass('public.deposits_legacy') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_class c
           WHERE c.oid = to_regclass('public.deposits_legacy')
             AND c.relkind = 'r'
       ) THEN
        ALTER TABLE public.deposits_legacy
            ALTER COLUMN date DROP DEFAULT,
            ALTER COLUMN date TYPE TIMESTAMPTZ USING date AT TIME ZONE 'UTC',
            ALTER COLUMN created_at DROP DEFAULT,
            ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
            ALTER COLUMN created_at SET DEFAULT NOW();
    END IF;
END;
$$;

ALTER TABLE public.schema_migrations
    ALTER COLUMN applied_at DROP DEFAULT,
    ALTER COLUMN applied_at TYPE TIMESTAMPTZ USING applied_at AT TIME ZONE 'UTC',
    ALTER COLUMN applied_at SET DEFAULT NOW();

ALTER TABLE public.instruments
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE public.portfolio_snapshots
    ALTER COLUMN snapshot_at TYPE TIMESTAMPTZ USING snapshot_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE public.portfolio_positions
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE public.operations
    ALTER COLUMN date TYPE TIMESTAMPTZ USING date AT TIME ZONE 'UTC',
    ALTER COLUMN cancel_date_time TYPE TIMESTAMPTZ USING cancel_date_time AT TIME ZONE 'UTC',
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE public.income_events
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE public.asset_aliases
    ALTER COLUMN first_seen_at TYPE TIMESTAMPTZ USING first_seen_at AT TIME ZONE 'UTC',
    ALTER COLUMN last_seen_at TYPE TIMESTAMPTZ USING last_seen_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at DROP DEFAULT,
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE public.rebalance_targets
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at DROP DEFAULT,
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE public.invest_notifications
    ALTER COLUMN operation_date TYPE TIMESTAMPTZ USING operation_date AT TIME ZONE 'UTC',
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE public.bot_daily_job_runs
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN completed_at TYPE TIMESTAMPTZ USING completed_at AT TIME ZONE 'UTC',
    ALTER COLUMN claimed_at TYPE TIMESTAMPTZ USING claimed_at AT TIME ZONE 'UTC',
    ALTER COLUMN heartbeat_at TYPE TIMESTAMPTZ USING heartbeat_at AT TIME ZONE 'UTC';

ALTER TABLE public.bot_notification_deliveries
    ALTER COLUMN claimed_at TYPE TIMESTAMPTZ USING claimed_at AT TIME ZONE 'UTC',
    ALTER COLUMN delivered_at TYPE TIMESTAMPTZ USING delivered_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at DROP DEFAULT,
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE public.payout_calendar_events
    ALTER COLUMN fetched_at TYPE TIMESTAMPTZ USING fetched_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at DROP DEFAULT,
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at SET DEFAULT NOW();

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
