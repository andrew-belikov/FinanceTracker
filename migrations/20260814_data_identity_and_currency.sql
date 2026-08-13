BEGIN;

-- An operation identity belongs to a broker account. Refuse ambiguous legacy
-- data instead of deleting or choosing one of colliding rows.
DO $$
DECLARE
    duplicate_groups BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO duplicate_groups
    FROM (
        SELECT account_id, operation_id
        FROM public.operations
        GROUP BY account_id, operation_id
        HAVING COUNT(*) > 1
    ) duplicates;

    IF duplicate_groups > 0 THEN
        RAISE EXCEPTION
            'operations contains duplicate account-scoped identities; manual remediation is required';
    END IF;
END;
$$;

ALTER TABLE public.operations
    DROP CONSTRAINT IF EXISTS uq_operations_operation_id;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.operations'::regclass
          AND conname = 'uq_operations_account_operation'
    ) THEN
        ALTER TABLE public.operations
            ADD CONSTRAINT uq_operations_account_operation
            UNIQUE (account_id, operation_id);
    END IF;
END;
$$;

-- The migration runner sets PostgreSQL TimeZone from the configured application
-- TIMEZONE. Legacy events are matched by their old UTC date and normalized to
-- the operation's local civil date. Already-normalized rows use their local date
-- on rerun, so the transformation remains stable.
ALTER TABLE public.income_events
    ADD COLUMN IF NOT EXISTS currency TEXT;

-- Remove both generations of the identity constraint before normalization.
-- A collision check below aborts and rolls this DDL back with the whole file.
ALTER TABLE public.income_events
    DROP CONSTRAINT IF EXISTS uq_income_events_account_figi_date_type;

ALTER TABLE public.income_events
    DROP CONSTRAINT IF EXISTS uq_income_events_account_figi_date_type_currency;

WITH event_resolution AS (
    SELECT
        ie.id,
        CASE
            WHEN COUNT(DISTINCT timezone(
                current_setting('TimeZone'),
                o.date AT TIME ZONE 'UTC'
            )::date) = 1
            THEN MAX(timezone(
                current_setting('TimeZone'),
                o.date AT TIME ZONE 'UTC'
            )::date)
            ELSE ie.event_date
        END AS resolved_event_date,
        CASE
            WHEN COUNT(DISTINCT UPPER(NULLIF(o.currency, ''))) = 1
            THEN MAX(UPPER(NULLIF(o.currency, '')))
            ELSE 'UNKNOWN'
        END AS resolved_currency
    FROM public.income_events ie
    LEFT JOIN public.operations o
      ON o.account_id = ie.account_id
     AND o.figi = ie.figi
     AND (
         ((ie.currency IS NULL OR BTRIM(ie.currency) = '') AND o.date::date = ie.event_date)
         OR
         ((ie.currency IS NOT NULL AND BTRIM(ie.currency) <> '') AND timezone(
             current_setting('TimeZone'),
             o.date AT TIME ZONE 'UTC'
         )::date = ie.event_date)
     )
     AND o.state = 'OPERATION_STATE_EXECUTED'
     AND (
         (ie.event_type = 'coupon' AND o.operation_type IN (
             'OPERATION_TYPE_COUPON',
             'OPERATION_TYPE_COUPON_TAX',
             'OPERATION_TYPE_BOND_TAX',
             'OPERATION_TYPE_BOND_TAX_PROGRESSIVE'
         ))
         OR
         (ie.event_type = 'dividend' AND o.operation_type IN (
             'OPERATION_TYPE_DIVIDEND',
             'OPERATION_TYPE_DIVIDEND_TAX',
             'OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE'
         ))
     )
    GROUP BY ie.id, ie.event_date
)
UPDATE public.income_events ie
SET
    event_date = event_resolution.resolved_event_date,
    currency = CASE
        WHEN ie.currency IS NULL OR BTRIM(ie.currency) = ''
        THEN event_resolution.resolved_currency
        ELSE UPPER(ie.currency)
    END
FROM event_resolution
WHERE ie.id = event_resolution.id;

UPDATE public.income_events
SET currency = 'UNKNOWN'
WHERE currency IS NULL OR BTRIM(currency) = '';

ALTER TABLE public.income_events
    ALTER COLUMN currency SET NOT NULL;

-- Never aggregate or discard distinct financial rows. If local-date
-- normalization produces a collision, abort the transaction for explicit
-- manual remediation with every original row restored by rollback.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.income_events
        GROUP BY account_id, figi, event_date, event_type, currency
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'income_events local identity collision; manual remediation is required';
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.income_events'::regclass
          AND conname = 'uq_income_events_account_figi_date_type_currency'
    ) THEN
        ALTER TABLE public.income_events
            ADD CONSTRAINT uq_income_events_account_figi_date_type_currency
            UNIQUE (account_id, figi, event_date, event_type, currency);
    END IF;
END;
$$;

COMMIT;
