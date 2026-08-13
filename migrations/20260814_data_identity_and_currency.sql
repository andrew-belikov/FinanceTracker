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

-- Legacy income events had no currency. The old reconciler grouped operations
-- by their UTC date, so that exact legacy identity is safe for backfill only
-- when it resolves to one currency. Ambiguous or missing provenance is kept as
-- UNKNOWN and remains separate; no FX conversion or row deletion is performed.
ALTER TABLE public.income_events
    ADD COLUMN IF NOT EXISTS currency TEXT;

WITH event_currencies AS (
    SELECT
        ie.id,
        CASE
            WHEN COUNT(DISTINCT UPPER(NULLIF(o.currency, ''))) = 1
            THEN MAX(UPPER(NULLIF(o.currency, '')))
            ELSE 'UNKNOWN'
        END AS resolved_currency
    FROM public.income_events ie
    LEFT JOIN public.operations o
      ON o.account_id = ie.account_id
     AND o.figi = ie.figi
     AND o.date::date = ie.event_date
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
    GROUP BY ie.id
)
UPDATE public.income_events ie
SET currency = event_currencies.resolved_currency
FROM event_currencies
WHERE ie.id = event_currencies.id
  AND ie.currency IS NULL;

UPDATE public.income_events
SET currency = 'UNKNOWN'
WHERE currency IS NULL OR BTRIM(currency) = '';

ALTER TABLE public.income_events
    ALTER COLUMN currency SET NOT NULL;

ALTER TABLE public.income_events
    DROP CONSTRAINT IF EXISTS uq_income_events_account_figi_date_type;

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
