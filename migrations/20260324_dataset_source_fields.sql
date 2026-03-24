BEGIN;

ALTER TABLE public.portfolio_positions
    ADD COLUMN IF NOT EXISTS instrument_uid TEXT,
    ADD COLUMN IF NOT EXISTS position_uid TEXT,
    ADD COLUMN IF NOT EXISTS asset_uid TEXT,
    ADD COLUMN IF NOT EXISTS raw_payload_json TEXT,
    ADD COLUMN IF NOT EXISTS current_nkd NUMERIC;

CREATE TABLE IF NOT EXISTS public.asset_aliases (
    id BIGSERIAL PRIMARY KEY,
    asset_uid TEXT NOT NULL,
    instrument_uid TEXT,
    figi TEXT,
    ticker TEXT,
    name TEXT,
    first_seen_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_seen_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_asset_aliases_asset_instrument_figi'
    ) THEN
        ALTER TABLE public.asset_aliases
            ADD CONSTRAINT uq_asset_aliases_asset_instrument_figi
            UNIQUE (asset_uid, instrument_uid, figi);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_asset_aliases_asset_uid ON public.asset_aliases (asset_uid);
CREATE INDEX IF NOT EXISTS ix_asset_aliases_instrument_uid ON public.asset_aliases (instrument_uid);
CREATE INDEX IF NOT EXISTS ix_asset_aliases_figi ON public.asset_aliases (figi);

INSERT INTO public.asset_aliases (
    asset_uid,
    instrument_uid,
    figi,
    ticker,
    name,
    first_seen_at,
    last_seen_at
)
SELECT
    o.asset_uid,
    o.instrument_uid,
    o.figi,
    i.ticker,
    COALESCE(NULLIF(o.name, ''), i.name, o.figi, o.asset_uid) AS name,
    MIN(o.date) AS first_seen_at,
    MAX(o.date) AS last_seen_at
FROM public.operations o
LEFT JOIN public.instruments i ON i.figi = o.figi
WHERE o.asset_uid IS NOT NULL
GROUP BY
    o.asset_uid,
    o.instrument_uid,
    o.figi,
    i.ticker,
    COALESCE(NULLIF(o.name, ''), i.name, o.figi, o.asset_uid)
ON CONFLICT (asset_uid, instrument_uid, figi) DO UPDATE
SET
    ticker = COALESCE(EXCLUDED.ticker, public.asset_aliases.ticker),
    name = COALESCE(EXCLUDED.name, public.asset_aliases.name),
    first_seen_at = LEAST(public.asset_aliases.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at = GREATEST(public.asset_aliases.last_seen_at, EXCLUDED.last_seen_at),
    updated_at = NOW();

UPDATE public.portfolio_positions pp
SET asset_uid = aa.asset_uid
FROM public.asset_aliases aa
WHERE pp.asset_uid IS NULL
  AND pp.figi IS NOT NULL
  AND aa.figi = pp.figi;

COMMIT;
