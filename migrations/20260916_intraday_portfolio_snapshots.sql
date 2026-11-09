-- Temporary append-only observations for the daily intraday portfolio push.
-- Raw rows are retained until delivery succeeds; compact summaries are durable.
BEGIN;

CREATE TABLE IF NOT EXISTS public.intraday_portfolio_snapshots (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    account_name TEXT,
    snapshot_at TIMESTAMPTZ NOT NULL,
    snapshot_date DATE NOT NULL,
    currency TEXT NOT NULL,
    total_value NUMERIC(18, 2),
    total_shares NUMERIC(18, 2),
    total_bonds NUMERIC(18, 2),
    total_etf NUMERIC(18, 2),
    total_currencies NUMERIC(18, 2),
    total_futures NUMERIC(18, 2),
    expected_yield NUMERIC(18, 2),
    expected_yield_pct NUMERIC(9, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_intraday_portfolio_snapshots_account_date_at
    ON public.intraday_portfolio_snapshots (account_id, snapshot_date, snapshot_at);

CREATE TABLE IF NOT EXISTS public.intraday_portfolio_positions (
    id SERIAL PRIMARY KEY,
    snapshot_id INTEGER NOT NULL REFERENCES public.intraday_portfolio_snapshots(id) ON DELETE CASCADE,
    figi TEXT NOT NULL,
    instrument_id INTEGER REFERENCES public.instruments(id),
    instrument_uid TEXT,
    position_uid TEXT,
    asset_uid TEXT,
    ticker TEXT,
    name TEXT,
    instrument_type TEXT,
    quantity NUMERIC(18, 6),
    currency TEXT,
    current_price NUMERIC(18, 4),
    current_nkd NUMERIC,
    position_value NUMERIC(18, 2),
    expected_yield NUMERIC(18, 2),
    expected_yield_pct NUMERIC(9, 4),
    weight_pct NUMERIC(9, 4),
    raw_payload_json TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.intraday_daily_summaries (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    summary_date DATE NOT NULL,
    currency TEXT NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    first_value NUMERIC(18, 2) NOT NULL,
    last_value NUMERIC(18, 2) NOT NULL,
    min_value NUMERIC(18, 2) NOT NULL,
    max_value NUMERIC(18, 2) NOT NULL,
    total_value_delta NUMERIC(18, 2),
    observation_count INTEGER NOT NULL,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_intraday_daily_summaries_account_date UNIQUE (account_id, summary_date)
);

COMMIT;
