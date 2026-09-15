BEGIN;

-- Versioned baseline for tables that historically were created by ORM create_all().
CREATE TABLE IF NOT EXISTS instruments (
    id SERIAL PRIMARY KEY, figi TEXT NOT NULL UNIQUE, ticker TEXT, name TEXT,
    class_code TEXT, instrument_type TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id SERIAL PRIMARY KEY, account_id TEXT NOT NULL, account_name TEXT,
    snapshot_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, snapshot_date DATE NOT NULL,
    currency TEXT NOT NULL, total_value NUMERIC(18, 2), total_shares NUMERIC(18, 2),
    total_bonds NUMERIC(18, 2), total_etf NUMERIC(18, 2), total_currencies NUMERIC(18, 2),
    total_futures NUMERIC(18, 2), expected_yield NUMERIC(18, 2), expected_yield_pct NUMERIC(9, 4),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_snapshot_account_date UNIQUE (account_id, snapshot_date)
);
CREATE INDEX IF NOT EXISTS ix_portfolio_snapshots_snapshot_at ON portfolio_snapshots (snapshot_at);
CREATE INDEX IF NOT EXISTS ix_portfolio_snapshots_snapshot_date ON portfolio_snapshots (snapshot_date);
CREATE TABLE IF NOT EXISTS portfolio_positions (
    id SERIAL PRIMARY KEY, snapshot_id INTEGER NOT NULL REFERENCES portfolio_snapshots(id),
    figi TEXT NOT NULL, instrument_id INTEGER REFERENCES instruments(id), instrument_uid TEXT,
    position_uid TEXT, asset_uid TEXT, ticker TEXT, name TEXT, instrument_type TEXT,
    quantity NUMERIC(18, 6), currency TEXT, current_price NUMERIC(18, 4),
    current_nkd NUMERIC, position_value NUMERIC(18, 2), expected_yield NUMERIC(18, 2),
    expected_yield_pct NUMERIC(9, 4), weight_pct NUMERIC(9, 4), raw_payload_json TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

COMMIT;
