CREATE TABLE IF NOT EXISTS income_events (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    figi VARCHAR NOT NULL,
    event_date DATE NOT NULL,
    event_type VARCHAR NOT NULL,
    gross_amount NUMERIC(18,2) NOT NULL,
    tax_amount NUMERIC(18,2) NOT NULL,
    net_amount NUMERIC(18,2) NOT NULL,
    net_yield_pct NUMERIC(9,4) NOT NULL,
    notified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_income_events_account_figi_date_type
        UNIQUE (account_id, figi, event_date, event_type)
);
