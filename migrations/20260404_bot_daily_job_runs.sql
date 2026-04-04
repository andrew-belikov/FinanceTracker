BEGIN;

CREATE TABLE IF NOT EXISTS bot_daily_job_runs (
    id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    run_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'started',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP NULL,
    sent_total INTEGER NULL,
    failed_total INTEGER NULL
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_bot_daily_job_runs_job_date'
    ) THEN
        ALTER TABLE bot_daily_job_runs
            ADD CONSTRAINT uq_bot_daily_job_runs_job_date
            UNIQUE (job_name, run_date);
    END IF;
END;
$$;

COMMIT;
