-- Persist missing currencies explicitly; UNKNOWN must not be silently presented as RUB.
BEGIN;

UPDATE public.portfolio_positions
SET currency = 'UNKNOWN'
WHERE currency IS NULL OR BTRIM(currency) = '';

UPDATE public.payout_calendar_events
SET currency = 'UNKNOWN'
WHERE currency IS NULL OR BTRIM(currency) = '';

COMMIT;
