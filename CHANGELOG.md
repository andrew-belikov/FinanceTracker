# Changelog

## [0.1.2] - 2026-03-24

- Reworked `/dataset` to use a period-first contract: removed misleading lifetime-style `total_return_*` fields and added `period_start_value`, `period_end_value`, `period_net_cashflow`, `period_pnl_abs`, `period_twr_pct`, `has_full_history_from_zero`, `positions_value_sum`, `reconciliation_gap_abs`, and `reconciliation_by_asset_type`.
- Added `logical_asset_id` and source-aware identity fields to dataset exports (`asset_uid`, `instrument_uid`, `position_uid`) to make downstream analytics resilient to FIGI changes such as the LQDT migration case.
- Normalized dividend/coupon tax operations into `income_tax` in dataset exports and fixed double counting: taxes already represented in `income_events` are no longer subtracted a second time from `operations`.
- Added source-model support for richer dataset exports: new migration `migrations/20260324_dataset_source_fields.sql`, extended `portfolio_positions` fields, and new `asset_aliases` table for logical asset mapping.
- Added `src/tracker/repair_operations_description_encoding.py` for one-off repair of mojibake in `operations.description` after Windows backup migrations, plus shared text helpers and tests.
- Updated README deployment and verification instructions for the new dataset migration, repair step, and `/dataset` contract.

## [0.1.1] - 2026-01-15

- Added unified structured JSON logging via `src/common/logging_setup.py`. All logs now emit JSON Lines to stdout/stderr to meet new logging requirements.
- Replaced all `print` statements in `src/tracker/app.py` with structured logging calls using the new logger. Errors are logged with `logger.exception` including stack traces.
- Updated `src/bot/bot.py` to use the unified logging module instead of `logging.basicConfig`.
- No functional changes to existing features or build process; the project continues to build and run as before.

## [0.1.0] - 2026-01-16

- Initial public version.
- Docker-based deployment (Postgres + tracker + Telegram bot).
- Config via .env.
