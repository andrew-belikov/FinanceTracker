# Changelog

## [0.1.1] - 2026-01-15

- Added unified structured JSON logging via `src/common/logging_setup.py`. All logs now emit JSON Lines to stdout/stderr to meet new logging requirements.
- Replaced all `print` statements in `src/tracker/app.py` with structured logging calls using the new logger. Errors are logged with `logger.exception` including stack traces.
- Updated `src/bot/bot.py` to use the unified logging module instead of `logging.basicConfig`.
- No functional changes to existing features or build process; the project continues to build and run as before.

## [0.1.0] - 2026-01-16

- Initial public version.
- Docker-based deployment (Postgres + tracker + Telegram bot).
- Config via .env.