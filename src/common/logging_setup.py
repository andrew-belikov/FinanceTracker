"""
Unified structured logging for FinanceTracker.

- Emits JSON Lines (one JSON object per line) to stdout/stderr.
- Adds stable schema fields: ts, level, service, env, logger, event, msg.
- Sanitizes sensitive data (tokens, passwords, secrets), including Telegram bot tokens in URLs.
"""

from __future__ import annotations

import datetime as _dt
import json as _json
import logging as _logging
import os as _os
import re as _re
import sys as _sys
import traceback as _traceback
from typing import Any, Dict, Optional


_REDACTED = "***REDACTED***"

_SENSITIVE_KEYS = {
    "token",
    "password",
    "secret",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "authorization",
}

# Bearer <token>
_RE_BEARER = _re.compile(r"(Bearer)\s+[A-Za-z0-9\-\._~\+\/]+=*", _re.IGNORECASE)

# Telegram bot token in URL: https://api.telegram.org/bot<token>/method
_RE_TG_URL = _re.compile(r"(https?://api\.telegram\.org/bot)([^/\s\"']+)", _re.IGNORECASE)

# Telegram token as standalone: bot<id>:<secret>
_RE_TG_TOKEN = _re.compile(r"\bbot\d{6,}:[A-Za-z0-9_-]{20,}\b")


def _safe_str(obj: Any) -> str:
    try:
        return str(obj)
    except Exception:
        return "<unprintable>"


def _sanitize_string(s: str) -> str:
    s = _RE_BEARER.sub(r"\1 " + _REDACTED, s)
    s = _RE_TG_URL.sub(r"\1" + _REDACTED, s)
    s = _RE_TG_TOKEN.sub(_REDACTED, s)
    return s


def _sanitize(value: Any) -> Any:
    """
    Best-effort recursive sanitization to avoid leaking secrets in logs.
    """
    if value is None:
        return None

    if isinstance(value, str):
        return _sanitize_string(value)

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]

    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            ks = _safe_str(k)
            if ks.strip().lower() in _SENSITIVE_KEYS:
                out[ks] = _REDACTED
            else:
                out[ks] = _sanitize(v)
        return out

    # Fallback for arbitrary objects (Decimal, datetime, etc.)
    return _sanitize_string(_safe_str(value))


class _JsonLineFormatter(_logging.Formatter):
    def format(self, record: _logging.LogRecord) -> str:
        try:
            ts = _dt.datetime.fromtimestamp(record.created, tz=_dt.timezone.utc).isoformat()

            service = _os.getenv("APP_SERVICE", "unknown_service")
            env = _os.getenv("APP_ENV", "dev")

            event = getattr(record, "event", None) or "log"
            msg = record.getMessage()

            payload: Dict[str, Any] = {
                "ts": ts,
                "level": record.levelname,
                "service": service,
                "env": env,
                "logger": record.name,
                "event": event,
                "msg": msg,
            }

            # Optional structured context
            ctx = getattr(record, "ctx", None)
            if ctx is not None:
                payload["ctx"] = ctx

            # Correlation fields if present
            for key in ("trace_id", "request_id", "job_id", "update_id"):
                val = getattr(record, key, None)
                if val is not None:
                    payload[key] = val

            # Structured error on exception logs
            if record.exc_info:
                etype, evalue, etb = record.exc_info
                payload["error"] = {
                    "type": getattr(etype, "__name__", "Exception"),
                    "message": _safe_str(evalue),
                    "stack": "".join(_traceback.format_exception(etype, evalue, etb)),
                    "where": f"{record.pathname}:{record.lineno} in {record.funcName}",
                }

            payload = _sanitize(payload)
            return _json.dumps(payload, ensure_ascii=False)

        except Exception:
            # Never break app because of logging formatter
            fallback = {
                "ts": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
                "level": "ERROR",
                "service": _os.getenv("APP_SERVICE", "unknown_service"),
                "env": _os.getenv("APP_ENV", "dev"),
                "logger": "logging_setup",
                "event": "logging_formatter_failed",
                "msg": "Failed to format log record",
                "ctx": {"original_logger": record.name},
                "error": {"message": "formatter_exception"},
            }
            return _json.dumps(fallback, ensure_ascii=False)


def configure_logging() -> None:
    """
    Configure root logging exactly once.

    - Output: stdout
    - Format: JSON lines
    - No network handlers, no file handlers
    """
    root = _logging.getLogger()
    if getattr(root, "_ft_json_logging_configured", False):
        return

    level_name = _os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(_logging, level_name, _logging.INFO)
    root.setLevel(level)

    # Replace handlers (avoid duplicate logs in some runtimes)
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = _logging.StreamHandler(_sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(_JsonLineFormatter())
    root.addHandler(handler)

    # Reduce noisy libraries that may include secrets in their log messages
    _logging.getLogger("httpx").setLevel(_logging.WARNING)
    _logging.getLogger("httpcore").setLevel(_logging.WARNING)
    _logging.getLogger("telegram").setLevel(_logging.INFO)
    _logging.getLogger("apscheduler").setLevel(_logging.INFO)

    root._ft_json_logging_configured = True


def get_logger(name: str) -> _logging.Logger:
    return _logging.getLogger(name)


def log_event(
    logger: _logging.Logger,
    level: int,
    event: str,
    msg: str,
    ctx: Optional[Dict[str, Any]] = None,
    **correlation: Any,
) -> None:
    extra: Dict[str, Any] = {"event": event}
    if ctx is not None:
        extra["ctx"] = ctx
    for k, v in correlation.items():
        extra[k] = v
    logger.log(level, msg, extra=extra)
