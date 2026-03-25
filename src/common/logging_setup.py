"""
Unified structured logging for FinanceTracker.

- Emits JSON Lines (one JSON object per line) to stdout.
- Adds stable schema fields: ts, level, service, env, logger, event, msg.
- Sanitizes sensitive data (tokens, passwords, secrets), including Telegram bot tokens in URLs.
- Provides a first-party adapter with an explicit (event, msg, ctx) contract.
"""

from __future__ import annotations

import datetime as _dt
import json as _json
import logging as _logging
import os as _os
import re as _re
import sys as _sys
import threading as _threading
import traceback as _traceback
from typing import Any, Dict, Mapping, Optional, TextIO


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

_CORRELATION_FIELDS = ("trace_id", "request_id", "job_id", "update_id")
_FIRST_PARTY_LOGGER_PREFIXES = (
    "__main__",
    "proxy_smoke",
    "xray_client",
    "bot",
    "tracker",
    "iis_tracker",
    "common",
)


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


def _normalize_ctx(ctx: Any) -> Optional[Dict[str, Any]]:
    if ctx is None:
        return None
    if isinstance(ctx, dict):
        return dict(ctx)
    return {"value": ctx}


def _merge_ctx(base: Optional[Mapping[str, Any]], **extra: Any) -> Optional[Dict[str, Any]]:
    merged: Dict[str, Any] = {}
    if base:
        merged.update(dict(base))
    for key, value in extra.items():
        if value is None:
            continue
        merged[key] = value
    return merged or None


def _format_record_message(record: _logging.LogRecord) -> tuple[str, Any]:
    try:
        return record.getMessage(), None
    except Exception:
        fallback = _safe_str(record.msg)
        if not record.args:
            return fallback, None
        return fallback, _sanitize(record.args)


def _is_first_party_logger(logger_name: str) -> bool:
    return any(
        logger_name == prefix or logger_name.startswith(prefix + ".")
        for prefix in _FIRST_PARTY_LOGGER_PREFIXES
    )


class StructuredLogger:
    def __init__(self, logger: _logging.Logger):
        self._logger = logger

    @property
    def name(self) -> str:
        return self._logger.name

    @property
    def raw_logger(self) -> _logging.Logger:
        return self._logger

    def log(
        self,
        level: int,
        event: str,
        msg: str,
        ctx: Optional[Mapping[str, Any]] = None,
        *,
        exc_info: Any = None,
        **correlation: Any,
    ) -> None:
        extra: Dict[str, Any] = {"event": _safe_str(event)}

        normalized_ctx = _normalize_ctx(ctx)
        if normalized_ctx is not None:
            extra["ctx"] = normalized_ctx

        for key, value in correlation.items():
            if value is None:
                continue
            extra[_safe_str(key)] = value

        self._logger.log(level, _safe_str(msg), extra=extra, exc_info=exc_info)

    def debug(self, event: str, msg: str, ctx: Optional[Mapping[str, Any]] = None, **correlation: Any) -> None:
        self.log(_logging.DEBUG, event, msg, ctx, **correlation)

    def info(self, event: str, msg: str, ctx: Optional[Mapping[str, Any]] = None, **correlation: Any) -> None:
        self.log(_logging.INFO, event, msg, ctx, **correlation)

    def warning(self, event: str, msg: str, ctx: Optional[Mapping[str, Any]] = None, **correlation: Any) -> None:
        self.log(_logging.WARNING, event, msg, ctx, **correlation)

    def error(self, event: str, msg: str, ctx: Optional[Mapping[str, Any]] = None, **correlation: Any) -> None:
        self.log(_logging.ERROR, event, msg, ctx, **correlation)

    def critical(self, event: str, msg: str, ctx: Optional[Mapping[str, Any]] = None, **correlation: Any) -> None:
        self.log(_logging.CRITICAL, event, msg, ctx, **correlation)

    def exception(self, event: str, msg: str, ctx: Optional[Mapping[str, Any]] = None, **correlation: Any) -> None:
        self.log(_logging.ERROR, event, msg, ctx, exc_info=True, **correlation)


class _JsonLineFormatter(_logging.Formatter):
    def format(self, record: _logging.LogRecord) -> str:
        try:
            ts = _dt.datetime.fromtimestamp(record.created, tz=_dt.timezone.utc).isoformat()

            service = _os.getenv("APP_SERVICE", "unknown_service")
            env = _os.getenv("APP_ENV", "dev")

            explicit_event = getattr(record, "event", None)
            event = explicit_event or "auto_log"
            msg, bad_args = _format_record_message(record)

            payload: Dict[str, Any] = {
                "ts": ts,
                "level": record.levelname,
                "service": service,
                "env": env,
                "logger": record.name,
                "event": _safe_str(event),
                "msg": msg,
            }

            # Optional structured context
            ctx = _normalize_ctx(getattr(record, "ctx", None))
            if explicit_event is None:
                ctx = _merge_ctx(
                    ctx,
                    event_source="auto" if _is_first_party_logger(record.name) else "library",
                )
            if bad_args is not None:
                ctx = _merge_ctx(ctx, logging_args=bad_args)
            if ctx is not None:
                payload["ctx"] = ctx

            # Correlation fields if present
            for key in _CORRELATION_FIELDS:
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


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(_logging.getLogger(name))


def log_event(
    logger: StructuredLogger | _logging.Logger,
    level: int,
    event: str,
    msg: str,
    ctx: Optional[Mapping[str, Any]] = None,
    **correlation: Any,
) -> None:
    if isinstance(logger, StructuredLogger):
        logger.log(level, event, msg, ctx, **correlation)
        return

    extra: Dict[str, Any] = {"event": event}
    if ctx is not None:
        extra["ctx"] = dict(ctx)
    for k, v in correlation.items():
        extra[k] = v
    logger.log(level, msg, extra=extra)


def relay_text_stream(
    logger: StructuredLogger,
    stream: TextIO,
    event: str,
    *,
    stream_name: str,
    ctx: Optional[Mapping[str, Any]] = None,
    level: int = _logging.INFO,
) -> _threading.Thread:
    base_ctx = dict(ctx or {})

    def _relay() -> None:
        try:
            for raw_line in iter(stream.readline, ""):
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                line_ctx = dict(base_ctx)
                line_ctx["stream"] = stream_name
                logger.log(level, event, line, line_ctx)
        except Exception:
            failure_ctx = dict(base_ctx)
            failure_ctx["stream"] = stream_name
            logger.exception(
                "child_process_stream_bridge_failed",
                "Failed to relay child process output.",
                failure_ctx,
            )
        finally:
            try:
                stream.close()
            except Exception:
                pass

    thread = _threading.Thread(
        target=_relay,
        name=f"log-relay-{logger.name}-{stream_name}",
        daemon=True,
    )
    thread.start()
    return thread
