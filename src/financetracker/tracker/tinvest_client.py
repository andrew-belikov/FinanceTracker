"""T-Invest read-only HTTP client and pagination."""

from __future__ import annotations

import copy
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Optional

import requests

from financetracker.tracker.http_policy import parse_retry_delay as parse_http_retry_delay, retry_delay_seconds as calculate_retry_delay_seconds
from financetracker.tracker.payloads import url_host as _url_host, url_path as _url_path

@dataclass(frozen=True)
class TInvestClientRuntime:
    api_session: Any
    api_token: str
    base_url: str
    account_status: str
    portfolio_currency: str
    http_timeout_seconds: float
    http_retry_total: int
    http_backoff_seconds: float
    http_max_backoff_seconds: float
    instrument_cache_ttl_seconds: float
    instrument_cache_max_entries: int
    operations_max_pages: int
    verify_ssl: bool
    logger: Any


_runtime: TInvestClientRuntime | None = None


# Configured by the tracker composition root; this module never reads environment.
def configure(**dependencies: Any) -> None:
    global _runtime
    _runtime = TInvestClientRuntime(
        api_session=dependencies["API_SESSION"],
        api_token=dependencies["API_TOKEN"],
        base_url=dependencies["BASE_URL"],
        account_status=dependencies["ACCOUNT_STATUS"],
        portfolio_currency=dependencies["PORTFOLIO_CURRENCY"],
        http_timeout_seconds=dependencies["HTTP_TIMEOUT_SECONDS"],
        http_retry_total=dependencies["HTTP_RETRY_TOTAL"],
        http_backoff_seconds=dependencies["HTTP_BACKOFF_SECONDS"],
        http_max_backoff_seconds=dependencies["HTTP_MAX_BACKOFF_SECONDS"],
        instrument_cache_ttl_seconds=dependencies["INSTRUMENT_CACHE_TTL_SECONDS"],
        instrument_cache_max_entries=dependencies["INSTRUMENT_CACHE_MAX_ENTRIES"],
        operations_max_pages=dependencies["OPERATIONS_MAX_PAGES"],
        verify_ssl=dependencies["VERIFY_SSL"],
        logger=dependencies["logger"],
    )


def _require_runtime() -> TInvestClientRuntime:
    if _runtime is None:
        raise RuntimeError("T-Invest client is not configured")
    return _runtime

def _build_response_body_ctx(resp, base_ctx: Optional[dict] = None) -> dict:
    """Return allowlisted response metadata without serializing an upstream body."""
    ctx = dict(base_ctx or {})
    content_type = resp.headers.get("Content-Type")
    if content_type:
        ctx["content_type"] = content_type
    content_length = resp.headers.get("Content-Length")
    if content_length and content_length.isdecimal():
        ctx["response_body_bytes"] = int(content_length)
    return ctx

RETRYABLE_HTTP_STATUSES = frozenset({408, 429, *range(500, 600)})
RETRYABLE_REQUEST_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def _parse_retry_delay(value: Optional[str], *, now: Optional[datetime] = None) -> Optional[float]:
    runtime = _require_runtime()
    return parse_http_retry_delay(
        value,
        max_backoff_seconds=runtime.http_max_backoff_seconds,
        now=now,
    )


def _retry_delay_seconds(resp, attempt: int) -> float:
    runtime = _require_runtime()
    return calculate_retry_delay_seconds(
        resp,
        attempt,
        backoff_seconds=runtime.http_backoff_seconds,
        max_backoff_seconds=runtime.http_max_backoff_seconds,
    )


def _retry_log_ctx(
    method_path: str,
    *,
    attempt: int,
    delay_seconds: float,
    resp=None,
    exception_type: Optional[str] = None,
) -> dict:
    runtime = _require_runtime()
    ctx = {
        "method_path": method_path,
        "attempt": attempt,
        "max_attempts": runtime.http_retry_total + 1,
        "delay_seconds": delay_seconds,
    }
    if exception_type:
        ctx["exception_type"] = exception_type
    if resp is not None:
        ctx["status_code"] = resp.status_code
        for header_name in (
            "x-tracking-id",
            "x-ratelimit-limit",
            "x-ratelimit-remaining",
            "x-ratelimit-reset",
        ):
            value = resp.headers.get(header_name)
            if value is not None:
                ctx[header_name.replace("-", "_")] = value
    return ctx


def _post_api_impl(method_path: str, payload: dict) -> dict:
    runtime = _require_runtime()
    url = f"{runtime.base_url}/{method_path}"

    headers = {
        "Authorization": f"Bearer {runtime.api_token}",
        "Content-Type": "application/json",
    }

    # Все текущие wrapper-ы вызывают read-only Get* методы. Если здесь появится
    # мутационный RPC (например, выставление заявки), автоматические повторы
    # должны быть отключены для него отдельно.
    max_attempts = runtime.http_retry_total + 1
    for attempt in range(1, max_attempts + 1):
        try:
            resp = runtime.api_session.post(
                url,
                headers=headers,
                json=payload,
                timeout=runtime.http_timeout_seconds,
                verify=runtime.verify_ssl,
            )
        except requests.exceptions.SSLError:
            runtime.logger.exception(
                "api_request_ssl_error",
                "SSL error while calling T-Invest API.",
                {"method_path": method_path, "url_host": _url_host(url)},
            )
            raise
        except RETRYABLE_REQUEST_EXCEPTIONS as exc:
            if attempt >= max_attempts:
                runtime.logger.exception(
                    "api_request_failed",
                    "HTTP request to T-Invest API failed after retries.",
                    {
                        "method_path": method_path,
                        "url_host": _url_host(url),
                        "attempts": attempt,
                    },
                )
                raise

            delay_seconds = min(
                runtime.http_backoff_seconds * (2 ** (attempt - 1)),
                runtime.http_max_backoff_seconds,
            )
            runtime.logger.warning(
                "api_retry_scheduled",
                "Retrying a failed T-Invest API request.",
                _retry_log_ctx(
                    method_path,
                    attempt=attempt,
                    delay_seconds=delay_seconds,
                    exception_type=type(exc).__name__,
                ),
            )
            time.sleep(delay_seconds)
            continue
        except requests.exceptions.RequestException:
            runtime.logger.exception(
                "api_request_failed",
                "HTTP request to T-Invest API failed.",
                {"method_path": method_path, "url_host": _url_host(url)},
            )
            raise

        if resp.status_code == 200:
            break

        if resp.status_code in RETRYABLE_HTTP_STATUSES and attempt < max_attempts:
            delay_seconds = _retry_delay_seconds(resp, attempt)
            runtime.logger.warning(
                "api_retry_scheduled",
                "Retrying a retryable T-Invest API response.",
                _retry_log_ctx(
                    method_path,
                    attempt=attempt,
                    delay_seconds=delay_seconds,
                    resp=resp,
                ),
            )
            time.sleep(delay_seconds)
            continue
        break

    if resp.status_code != 200:
        error_ctx = {
            "method_path": method_path,
            "url_host": _url_host(url),
            "path": _url_path(url),
            "status_code": resp.status_code,
        }
        runtime.logger.error(
            "api_http_error",
            "T-Invest API returned a non-200 response.",
            error_ctx,
        )
        runtime.logger.error(
            "api_http_error_metadata",
            "Logged allowlisted T-Invest API error response metadata.",
            _build_response_body_ctx(resp, error_ctx),
        )
        raise RuntimeError(f"API HTTP {resp.status_code}")

    try:
        return resp.json()
    except json.JSONDecodeError:
        error_ctx = {
            "method_path": method_path,
            "url_host": _url_host(url),
            "path": _url_path(url),
            "status_code": resp.status_code,
        }
        runtime.logger.error(
            "api_json_decode_error",
            "Failed to decode JSON from T-Invest API response.",
            error_ctx,
        )
        runtime.logger.error(
            "api_json_decode_error_metadata",
            "Logged allowlisted non-JSON response metadata.",
            _build_response_body_ctx(resp, error_ctx),
        )
        raise


post_api = _post_api_impl

# ============ API WRAPPERS ============

def api_get_accounts() -> dict:
    runtime = _require_runtime()
    return post_api(
        "tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts",
        {"status": runtime.account_status},
    )


def api_get_portfolio(account_id: str) -> dict:
    runtime = _require_runtime()
    return post_api(
        "tinkoff.public.invest.api.contract.v1.OperationsService/GetPortfolio",
        {
            "accountId": account_id,
            "currency": runtime.portfolio_currency,
        },
    )


_INSTRUMENT_CACHE: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_INSTRUMENT_CACHE_LOCK = Lock()


def _get_cached_instrument(figi: str) -> Optional[dict]:
    runtime = _require_runtime()
    if runtime.instrument_cache_ttl_seconds <= 0 or runtime.instrument_cache_max_entries <= 0:
        return None

    now = time.monotonic()
    with _INSTRUMENT_CACHE_LOCK:
        cached = _INSTRUMENT_CACHE.get(figi)
        if cached is None:
            return None
        expires_at, instrument = cached
        if expires_at <= now:
            del _INSTRUMENT_CACHE[figi]
            return None
        _INSTRUMENT_CACHE.move_to_end(figi)
        return copy.deepcopy(instrument)


def _cache_instrument(figi: str, instrument: Optional[dict]) -> None:
    runtime = _require_runtime()
    if (
        not instrument
        or runtime.instrument_cache_ttl_seconds <= 0
        or runtime.instrument_cache_max_entries <= 0
    ):
        return

    expires_at = time.monotonic() + runtime.instrument_cache_ttl_seconds
    with _INSTRUMENT_CACHE_LOCK:
        _INSTRUMENT_CACHE[figi] = (expires_at, copy.deepcopy(instrument))
        _INSTRUMENT_CACHE.move_to_end(figi)
        while len(_INSTRUMENT_CACHE) > runtime.instrument_cache_max_entries:
            _INSTRUMENT_CACHE.popitem(last=False)


def api_get_instrument_by_figi(figi: str) -> Optional[dict]:
    cached = _get_cached_instrument(figi)
    if cached is not None:
        return cached

    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy",
        {
            "idType": "INSTRUMENT_ID_TYPE_FIGI",
            "id": figi,
        },
    )
    instrument = data.get("instrument")
    _cache_instrument(figi, instrument)
    return instrument


def api_get_bond_coupons(
    instrument_id: str,
    from_iso: str,
    to_iso: str,
) -> list[dict]:
    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetBondCoupons",
        {
            "instrumentId": instrument_id,
            "from": from_iso,
            "to": to_iso,
        },
    )
    events = data.get("events") or []
    if not isinstance(events, list):
        raise RuntimeError("GetBondCoupons returned invalid events")
    return events


def api_get_dividends(
    instrument_id: str,
    from_iso: str,
    to_iso: str,
) -> list[dict]:
    data = post_api(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/GetDividends",
        {
            "instrumentId": instrument_id,
            "from": from_iso,
            "to": to_iso,
        },
    )
    dividends = data.get("dividends") or []
    if not isinstance(dividends, list):
        raise RuntimeError("GetDividends returned invalid dividends")
    return dividends


class OperationsPaginationError(RuntimeError):
    """Pagination contract violation that must roll back a partial operation sync."""


def _iter_operation_pages(
    account_id: str,
    from_date: Optional[str],
    *,
    to_iso: Optional[str] = None,
    max_pages: Optional[int] = None,
):
    """Yield complete operation pages while enforcing cursor and page limits."""
    from_iso = from_date or "2000-01-01T00:00:00Z"
    fixed_to_iso = to_iso or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    runtime = _require_runtime()
    page_limit = runtime.operations_max_pages if max_pages is None else max(1, max_pages)
    cursor = ""
    seen_cursors = {cursor}

    for page_number in range(1, page_limit + 1):
        payload = {
            "accountId": account_id,
            "from": from_iso,
            "to": fixed_to_iso,
            "cursor": cursor,
            "limit": 1000,
            "withoutTrades": True,
        }
        data = post_api(
            "tinkoff.public.invest.api.contract.v1.OperationsService/GetOperationsByCursor",
            payload,
        )
        operations = data.get("items")
        if operations is None:
            operations = data.get("operations", [])
        if not isinstance(operations, list):
            raise OperationsPaginationError("T-Invest API returned malformed operations items")

        has_next = data.get("hasNext", False)
        if not isinstance(has_next, bool):
            raise OperationsPaginationError("T-Invest API returned non-boolean hasNext")
        runtime.logger.info(
            "operations_page_loaded",
            "Loaded operations page from T-Invest API.",
            {
                "account_id": account_id,
                "page_number": page_number,
                "page_items_count": len(operations),
                "has_next": has_next,
            },
        )
        yield operations

        if not has_next:
            return

        next_cursor = data.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            runtime.logger.error(
                "operations_next_cursor_missing",
                "T-Invest API reported another page without a next cursor.",
                {"account_id": account_id, "page_number": page_number},
            )
            raise OperationsPaginationError("hasNext=true without nextCursor")

        if next_cursor in seen_cursors:
            runtime.logger.error(
                "operations_cursor_repeated",
                "T-Invest API repeated an operations cursor.",
                {"account_id": account_id, "page_number": page_number},
            )
            raise OperationsPaginationError("operations cursor repeated")

        if page_number >= page_limit:
            runtime.logger.error(
                "operations_max_pages_reached",
                "Operations page limit reached before pagination completed.",
                {
                    "account_id": account_id,
                    "page_number": page_number,
                    "max_pages": page_limit,
                },
            )
            raise OperationsPaginationError("operations page limit reached")

        seen_cursors.add(next_cursor)
        cursor = next_cursor


def api_get_operations_by_cursor(account_id: str, opened_iso: Optional[str]):
    """Compatibility iterator over operations backed by guarded page traversal."""
    for operations in _iter_operation_pages(account_id, opened_iso):
        yield from operations
