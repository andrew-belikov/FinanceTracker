"""Canonical reporting-account selection for bot read use cases."""

from __future__ import annotations

from sqlalchemy import text

from financetracker.bot.runtime import TINKOFF_ACCOUNT_ID


def normalize_reporting_account_id(raw_value: str | None) -> str | None:
    value = (raw_value or "").strip()
    if not value or value.lower() == "auto":
        return None
    return value


def choose_reporting_account_id(
    explicit_account_id: str | None,
    latest_snapshot_account_id: str | None,
) -> str | None:
    normalized_explicit = normalize_reporting_account_id(explicit_account_id)
    if normalized_explicit:
        return normalized_explicit

    latest_value = (latest_snapshot_account_id or "").strip()
    return latest_value or None


def get_latest_snapshot_account_id(session) -> str | None:
    return session.execute(
        text(
            """
            SELECT account_id
            FROM portfolio_snapshots
            ORDER BY snapshot_date DESC, snapshot_at DESC, id DESC
            LIMIT 1
            """
        )
    ).scalar()


def resolve_reporting_account_id(session) -> str | None:
    return choose_reporting_account_id(
        TINKOFF_ACCOUNT_ID,
        get_latest_snapshot_account_id(session),
    )
