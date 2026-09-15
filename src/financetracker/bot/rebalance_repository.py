"""PostgreSQL persistence for rebalance targets."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text

from financetracker.bot.runtime import decimal_to_str, normalize_decimal


def get_rebalance_targets(
    session,
    account_id: str,
    asset_classes: tuple[str, ...],
) -> dict[str, Decimal]:
    rows = (
        session.execute(
            text(
                """
                SELECT asset_class, target_weight_pct
                FROM rebalance_targets
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    targets = {asset_class: Decimal("0") for asset_class in asset_classes}
    for row in rows:
        asset_class = row["asset_class"]
        if asset_class in targets:
            targets[asset_class] = normalize_decimal(row["target_weight_pct"])
    return targets


def replace_rebalance_targets(
    session,
    account_id: str,
    targets: dict[str, Decimal],
    asset_classes: tuple[str, ...],
) -> bool:
    try:
        now_utc = datetime.now(timezone.utc)
        session.execute(
            text("DELETE FROM rebalance_targets WHERE account_id = :account_id"),
            {"account_id": account_id},
        )
        session.execute(
            text(
                """
                INSERT INTO rebalance_targets (
                    account_id, asset_class, target_weight_pct, created_at, updated_at
                ) VALUES (
                    :account_id, :asset_class, :target_weight_pct, :created_at, :updated_at
                )
                """
            ),
            [
                {
                    "account_id": account_id,
                    "asset_class": asset_class,
                    "target_weight_pct": decimal_to_str(targets[asset_class]),
                    "created_at": now_utc,
                    "updated_at": now_utc,
                }
                for asset_class in asset_classes
            ],
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return True
