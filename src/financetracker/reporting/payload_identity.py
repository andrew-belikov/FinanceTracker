"""Asset identity and position-flow transformations for monthly reporting."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from financetracker.domain.assets import build_logical_asset_id
from financetracker.reporting.runtime import normalize_decimal


def pick_alias_row(
    row: dict[str, Any],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    instrument_uid = row.get("instrument_uid")
    figi = row.get("figi")
    if instrument_uid:
        alias_row = alias_by_instrument_uid.get(instrument_uid)
        if alias_row is not None:
            return alias_row
    if figi:
        return alias_by_figi.get(figi)
    return None


def build_asset_identity(
    row: dict[str, Any],
    *,
    alias_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
    instrument_uid = row.get("instrument_uid") or (alias_row.get("instrument_uid") if alias_row is not None else None)
    figi = row.get("figi") or (alias_row.get("figi") if alias_row is not None else None)
    ticker = (row.get("ticker") or (alias_row.get("ticker") if alias_row is not None else "")).strip()
    name = (row.get("name") or row.get("instrument_name") or (alias_row.get("name") if alias_row is not None else "")).strip()

    logical_asset_id = build_logical_asset_id(
        asset_uid=asset_uid,
        instrument_uid=instrument_uid,
        figi=figi,
    )
    if logical_asset_id is None:
        logical_asset_id = ticker or name or figi or "unknown_asset"

    return {
        "logical_asset_id": logical_asset_id,
        "asset_uid": asset_uid,
        "instrument_uid": instrument_uid,
        "figi": figi,
        "ticker": ticker,
        "name": name,
    }


def normalize_positions(
    rows: list[dict[str, Any]],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        alias_row = pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
        identity = build_asset_identity(row, alias_row=alias_row)
        normalized.append(
            {
                **identity,
                "ticker": identity["ticker"] or (row.get("ticker") or ""),
                "name": identity["name"] or (row.get("name") or identity["figi"] or ""),
                "instrument_type": row.get("instrument_type"),
                "quantity": normalize_decimal(row.get("quantity")),
                "currency": row.get("currency"),
                "position_value": normalize_decimal(row.get("position_value")),
                "expected_yield": normalize_decimal(row.get("expected_yield")),
                "expected_yield_pct": normalize_decimal(row.get("expected_yield_pct")),
                "weight_pct": normalize_decimal(row.get("weight_pct")),
            }
        )
    normalized.sort(
        key=lambda item: (
            normalize_decimal(item.get("position_value")),
            item.get("ticker") or "",
            item.get("name") or "",
        ),
        reverse=True,
    )
    return normalized


def build_position_flow_groups(
    start_positions: list[dict[str, Any]],
    end_positions: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    start_map = {row["logical_asset_id"]: row for row in start_positions}
    end_map = {row["logical_asset_id"]: row for row in end_positions}

    grouped: dict[str, list[dict[str, Any]]] = {
        "new": [],
        "closed": [],
        "increased": [],
        "decreased": [],
    }

    for logical_asset_id in sorted(set(start_map) | set(end_map)):
        start_row = start_map.get(logical_asset_id)
        end_row = end_map.get(logical_asset_id)
        if start_row is None and end_row is None:
            continue

        start_qty = normalize_decimal(start_row.get("quantity")) if start_row is not None else Decimal("0")
        end_qty = normalize_decimal(end_row.get("quantity")) if end_row is not None else Decimal("0")
        start_value = normalize_decimal(start_row.get("position_value")) if start_row is not None else Decimal("0")
        end_value = normalize_decimal(end_row.get("position_value")) if end_row is not None else Decimal("0")

        base_row = end_row or start_row or {}
        item = {
            "logical_asset_id": logical_asset_id,
            "ticker": base_row.get("ticker") or "",
            "name": base_row.get("name") or base_row.get("figi") or logical_asset_id,
            "instrument_type": base_row.get("instrument_type"),
            "start_qty": start_qty,
            "end_qty": end_qty,
            "delta_qty": end_qty - start_qty,
            "start_value": start_value,
            "end_value": end_value,
            "delta_value": end_value - start_value,
        }

        if start_row is None and end_row is not None:
            grouped["new"].append(item)
        elif start_row is not None and end_row is None:
            grouped["closed"].append(item)
        elif end_qty > start_qty:
            grouped["increased"].append(item)
        elif end_qty < start_qty:
            grouped["decreased"].append(item)

    for items in grouped.values():
        items.sort(
            key=lambda item: (
                abs(normalize_decimal(item["delta_value"])),
                abs(normalize_decimal(item["delta_qty"])),
                item["ticker"] or "",
                item["name"] or "",
            ),
            reverse=True,
        )

    return grouped
