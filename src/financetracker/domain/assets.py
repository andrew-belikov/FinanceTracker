"""Stable identifiers and deterministic calculations for portfolio assets."""

from decimal import Decimal


SNAPSHOT_TOTAL_FIELDS = {
    "share": "total_shares",
    "bond": "total_bonds",
    "etf": "total_etf",
    "currency": "total_currencies",
    "futures": "total_futures",
    "future": "total_futures",
}


def build_logical_asset_id(
    *, asset_uid: str | None, instrument_uid: str | None, figi: str | None
) -> str | None:
    """Prefer the strongest available identity without fabricating a key."""
    return asset_uid or instrument_uid or figi


def build_asset_alias_lookup(alias_rows: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    by_instrument_uid: dict[str, dict] = {}
    by_figi: dict[str, dict] = {}
    for row in alias_rows:
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid and instrument_uid not in by_instrument_uid:
            by_instrument_uid[instrument_uid] = row
        if figi and figi not in by_figi:
            by_figi[figi] = row
    return by_instrument_uid, by_figi


def build_reconciliation_by_asset_type(
    latest_snapshot: dict,
    positions_rows: list[dict],
) -> tuple[list[dict], Decimal, Decimal]:
    """Compare snapshot totals with the sum of position values by asset type."""
    positions_sum_total = Decimal("0")
    grouped_position_sums: dict[str, Decimal] = {}
    grouped_current_nkd_sums: dict[str, Decimal] = {}

    for row in positions_rows:
        instrument_type = (row.get("instrument_type") or "other").strip().lower()
        position_value = Decimal(str(row.get("position_value") or 0))
        current_nkd = Decimal(str(row.get("current_nkd") or 0))
        positions_sum_total += position_value
        grouped_position_sums[instrument_type] = grouped_position_sums.get(instrument_type, Decimal("0")) + position_value
        grouped_current_nkd_sums[instrument_type] = grouped_current_nkd_sums.get(instrument_type, Decimal("0")) + current_nkd

    reconciliation_rows: list[dict] = []
    for instrument_type, snapshot_field in SNAPSHOT_TOTAL_FIELDS.items():
        if instrument_type == "future":
            continue
        snapshot_total = Decimal(str(latest_snapshot.get(snapshot_field) or 0))
        positions_sum = grouped_position_sums.get(instrument_type, Decimal("0"))
        current_nkd_sum = grouped_current_nkd_sums.get(instrument_type, Decimal("0"))
        reconciliation_rows.append(
            {
                "asset_type": instrument_type,
                "snapshot_total": snapshot_total,
                "positions_sum": positions_sum,
                "gap_abs": snapshot_total - positions_sum,
                "observed_current_nkd_sum": current_nkd_sum,
            }
        )

    reconciliation_gap_abs = Decimal(str(latest_snapshot.get("total_value") or 0)) - positions_sum_total
    return reconciliation_rows, positions_sum_total, reconciliation_gap_abs
