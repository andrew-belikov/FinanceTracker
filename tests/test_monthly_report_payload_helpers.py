import unittest
from datetime import date
from decimal import Decimal

from financetracker.reporting import report_payload


def resolve_monthly_asset_identity(
    row: dict,
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> dict:
    alias_row = report_payload._pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
    return report_payload._build_asset_identity(row, alias_row=alias_row)


def build_monthly_position_flow_groups(
    start_positions: list[dict],
    end_positions: list[dict],
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> dict[str, list[dict]]:
    normalized_start = report_payload._normalize_positions(
        start_positions, alias_by_instrument_uid, alias_by_figi
    )
    normalized_end = report_payload._normalize_positions(end_positions, alias_by_instrument_uid, alias_by_figi)
    return report_payload.build_position_flow_groups(normalized_start, normalized_end)


def build_monthly_instrument_payload(
    eod_rows: list[dict],
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> tuple[list[dict], dict[str, list[dict]]]:
    timeseries = report_payload.build_instrument_eod_timeseries(
        eod_rows, alias_by_instrument_uid, alias_by_figi
    )
    return timeseries, report_payload.build_instrument_movers(timeseries, limit=10)


class MonthlyReportPayloadHelperTests(unittest.TestCase):
    def test_resolve_monthly_asset_identity_uses_alias_lookup(self):
        alias_by_instrument_uid = {
            "inst-1": {
                "asset_uid": "asset-1",
                "instrument_uid": "inst-1",
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha Asset",
            }
        }

        identity = resolve_monthly_asset_identity(
            {
                "instrument_uid": "inst-1",
                "figi": "FIGI-1",
                "ticker": "",
                "name": "",
                "asset_uid": None,
            },
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi={},
        )

        self.assertEqual(identity["logical_asset_id"], "asset-1")
        self.assertEqual(identity["ticker"], "AAA")
        self.assertEqual(identity["name"], "Alpha Asset")

    def test_build_monthly_position_flow_groups_splits_core_buckets(self):
        alias_lookup = {}
        start_positions = [
            {
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "instrument_type": "share",
                "quantity": 1,
                "position_value": 100,
            },
            {
                "figi": "FIGI-2",
                "ticker": "BBB",
                "name": "Beta",
                "instrument_uid": "inst-2",
                "asset_uid": "asset-2",
                "instrument_type": "bond",
                "quantity": 4,
                "position_value": 80,
            },
        ]
        end_positions = [
            {
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "instrument_type": "share",
                "quantity": 3,
                "position_value": 130,
            },
            {
                "figi": "FIGI-3",
                "ticker": "CCC",
                "name": "Gamma",
                "instrument_uid": "inst-3",
                "asset_uid": "asset-3",
                "instrument_type": "etf",
                "quantity": 2,
                "position_value": 200,
            },
        ]

        groups = build_monthly_position_flow_groups(
            start_positions,
            end_positions,
            alias_by_instrument_uid=alias_lookup,
            alias_by_figi=alias_lookup,
        )

        self.assertEqual(len(groups["new"]), 1)
        self.assertEqual(len(groups["closed"]), 1)
        self.assertEqual(len(groups["increased"]), 1)
        self.assertEqual(len(groups["decreased"]), 0)
        self.assertEqual(groups["new"][0]["logical_asset_id"], "asset-3")
        self.assertEqual(groups["closed"][0]["logical_asset_id"], "asset-2")
        self.assertEqual(groups["increased"][0]["delta_qty"], Decimal("2"))

    def test_build_monthly_instrument_payload_groups_by_asset(self):
        eod_rows = [
            {
                "snapshot_id": 1,
                "snapshot_date": date(2026, 4, 1),
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_type": "share",
                "quantity": 1,
                "position_value": 100,
                "expected_yield": 10,
                "expected_yield_pct": 10,
                "weight_pct": 25,
            },
            {
                "snapshot_id": 2,
                "snapshot_date": date(2026, 4, 2),
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_type": "share",
                "quantity": 2,
                "position_value": 140,
                "expected_yield": 14,
                "expected_yield_pct": 10,
                "weight_pct": 30,
            },
            {
                "snapshot_id": 1,
                "snapshot_date": date(2026, 4, 1),
                "instrument_uid": "inst-2",
                "asset_uid": "asset-2",
                "figi": "FIGI-2",
                "ticker": "BBB",
                "name": "Beta",
                "instrument_type": "bond",
                "quantity": 4,
                "position_value": 80,
                "expected_yield": 5,
                "expected_yield_pct": 6,
                "weight_pct": 20,
            },
        ]

        payload_rows, movers = build_monthly_instrument_payload(
            eod_rows,
            alias_by_instrument_uid={},
            alias_by_figi={},
        )

        self.assertEqual(len(payload_rows), 2)
        self.assertEqual(payload_rows[0]["logical_asset_id"], "asset-1")
        self.assertEqual(payload_rows[0]["stats"]["eod_end_position_value"], Decimal("140"))
        self.assertEqual(len(payload_rows[0]["series"]), 2)
        self.assertEqual(movers["top_growth"][0]["logical_asset_id"], "asset-1")
        self.assertEqual(movers["top_drawdown"], [])


if __name__ == "__main__":
    unittest.main()
