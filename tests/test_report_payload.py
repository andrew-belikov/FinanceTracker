import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_payload


class ReportPayloadSerializationTests(unittest.TestCase):
    def test_serialize_monthly_report_payload_normalizes_nested_types(self):
        payload = {
            "amount": Decimal("12.50"),
            "date": date(2026, 4, 30),
            "datetime": datetime(2026, 4, 30, 12, 15, tzinfo=timezone.utc),
            "nested": {
                "tuple_values": (Decimal("1.0"), date(2026, 4, 1)),
                "list_values": [Decimal("2.25"), None],
            },
        }

        serialized = report_payload.serialize_monthly_report_payload(payload)

        self.assertEqual(serialized["amount"], "12.50")
        self.assertEqual(serialized["date"], "2026-04-30")
        self.assertEqual(serialized["datetime"], "2026-04-30T12:15:00+00:00")
        self.assertEqual(serialized["nested"]["tuple_values"], ["1.0", "2026-04-01"])
        self.assertEqual(serialized["nested"]["list_values"], ["2.25", None])


class PositionFlowGroupsTests(unittest.TestCase):
    def test_build_position_flow_groups_groups_by_direction_and_fallback_id(self):
        start_positions = [
            {
                "ticker": "SBER",
                "name": "Sber",
                "instrument_type": "share",
                "quantity": Decimal("10"),
                "position_value": Decimal("400"),
            },
            {
                "figi": "figi-bond",
                "ticker": "BOND",
                "name": "Bond",
                "instrument_type": "bond",
                "quantity": Decimal("5"),
                "position_value": Decimal("300"),
            },
            {
                "ticker": "OLD",
                "name": "Closed",
                "instrument_type": "share",
                "quantity": Decimal("4"),
                "position_value": Decimal("200"),
            },
        ]
        end_positions = [
            {
                "ticker": "SBER",
                "name": "Sber",
                "instrument_type": "share",
                "quantity": Decimal("12"),
                "position_value": Decimal("500"),
            },
            {
                "figi": "figi-bond",
                "ticker": "BOND",
                "name": "Bond",
                "instrument_type": "bond",
                "quantity": Decimal("3"),
                "position_value": Decimal("220"),
            },
            {
                "ticker": "NEW",
                "name": "New",
                "instrument_type": "etf",
                "quantity": Decimal("8"),
                "position_value": Decimal("600"),
            },
        ]

        groups = report_payload.build_position_flow_groups(start_positions, end_positions)

        self.assertEqual([row["logical_asset_id"] for row in groups["new"]], ["NEW"])
        self.assertEqual([row["logical_asset_id"] for row in groups["closed"]], ["OLD"])
        self.assertEqual([row["logical_asset_id"] for row in groups["increased"]], ["SBER"])
        self.assertEqual([row["logical_asset_id"] for row in groups["decreased"]], ["figi-bond"])
        self.assertEqual(groups["increased"][0]["delta_qty"], Decimal("2"))
        self.assertEqual(groups["increased"][0]["delta_value"], Decimal("100"))
        self.assertEqual(groups["decreased"][0]["delta_qty"], Decimal("-2"))
        self.assertEqual(groups["decreased"][0]["delta_value"], Decimal("-80"))


class InstrumentEodTimeseriesTests(unittest.TestCase):
    def test_build_instrument_eod_timeseries_groups_points_and_stats(self):
        snapshot_rows = [
            {"id": 1, "snapshot_date": date(2026, 4, 1)},
            {"id": 2, "snapshot_date": date(2026, 4, 2)},
            {"id": 3, "snapshot_date": date(2026, 4, 3)},
        ]
        positions_by_snapshot_id = {
            1: [
                {
                    "ticker": "SBER",
                    "instrument_type": "share",
                    "quantity": Decimal("10"),
                    "position_value": Decimal("100"),
                    "expected_yield": Decimal("10"),
                    "expected_yield_pct": Decimal("10"),
                    "weight_pct": Decimal("25"),
                },
                {
                    "ticker": "BOND",
                    "instrument_type": "bond",
                    "quantity": Decimal("5"),
                    "position_value": Decimal("200"),
                    "expected_yield": Decimal("20"),
                    "expected_yield_pct": Decimal("10"),
                    "weight_pct": Decimal("50"),
                },
            ],
            2: [
                {
                    "ticker": "SBER",
                    "instrument_type": "share",
                    "quantity": Decimal("11"),
                    "position_value": Decimal("130"),
                    "expected_yield": Decimal("15"),
                    "expected_yield_pct": Decimal("11.5"),
                    "weight_pct": Decimal("30"),
                },
                {
                    "ticker": "BOND",
                    "instrument_type": "bond",
                    "quantity": Decimal("5"),
                    "position_value": Decimal("180"),
                    "expected_yield": Decimal("18"),
                    "expected_yield_pct": Decimal("10"),
                    "weight_pct": Decimal("45"),
                },
            ],
            3: [
                {
                    "ticker": "SBER",
                    "instrument_type": "share",
                    "quantity": Decimal("12"),
                    "position_value": Decimal("120"),
                    "expected_yield": Decimal("8"),
                    "expected_yield_pct": Decimal("6.5"),
                    "weight_pct": Decimal("28"),
                },
                {
                    "ticker": "BOND",
                    "instrument_type": "bond",
                    "quantity": Decimal("4"),
                    "position_value": Decimal("220"),
                    "expected_yield": Decimal("25"),
                    "expected_yield_pct": Decimal("11.3"),
                    "weight_pct": Decimal("52"),
                },
            ],
        }

        series = report_payload.build_instrument_eod_timeseries(snapshot_rows, positions_by_snapshot_id)

        self.assertEqual([row["ticker"] for row in series], ["BOND", "SBER"])
        sber = next(row for row in series if row["ticker"] == "SBER")
        self.assertEqual([point["date"] for point in sber["series"]], [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)])
        self.assertEqual(sber["stats"]["eod_min_position_value"], Decimal("100"))
        self.assertEqual(sber["stats"]["eod_max_position_value"], Decimal("130"))
        self.assertEqual(sber["stats"]["eod_end_position_value"], Decimal("120"))
        self.assertEqual(sber["stats"]["month_change_abs"], Decimal("20"))
        self.assertEqual(sber["stats"]["month_change_pct"], Decimal("20"))
        self.assertEqual(sber["stats"]["max_rise_abs"], Decimal("30"))
        self.assertEqual(sber["stats"]["max_drawdown_abs"], Decimal("-30"))


class MonthlyReportPayloadBuilderTests(unittest.TestCase):
    def test_build_monthly_report_payload_serializes_contract_and_summary(self):
        session = object()
        month_daily_rows = [
            {
                "id": 101,
                "snapshot_date": date(2026, 4, 1),
                "snapshot_at": datetime(2026, 4, 1, 21, 0, tzinfo=timezone.utc),
                "currency": "RUB",
                "total_value": Decimal("1100"),
                "expected_yield": Decimal("25"),
                "expected_yield_pct": Decimal("2.27"),
            },
            {
                "id": 102,
                "snapshot_date": date(2026, 4, 2),
                "snapshot_at": datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc),
                "currency": "RUB",
                "total_value": Decimal("1200"),
                "expected_yield": Decimal("35"),
                "expected_yield_pct": Decimal("2.92"),
            },
        ]
        start_snapshot = {
            "id": 10,
            "snapshot_date": date(2026, 3, 31),
            "snapshot_at": datetime(2026, 3, 31, 21, 0, tzinfo=timezone.utc),
            "total_value": Decimal("1000"),
        }
        end_snapshot = {
            "id": 20,
            "snapshot_date": date(2026, 4, 30),
            "snapshot_at": datetime(2026, 4, 30, 21, 0, tzinfo=timezone.utc),
            "total_value": Decimal("1200"),
        }
        end_snapshot_with_totals = {
            **end_snapshot,
            "currency": "RUB",
            "total_shares": Decimal("500"),
            "total_bonds": Decimal("200"),
            "total_etf": Decimal("500"),
            "total_currencies": Decimal("0"),
            "total_futures": Decimal("0"),
        }
        start_positions = [
            {
                "ticker": "SBER",
                "name": "Sber",
                "instrument_type": "share",
                "quantity": Decimal("10"),
                "position_value": Decimal("400"),
                "expected_yield": Decimal("20"),
                "expected_yield_pct": Decimal("5"),
                "weight_pct": Decimal("40"),
            },
            {
                "ticker": "BOND",
                "name": "Bond",
                "instrument_type": "bond",
                "quantity": Decimal("5"),
                "position_value": Decimal("300"),
                "expected_yield": Decimal("15"),
                "expected_yield_pct": Decimal("5"),
                "weight_pct": Decimal("30"),
            },
            {
                "ticker": "OLD",
                "name": "Closed",
                "instrument_type": "share",
                "quantity": Decimal("4"),
                "position_value": Decimal("300"),
                "expected_yield": Decimal("12"),
                "expected_yield_pct": Decimal("4"),
                "weight_pct": Decimal("30"),
            },
        ]
        end_positions = [
            {
                "ticker": "SBER",
                "name": "Sber",
                "instrument_type": "share",
                "quantity": Decimal("12"),
                "position_value": Decimal("500"),
                "expected_yield": Decimal("25"),
                "expected_yield_pct": Decimal("5"),
                "weight_pct": Decimal("41.7"),
            },
            {
                "ticker": "BOND",
                "name": "Bond",
                "instrument_type": "bond",
                "quantity": Decimal("3"),
                "position_value": Decimal("200"),
                "expected_yield": Decimal("10"),
                "expected_yield_pct": Decimal("5"),
                "weight_pct": Decimal("16.7"),
            },
            {
                "ticker": "NEW",
                "name": "New",
                "instrument_type": "etf",
                "quantity": Decimal("8"),
                "position_value": Decimal("500"),
                "expected_yield": Decimal("0"),
                "expected_yield_pct": Decimal("0"),
                "weight_pct": Decimal("41.7"),
            },
        ]
        positions_by_snapshot_id = {
            101: [
                {
                    "ticker": "SBER",
                    "instrument_type": "share",
                    "quantity": Decimal("10"),
                    "position_value": Decimal("420"),
                    "expected_yield": Decimal("22"),
                    "expected_yield_pct": Decimal("5.2"),
                    "weight_pct": Decimal("38"),
                },
                {
                    "ticker": "BOND",
                    "instrument_type": "bond",
                    "quantity": Decimal("5"),
                    "position_value": Decimal("300"),
                    "expected_yield": Decimal("15"),
                    "expected_yield_pct": Decimal("5"),
                    "weight_pct": Decimal("27"),
                },
            ],
            102: [
                {
                    "ticker": "SBER",
                    "instrument_type": "share",
                    "quantity": Decimal("11"),
                    "position_value": Decimal("500"),
                    "expected_yield": Decimal("30"),
                    "expected_yield_pct": Decimal("6"),
                    "weight_pct": Decimal("41.7"),
                },
                {
                    "ticker": "BOND",
                    "instrument_type": "bond",
                    "quantity": Decimal("4"),
                    "position_value": Decimal("200"),
                    "expected_yield": Decimal("20"),
                    "expected_yield_pct": Decimal("10"),
                    "weight_pct": Decimal("16.7"),
                },
            ],
        }
        operations_rows = [
            {
                "operation_id": "dep-1",
                "date": datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc),
                "amount": Decimal("100"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_INPUT",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": None,
                "asset_uid": None,
                "figi": None,
                "name": "Deposit",
                "commission": Decimal("0"),
                "yield": Decimal("0"),
                "description": "Пополнение",
                "source": "bank",
                "price": None,
                "quantity": None,
            },
            {
                "operation_id": "fee-1",
                "date": datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                "amount": Decimal("-2"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_BROKER_FEE",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": None,
                "asset_uid": None,
                "figi": None,
                "name": "Fee",
                "commission": Decimal("-2"),
                "yield": Decimal("0"),
                "description": "Комиссия",
                "source": "broker",
                "price": None,
                "quantity": None,
            },
            {
                "operation_id": "tax-1",
                "date": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
                "amount": Decimal("-3"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_TAX",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": None,
                "asset_uid": None,
                "figi": None,
                "name": "Tax",
                "commission": Decimal("0"),
                "yield": Decimal("0"),
                "description": "Налог",
                "source": "broker",
                "price": None,
                "quantity": None,
            },
        ]
        income_rows = [
            {
                "event_date": date(2026, 4, 2),
                "event_type": "coupon",
                "figi": "figi-sber",
                "ticker": "SBER",
                "instrument_name": "Sber",
                "gross_amount": Decimal("15"),
                "tax_amount": Decimal("-2"),
                "net_amount": Decimal("13"),
                "net_yield_pct": Decimal("1.3"),
                "notified": True,
            },
            {
                "event_date": date(2026, 4, 2),
                "event_type": "dividend",
                "figi": "figi-bond",
                "ticker": "BOND",
                "instrument_name": "Bond",
                "gross_amount": Decimal("25"),
                "tax_amount": Decimal("-5"),
                "net_amount": Decimal("20"),
                "net_yield_pct": Decimal("2.0"),
                "notified": False,
            },
        ]

        with (
            mock.patch.object(report_payload, "get_daily_snapshot_rows", return_value=month_daily_rows),
            mock.patch.object(report_payload, "get_month_snapshots", return_value=(start_snapshot, end_snapshot)),
            mock.patch.object(report_payload, "get_latest_snapshot_with_totals_before_date", return_value=end_snapshot_with_totals),
            mock.patch.object(report_payload, "get_positions_for_snapshot", side_effect=lambda _session, snapshot_id: {
                10: start_positions,
                20: end_positions,
                101: positions_by_snapshot_id[101],
                102: positions_by_snapshot_id[102],
            }[snapshot_id]),
            mock.patch.object(report_payload, "get_asset_alias_rows", return_value=[]),
            mock.patch.object(report_payload, "get_dataset_operations", return_value=operations_rows),
            mock.patch.object(report_payload, "get_income_events_for_period", return_value=income_rows),
            mock.patch.object(report_payload, "get_income_for_period", return_value=(Decimal("13"), Decimal("20"))),
            mock.patch.object(report_payload, "get_deposits_for_period", return_value=52000.0),
            mock.patch.object(
                report_payload,
                "compute_realized_by_asset",
                return_value=([
                    {
                        "figi": "figi-sber",
                        "ticker": "SBER",
                        "name": "Sber",
                        "amount": Decimal("5.50"),
                    }
                ], Decimal("5.50")),
            ),
            mock.patch.object(
                report_payload,
                "compute_income_by_asset_net",
                return_value=([
                    {
                        "figi": "figi-sber",
                        "ticker": "SBER",
                        "name": "Sber",
                        "amount": Decimal("33"),
                        "income_kind": "income",
                    }
                ], Decimal("33")),
            ),
            mock.patch.object(report_payload, "get_rebalance_targets", return_value={
                "stocks": Decimal("50"),
                "bonds": Decimal("30"),
                "etf": Decimal("20"),
                "currency": Decimal("0"),
            }),
            mock.patch.object(report_payload, "resolve_reporting_account_id", return_value="acct-1"),
        ):
            payload = report_payload.build_monthly_report_payload(
                session,
                year=2026,
                month=4,
                account_id="acct-1",
                now=datetime(2026, 4, 15, tzinfo=timezone.utc),
            )

        self.assertEqual(payload["schema_version"], "monthly_report_payload.v1")
        self.assertEqual(payload["meta"]["report_kind"], "monthly_review")
        self.assertEqual(payload["meta"]["period_label_ru"], "апреля 2026")
        self.assertEqual(payload["meta"]["period_start"], "2026-04-01")
        self.assertEqual(payload["meta"]["period_end"], "2026-04-30")
        self.assertEqual(payload["summary_metrics"]["current_value"], "1200")
        self.assertEqual(payload["summary_metrics"]["period_pnl_abs"], "72")
        self.assertEqual(payload["summary_metrics"]["deposits_ytd"], "52000.0")
        self.assertIsInstance(payload["summary_metrics"]["period_twr_pct"], str)
        self.assertEqual(payload["position_flow_groups"]["new"][0]["logical_asset_id"], "NEW")
        self.assertEqual(payload["position_flow_groups"]["increased"][0]["delta_qty"], "2")
        sber_row = next(row for row in payload["instrument_eod_timeseries"] if row["ticker"] == "SBER")
        self.assertEqual(sber_row["stats"]["month_change_abs"], "80")
        self.assertEqual(payload["instrument_movers"]["top_growth"][0]["ticker"], "SBER")
        self.assertEqual(payload["income_events"][0]["event_date"], "2026-04-02")
        self.assertEqual(payload["open_pl_end"][0]["amount"], "25")
        self.assertEqual(payload["data_quality"]["has_rebalance_targets"], True)
        self.assertEqual(payload["rebalance_snapshot"]["snapshot_date"], "2026-04-30")


if __name__ == "__main__":
    unittest.main()
