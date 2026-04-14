import sys
import unittest
from contextlib import ExitStack
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_payload  # noqa: E402


class FakeSession:
    pass


class ReportPayloadHelpersTests(unittest.TestCase):
    def test_serialize_report_payload_handles_decimal_dates_and_nested_data(self):
        payload = {
            "amount": Decimal("12.34"),
            "when": date(2026, 4, 15),
            "at": datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc),
            "nested": [{"value": Decimal("1.5")}],
        }

        serialized = report_payload.serialize_report_payload(payload)

        self.assertEqual(serialized["amount"], "12.34")
        self.assertEqual(serialized["when"], "2026-04-15")
        self.assertEqual(serialized["at"], "2026-04-15T09:30:00+00:00")
        self.assertEqual(serialized["nested"][0]["value"], "1.5")

    def test_build_position_flow_groups_splits_changes_into_expected_buckets(self):
        start_positions = [
            {
                "logical_asset_id": "eqmx",
                "ticker": "EQMX",
                "name": "EQMX",
                "instrument_type": "etf",
                "quantity": Decimal("10"),
                "position_value": Decimal("1000"),
            },
            {
                "logical_asset_id": "gazp",
                "ticker": "GAZP",
                "name": "GAZP",
                "instrument_type": "share",
                "quantity": Decimal("5"),
                "position_value": Decimal("500"),
            },
        ]
        end_positions = [
            {
                "logical_asset_id": "eqmx",
                "ticker": "EQMX",
                "name": "EQMX",
                "instrument_type": "etf",
                "quantity": Decimal("15"),
                "position_value": Decimal("1600"),
            },
            {
                "logical_asset_id": "bond1",
                "ticker": "BOND1",
                "name": "Bond 1",
                "instrument_type": "bond",
                "quantity": Decimal("3"),
                "position_value": Decimal("300"),
            },
        ]

        groups = report_payload.build_position_flow_groups(start_positions, end_positions)

        self.assertEqual([row["ticker"] for row in groups["new"]], ["BOND1"])
        self.assertEqual([row["ticker"] for row in groups["closed"]], ["GAZP"])
        self.assertEqual([row["ticker"] for row in groups["increased"]], ["EQMX"])
        self.assertEqual(groups["decreased"], [])

    def test_build_instrument_eod_timeseries_computes_stats_and_movers(self):
        alias_by_instrument_uid = {}
        alias_by_figi = {
            "figi-eqmx": {
                "asset_uid": "asset-eqmx",
                "instrument_uid": "inst-eqmx",
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
            }
        }
        rows = [
            {
                "snapshot_id": 101,
                "snapshot_date": date(2026, 4, 1),
                "snapshot_at": datetime(2026, 4, 1, 18, tzinfo=timezone.utc),
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("10"),
                "currency": "RUB",
                "position_value": Decimal("1000"),
                "expected_yield": Decimal("-50"),
                "expected_yield_pct": Decimal("-5"),
                "weight_pct": Decimal("20"),
            },
            {
                "snapshot_id": 102,
                "snapshot_date": date(2026, 4, 2),
                "snapshot_at": datetime(2026, 4, 2, 18, tzinfo=timezone.utc),
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("10"),
                "currency": "RUB",
                "position_value": Decimal("1200"),
                "expected_yield": Decimal("120"),
                "expected_yield_pct": Decimal("12"),
                "weight_pct": Decimal("24"),
            },
            {
                "snapshot_id": 103,
                "snapshot_date": date(2026, 4, 3),
                "snapshot_at": datetime(2026, 4, 3, 18, tzinfo=timezone.utc),
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("10"),
                "currency": "RUB",
                "position_value": Decimal("1100"),
                "expected_yield": Decimal("40"),
                "expected_yield_pct": Decimal("4"),
                "weight_pct": Decimal("22"),
            },
        ]

        series = report_payload.build_instrument_eod_timeseries(rows, alias_by_instrument_uid, alias_by_figi)
        movers = report_payload.build_instrument_movers(series)

        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["logical_asset_id"], "asset-eqmx")
        self.assertEqual(series[0]["stats"]["eod_min_position_value"], Decimal("1000"))
        self.assertEqual(series[0]["stats"]["max_rise_abs"], Decimal("170"))
        self.assertEqual(series[0]["stats"]["max_drawdown_abs"], Decimal("-80"))
        self.assertEqual(movers["top_growth"][0]["ticker"], "EQMX")
        self.assertEqual(movers["top_drawdown"][0]["ticker"], "EQMX")


class MonthlyReportPayloadBuilderTests(unittest.TestCase):
    def _build_payload(self):
        fake_session = FakeSession()
        start_snapshot = {
            "id": 10,
            "snapshot_date": date(2026, 3, 31),
            "snapshot_at": datetime(2026, 3, 31, 18, tzinfo=timezone.utc),
            "total_value": Decimal("10000"),
        }
        end_snapshot = {
            "id": 30,
            "snapshot_date": date(2026, 4, 30),
            "snapshot_at": datetime(2026, 4, 30, 18, tzinfo=timezone.utc),
            "total_value": Decimal("11150"),
        }
        daily_rows = [
            {
                "id": 21,
                "snapshot_date": date(2026, 4, 1),
                "snapshot_at": datetime(2026, 4, 1, 18, tzinfo=timezone.utc),
                "currency": "RUB",
                "total_value": Decimal("10200"),
                "expected_yield": Decimal("200"),
                "expected_yield_pct": Decimal("2"),
            },
            {
                "id": 22,
                "snapshot_date": date(2026, 4, 2),
                "snapshot_at": datetime(2026, 4, 2, 18, tzinfo=timezone.utc),
                "currency": "RUB",
                "total_value": Decimal("10700"),
                "expected_yield": Decimal("420"),
                "expected_yield_pct": Decimal("4.1"),
            },
            {
                "id": 30,
                "snapshot_date": date(2026, 4, 30),
                "snapshot_at": datetime(2026, 4, 30, 18, tzinfo=timezone.utc),
                "currency": "RUB",
                "total_value": Decimal("11150"),
                "expected_yield": Decimal("315"),
                "expected_yield_pct": Decimal("2.9"),
            },
        ]
        start_positions = [
            {
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("10"),
                "currency": "RUB",
                "current_price": Decimal("100"),
                "current_nkd": Decimal("0"),
                "position_value": Decimal("1000"),
                "expected_yield": Decimal("100"),
                "expected_yield_pct": Decimal("10"),
                "weight_pct": Decimal("10"),
            },
            {
                "figi": "figi-gazp",
                "ticker": "GAZP",
                "name": "Gazprom",
                "instrument_uid": "inst-gazp",
                "asset_uid": "asset-gazp",
                "instrument_type": "share",
                "quantity": Decimal("5"),
                "currency": "RUB",
                "current_price": Decimal("100"),
                "current_nkd": Decimal("0"),
                "position_value": Decimal("500"),
                "expected_yield": Decimal("-50"),
                "expected_yield_pct": Decimal("-10"),
                "weight_pct": Decimal("5"),
            },
        ]
        end_positions = [
            {
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("14"),
                "currency": "RUB",
                "current_price": Decimal("110"),
                "current_nkd": Decimal("0"),
                "position_value": Decimal("1540"),
                "expected_yield": Decimal("220"),
                "expected_yield_pct": Decimal("16"),
                "weight_pct": Decimal("13.8"),
            },
            {
                "figi": "figi-bond1",
                "ticker": "BOND1",
                "name": "Bond 1",
                "instrument_uid": "inst-bond1",
                "asset_uid": "asset-bond1",
                "instrument_type": "bond",
                "quantity": Decimal("3"),
                "currency": "RUB",
                "current_price": Decimal("100"),
                "current_nkd": Decimal("5"),
                "position_value": Decimal("305"),
                "expected_yield": Decimal("95"),
                "expected_yield_pct": Decimal("4"),
                "weight_pct": Decimal("2.7"),
            },
        ]
        alias_rows = [
            {
                "asset_uid": "asset-eqmx",
                "instrument_uid": "inst-eqmx",
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "first_seen_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "last_seen_at": datetime(2026, 4, 30, tzinfo=timezone.utc),
            },
            {
                "asset_uid": "asset-gazp",
                "instrument_uid": "inst-gazp",
                "figi": "figi-gazp",
                "ticker": "GAZP",
                "name": "Gazprom",
                "first_seen_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "last_seen_at": datetime(2026, 3, 31, tzinfo=timezone.utc),
            },
            {
                "asset_uid": "asset-bond1",
                "instrument_uid": "inst-bond1",
                "figi": "figi-bond1",
                "ticker": "BOND1",
                "name": "Bond 1",
                "first_seen_at": datetime(2026, 4, 2, tzinfo=timezone.utc),
                "last_seen_at": datetime(2026, 4, 30, tzinfo=timezone.utc),
            },
        ]
        operations_rows = [
            {
                "operation_id": "dep-1",
                "date": datetime(2026, 4, 1, 10, tzinfo=timezone.utc),
                "amount": Decimal("5000"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_INPUT",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": None,
                "asset_uid": None,
                "figi": None,
                "name": "Пополнение",
                "commission": Decimal("0"),
                "yield": Decimal("0"),
                "description": "deposit",
                "source": "broker",
                "price": Decimal("0"),
                "quantity": Decimal("0"),
            },
            {
                "operation_id": "buy-1",
                "date": datetime(2026, 4, 2, 10, tzinfo=timezone.utc),
                "amount": Decimal("-440"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_BUY",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": "inst-eqmx",
                "asset_uid": "asset-eqmx",
                "figi": "figi-eqmx",
                "name": "EQMX ETF",
                "commission": Decimal("-10"),
                "yield": Decimal("0"),
                "description": "buy",
                "source": "broker",
                "price": Decimal("110"),
                "quantity": Decimal("4"),
            },
            {
                "operation_id": "sell-1",
                "date": datetime(2026, 4, 15, 10, tzinfo=timezone.utc),
                "amount": Decimal("420"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_SELL",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": "inst-gazp",
                "asset_uid": "asset-gazp",
                "figi": "figi-gazp",
                "name": "Gazprom",
                "commission": Decimal("-5"),
                "yield": Decimal("-80"),
                "description": "sell",
                "source": "broker",
                "price": Decimal("84"),
                "quantity": Decimal("5"),
            },
            {
                "operation_id": "fee-1",
                "date": datetime(2026, 4, 20, 10, tzinfo=timezone.utc),
                "amount": Decimal("-35"),
                "currency": "RUB",
                "operation_type": "OPERATION_TYPE_BROKER_FEE",
                "state": "OPERATION_STATE_EXECUTED",
                "instrument_uid": None,
                "asset_uid": None,
                "figi": None,
                "name": "Комиссия",
                "commission": Decimal("-35"),
                "yield": Decimal("0"),
                "description": "fee",
                "source": "broker",
                "price": Decimal("0"),
                "quantity": Decimal("0"),
            },
        ]
        income_rows = [
            {
                "event_date": date(2026, 4, 18),
                "event_type": "coupon",
                "figi": "figi-bond1",
                "ticker": "BOND1",
                "instrument_name": "Bond 1",
                "gross_amount": Decimal("140.70"),
                "tax_amount": Decimal("12.10"),
                "net_amount": Decimal("128.60"),
                "net_yield_pct": Decimal("1.7"),
                "notified": True,
            }
        ]
        instrument_eod_rows = [
            {
                "snapshot_id": 21,
                "snapshot_date": date(2026, 4, 1),
                "snapshot_at": datetime(2026, 4, 1, 18, tzinfo=timezone.utc),
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "position_uid": "pos-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("10"),
                "currency": "RUB",
                "position_value": Decimal("1000"),
                "expected_yield": Decimal("-50"),
                "expected_yield_pct": Decimal("-5"),
                "weight_pct": Decimal("10"),
            },
            {
                "snapshot_id": 22,
                "snapshot_date": date(2026, 4, 2),
                "snapshot_at": datetime(2026, 4, 2, 18, tzinfo=timezone.utc),
                "figi": "figi-eqmx",
                "ticker": "EQMX",
                "name": "EQMX ETF",
                "instrument_uid": "inst-eqmx",
                "position_uid": "pos-eqmx",
                "asset_uid": "asset-eqmx",
                "instrument_type": "etf",
                "quantity": Decimal("14"),
                "currency": "RUB",
                "position_value": Decimal("1540"),
                "expected_yield": Decimal("220"),
                "expected_yield_pct": Decimal("16"),
                "weight_pct": Decimal("13.8"),
            },
            {
                "snapshot_id": 30,
                "snapshot_date": date(2026, 4, 30),
                "snapshot_at": datetime(2026, 4, 30, 18, tzinfo=timezone.utc),
                "figi": "figi-bond1",
                "ticker": "BOND1",
                "name": "Bond 1",
                "instrument_uid": "inst-bond1",
                "position_uid": "pos-bond1",
                "asset_uid": "asset-bond1",
                "instrument_type": "bond",
                "quantity": Decimal("3"),
                "currency": "RUB",
                "position_value": Decimal("305"),
                "expected_yield": Decimal("95"),
                "expected_yield_pct": Decimal("4"),
                "weight_pct": Decimal("2.7"),
            },
        ]

        def positions_for_snapshot(_session, snapshot_id):
            if snapshot_id == 10:
                return start_positions
            if snapshot_id == 30:
                return end_positions
            raise AssertionError(f"unexpected snapshot_id: {snapshot_id}")

        def deposits_for_period(_session, _account_id, start_dt, _end_dt):
            if start_dt.month == 1:
                return Decimal("15000")
            return Decimal("5000")

        patches = [
            mock.patch.object(report_payload, "resolve_reporting_account_id", return_value="acc"),
            mock.patch.object(report_payload, "get_month_snapshots", return_value=(start_snapshot, end_snapshot)),
            mock.patch.object(report_payload, "get_period_daily_snapshot_rows", return_value=daily_rows),
            mock.patch.object(report_payload, "get_positions_for_snapshot", side_effect=positions_for_snapshot),
            mock.patch.object(report_payload, "get_asset_alias_rows", return_value=alias_rows),
            mock.patch.object(report_payload, "get_dataset_operations", return_value=operations_rows),
            mock.patch.object(report_payload, "get_income_events_for_period", return_value=income_rows),
            mock.patch.object(report_payload, "get_instrument_eod_rows", return_value=instrument_eod_rows),
            mock.patch.object(
                report_payload,
                "compute_twr_timeseries",
                return_value=(
                    [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 30)],
                    [10000.0, 10700.0, 11150.0],
                    [0.0, 0.05, 0.081],
                ),
            ),
            mock.patch.object(report_payload, "get_deposits_for_period", side_effect=deposits_for_period),
            mock.patch.object(report_payload, "get_net_external_flow_for_period", return_value=Decimal("4500")),
            mock.patch.object(report_payload, "get_income_for_period", return_value=(Decimal("128.60"), Decimal("50.25"))),
            mock.patch.object(report_payload, "get_commissions_for_period", return_value=Decimal("35")),
            mock.patch.object(report_payload, "get_taxes_for_period", return_value=Decimal("12.10")),
            mock.patch.object(
                report_payload,
                "get_rebalance_targets",
                return_value={
                    "stocks": Decimal("40"),
                    "bonds": Decimal("20"),
                    "etf": Decimal("30"),
                    "currency": Decimal("10"),
                },
            ),
            mock.patch.object(
                report_payload,
                "compute_realized_by_asset",
                return_value=(
                    [{"figi": "figi-gazp", "name": "Gazprom", "ticker": "GAZP", "amount": Decimal("-85")}],
                    Decimal("-85"),
                ),
            ),
            mock.patch.object(
                report_payload,
                "compute_income_by_asset_net",
                return_value=([], Decimal("0")),
            ),
            mock.patch.object(report_payload, "get_unrealized_at_period_end", return_value=Decimal("315")),
        ]

        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            return report_payload.build_monthly_report_payload(
                fake_session,
                year=2026,
                month=4,
                account_id="acc",
            )

    def test_build_monthly_report_payload_returns_serialized_contract(self):
        payload = self._build_payload()

        self.assertEqual(payload["schema_version"], "monthly_report_payload.v1")
        self.assertEqual(payload["meta"]["period_label_ru"], "апрель 2026")
        self.assertEqual(payload["summary_metrics"]["start_value"], "10000")
        self.assertEqual(payload["summary_metrics"]["end_value"], "11150")
        self.assertEqual(payload["summary_metrics"]["withdrawals"], "500")
        self.assertEqual(payload["summary_metrics"]["period_twr_pct"], "8.1")
        self.assertEqual(payload["summary_metrics"]["open_pl_end_total"], "315")
        self.assertEqual(payload["positions_current"][0]["ticker"], "EQMX")
        self.assertEqual(payload["position_flow_groups"]["new"][0]["ticker"], "BOND1")
        self.assertEqual(payload["position_flow_groups"]["closed"][0]["ticker"], "GAZP")
        self.assertEqual(payload["instrument_movers"]["top_growth"][0]["ticker"], "EQMX")
        self.assertEqual(payload["income_events"][0]["ticker"], "BOND1")
        self.assertTrue(payload["rebalance_snapshot"]["rows"])

    def test_build_monthly_ai_input_keeps_overview_and_trims_large_lists(self):
        payload = self._build_payload()
        ai_input = report_payload.build_monthly_ai_input(payload, max_input_chars=1200)

        self.assertEqual(ai_input["schema_version"], "monthly_ai_input.v1")
        self.assertEqual(ai_input["meta"]["style"], "calm precise non-promotional")
        self.assertTrue(ai_input["overview_facts"]["highlights"])
        self.assertLessEqual(len(ai_input["cashflow_facts"]["operations_top"]), 5)


if __name__ == "__main__":
    unittest.main()
