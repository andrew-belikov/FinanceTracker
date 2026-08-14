import importlib.util
import os
from pathlib import Path
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRACKER_DIR = PROJECT_ROOT / "src" / "tracker"
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(TRACKER_DIR))

APP_SPEC = importlib.util.spec_from_file_location(
    "tracker_app_data_remediation_under_test",
    TRACKER_DIR / "app.py",
)
tracker_app = importlib.util.module_from_spec(APP_SPEC)
assert APP_SPEC.loader is not None
with mock.patch.dict(
    os.environ,
    {
        "DB_DSN": "sqlite://",
        "VERIFY_SSL": "true",
        "TINVEST_API_TOKEN": "test-token",
    },
):
    APP_SPEC.loader.exec_module(tracker_app)


class DataIntegrityRemediationTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
        tracker_app.Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    def test_p1_03_local_civil_bounds_and_utc_grouping_cover_dst(self):
        from common.time_utils import local_civil_bounds_to_utc_naive, utc_naive_to_local_date

        cases = (
            (
                "day",
                date(2026, 8, 14),
                date(2026, 8, 15),
                "Europe/Moscow",
                datetime(2026, 8, 13, 21),
                datetime(2026, 8, 14, 21),
            ),
            (
                "month",
                date(2026, 8, 1),
                date(2026, 9, 1),
                "Europe/Moscow",
                datetime(2026, 7, 31, 21),
                datetime(2026, 8, 31, 21),
            ),
            (
                "year",
                date(2026, 1, 1),
                date(2027, 1, 1),
                "Europe/Moscow",
                datetime(2025, 12, 31, 21),
                datetime(2026, 12, 31, 21),
            ),
            (
                "dst-spring",
                date(2026, 3, 29),
                date(2026, 3, 30),
                "Europe/Berlin",
                datetime(2026, 3, 28, 23),
                datetime(2026, 3, 29, 22),
            ),
            (
                "dst-autumn",
                date(2026, 10, 25),
                date(2026, 10, 26),
                "Europe/Berlin",
                datetime(2026, 10, 24, 22),
                datetime(2026, 10, 25, 23),
            ),
        )
        for label, local_start, local_end, zone, expected_start, expected_end in cases:
            with self.subTest(label):
                start, end = local_civil_bounds_to_utc_naive(local_start, local_end, zone)
                self.assertEqual((start, end), (expected_start, expected_end))
                self.assertEqual(utc_naive_to_local_date(start, zone), local_start)

    def test_p1_09_income_event_identity_includes_currency(self):
        event = tracker_app.IncomeEvent(
            account_id="account",
            figi="FIGI1",
            event_date=date(2026, 1, 15),
            event_type="coupon",
            currency="USD",
            gross_amount=Decimal("1"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("1"),
            net_yield_pct=Decimal("1"),
        )
        self.assertEqual(event.currency, "USD")
        unique_columns = {
            tuple(column.name for column in constraint.columns)
            for constraint in tracker_app.IncomeEvent.__table__.constraints
            if constraint.__class__.__name__ == "UniqueConstraint"
        }
        self.assertIn(
            ("account_id", "figi", "event_date", "event_type", "currency"),
            unique_columns,
        )

    def test_p1_09_reconciliation_keeps_nominal_currencies_separate(self):
        with self.Session() as session:
            for suffix, currency, amount in (
                ("rub", "RUB", 100),
                ("usd", "USD", 5),
                ("unknown", "UNKNOWN", 2),
            ):
                session.add(
                    tracker_app.Operation(
                        account_id="account",
                        operation_id=f"coupon-{suffix}",
                        operation_type="OPERATION_TYPE_COUPON",
                        state="OPERATION_STATE_EXECUTED",
                        figi="FIGI1",
                        date=datetime(2026, 1, 15, 12),
                        amount=amount,
                        currency=currency,
                    )
                )
            session.commit()

            affected = {
                ("FIGI1", date(2026, 1, 15), "coupon", currency)
                for currency in ("RUB", "USD", "UNKNOWN")
            }
            tracker_app._reconcile_income_events(session, "account", affected)
            session.commit()
            rows = session.query(tracker_app.IncomeEvent).order_by(tracker_app.IncomeEvent.currency).all()

        self.assertEqual(
            [(row.currency, float(row.net_amount)) for row in rows],
            [("RUB", 100.0), ("UNKNOWN", 2.0), ("USD", 5.0)],
        )

    def test_p1_10_same_operation_id_on_two_accounts_stays_two_rows(self):
        payload = {
            "id": "shared-broker-id",
            "type": "OPERATION_TYPE_INPUT",
            "state": "OPERATION_STATE_EXECUTED",
            "date": "2026-01-15T12:00:00Z",
            "payment": {"units": "100", "nano": 0, "currency": "rub"},
        }
        with self.Session() as session:
            tracker_app._upsert_operation(session, "first", payload)
            tracker_app._upsert_operation(session, "second", payload)
            session.commit()
            rows = session.query(tracker_app.Operation).order_by(tracker_app.Operation.account_id).all()

        self.assertEqual([(row.account_id, row.operation_id) for row in rows], [
            ("first", "shared-broker-id"),
            ("second", "shared-broker-id"),
        ])

    def test_p1_11_account_selection_is_exact_and_unambiguous(self):
        accounts = {
            "accounts": [
                {"id": "first", "status": "ACCOUNT_STATUS_OPEN"},
                {"id": "second", "status": "ACCOUNT_STATUS_OPEN"},
            ]
        }
        with mock.patch.object(tracker_app, "TINKOFF_ACCOUNT_ID", "missing"):
            with self.assertRaisesRegex(RuntimeError, "not found"):
                tracker_app.choose_account(accounts)
        with mock.patch.object(tracker_app, "TINKOFF_ACCOUNT_ID", "auto"):
            with self.assertRaisesRegex(RuntimeError, "exactly one open account"):
                tracker_app.choose_account(accounts)
            with self.assertRaisesRegex(RuntimeError, "No accounts|exactly one open account"):
                tracker_app.choose_account({"accounts": []})
        with mock.patch.object(tracker_app, "TINKOFF_ACCOUNT_ID", ""):
            self.assertEqual(
                tracker_app.choose_account({"accounts": [accounts["accounts"][0]]})["id"],
                "first",
            )

    def test_p2_03_cost_basis_is_as_of_event_date(self):
        with self.Session() as session:
            for snapshot_date, snapshot_at, basis in (
                (date(2026, 1, 10), datetime(2026, 1, 10, 20), Decimal("1000")),
                (date(2026, 1, 20), datetime(2026, 1, 20, 20), Decimal("2000")),
            ):
                snapshot = tracker_app.PortfolioSnapshot(
                    account_id="account",
                    snapshot_at=snapshot_at,
                    snapshot_date=snapshot_date,
                    currency="RUB",
                )
                session.add(snapshot)
                session.flush()
                session.add(
                    tracker_app.PortfolioPosition(
                        snapshot_id=snapshot.id,
                        figi="FIGI1",
                        currency="RUB",
                        position_value=basis,
                        expected_yield=Decimal("0"),
                    )
                )
            session.commit()

            result = tracker_app.get_latest_cost_basis(
                session,
                "account",
                "FIGI1",
                as_of_date=date(2026, 1, 15),
            )

        self.assertEqual(result, 1000.0)

    def test_p2_03_late_tax_keeps_event_date_cost_basis(self):
        with self.Session() as session:
            for snapshot_date, basis in (
                (date(2026, 1, 10), Decimal("1000")),
                (date(2026, 1, 20), Decimal("2000")),
            ):
                snapshot = tracker_app.PortfolioSnapshot(
                    account_id="account",
                    snapshot_at=datetime.combine(snapshot_date, datetime.min.time()),
                    snapshot_date=snapshot_date,
                    currency="RUB",
                )
                session.add(snapshot)
                session.flush()
                session.add(
                    tracker_app.PortfolioPosition(
                        snapshot_id=snapshot.id,
                        figi="FIGI1",
                        currency="RUB",
                        position_value=basis,
                        expected_yield=Decimal("0"),
                    )
                )
            for operation_id, operation_type, amount in (
                ("coupon", "OPERATION_TYPE_COUPON", 100),
                ("late-tax", "OPERATION_TYPE_BOND_TAX", -13),
            ):
                session.add(
                    tracker_app.Operation(
                        account_id="account",
                        operation_id=operation_id,
                        operation_type=operation_type,
                        state="OPERATION_STATE_EXECUTED",
                        figi="FIGI1",
                        date=datetime(2026, 1, 15, 12),
                        amount=amount,
                        currency="RUB",
                    )
                )
            session.commit()

            tracker_app._reconcile_income_events(
                session,
                "account",
                {("FIGI1", date(2026, 1, 15), "coupon", "RUB")},
            )
            session.commit()
            event = session.query(tracker_app.IncomeEvent).one()

        self.assertEqual(float(event.net_amount), 87.0)
        self.assertEqual(float(event.net_yield_pct), 8.7)

    def test_p2_04_explicit_cancel_deactivates_income_event(self):
        with self.Session() as session:
            session.add(
                tracker_app.Operation(
                    account_id="account",
                    operation_id="coupon",
                    operation_type="OPERATION_TYPE_COUPON",
                    state="OPERATION_STATE_CANCELED",
                    figi="FIGI1",
                    date=datetime(2026, 1, 15, 12),
                    amount=Decimal("100"),
                    currency="RUB",
                )
            )
            session.add(
                tracker_app.IncomeEvent(
                    account_id="account",
                    figi="FIGI1",
                    event_date=date(2026, 1, 15),
                    event_type="coupon",
                    currency="RUB",
                    gross_amount=100,
                    tax_amount=0,
                    net_amount=100,
                    net_yield_pct=10,
                    notified=False,
                )
            )
            session.commit()

            stats = tracker_app._reconcile_income_events(
                session,
                "account",
                {("FIGI1", date(2026, 1, 15), "coupon", "RUB")},
            )
            session.commit()

            self.assertEqual(session.query(tracker_app.IncomeEvent).count(), 0)
            self.assertEqual(stats["income_deactivated"], 1)

    def test_p2_04_missing_api_window_preserves_income_event(self):
        with self.Session() as session:
            session.add(
                tracker_app.IncomeEvent(
                    account_id="account",
                    figi="FIGI1",
                    event_date=date(2026, 1, 15),
                    event_type="coupon",
                    currency="RUB",
                    gross_amount=100,
                    tax_amount=0,
                    net_amount=100,
                    net_yield_pct=10,
                    notified=False,
                )
            )
            session.commit()

            stats = tracker_app._reconcile_income_events(session, "account", set())
            session.commit()

            self.assertEqual(session.query(tracker_app.IncomeEvent).count(), 1)
            self.assertEqual(stats["income_deactivated"], 0)

    def test_p2_04_pending_transition_does_not_mark_event_for_deactivation(self):
        pending = {
            "id": "coupon",
            "type": "OPERATION_TYPE_COUPON",
            "state": "OPERATION_STATE_PENDING",
            "figi": "FIGI1",
            "date": "2026-01-15T12:00:00Z",
            "payment": {"units": "100", "nano": 0, "currency": "rub"},
        }
        affected = set()
        with self.Session() as session:
            with mock.patch.object(tracker_app, "_iter_operation_pages", return_value=iter([[pending]])):
                tracker_app._sync_operations(
                    session,
                    "account",
                    None,
                    affected_income_keys=affected,
                )
        self.assertEqual(affected, set())

    def test_p2_04_sparse_cancel_preserves_currency_identity(self):
        with self.Session() as session:
            session.add(
                tracker_app.Operation(
                    account_id="account",
                    operation_id="coupon",
                    operation_type="OPERATION_TYPE_COUPON",
                    state="OPERATION_STATE_EXECUTED",
                    figi="FIGI1",
                    date=datetime(2026, 1, 15, 12),
                    amount=100,
                    currency="RUB",
                )
            )
            session.add(
                tracker_app.IncomeEvent(
                    account_id="account",
                    figi="FIGI1",
                    event_date=date(2026, 1, 15),
                    event_type="coupon",
                    currency="RUB",
                    gross_amount=100,
                    tax_amount=0,
                    net_amount=100,
                    net_yield_pct=10,
                    notified=False,
                )
            )
            session.commit()

            affected = set()
            with mock.patch.object(
                tracker_app,
                "_iter_operation_pages",
                return_value=iter(
                    [[{
                        "id": "coupon",
                        "type": "OPERATION_TYPE_COUPON",
                        "state": "OPERATION_STATE_CANCELED",
                        "figi": "FIGI1",
                        "date": "2026-01-15T12:00:00Z",
                        "payment": {},
                    }]]
                ),
            ):
                tracker_app._sync_operations(
                    session,
                    "account",
                    None,
                    affected_income_keys=affected,
                )
            tracker_app._reconcile_income_events(session, "account", affected)
            session.commit()

            operation = session.query(tracker_app.Operation).one()
            self.assertEqual(operation.currency, "RUB")
            self.assertEqual(affected, {("FIGI1", date(2026, 1, 15), "coupon", "RUB")})
            self.assertEqual(session.query(tracker_app.IncomeEvent).count(), 0)

    def test_p2_10_missing_timestamp_aborts_before_any_write(self):
        with self.Session() as session:
            with self.assertRaisesRegex(ValueError, "timestamp"):
                tracker_app._upsert_operation(
                    session,
                    "account",
                    {
                        "id": "missing-date",
                        "type": "OPERATION_TYPE_INPUT",
                        "payment": {"units": "100", "nano": 0, "currency": "rub"},
                    },
                )
            self.assertEqual(session.query(tracker_app.Operation).count(), 0)

    def test_p2_10_malformed_page_rolls_back_without_watermark_advance(self):
        page = [
            {
                "id": "valid-first",
                "type": "OPERATION_TYPE_INPUT",
                "date": "2026-01-15T12:00:00Z",
                "payment": {"units": "100", "nano": 0, "currency": "rub"},
            },
            {
                "id": "malformed-second",
                "type": "OPERATION_TYPE_INPUT",
                "date": "not-a-timestamp",
                "payment": {"units": "200", "nano": 0, "currency": "rub"},
            },
        ]
        with self.Session() as session:
            with mock.patch.object(tracker_app, "_iter_operation_pages", return_value=iter([page])):
                with self.assertRaisesRegex(ValueError, "timestamp"):
                    tracker_app._sync_operations(session, "account", None)
            session.rollback()
            self.assertEqual(session.query(tracker_app.Operation).count(), 0)


class MigrationReadOnlyContractTests(unittest.TestCase):
    def test_p1_03_legacy_income_dates_are_normalized_collision_safe(self):
        migration_source = (
            PROJECT_ROOT / "migrations" / "20260814_data_identity_and_currency.sql"
        ).read_text(encoding="utf-8")
        migrate_source = (TRACKER_DIR / "migrate.py").read_text(encoding="utf-8")
        self.assertIn("current_setting('TimeZone')", migration_source)
        self.assertIn("resolved_event_date", migration_source)
        self.assertIn("local identity collision; manual remediation is required", migration_source)
        self.assertIn("set_config('TimeZone'", migrate_source)

    def test_p1_03_invalid_migration_timezone_fails_closed(self):
        migrate_path = TRACKER_DIR / "migrate.py"
        spec = importlib.util.spec_from_file_location(
            "tracker_migrate_timezone_under_test",
            migrate_path,
        )
        migrate_module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(migrate_module)
        with mock.patch.object(migrate_module, "MIGRATION_TIMEZONE", "Invalid/Timezone"):
            with self.assertRaisesRegex(
                migrate_module.RuntimeConfigurationError,
                "timezone is invalid",
            ):
                migrate_module.validate_migration_timezone()

    def test_p2_08_check_mode_does_not_create_ledger_or_run_ddl(self):
        migrate_path = TRACKER_DIR / "migrate.py"
        source = migrate_path.read_text(encoding="utf-8")
        check_branch = source.split("def run_migrations", 1)[1].split("def main", 1)[0]
        self.assertIn("_run_check_only", check_branch)
        self.assertLess(check_branch.index("if check_only"), check_branch.index("SCHEMA_MIGRATIONS_DDL"))

    def test_p2_09_optional_income_queries_use_savepoint(self):
        source = (PROJECT_ROOT / "src" / "bot" / "queries.py").read_text(encoding="utf-8")
        self.assertIn("begin_nested()", source)


if __name__ == "__main__":
    unittest.main()
