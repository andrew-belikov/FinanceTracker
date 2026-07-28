import importlib.util
import os
from pathlib import Path
import sys
import unittest
from datetime import date, datetime
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRACKER_DIR = PROJECT_ROOT / "src" / "tracker"
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(TRACKER_DIR))

APP_SPEC = importlib.util.spec_from_file_location(
    "tracker_app_under_test",
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


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None, text=""):
        self.status_code = status_code
        self.payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self.payload


class TInvestHttpClientTests(unittest.TestCase):
    def setUp(self):
        tracker_app._INSTRUMENT_CACHE.clear()
        self.config_patch = mock.patch.multiple(
            tracker_app,
            HTTP_RETRY_TOTAL=3,
            HTTP_BACKOFF_SECONDS=1.0,
            HTTP_MAX_BACKOFF_SECONDS=60.0,
            INSTRUMENT_CACHE_TTL_SECONDS=86400.0,
            INSTRUMENT_CACHE_MAX_ENTRIES=1024,
        )
        self.config_patch.start()

    def tearDown(self):
        self.config_patch.stop()
        tracker_app._INSTRUMENT_CACHE.clear()

    def test_retries_retryable_statuses_with_server_delay_and_backoff(self):
        responses = [
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(503),
            FakeResponse(200, {"accounts": []}),
        ]

        with (
            mock.patch.object(tracker_app.API_SESSION, "post", side_effect=responses) as post,
            mock.patch.object(tracker_app.time, "sleep") as sleep,
        ):
            result = tracker_app.post_api("UsersService/GetAccounts", {})

        self.assertEqual(result, {"accounts": []})
        self.assertEqual(post.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [0.0, 2.0],
        )

    def test_does_not_retry_non_retryable_http_status(self):
        with mock.patch.object(
            tracker_app.API_SESSION,
            "post",
            return_value=FakeResponse(400, {"message": "bad request"}),
        ) as post:
            with self.assertRaisesRegex(RuntimeError, "API HTTP 400"):
                tracker_app.post_api("UsersService/GetAccounts", {})

        post.assert_called_once()

    def test_retries_transport_errors_and_reuses_the_same_session(self):
        retryable_errors = (
            tracker_app.requests.exceptions.ConnectionError,
            tracker_app.requests.exceptions.Timeout,
            tracker_app.requests.exceptions.ChunkedEncodingError,
        )

        for error_type in retryable_errors:
            with self.subTest(error_type=error_type.__name__):
                with (
                    mock.patch.object(
                        tracker_app.API_SESSION,
                        "post",
                        side_effect=[
                            error_type("temporary"),
                            FakeResponse(200, {"ok": True}),
                        ],
                    ) as post,
                    mock.patch.object(tracker_app.time, "sleep") as sleep,
                ):
                    result = tracker_app.post_api("OperationsService/GetPortfolio", {})

                self.assertEqual(result, {"ok": True})
                self.assertEqual(post.call_count, 2)
                sleep.assert_called_once_with(1.0)

    def test_instrument_metadata_is_cached_and_returned_as_a_copy(self):
        instrument = {"figi": "FIGI1", "name": "Bond"}
        with mock.patch.object(
            tracker_app,
            "post_api",
            return_value={"instrument": instrument},
        ) as post:
            first = tracker_app.api_get_instrument_by_figi("FIGI1")
            first["name"] = "mutated"
            second = tracker_app.api_get_instrument_by_figi("FIGI1")

        post.assert_called_once()
        self.assertEqual(second["name"], "Bond")


class OperationsPaginationTests(unittest.TestCase):
    def test_repeated_cursor_raises_before_a_third_request(self):
        pages = [
            {"items": [{"id": "1"}], "hasNext": True, "nextCursor": "A"},
            {"items": [{"id": "2"}], "hasNext": True, "nextCursor": "A"},
        ]

        with mock.patch.object(tracker_app, "post_api", side_effect=pages) as post:
            with self.assertRaisesRegex(
                tracker_app.OperationsPaginationError,
                "cursor repeated",
            ):
                list(
                    tracker_app._iter_operation_pages(
                        "account",
                        "2026-01-01T00:00:00Z",
                        to_iso="2026-02-01T00:00:00Z",
                    )
                )

        self.assertEqual(post.call_count, 2)

    def test_page_limit_raises_instead_of_returning_partial_success(self):
        pages = [
            {"items": [], "hasNext": True, "nextCursor": "A"},
            {"items": [], "hasNext": True, "nextCursor": "B"},
        ]

        with mock.patch.object(tracker_app, "post_api", side_effect=pages):
            with self.assertRaisesRegex(
                tracker_app.OperationsPaginationError,
                "page limit reached",
            ):
                list(
                    tracker_app._iter_operation_pages(
                        "account",
                        None,
                        max_pages=2,
                    )
                )

    def test_fixed_to_timestamp_is_used_for_every_page(self):
        pages = [
            {"items": [], "hasNext": True, "nextCursor": "A"},
            {"items": [{"id": "2"}], "hasNext": False, "nextCursor": ""},
        ]

        with mock.patch.object(tracker_app, "post_api", side_effect=pages) as post:
            result = list(
                tracker_app._iter_operation_pages(
                    "account",
                    None,
                    to_iso="2026-02-01T00:00:00Z",
                    max_pages=2,
                )
            )

        self.assertEqual(result, [[], [{"id": "2"}]])
        payloads = [call.args[1] for call in post.call_args_list]
        self.assertEqual(
            [payload["to"] for payload in payloads],
            ["2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z"],
        )
        self.assertEqual([payload["cursor"] for payload in payloads], ["", "A"])

    def test_has_next_without_cursor_is_a_contract_error(self):
        with mock.patch.object(
            tracker_app,
            "post_api",
            return_value={"items": [], "hasNext": True},
        ):
            with self.assertRaisesRegex(
                tracker_app.OperationsPaginationError,
                "without nextCursor",
            ):
                list(tracker_app._iter_operation_pages("account", None))

    def test_active_sync_does_not_swallow_pagination_error(self):
        with mock.patch.object(
            tracker_app,
            "_iter_operation_pages",
            side_effect=tracker_app.OperationsPaginationError("cursor repeated"),
        ):
            with self.assertRaisesRegex(
                tracker_app.OperationsPaginationError,
                "cursor repeated",
            ):
                tracker_app._sync_operations(mock.Mock(), "account", None)


class IncomeEventReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
        tracker_app.Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    @staticmethod
    def operation(
        *,
        operation_id,
        operation_type,
        amount,
        state="OPERATION_STATE_EXECUTED",
        account_id="account",
    ):
        return tracker_app.Operation(
            account_id=account_id,
            operation_id=operation_id,
            operation_type=operation_type,
            state=state,
            figi="FIGI1",
            date=datetime(2026, 1, 15, 12, 0, 0),
            amount=amount,
            currency="RUB",
        )

    def test_late_coupon_tax_updates_existing_event_from_full_history(self):
        with self.Session() as session:
            session.add(
                self.operation(
                    operation_id="coupon",
                    operation_type="OPERATION_TYPE_COUPON",
                    amount=100,
                )
            )
            session.add(
                tracker_app.IncomeEvent(
                    account_id="account",
                    figi="FIGI1",
                    event_date=date(2026, 1, 15),
                    event_type="coupon",
                    gross_amount=100,
                    tax_amount=0,
                    net_amount=100,
                    net_yield_pct=10,
                    notified=True,
                )
            )
            session.commit()

            session.add(
                self.operation(
                    operation_id="late-tax",
                    operation_type="OPERATION_TYPE_BOND_TAX",
                    amount=-13,
                )
            )
            with mock.patch.object(
                tracker_app,
                "get_latest_cost_basis",
                return_value=1000,
            ):
                stats = tracker_app._reconcile_income_events(
                    session,
                    "account",
                    {("FIGI1", date(2026, 1, 15), "coupon")},
                )
            session.commit()

            event = session.query(tracker_app.IncomeEvent).one()
            self.assertEqual(float(event.gross_amount), 100.0)
            self.assertEqual(float(event.tax_amount), -13.0)
            self.assertEqual(float(event.net_amount), 87.0)
            self.assertEqual(float(event.net_yield_pct), 8.7)
            self.assertTrue(event.notified)
            self.assertEqual(stats["income_updated"], 1)

    def test_non_executed_income_does_not_delete_existing_history(self):
        with self.Session() as session:
            session.add(
                self.operation(
                    operation_id="coupon",
                    operation_type="OPERATION_TYPE_COUPON",
                    amount=100,
                    state="OPERATION_STATE_CANCELED",
                )
            )
            session.add(
                tracker_app.IncomeEvent(
                    account_id="account",
                    figi="FIGI1",
                    event_date=date(2026, 1, 15),
                    event_type="coupon",
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
                {("FIGI1", date(2026, 1, 15), "coupon")},
            )
            session.commit()

            self.assertEqual(session.query(tracker_app.IncomeEvent).count(), 1)
            self.assertEqual(stats["income_created"], 0)
            self.assertEqual(stats["income_updated"], 0)

    def test_active_sync_collects_income_key_for_reconciliation(self):
        operation_payload = {
            "id": "late-tax",
            "type": "OPERATION_TYPE_BOND_TAX",
            "state": "OPERATION_STATE_EXECUTED",
            "figi": "FIGI1",
            "date": "2026-01-15T12:00:00Z",
            "payment": {"units": "-13", "nano": 0, "currency": "rub"},
        }
        affected_keys = set()

        with self.Session() as session:
            with mock.patch.object(
                tracker_app,
                "_iter_operation_pages",
                return_value=iter([[operation_payload]]),
            ):
                stats = tracker_app._sync_operations(
                    session,
                    "account",
                    None,
                    affected_income_keys=affected_keys,
                )
            session.commit()

        self.assertEqual(stats["new"], 1)
        self.assertEqual(
            affected_keys,
            {("FIGI1", date(2026, 1, 15), "coupon")},
        )

    def test_unchanged_event_keeps_original_yield_and_notification_state(self):
        with self.Session() as session:
            session.add(
                self.operation(
                    operation_id="coupon",
                    operation_type="OPERATION_TYPE_COUPON",
                    amount=100,
                )
            )
            session.add(
                tracker_app.IncomeEvent(
                    account_id="account",
                    figi="FIGI1",
                    event_date=date(2026, 1, 15),
                    event_type="coupon",
                    gross_amount=100,
                    tax_amount=0,
                    net_amount=100,
                    net_yield_pct=10,
                    notified=True,
                )
            )
            session.commit()

            with mock.patch.object(
                tracker_app,
                "get_latest_cost_basis",
                return_value=500,
            ) as get_cost_basis:
                stats = tracker_app._reconcile_income_events(
                    session,
                    "account",
                    {("FIGI1", date(2026, 1, 15), "coupon")},
                )
            session.commit()

            event = session.query(tracker_app.IncomeEvent).one()
            self.assertEqual(float(event.net_yield_pct), 10.0)
            self.assertTrue(event.notified)
            self.assertEqual(stats["income_updated"], 0)
            get_cost_basis.assert_not_called()

if __name__ == "__main__":
    unittest.main()
