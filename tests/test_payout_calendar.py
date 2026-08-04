import importlib.util
import os
import asyncio
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRACKER_DIR = PROJECT_ROOT / "src" / "tracker"
BOT_DIR = PROJECT_ROOT / "src" / "bot"
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(TRACKER_DIR))
sys.path.insert(0, str(BOT_DIR))

APP_SPEC = importlib.util.spec_from_file_location(
    "tracker_app_payout_calendar_under_test",
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

from services import render_payout_calendar_text
from queries import get_payout_calendar_events
import jobs as bot_jobs


class PayoutCalendarSyncTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
        tracker_app.Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    def add_portfolio(self, session):
        bond = tracker_app.Instrument(
            figi="BOND1",
            ticker="BOND",
            name="Облигация",
            instrument_type="bond",
        )
        share = tracker_app.Instrument(
            figi="SHARE1",
            ticker="SHARE",
            name="Акция",
            instrument_type="share",
        )
        snapshot = tracker_app.PortfolioSnapshot(
            account_id="account",
            account_name="ИИС",
            snapshot_at=datetime(2026, 7, 28, 8, 0, 0),
            snapshot_date=date(2026, 7, 28),
            currency="RUB",
        )
        session.add_all([bond, share, snapshot])
        session.flush()
        session.add_all(
            [
                tracker_app.PortfolioPosition(
                    snapshot_id=snapshot.id,
                    instrument_id=bond.id,
                    figi=bond.figi,
                    instrument_uid="bond-uid",
                    ticker=bond.ticker,
                    name=bond.name,
                    instrument_type="bond",
                    quantity=Decimal("10"),
                    currency="RUB",
                    position_value=Decimal("1000"),
                    expected_yield=Decimal("100"),
                ),
                tracker_app.PortfolioPosition(
                    snapshot_id=snapshot.id,
                    instrument_id=share.id,
                    figi=share.figi,
                    instrument_uid="share-uid",
                    ticker=share.ticker,
                    name=share.name,
                    instrument_type="share",
                    quantity=Decimal("3"),
                    currency="RUB",
                ),
            ]
        )
        session.commit()

    @staticmethod
    def coupon_events():
        return [
            {
                "couponDate": "2026-08-01T00:00:00Z",
                "couponNumber": "7",
                "couponStartDate": "2026-05-01T00:00:00Z",
                "couponEndDate": "2026-08-01T00:00:00Z",
                "couponPeriod": 92,
                "payOneBond": {
                    "units": "12",
                    "nano": 500_000_000,
                    "currency": "rub",
                },
                "couponType": "COUPON_TYPE_CONSTANT",
            }
        ]

    @staticmethod
    def dividend_events():
        return [
            {
                "paymentDate": "2026-08-02T00:00:00Z",
                "recordDate": "2026-07-20T00:00:00Z",
                "lastBuyDate": "2026-07-17T00:00:00Z",
                "declaredDate": "2026-07-01T00:00:00Z",
                "dividendNet": {
                    "units": "5",
                    "nano": 0,
                    "currency": "rub",
                },
                "dividendType": "Regular Cash",
            }
        ]

    def test_sync_persists_coupon_and_dividend_amounts_for_current_quantity(self):
        with self.Session() as session:
            self.add_portfolio(session)
            with (
                mock.patch.object(tracker_app, "local_today", return_value=date(2026, 7, 28)),
                mock.patch.object(
                    tracker_app,
                    "api_get_bond_coupons",
                    return_value=self.coupon_events(),
                ),
                mock.patch.object(
                    tracker_app,
                    "api_get_dividends",
                    return_value=self.dividend_events(),
                ) as get_dividends,
            ):
                stats = tracker_app.sync_payout_calendar_for_account(session, "account")
                session.commit()

            rows = (
                session.query(tracker_app.PayoutCalendarEvent)
                .order_by(tracker_app.PayoutCalendarEvent.payment_date)
                .all()
            )
            query_rows = get_payout_calendar_events(
                session,
                "account",
                date(2026, 7, 28),
                date(2026, 10, 25),
            )

        self.assertEqual(stats, {"positions": 2, "events": 2, "failed": 0})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].event_type, "coupon")
        self.assertEqual(rows[0].amount_per_unit, Decimal("12.500000000"))
        self.assertEqual(rows[0].expected_amount, Decimal("125.00"))
        self.assertEqual(rows[0].coupon_start_date, date(2026, 5, 1))
        self.assertEqual(rows[0].coupon_end_date, date(2026, 8, 1))
        self.assertEqual(rows[0].coupon_period_days, 92)
        self.assertEqual(rows[0].currency, "RUB")
        self.assertEqual(query_rows[0]["coupon_period_days"], 92)
        self.assertEqual(query_rows[0]["cost_basis"], Decimal("900"))
        self.assertEqual(rows[1].event_type, "dividend")
        self.assertEqual(rows[1].expected_amount, Decimal("15.00"))
        self.assertEqual(rows[1].last_buy_date, date(2026, 7, 17))
        self.assertEqual([row["event_type"] for row in query_rows], ["coupon", "dividend"])
        self.assertEqual(
            get_dividends.call_args.args[1],
            "2025-07-27T21:00:00Z",
        )

    def test_failed_refresh_preserves_previous_calendar_rows(self):
        with self.Session() as session:
            self.add_portfolio(session)
            with (
                mock.patch.object(tracker_app, "local_today", return_value=date(2026, 7, 28)),
                mock.patch.object(
                    tracker_app,
                    "api_get_bond_coupons",
                    return_value=self.coupon_events(),
                ),
                mock.patch.object(tracker_app, "api_get_dividends", return_value=[]),
            ):
                tracker_app.sync_payout_calendar_for_account(session, "account")
                session.commit()

            with (
                mock.patch.object(tracker_app, "local_today", return_value=date(2026, 7, 28)),
                mock.patch.object(
                    tracker_app,
                    "api_get_bond_coupons",
                    side_effect=RuntimeError("temporary API failure"),
                ),
                mock.patch.object(tracker_app, "api_get_dividends", return_value=[]),
            ):
                stats = tracker_app.sync_payout_calendar_for_account(session, "account")
                session.commit()

            coupons = session.query(tracker_app.PayoutCalendarEvent).filter_by(
                event_type="coupon"
            ).all()

        self.assertEqual(stats["failed"], 1)
        self.assertEqual(len(coupons), 1)
        self.assertEqual(coupons[0].expected_amount, Decimal("125.00"))

    def test_query_exposes_previous_known_coupon_for_unknown_future_payment(self):
        with self.Session() as session:
            self.add_portfolio(session)
            session.add_all(
                [
                    tracker_app.PayoutCalendarEvent(
                        account_id="account",
                        figi="BOND1",
                        event_type="coupon",
                        event_uid="coupon-1",
                        payment_date=date(2026, 9, 7),
                        amount_per_unit=Decimal("13.03"),
                        quantity=Decimal("10"),
                        expected_amount=Decimal("130.30"),
                        currency="RUB",
                        fetched_at=datetime(2026, 8, 4, 6, 0, 0),
                    ),
                    tracker_app.PayoutCalendarEvent(
                        account_id="account",
                        figi="BOND1",
                        event_type="coupon",
                        event_uid="coupon-2",
                        payment_date=date(2026, 10, 7),
                        amount_per_unit=None,
                        quantity=Decimal("10"),
                        expected_amount=None,
                        currency="RUB",
                        fetched_at=datetime(2026, 8, 4, 6, 0, 0),
                    ),
                ]
            )
            session.commit()

            rows = get_payout_calendar_events(
                session,
                "account",
                date(2026, 10, 1),
                date(2026, 10, 31),
            )

        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["expected_amount"])
        self.assertEqual(
            rows[0]["previous_coupon_amount_per_unit"],
            Decimal("13.030000000"),
        )

    def test_successful_empty_refresh_removes_cancelled_event(self):
        with self.Session() as session:
            self.add_portfolio(session)
            with (
                mock.patch.object(tracker_app, "local_today", return_value=date(2026, 7, 28)),
                mock.patch.object(
                    tracker_app,
                    "api_get_bond_coupons",
                    return_value=self.coupon_events(),
                ),
                mock.patch.object(tracker_app, "api_get_dividends", return_value=[]),
            ):
                tracker_app.sync_payout_calendar_for_account(session, "account")
                session.commit()

            with (
                mock.patch.object(tracker_app, "local_today", return_value=date(2026, 7, 28)),
                mock.patch.object(tracker_app, "api_get_bond_coupons", return_value=[]),
                mock.patch.object(tracker_app, "api_get_dividends", return_value=[]),
            ):
                tracker_app.sync_payout_calendar_for_account(session, "account")
                session.commit()

            rows_count = session.query(tracker_app.PayoutCalendarEvent).count()

        self.assertEqual(rows_count, 0)

    def test_unknown_coupon_amount_is_kept_and_cancelled_dividend_is_ignored(self):
        unknown_coupon = self.coupon_events()
        unknown_coupon[0]["payOneBond"] = {
            "units": "0",
            "nano": 0,
            "currency": "rub",
        }
        cancelled_dividend = self.dividend_events()
        cancelled_dividend[0]["dividendType"] = "Cancelled"

        with self.Session() as session:
            self.add_portfolio(session)
            with (
                mock.patch.object(tracker_app, "local_today", return_value=date(2026, 7, 28)),
                mock.patch.object(
                    tracker_app,
                    "api_get_bond_coupons",
                    return_value=unknown_coupon,
                ),
                mock.patch.object(
                    tracker_app,
                    "api_get_dividends",
                    return_value=cancelled_dividend,
                ),
            ):
                tracker_app.sync_payout_calendar_for_account(session, "account")
                session.commit()

            rows = session.query(tracker_app.PayoutCalendarEvent).all()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "coupon")
        self.assertIsNone(rows[0].amount_per_unit)
        self.assertIsNone(rows[0].expected_amount)


class PayoutCalendarRenderingTests(unittest.TestCase):
    def test_renderer_separates_currencies_and_marks_unknown_amounts(self):
        rows = [
            {
                "figi": "BOND1",
                "instrument_name": "ОФЗ",
                "event_type": "coupon",
                "payment_date": date(2026, 8, 1),
                "coupon_start_date": date(2026, 5, 1),
                "coupon_end_date": date(2026, 8, 1),
                "coupon_period_days": 92,
                "expected_amount": Decimal("125.50"),
                "cost_basis": Decimal("10000"),
                "currency": "RUB",
                "fetched_at": datetime(2026, 7, 28, 6, 0, 0),
            },
            {
                "figi": "SHARE1",
                "instrument_name": "USD Share",
                "event_type": "dividend",
                "payment_date": date(2026, 8, 2),
                "expected_amount": Decimal("10.00"),
                "currency": "USD",
                "fetched_at": datetime(2026, 7, 28, 7, 0, 0),
            },
            {
                "figi": "FLOAT1",
                "instrument_name": "Флоатер",
                "event_type": "coupon",
                "payment_date": date(2026, 8, 3),
                "expected_amount": None,
                "currency": "RUB",
                "fetched_at": datetime(2026, 7, 28, 8, 0, 0),
            },
        ]

        text = render_payout_calendar_text(
            rows,
            start_date=date(2026, 7, 28),
            end_date=date(2026, 10, 25),
            heading="Календарь",
        )

        self.assertIn("Ожидаемая сумма после расчётного налога 13 %", text)
        self.assertIn("109.19 ₽", text)
        self.assertIn("≈ 4.33 % годовых", text)
        self.assertIn("8.70 USD", text)
        self.assertIn("Без известной суммы: 1", text)
        self.assertIn("По месяцам:", text)
        self.assertIn("Август 2026: 109.19 ₽ · 8.70 USD · без известной суммы: 1", text)
        self.assertIn("03.08 · купон · Флоатер · сумма уточняется", text)
        self.assertIn("Данные обновлены: 28.07.2026 09:00 МСК", text)
        self.assertIn("Фактическая сумма и налог могут отличаться", text)
        self.assertIn("это не YTM и не прогноз доходности", text)

    def test_renderer_aggregates_known_net_amounts_by_month(self):
        rows = [
            {
                "figi": "BOND1",
                "instrument_name": "Облигация",
                "event_type": "coupon",
                "payment_date": date(2026, 8, 8),
                "expected_amount": Decimal("100.00"),
                "currency": "RUB",
            },
            {
                "figi": "BOND2",
                "instrument_name": "Облигация 2",
                "event_type": "coupon",
                "payment_date": date(2026, 9, 7),
                "expected_amount": Decimal("200.00"),
                "currency": "RUB",
            },
        ]

        text = render_payout_calendar_text(
            rows,
            start_date=date(2026, 8, 4),
            end_date=date(2026, 11, 1),
            heading="Календарь",
        )

        self.assertIn("Август 2026: 87.00 ₽", text)
        self.assertIn("Сентябрь 2026: 174.00 ₽", text)

    def test_renderer_estimates_unknown_coupon_from_previous_payment(self):
        rows = [
            {
                "figi": "FLOAT1",
                "instrument_name": "Флоатер",
                "event_type": "coupon",
                "payment_date": date(2026, 10, 7),
                "expected_amount": None,
                "previous_coupon_amount_per_unit": Decimal("13.03"),
                "quantity": Decimal("10"),
                "currency": "RUB",
            }
        ]

        text = render_payout_calendar_text(
            rows,
            start_date=date(2026, 8, 4),
            end_date=date(2026, 11, 1),
            heading="Календарь",
        )

        self.assertIn(
            "Ожидаемая сумма после расчётного налога 13 %: ~ 113.36 ₽",
            text,
        )
        self.assertIn("Октябрь 2026: ~ 113.36 ₽", text)
        self.assertIn(
            "07.10 · купон · Флоатер · ~ 113.36 ₽ · по предыдущему купону",
            text,
        )
        self.assertNotIn("Без известной суммы", text)
        self.assertIn("Суммы с `~` оценены по предыдущему купону", text)

    def test_renderer_explains_empty_declared_calendar(self):
        text = render_payout_calendar_text(
            [],
            start_date=date(2026, 7, 28),
            end_date=date(2026, 10, 25),
            heading="Календарь",
        )

        self.assertIn("Ожидаемых купонов и объявленных дивидендов нет", text)
        self.assertIn("после официального объявления", text)


class WeeklyPayoutDeliveryTests(unittest.TestCase):
    def test_weekly_job_builds_monday_to_sunday_window_and_sends_to_all_chats(self):
        @contextmanager
        def fake_db_session():
            yield object()

        context = SimpleNamespace(bot=object())
        now_local = datetime(
            2026,
            8,
            3,
            10,
            0,
            0,
            tzinfo=bot_jobs.PAYOUT_WEEKLY_TZ,
        )
        with (
            mock.patch.object(
                bot_jobs,
                "_claim_scheduled_job_run",
                return_value=(True, True),
            ),
            mock.patch.object(bot_jobs, "db_session", fake_db_session),
            mock.patch.object(
                bot_jobs,
                "resolve_reporting_account_id",
                return_value="account",
            ),
            mock.patch.object(
                bot_jobs,
                "build_payout_calendar_text_for_account",
                return_value="weekly digest",
            ) as build_text,
            mock.patch.object(
                bot_jobs,
                "safe_send_message",
                new=mock.AsyncMock(),
            ) as send_message,
            mock.patch.object(bot_jobs, "_finalize_scheduled_job_run") as finalize,
            mock.patch.object(bot_jobs, "TARGET_CHAT_IDS", {1, 2}),
        ):
            asyncio.run(
                bot_jobs._run_payout_weekly_job(
                    context,
                    trigger_source="scheduled",
                    now_local=now_local,
                )
            )

        self.assertEqual(build_text.call_args.kwargs["start_date"], date(2026, 8, 3))
        self.assertEqual(build_text.call_args.kwargs["end_date"], date(2026, 8, 9))
        self.assertEqual(send_message.await_count, 2)
        self.assertEqual(finalize.call_args.kwargs["sent_total"], 2)
        self.assertEqual(finalize.call_args.kwargs["failed_total"], 0)


if __name__ == "__main__":
    unittest.main()
