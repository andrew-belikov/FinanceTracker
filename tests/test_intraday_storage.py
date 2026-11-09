from datetime import date, datetime, timezone
from decimal import Decimal
import unittest
from zoneinfo import ZoneInfo

from financetracker.database.base import Base
from financetracker.database.intraday import (
    get_intraday_day,
    mark_intraday_daily_summary_delivered,
    purge_delivered_intraday_days_before,
    upsert_intraday_daily_summary,
)
from financetracker.database.models import (
    IntradayDailySummary,
    IntradayPortfolioPosition,
    IntradayPortfolioSnapshot,
    PortfolioPosition,
    PortfolioSnapshot,
)
from financetracker.database.session import create_session_factory
from financetracker.tracker.snapshot_sync import (
    is_intraday_capture_window,
    take_snapshot_for_account,
)


class _Logger:
    def info(self, *_args, **_kwargs):
        pass


class IntradayStorageTests(unittest.TestCase):
    def setUp(self):
        self.engine, self.session_factory = create_session_factory("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.observed_date = date(2026, 9, 16)
        self.account = {"id": "account-1", "name": "ИИС"}

    def tearDown(self):
        self.engine.dispose()

    def test_snapshot_poll_appends_intraday_rows_without_changing_daily_contract(self):
        observed_at = iter(
            (
                datetime(2026, 9, 16, 7, 0, tzinfo=timezone.utc),
                datetime(2026, 9, 16, 7, 1, tzinfo=timezone.utc),
            )
        )
        portfolios = iter((self._portfolio(100), self._portfolio(125)))

        with self.session_factory() as session:
            for _ in range(2):
                take_snapshot_for_account(
                    session,
                    self.account,
                    api_get_portfolio=lambda _account_id: next(portfolios),
                    api_get_instrument_by_figi=lambda _figi: {
                        "ticker": "TEST",
                        "name": "Тест",
                    },
                    utc_now=lambda: next(observed_at),
                    local_today=lambda: self.observed_date,
                    portfolio_currency="rub",
                    logger=_Logger(),
                    should_capture_intraday=self._capture_intraday,
                )

            self.assertEqual(session.query(PortfolioSnapshot).count(), 1)
            self.assertEqual(session.query(PortfolioPosition).count(), 1)
            self.assertEqual(session.query(IntradayPortfolioSnapshot).count(), 2)
            self.assertEqual(session.query(IntradayPortfolioPosition).count(), 2)
            self.assertEqual(
                session.query(PortfolioSnapshot).one().total_value,
                Decimal("125.00"),
            )

            day = get_intraday_day(session, "account-1", self.observed_date)
            self.assertEqual(
                [snapshot.total_value for snapshot in day.snapshots],
                [Decimal("100.00"), Decimal("125.00")],
            )
            self.assertEqual(len(day.positions), 1)
            self.assertEqual(day.positions[0].identity, "asset-1")
            self.assertEqual(day.positions[0].first_value, Decimal("100.00"))
            self.assertEqual(day.positions[0].last_value, Decimal("125.00"))

            summary = upsert_intraday_daily_summary(session, day)
            self.assertEqual(summary.observation_count, 2)
            self.assertEqual(summary.min_value, Decimal("100.00"))
            self.assertEqual(summary.max_value, Decimal("125.00"))
            self.assertEqual(summary.total_value_delta, Decimal("25.00"))
            self.assertEqual(session.query(IntradayDailySummary).count(), 1)
            self.assertTrue(
                mark_intraday_daily_summary_delivered(
                    session,
                    "account-1",
                    self.observed_date,
                )
            )
            self.assertIsNotNone(summary.delivered_at)

            self.assertEqual(
                purge_delivered_intraday_days_before(session, "account-1", self.observed_date),
                0,
            )
            self.assertEqual(session.query(IntradayPortfolioSnapshot).count(), 2)
            self.assertEqual(
                purge_delivered_intraday_days_before(session, "account-1", date(2026, 9, 17)),
                2,
            )
            self.assertEqual(session.query(IntradayPortfolioSnapshot).count(), 0)
            self.assertEqual(session.query(IntradayPortfolioPosition).count(), 0)
            self.assertEqual(session.query(IntradayDailySummary).count(), 1)

    def _take_snapshot(self, session, account_id, day, value=100):
        take_snapshot_for_account(
            session, {"id": account_id, "name": "Тест"},
            api_get_portfolio=lambda _: self._portfolio(value),
            api_get_instrument_by_figi=lambda _: {"ticker": "TEST", "name": "Тест"},
            utc_now=lambda: datetime(day.year, day.month, day.day, 7, tzinfo=timezone.utc),
            local_today=lambda: day, portfolio_currency="RUB", logger=_Logger(),
            should_capture_intraday=self._capture_intraday,
        )

    def test_next_capture_purges_only_older_delivered_days_of_same_account(self):
        with self.session_factory() as session:
            for account, day in [("account-1", date(2026, 9, 14)), ("account-1", date(2026, 9, 15)), ("account-2", date(2026, 9, 15))]:
                self._take_snapshot(session, account, day)
                upsert_intraday_daily_summary(session, get_intraday_day(session, account, day))
            for account, day in [("account-1", date(2026, 9, 14)), ("account-2", date(2026, 9, 15))]:
                mark_intraday_daily_summary_delivered(session, account, day)
            session.commit()

            self._take_snapshot(session, "account-1", self.observed_date)
            session.commit()
            self.assertEqual(len(get_intraday_day(session, "account-1", date(2026, 9, 14)).snapshots), 0)
            self.assertEqual(len(get_intraday_day(session, "account-1", date(2026, 9, 15)).snapshots), 1)
            self.assertEqual(len(get_intraday_day(session, "account-2", date(2026, 9, 15)).snapshots), 1)
            self.assertEqual(len(get_intraday_day(session, "account-1", self.observed_date).snapshots), 1)
            self.assertEqual(session.query(IntradayPortfolioPosition).count(), 3)
            self.assertEqual(session.query(IntradayDailySummary).count(), 3)
            self._take_snapshot(session, "account-1", self.observed_date, value=125)
            self.assertEqual(len(get_intraday_day(session, "account-1", self.observed_date).snapshots), 2)

    def test_failed_new_capture_rolls_back_cleanup_and_new_observation(self):
        previous = date(2026, 9, 15)
        with self.session_factory() as session:
            self._take_snapshot(session, "account-1", previous)
            upsert_intraday_daily_summary(session, get_intraday_day(session, "account-1", previous))
            mark_intraday_daily_summary_delivered(session, "account-1", previous)
            session.commit()
            self._take_snapshot(session, "account-1", self.observed_date)
            session.rollback()
            self.assertEqual(len(get_intraday_day(session, "account-1", previous).snapshots), 1)
            self.assertEqual(len(get_intraday_day(session, "account-1", previous).positions), 1)
            self.assertEqual(len(get_intraday_day(session, "account-1", self.observed_date).snapshots), 0)

    def test_weekend_and_after_cutoff_do_not_purge_retained_days(self):
        previous = date(2026, 9, 18)
        with self.session_factory() as session:
            self._take_snapshot(session, "account-1", previous)
            upsert_intraday_daily_summary(session, get_intraday_day(session, "account-1", previous))
            mark_intraday_daily_summary_delivered(session, "account-1", previous)
            session.commit()
            self._take_snapshot(session, "account-1", date(2026, 9, 19))
            self.assertEqual(len(get_intraday_day(session, "account-1", previous).snapshots), 1)
            self._take_snapshot(session, "account-1", date(2026, 9, 21))
            self.assertEqual(len(get_intraday_day(session, "account-1", previous).snapshots), 0)

    def test_mark_delivered_leaves_missing_summary_unchanged(self):
        with self.session_factory() as session:
            self.assertFalse(
                mark_intraday_daily_summary_delivered(
                    session,
                    "account-1",
                    self.observed_date,
                )
            )

    def test_after_cutoff_and_weekend_keep_daily_snapshots_without_raw_intraday_rows(self):
        observed = (
            (datetime(2026, 9, 16, 16, 10, tzinfo=timezone.utc), date(2026, 9, 16)),
            (datetime(2026, 9, 19, 7, 0, tzinfo=timezone.utc), date(2026, 9, 19)),
        )
        with self.session_factory() as session:
            for index, (observed_at, snapshot_date) in enumerate(observed, start=1):
                take_snapshot_for_account(
                    session,
                    self.account,
                    api_get_portfolio=lambda _account_id, value=100 + index: self._portfolio(value),
                    api_get_instrument_by_figi=lambda _figi: {
                        "ticker": "TEST",
                        "name": "Тест",
                    },
                    utc_now=lambda observed_at=observed_at: observed_at,
                    local_today=lambda snapshot_date=snapshot_date: snapshot_date,
                    portfolio_currency="RUB",
                    logger=_Logger(),
                    should_capture_intraday=lambda value: is_intraday_capture_window(
                        value,
                        local_tz=ZoneInfo("Europe/Moscow"),
                        cutoff_hour=19,
                        cutoff_minute=10,
                    ),
                )

            self.assertEqual(session.query(PortfolioSnapshot).count(), 2)
            self.assertEqual(session.query(IntradayPortfolioSnapshot).count(), 0)
            self.assertEqual(session.query(IntradayPortfolioPosition).count(), 0)

    def test_capture_window_stops_exactly_at_cutoff(self):
        local_tz = ZoneInfo("Europe/Moscow")
        self.assertTrue(
            is_intraday_capture_window(
                datetime(2026, 9, 16, 16, 9, tzinfo=timezone.utc),
                local_tz=local_tz,
                cutoff_hour=19,
                cutoff_minute=10,
            )
        )
        self.assertFalse(
            is_intraday_capture_window(
                datetime(2026, 9, 16, 16, 10, tzinfo=timezone.utc),
                local_tz=local_tz,
                cutoff_hour=19,
                cutoff_minute=10,
            )
        )

    @staticmethod
    def _portfolio(value: int) -> dict:
        return {
            "totalAmountPortfolio": {"units": str(value), "nano": 0},
            "totalAmountShares": {"units": str(value), "nano": 0},
            "totalAmountBonds": {"units": "0", "nano": 0},
            "totalAmountEtf": {"units": "0", "nano": 0},
            "totalAmountCurrencies": {"units": "0", "nano": 0},
            "totalAmountFutures": {"units": "0", "nano": 0},
            "expectedYield": {"units": "0", "nano": 0},
            "positions": [
                {
                    "figi": "BBG000TEST",
                    "asset_uid": "asset-1",
                    "instrument_uid": "instrument-1",
                    "quantity": {"units": "1", "nano": 0},
                    "currentPrice": {"units": str(value), "nano": 0},
                    "expectedYield": {"units": "0", "nano": 0},
                    "instrumentType": "share",
                }
            ],
        }

    @staticmethod
    def _capture_intraday(observed_at: datetime) -> bool:
        return is_intraday_capture_window(
            observed_at,
            local_tz=ZoneInfo("Europe/Moscow"),
            cutoff_hour=19,
            cutoff_minute=10,
        )


if __name__ == "__main__":
    unittest.main()
