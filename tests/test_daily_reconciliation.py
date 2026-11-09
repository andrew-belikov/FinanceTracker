from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from financetracker.bot import daily_report_repository as repository, today_report
from financetracker.bot.intraday_daily_push import build_intraday_daily_push_payload_from_day
from financetracker.database.base import Base
from financetracker.database.intraday import (
    IntradayDay, IntradayPosition, IntradaySnapshot, get_intraday_day,
)
from financetracker.database.models import (
    IntradayPortfolioPosition, IntradayPortfolioSnapshot, PortfolioPosition, PortfolioSnapshot,
)
from financetracker.domain.daily_change import calculate_daily_change, pair_positions


D = Decimal
DAY = date(2026, 2, 10)
START = datetime(2026, 2, 9, 20, 59, tzinfo=timezone.utc)
END = datetime(2026, 2, 10, 12, tzinfo=timezone.utc)


def position(**changes):
    row = dict(figi="synthetic-bond", name="Тестовая облигация", ticker="TEST", currency="RUB",
               first_value=D("1000"), last_value=D("1010"), first_quantity=D("2"),
               last_quantity=D("2"), first_nkd=D("10"), last_nkd=D("0"))
    return dict(row, **changes)


def report_day():
    pos = IntradayPosition(identity="synthetic-bond", **position())
    return IntradayDay("synthetic-account", DAY,
                       (IntradaySnapshot(START + timedelta(minutes=2), D("1020"), "RUB"),
                        IntradaySnapshot(END, D("1010"), "RUB")), (pos,),
                       IntradaySnapshot(START, D("1020"), "RUB"))


def payload(day=None, events=()):
    return build_intraday_daily_push_payload_from_day(day=day or report_day(), cash_events=events)


class DailyChangeTests(unittest.TestCase):
    def calculate(self, **changes):
        args = dict(opening=D("1020"), closing=D("1010"), positions=[position()])
        return calculate_daily_change(**dict(args, **changes))

    def assert_reconciles(self, result):
        self.assertEqual(sum(result["components"].values()), result["change"])

    def test_coupon_reset_explains_loss_despite_price_gain(self):
        result = self.calculate()
        self.assertEqual(result["components"]["prices"], D("10"))
        self.assertEqual(result["components"]["coupon_reset"], D("-20"))
        self.assertEqual(result["components"]["other"], 0)
        self.assertEqual(result["change"], D("-10"))
        self.assert_reconciles(result)

    def test_receipt_and_tax_count_once(self):
        result = self.calculate(closing=D("1027"), cash_events=[
            {"kind": "coupon", "amount": D("20")}, {"kind": "tax", "amount": D("-3")},
        ])
        self.assertEqual(result["change"], D("7"))
        self.assertEqual(result["components"]["other"], 0)
        self.assert_reconciles(result)

    def test_next_day_receipt_has_no_second_coupon_reset(self):
        result = self.calculate(opening=D("1010"), closing=D("1027"),
                                positions=[position(first_value=D("1010"), first_nkd=D("0"))],
                                cash_events=[{"kind": "coupon", "amount": D("20")}, {"kind": "tax", "amount": D("-3")}])
        self.assertEqual(result["components"]["coupon_reset"], 0)
        self.assertEqual(result["components"]["other"], 0)
        self.assert_reconciles(result)

    def test_deposits_withdrawals_are_excluded_but_deduction_is_income(self):
        result = self.calculate(closing=D("1060"), cash_events=[
            {"kind": "deposit", "amount": D("100")}, {"kind": "withdrawal", "amount": D("-60")},
            {"kind": "deduction", "amount": D("10")},
        ])
        self.assertEqual(result["change"], 0)
        self.assertEqual(result["components"]["deduction"], D("10"))
        self.assertEqual(result["components"]["other"], 0)
        self.assert_reconciles(result)

    def test_trade_or_quantity_change_is_never_reported_as_price_growth(self):
        for rows, events in (([position(last_quantity=D("4"))], []),
                             ([position()], [{"kind": "trade", "figi": "synthetic-bond", "amount": -100}])):
            with self.subTest(rows=rows, events=events):
                result = self.calculate(positions=rows, cash_events=events)
                self.assertEqual(result["movers"], [])
                self.assertTrue(result["incomplete_attribution"])
                self.assert_reconciles(result)

    def test_foreign_currency_or_missing_endpoints_go_to_residual(self):
        for row in (position(currency="USD"), position(first_value=None), position(last_value=None)):
            result = self.calculate(positions=[row])
            self.assertEqual(result["components"]["prices"], 0)
            self.assertEqual(result["components"]["other"], D("-10"))
            self.assert_reconciles(result)

    def test_fractional_accrual_rounding_keeps_displayed_kopeks_equal(self):
        result = self.calculate(positions=[position(first_quantity=D("0.333333"), last_quantity=D("0.333333"))])
        self.assertTrue(all(value.as_tuple().exponent == -2 for value in result["components"].values()))
        self.assert_reconciles(result)

    def test_accrual_fees_dividends_refunds_are_separate(self):
        result = self.calculate(closing=D("1033"), positions=[position(last_nkd=D("11"))], cash_events=[
            {"kind": "dividend", "amount": 5}, {"kind": "commission", "amount": -5},
            {"kind": "tax_refund", "amount": 1},
        ])
        self.assertEqual(result["components"]["accrual"], D("2"))
        self.assertEqual(result["components"]["other"], 0)
        self.assert_reconciles(result)

    def test_new_closed_and_alias_positions_use_complete_endpoints(self):
        before = [dict(figi="old", asset_uid="asset", position_value=100), dict(figi="closed", position_value=50)]
        after = [dict(figi="replacement", asset_uid="asset", position_value=110), dict(figi="new", position_value=30)]
        rows = {row["identity"]: row for row in pair_positions(before, after)}
        self.assertEqual((rows["asset"]["first_value"], rows["asset"]["last_value"]), (100, 110))
        self.assertIsNone(rows["closed"]["last_value"])
        self.assertIsNone(rows["new"]["first_value"])


class DailyPresentationTests(unittest.TestCase):
    def schedule(self):
        return {"kind": "coupon_schedule", "amount": 0, "figi": "synthetic-bond", "payment_date": DAY + timedelta(days=1), "amount_per_unit": D("11")}

    def test_expected_coupon_is_only_a_note_not_a_component(self):
        base, with_schedule = payload(), payload(events=[self.schedule()])
        self.assertEqual(base["change"], with_schedule["change"])
        self.assertEqual(base["components"], with_schedule["components"])
        self.assertIn("22,00 ₽ до налога", with_schedule["text"])
        self.assertIn("Зачисление пока не подтверждено", with_schedule["text"])
        self.assertIn("Цены бумаг: +10,00 ₽", with_schedule["text"])
        self.assertIn("Снижение накопленных купонов: −20,00 ₽", with_schedule["text"])

    def test_unknown_calendar_does_not_promise_a_payment(self):
        self.assertIn("пока не подтверждена календарём", payload()["text"])
        self.assertNotIn("По календарю выплата", payload()["text"])

    def test_receipt_note_requires_same_instrument(self):
        event = {"kind": "coupon", "amount": 22, "figi": "different-bond"}
        self.assertIn("Зачисление пока не подтверждено", payload(events=[self.schedule(), event])["text"])
        event["figi"] = "synthetic-bond"
        self.assertIn("Зачисление купона найдено", payload(events=[self.schedule(), event])["text"])

    def test_same_endpoint_today_and_digest_have_identical_changes_and_components(self):
        day = report_day()
        today = replace(day, snapshots=(day.snapshots[-1],))
        first = build_intraday_daily_push_payload_from_day(day=today, minimum_observations=1)
        second = payload(day)
        self.assertEqual(first["change"], second["change"])
        self.assertEqual(first["components"], second["components"])
        self.assertEqual(first["opening_value"], second["opening_value"])
        self.assertNotIn("Диапазон наблюдений", first["text"])
        self.assertNotIn("Амплитуда", first["text"])

    def test_one_intraday_observation_does_not_create_a_daily_digest(self):
        day = report_day()
        self.assertIsNone(payload(replace(day, snapshots=(day.snapshots[-1],))))

    def test_mixed_portfolio_currencies_fail_without_implicit_conversion(self):
        day = report_day()
        with self.assertRaises(ValueError):
            payload(replace(day, baseline=replace(day.baseline, currency="USD")))

    def test_chart_has_separate_complete_reconciliation_and_prices_panels(self):
        from financetracker.bot.charts import build_intraday_daily_push_chart
        figures = []
        report = payload()
        with patch("matplotlib.figure.Figure.savefig", autospec=True, side_effect=lambda fig, *args, **kwargs: figures.append(fig)):
            build_intraday_daily_push_chart("unused.png", report_date=DAY, snapshots=report["snapshots"],
                                            positive_movers=report["positive_movers"], negative_movers=report["negative_movers"],
                                            change_components=report["components"])
        self.assertEqual(len(figures[0].axes), 3)
        self.assertIn("без НКД", figures[0].axes[1].get_title(loc="left"))
        self.assertIn("-10,00 ₽", figures[0].axes[2].get_title(loc="left"))
        self.assertEqual([row.get_width() for row in figures[0].axes[2].patches], [10, -20])


    def test_many_coupon_resets_keep_all_amounts_but_bound_detailed_notes(self):
        day = report_day()
        day = replace(day, positions=tuple(replace(day.positions[0], identity=str(i), figi=str(i)) for i in range(8)))
        report = payload(day)
        self.assertEqual(report["reconciliation"]["components"]["coupon_reset"], D("-160"))
        self.assertEqual(report["text"].count("Снижение НКД:"), 3)
        self.assertIn("Ещё 5 бумаг", report["text"])



class DailyRepositoryTests(unittest.TestCase):
    def test_previous_close_is_shared_and_missing_positions_are_not_carried_forward(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            previous = PortfolioSnapshot(account_id="a", snapshot_date=DAY - timedelta(days=1), snapshot_at=START, total_value=1020, currency="RUB")
            current = PortfolioSnapshot(account_id="a", snapshot_date=DAY, snapshot_at=END, total_value=1010, currency="RUB")
            first = IntradayPortfolioSnapshot(account_id="a", snapshot_date=DAY, snapshot_at=START + timedelta(minutes=2), total_value=1020, currency="RUB")
            last = IntradayPortfolioSnapshot(account_id="a", snapshot_date=DAY, snapshot_at=END, total_value=1010, currency="RUB")
            session.add_all([previous, current, first, last])
            session.flush()
            for cls, snap, value, nkd in ((PortfolioPosition, previous, 1000, 10), (PortfolioPosition, current, 1010, 0),
                                         (IntradayPortfolioPosition, first, 1000, 10), (IntradayPortfolioPosition, last, 1010, 0)):
                session.add(cls(snapshot_id=snap.id, figi="synthetic-bond", name="Тест", ticker="TEST", currency="RUB", quantity=2, position_value=value, current_nkd=nkd))
            session.add(PortfolioPosition(snapshot_id=previous.id, figi="closed", quantity=1, position_value=5, currency="RUB"))
            session.commit()
            daily = repository.load_today_day(session, "a")
            intraday = get_intraday_day(session, "a", DAY)
            self.assertEqual(daily.baseline.total_value, intraday.baseline.total_value)
            self.assertEqual(daily.positions, intraday.positions)
            closed = next(row for row in intraday.positions if row.figi == "closed")
            self.assertIsNone(closed.last_value)
            self.assertEqual(repository.report_bounds(daily), (START.replace(tzinfo=None), END.replace(tzinfo=None)))
        engine.dispose()

    def test_cash_reads_exact_bounds_and_includes_taxes_only_once(self):
        session = Mock()
        rows = [dict(figi="bond", operation_type=typ, amount=amount, cashflow_category=None) for typ, amount in
                (("OPERATION_TYPE_COUPON", D("20")), ("OPERATION_TYPE_BOND_TAX", D("-3")),
                 ("OPERATION_TYPE_TAX", D("2")), ("OPERATION_TYPE_BUY", D("-50")))]
        session.execute.return_value.mappings.return_value.all.return_value = rows
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        events = repository.get_daily_report_events(session, "a", DAY, "RUB", start=START, end=END)
        self.assertEqual([event["kind"] for event in events], ["coupon", "tax", "tax_refund", "trade"])
        query, params = session.execute.call_args.args
        self.assertIn("date > :start AND date <= :end", str(query).replace("d.", ""))
        self.assertEqual((params["start"], params["end"]), (START, END))
        self.assertNotIn("UPPER(currency) =", str(query))

    def test_foreign_external_flow_is_not_disguised_as_fully_adjusted_result(self):
        session = Mock()
        session.execute.return_value.mappings.return_value.all.return_value = [
            dict(figi=None, operation_type="OPERATION_TYPE_INPUT", amount=D("100"), currency="USD")]
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        events = repository.get_daily_report_events(session, "a", DAY, "RUB", start=START, end=END)
        text = payload(events=events)["text"]
        self.assertIn("Валютные пополнения и выводы исключены не полностью", text)
        self.assertIn("Изменение:", text)
        self.assertNotIn("Изменение без пополнений", text)

    def test_future_calendar_is_not_used_to_explain_a_historical_report(self):
        session = Mock()
        session.execute.return_value.mappings.return_value.all.return_value = []
        schedule = SimpleNamespace(figi="bond", amount_per_unit=D("11"), payment_date=DAY, fetched_at=END + timedelta(days=1))
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [schedule]
        self.assertEqual(repository.get_daily_report_events(session, "a", DAY, "RUB", start=START, end=END), [])


class TodayReportTests(unittest.TestCase):
    def render(self, day=None, events=(), contributions=D("1010"), currencies=()):
        self.bounds = Mock(return_value=list(events))
        with (patch.object(today_report, "db_session", return_value=nullcontext(Mock())),
              patch.object(today_report, "resolve_reporting_account_id", return_value="a"),
              patch.object(today_report, "load_today_day", return_value=day or report_day()),
              patch.object(today_report, "get_daily_report_events", self.bounds),
              patch.object(today_report, "get_net_external_contributions", return_value=contributions),
              patch.object(today_report, "get_income_currency_breakdown_for_period", return_value=list(currencies))):
            return today_report.build_today_report()

    def test_current_view_uses_exact_snapshot_bounds_and_keeps_lifetime_net_contributions(self):
        text = self.render()
        self.assertIn("Результат за всё время по текущей оценке: 0,00 ₽ (0,00%)", text)
        self.assertEqual(self.bounds.call_args.kwargs, {"start": START, "end": END})

    def test_snapshot_gap_excludes_full_interval_flow(self):
        day = report_day()
        day = replace(day, baseline=replace(day.baseline, observed_at=START - timedelta(days=3)))
        text = self.render(day, events=[{"kind": "deposit", "amount": 50}])
        self.assertIn("Изменение: −60,00 ₽", text)
        self.assertEqual(self.bounds.call_args.kwargs["start"], START - timedelta(days=3))

    def test_single_snapshot_does_not_invent_a_result(self):
        day = report_day()
        day = replace(day, baseline=None, snapshots=(day.snapshots[-1],))
        text = self.render(day)
        self.assertIn("Нет предыдущей оценки", text)
        self.assertNotIn("Изменение:", text)

    def test_other_income_currencies_remain_visible_without_implicit_conversion(self):
        text = self.render(currencies=[{"currency": "USD", "coupons": D("4"), "dividends": D("0"), "taxes": D("1"), "tax_refunds": D("0")},
                                       {"currency": "UNKNOWN", "coupons": D("2"), "dividends": D("0"), "taxes": D("0"), "tax_refunds": D("0")}])
        self.assertIn("USD", text)
        self.assertIn("UNKNOWN", text)
        self.assertIn("не включена", text)


class TodayCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_security_names_are_sent_as_literal_text(self):
        from unittest.mock import AsyncMock
        from financetracker.bot import handlers
        context = SimpleNamespace(bot=object())
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=1))
        text = "Фонд *акций* (TEST_NAME)"
        with (patch.object(handlers, "log_update_received"),
              patch.object(handlers, "require_authorized_private_chat", new=AsyncMock(return_value=True)),
              patch.object(handlers, "run_blocking_command", new=AsyncMock(return_value=text)),
              patch.object(handlers, "safe_send_message", new_callable=AsyncMock) as send):
            await handlers.cmd_today(update, context)
        send.assert_awaited_once_with(context.bot, 1, text, parse_mode=None)
