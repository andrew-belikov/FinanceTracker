from decimal import Decimal
import unittest
from unittest.mock import Mock
from datetime import date, datetime, timezone

from financetracker.domain.daily_change import calculate_daily_change
from financetracker.bot.daily_report_repository import get_daily_report_events


D = Decimal


def holding(**changes):
    return dict(dict(figi='synthetic-fund', name='Тестовый фонд', ticker='TEST', instrument_type='etf', currency='RUB',
                     first_value=D('100'), last_value=D('132'), first_quantity=D('10'), last_quantity=D('12'),
                     first_nkd=None, last_nkd=None), **changes)


def trade(quantity, price, *, nkd=None, figi='synthetic-fund', instrument_type='etf', amount=None):
    quantity, price = D(str(quantity)), D(str(price))
    nkd = D(str(nkd)) if nkd is not None else None
    payment = -quantity * price + (-abs(nkd or 0) if quantity > 0 else abs(nkd or 0))
    return dict(kind='trade', figi=figi, quantity_delta=quantity, unit_price=price, accrued_int=nkd,
                instrument_type=instrument_type, name='Тестовая бумага', currency='RUB',
                amount=D(str(amount)) if amount is not None else payment)


class DailyTradeAttributionTests(unittest.TestCase):
    def check(self, positions, trades, expected_prices, expected_accrual=0, fees=0):
        expected_prices, expected_accrual, fees = map(lambda x: D(str(x)), (expected_prices, expected_accrual, fees))
        events = list(trades) + ([dict(kind='commission', amount=fees)] if fees else [])
        result = calculate_daily_change(opening=D('1000'), closing=D('1000')+expected_prices+expected_accrual+fees,
                                        positions=positions, cash_events=events)
        self.assertEqual(result['components']['prices'], expected_prices)
        self.assertEqual(result['components']['accrual'], expected_accrual)
        self.assertEqual(result['components']['commission'], fees)
        self.assertEqual(result['components']['other'], 0)
        self.assertFalse(result['incomplete_attribution'])
        self.assertEqual(sum(result['components'].values()), result['change'])
        return result

    def test_purchase_of_existing_fund_keeps_price_change_and_fee_separate(self):
        self.check([holding()], [trade(2, '10.5')], 11, fees='-0.10')

    def test_partial_sale_preserves_realized_and_unrealized_daily_result(self):
        self.check([holding(last_value=D('72'), last_quantity=D('6'))], [trade(-4, 11)], 16, fees=-1)

    def test_new_holding_purchase_is_not_market_growth(self):
        self.check([holding(first_value=None, first_quantity=None, last_value=D('55'), last_quantity=D('5'))],
                   [trade(5, 10)], 5)

    def test_closed_holding_sale_is_not_market_loss(self):
        self.check([holding(last_value=None, last_quantity=None)], [trade(-10, 11)], 10)

    def test_buy_and_sell_with_same_final_quantity(self):
        self.check([holding(last_value=D('100'), last_quantity=D('10'))], [trade(3, 10), trade(-3, 11)], 3)

    def test_round_trip_without_any_endpoint_holding(self):
        result=self.check([], [trade(3, 10), trade(-3, 11)], 3)
        self.assertEqual(result['movers'][0]['contribution'], 3)

    def test_multiple_purchases(self):
        self.check([holding(last_quantity=D('15'), last_value=D('165'))], [trade(2, '10.5'), trade(3, 10)], 14)

    def test_bond_purchase_separates_paid_accrued_interest(self):
        self.check([holding(instrument_type='bond', first_quantity=D('2'), last_quantity=D('3'),
                            first_value=D('200'), last_value=D('303'), first_nkd=D('10'), last_nkd=D('11'))],
                   [trade(1, 100, nkd='10.5', instrument_type='bond')], 3, '2.5')

    def test_bond_sale_separates_received_accrued_interest(self):
        self.check([holding(instrument_type='bond', first_quantity=D('3'), last_quantity=D('2'),
                            first_value=D('300'), last_value=D('202'), first_nkd=D('10'), last_nkd=D('11'))],
                   [trade(-1, 101, nkd='10.5', instrument_type='bond')], 3, '2.5')

    def test_new_bond_with_paid_nkd(self):
        self.check([holding(instrument_type='bond', first_quantity=None, last_quantity=D('1'),
                            first_value=None, last_value=D('105'), first_nkd=None, last_nkd=D('11'))],
                   [trade(1, 100, nkd='10.5', instrument_type='bond')], 5, '.5')

    def test_fully_closed_bond_preserves_accrual_until_sale(self):
        self.check([holding(instrument_type='bond', first_quantity=D('2'), last_quantity=None,
                            first_value=D('200'), last_value=None, first_nkd=D('10'), last_nkd=None)],
                   [trade(-2, 105, nkd=22, instrument_type='bond')], 10, 2)

    def test_cash_balance_change_is_not_a_security_price_change(self):
        self.check([holding(), holding(figi='synthetic-ruble', instrument_type='currency',
                                     first_value=D('100'), last_value=D('50'), last_quantity=D('5'))],
                   [trade(2, '10.5')], 11)

    def test_quantity_mismatch_missing_price_and_inconsistent_cash_stay_unconfirmed(self):
        for execution in (trade(1, '10.5'), dict(trade(2, '10.5'), unit_price=None),
                          trade(2, '10.5', amount='-21.60'), dict(trade(2, '10.5'), quantity_delta=None)):
            with self.subTest(execution=execution):
                result=calculate_daily_change(opening=1000, closing=1011, positions=[holding()], cash_events=[execution])
                self.assertEqual(result['components']['prices'], 0)
                self.assertEqual(result['components']['other'], 11)
                self.assertTrue(result['incomplete_attribution'])

    def test_trade_with_missing_bond_nkd_is_not_assumed_clean(self):
        result=calculate_daily_change(opening=1000, closing=1011,
            positions=[holding(instrument_type='bond', first_nkd=D('10'), last_nkd=D('11'))],
            cash_events=[trade(2, '10.5', amount='-25', instrument_type='bond')])
        self.assertEqual(result['components']['prices'], 0)
        self.assertEqual(result['components']['other'], 11)

    def test_nontrading_quantity_change_remains_unexplained(self):
        result=calculate_daily_change(opening=1000, closing=1011, positions=[holding()])
        self.assertEqual(result['components']['prices'], 0)
        self.assertTrue(result['incomplete_attribution'])


class TradeRepositoryTests(unittest.TestCase):
    def test_executed_units_prices_and_nkd_are_preserved_without_fee_duplication(self):
        session=Mock()
        session.execute.return_value.mappings.return_value.all.return_value=[
            dict(figi='synthetic-bond', name='Тестовая облигация', operation_type='OPERATION_TYPE_BUY',
                 amount=D('-220'), currency='RUB', cashflow_category=None, quantity=5, quantity_done=2,
                 price=D('100'), accrued_int=D('20'), instrument_type='bond'),
            dict(figi='synthetic-bond', name='Тестовая облигация', operation_type='OPERATION_TYPE_SELL',
                 amount=D('111'), currency='RUB', cashflow_category=None, quantity=1, quantity_done=None,
                 price=D('101'), accrued_int=D('10'), instrument_type='bond')]
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value=[]
        events=get_daily_report_events(session,'synthetic-account',date(2026,2,10),'RUB',
                                       start=datetime(2026,2,10,tzinfo=timezone.utc),end=datetime(2026,2,11,tzinfo=timezone.utc))
        self.assertEqual(events[0]['quantity_delta'], 2)
        self.assertEqual(events[1]['quantity_delta'], -1)
        self.assertEqual(events[0]['accrued_int'], 20)
        self.assertEqual(events[0]['amount'], -220)
        self.assertEqual(events[0]['unit_price'], 100)
        self.assertEqual(len(events), 2)
        query=session.execute.call_args.args[0]
        self.assertIn('JOIN operations o ON o.id = d.id',str(query))
