from __future__ import annotations

from datetime import datetime  # noqa: F401 - compatibility monkeypatch seam

from financetracker.domain.assets import build_logical_asset_id  # noqa: F401
from financetracker.bot.runtime import TZ, db_session  # noqa: F401 - compatibility monkeypatch seams




# Compatibility facade for existing bot callers. Owner modules must not import this module.
from financetracker.bot import (
    payout_service as _payout_service,
    performance_service as _performance_service,
    rebalance_service as _rebalance_service,
    summary_service as _summary_service,
    year_summary_service as _year_summary_service,
    alerts_service as _alerts_service,
)
from financetracker.bot.alerts_service import (  # noqa: F401 - compatibility monkeypatch seams
    get_max_snapshot_before_date,
    get_snapshot_for_date,
)
from financetracker.bot.portfolio_repository import (  # noqa: F401 - compatibility monkeypatch seams
    get_latest_snapshot_with_id,
    get_latest_snapshots,
    get_month_snapshots,
    get_positions_for_snapshot,
)
from financetracker.bot.reporting_account import resolve_reporting_account_id  # noqa: F401

_OWNER_EXPORTS = {
    _payout_service: ('render_payout_calendar_text', 'build_payout_calendar_text_for_account',),
    _performance_service: ('append_tax_refund_line', 'append_income_currency_breakdown', 'render_twr_summary_text', 'build_net_external_flow_by_day', 'compute_twr_timeseries', 'build_xirr_external_cashflows', 'compute_portfolio_xirr_and_run_rate',),
    _rebalance_service: ('format_decimal_number', 'format_decimal_pct', 'format_decimal_pp', 'format_rebalance_weight', 'format_human_date_ru', 'parse_rebalance_targets_args', 'get_rebalance_targets', 'replace_rebalance_targets', 'get_latest_rebalance_snapshot', '_build_rebalance_diff_lines', '_build_out_of_model_lines', 'build_targets_text_for_account', 'build_rebalance_text_for_account', 'build_invest_text_for_account',),
    _summary_service: ('build_help_text', 'compute_positions_diff_lines', 'compute_positions_diff_grouped', 'build_today_summary', 'build_week_summary', 'build_month_summary', '_resolve_monthly_asset_identity', '_build_monthly_position_flow_groups', '_build_monthly_instrument_payload', 'build_structure_text',),
    _year_summary_service: ('get_year_period', '_format_asset_lines', 'build_year_summary',),
    _alerts_service: ('build_triggers_messages', '_format_alert_date', 'build_yesterday_peak_alert_message',),
}

_OWNER_EXPORT_NAMES = {name for names in _OWNER_EXPORTS.values() for name in names}
_FACADE_EXPORTS = {}

_OWNER_DEPENDENCIES = {
    _payout_service: ("PAYOUT_CALENDAR_MAX_LISTED_EVENTS", "TZ"),
    _performance_service: ("datetime", "get_latest_snapshot_with_id"),
    _rebalance_service: ("get_latest_snapshot_with_id", "get_positions_for_snapshot"),
    _summary_service: (
        "PHRASES_AHEAD", "PHRASES_ON_TRACK", "PHRASES_BEHIND", "TZ",
        "build_logical_asset_id", "datetime", "db_session",
        "get_commissions_for_period", "get_deposits_for_period",
        "get_income_currency_breakdown_for_period", "get_income_for_period",
        "get_iis_tax_deductions_for_period", "get_last_snapshot_before_date",
        "get_latest_snapshot_with_id", "get_latest_snapshots", "get_month_snapshots",
        "get_net_external_contributions", "get_net_external_flow_for_period",
        "get_positions_for_snapshot",
        "get_taxes_for_period", "get_tax_refunds_for_period",
        "resolve_reporting_account_id",
    ),
    _year_summary_service: (
        "TZ", "YEAR_REPORT_TOP_N", "datetime", "db_session",
        "get_net_external_flow_for_period", "resolve_reporting_account_id",
    ),
    _alerts_service: (
        "TZ", "datetime", "db_session", "get_deposits_for_period",
        "get_latest_snapshots", "get_max_snapshot_before_date",
        "get_snapshot_for_date", "resolve_reporting_account_id",
    ),
}

# Query seams are still owned by the summary use case.  They remain here only
# while callers rely on patching the old services module.
get_commissions_for_period = _summary_service.get_commissions_for_period
get_deposits_for_period = _summary_service.get_deposits_for_period
get_income_currency_breakdown_for_period = _summary_service.get_income_currency_breakdown_for_period
get_income_for_period = _summary_service.get_income_for_period
get_iis_tax_deductions_for_period = _summary_service.get_iis_tax_deductions_for_period
get_last_snapshot_before_date = _summary_service.get_last_snapshot_before_date
get_net_external_contributions = _summary_service.get_net_external_contributions
get_net_external_flow_for_period = _summary_service.get_net_external_flow_for_period
get_taxes_for_period = _summary_service.get_taxes_for_period
get_tax_refunds_for_period = _summary_service.get_tax_refunds_for_period

def _sync_owner_dependencies(owner) -> None:
    """Forward only documented compatibility seams to the owning module."""
    for name in _OWNER_DEPENDENCIES[owner]:
        setattr(owner, name, globals()[name])
    for name in _OWNER_EXPORTS[owner]:
        value = globals().get(name)
        if value is not None and value is not _FACADE_EXPORTS[name]:
            setattr(owner, name, value)

def _delegate(owner, name):
    def delegated(*args, **kwargs):
        _sync_owner_dependencies(owner)
        return getattr(owner, name)(*args, **kwargs)
    delegated.__name__ = name
    delegated.__qualname__ = name
    return delegated

for _owner, _names in _OWNER_EXPORTS.items():
    for _name in _names:
        globals()[_name] = _delegate(_owner, _name)

_FACADE_EXPORTS = {name: globals()[name] for name in _OWNER_EXPORT_NAMES}

YEAR_REPORT_TOP_N = _year_summary_service.YEAR_REPORT_TOP_N
PAYOUT_CALENDAR_MAX_LISTED_EVENTS = _payout_service.PAYOUT_CALENDAR_MAX_LISTED_EVENTS
PHRASES_AHEAD = _summary_service.PHRASES_AHEAD
PHRASES_ON_TRACK = _summary_service.PHRASES_ON_TRACK
PHRASES_BEHIND = _summary_service.PHRASES_BEHIND

del _name, _names, _owner
