"""SQLAlchemy models owned by the database infrastructure layer."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from financetracker.common.time_utils import utc_now
from financetracker.database.base import Base


class Instrument(Base):
    __tablename__ = "instruments"

    id = Column(Integer, primary_key=True)
    figi = Column(String, unique=True, nullable=False)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    class_code = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint("account_id", "snapshot_date", name="uq_snapshot_account_date"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    account_name = Column(String, nullable=True)
    snapshot_at = Column(DateTime(timezone=True), nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    currency = Column(String, nullable=False)
    total_value = Column(Numeric(18, 2), nullable=True)
    total_shares = Column(Numeric(18, 2), nullable=True)
    total_bonds = Column(Numeric(18, 2), nullable=True)
    total_etf = Column(Numeric(18, 2), nullable=True)
    total_currencies = Column(Numeric(18, 2), nullable=True)
    total_futures = Column(Numeric(18, 2), nullable=True)
    expected_yield = Column(Numeric(18, 2), nullable=True)
    expected_yield_pct = Column(Numeric(9, 4), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)

    positions = relationship("PortfolioPosition", back_populates="snapshot", cascade="all, delete-orphan")


class PortfolioPosition(Base):
    __tablename__ = "portfolio_positions"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("portfolio_snapshots.id"), nullable=False)
    figi = Column(String, nullable=False)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), nullable=True)
    instrument_uid = Column(String, nullable=True)
    position_uid = Column(String, nullable=True)
    asset_uid = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    quantity = Column(Numeric(18, 6), nullable=True)
    currency = Column(String, nullable=True)
    current_price = Column(Numeric(18, 4), nullable=True)
    current_nkd = Column(Numeric, nullable=True)
    position_value = Column(Numeric(18, 2), nullable=True)
    expected_yield = Column(Numeric(18, 2), nullable=True)
    expected_yield_pct = Column(Numeric(9, 4), nullable=True)
    weight_pct = Column(Numeric(9, 4), nullable=True)
    raw_payload_json = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)

    snapshot = relationship("PortfolioSnapshot", back_populates="positions")
    instrument = relationship("Instrument")


class Operation(Base):
    __tablename__ = "operations"
    __table_args__ = (
        UniqueConstraint("account_id", "operation_id", name="uq_operations_account_operation"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    operation_id = Column(String, nullable=False)
    operation_type = Column(String, nullable=False)
    cashflow_category = Column(String, nullable=True)
    cursor = Column(String, nullable=True)
    broker_account_id = Column(String, nullable=True)
    parent_operation_id = Column(String, nullable=True)
    name = Column(String, nullable=True)
    state = Column(String, nullable=True)
    instrument_uid = Column(String, nullable=True)
    figi = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    instrument_kind = Column(String, nullable=True)
    position_uid = Column(String, nullable=True)
    asset_uid = Column(String, nullable=True)
    date = Column(DateTime(timezone=True), nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    price = Column(Numeric(18, 9), nullable=True)
    commission = Column(Numeric(18, 9), nullable=True)
    yield_amount = Column("yield", Numeric(18, 9), nullable=True)
    yield_relative = Column(Numeric(18, 9), nullable=True)
    accrued_int = Column(Numeric(18, 9), nullable=True)
    quantity = Column(BigInteger, nullable=True)
    quantity_rest = Column(BigInteger, nullable=True)
    quantity_done = Column(BigInteger, nullable=True)
    currency = Column(String, nullable=False)
    cancel_date_time = Column(DateTime(timezone=True), nullable=True)
    cancel_reason = Column(String, nullable=True)
    description = Column(String, nullable=True)
    source = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)


class IncomeEvent(Base):
    __tablename__ = "income_events"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "figi", "event_date", "event_type", "currency",
            name="uq_income_events_account_figi_date_type_currency",
        ),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    figi = Column(String, nullable=False)
    event_date = Column(Date, nullable=False)
    event_type = Column(String, nullable=False)
    currency = Column(String, nullable=False)
    gross_amount = Column(Numeric(18, 2), nullable=False)
    tax_amount = Column(Numeric(18, 2), nullable=False)
    net_amount = Column(Numeric(18, 2), nullable=False)
    net_yield_pct = Column(Numeric(9, 4), nullable=False)
    notified = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)


class PayoutCalendarEvent(Base):
    __tablename__ = "payout_calendar_events"
    __table_args__ = (
        UniqueConstraint("account_id", "figi", "event_type", "event_uid", name="uq_payout_calendar_event_source"),
        Index("ix_payout_calendar_events_account_payment", "account_id", "payment_date"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    figi = Column(String, nullable=False)
    instrument_uid = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    instrument_type = Column(String, nullable=True)
    event_type = Column(String, nullable=False)
    event_uid = Column(String, nullable=False)
    payment_date = Column(Date, nullable=False, index=True)
    record_date = Column(Date, nullable=True)
    last_buy_date = Column(Date, nullable=True)
    coupon_start_date = Column(Date, nullable=True)
    coupon_end_date = Column(Date, nullable=True)
    coupon_period_days = Column(Integer, nullable=True)
    amount_per_unit = Column(Numeric(18, 9), nullable=True)
    quantity = Column(Numeric(18, 6), nullable=False)
    expected_amount = Column(Numeric(18, 2), nullable=True)
    currency = Column(String, nullable=True)
    source_event_type = Column(String, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)


class AssetAlias(Base):
    __tablename__ = "asset_aliases"
    __table_args__ = (
        UniqueConstraint("asset_uid", "instrument_uid", "figi", name="uq_asset_aliases_asset_instrument_figi"),
    )

    id = Column(Integer, primary_key=True)
    asset_uid = Column(String, nullable=False)
    instrument_uid = Column(String, nullable=True)
    figi = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=True)
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)


class RebalanceTarget(Base):
    __tablename__ = "rebalance_targets"
    __table_args__ = (
        UniqueConstraint("account_id", "asset_class", name="uq_rebalance_targets_account_class"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    asset_class = Column(String, nullable=False)
    target_weight_pct = Column(Numeric(9, 4), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, server_default=func.now(), nullable=False)


class InvestNotification(Base):
    __tablename__ = "invest_notifications"
    __table_args__ = (
        UniqueConstraint("account_id", "operation_id", name="uq_invest_notifications_account_operation"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False)
    operation_id = Column(String, nullable=False)
    operation_date = Column(DateTime(timezone=True), nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now, server_default=func.now(), nullable=False)
