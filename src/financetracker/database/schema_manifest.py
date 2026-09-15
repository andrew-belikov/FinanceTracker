"""Declarative PostgreSQL schema contract checked after versioned migrations."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

from sqlalchemy import inspect, text

SCHEMA_MANIFEST_VERSION = 2
TEXT = "text"
TIMESTAMP = "timestamp with time zone"
NUMERIC = "numeric"


@dataclass(frozen=True)
class Column:
    type_name: str
    nullable: bool
    default: str | None = None


@dataclass(frozen=True)
class Table:
    columns: dict[str, Column]
    primary_key: tuple[str, ...] = ("id",)
    unique_constraints: dict[str, tuple[str, ...]] | None = None
    indexes: dict[str, tuple[str, ...]] | None = None
    foreign_keys: dict[str, tuple[tuple[str, ...], str, tuple[str, ...]]] | None = None


def columns(required: dict[str, str], optional: dict[str, str] | None = None, *, defaults: dict[str, str] | None = None) -> dict[str, Column]:
    defaults = defaults or {}
    return {
        **{name: Column(type_name, False, defaults.get(name)) for name, type_name in required.items()},
        **{name: Column(type_name, True, defaults.get(name)) for name, type_name in (optional or {}).items()},
    }


# SQL migrations are the writer. This source-controlled manifest is the reader
# contract: it deliberately does not derive from ORM metadata.
TABLES: dict[str, Table] = {
    "schema_migrations": Table(columns({"filename": TEXT, "checksum_sha256": TEXT, "applied_at": TIMESTAMP}, defaults={"applied_at": "now"}), primary_key=("filename",)),
    "instruments": Table(columns({"id": "integer", "figi": TEXT, "created_at": TIMESTAMP}, {"ticker": TEXT, "name": TEXT, "class_code": TEXT, "instrument_type": TEXT}, defaults={"id": "nextval", "created_at": "now"}), unique_constraints={"instruments_figi_key": ("figi",)}),
    "portfolio_snapshots": Table(columns({"id": "integer", "account_id": TEXT, "snapshot_at": TIMESTAMP, "snapshot_date": "date", "currency": TEXT, "created_at": TIMESTAMP}, {"account_name": TEXT, "total_value": "numeric(18, 2)", "total_shares": "numeric(18, 2)", "total_bonds": "numeric(18, 2)", "total_etf": "numeric(18, 2)", "total_currencies": "numeric(18, 2)", "total_futures": "numeric(18, 2)", "expected_yield": "numeric(18, 2)", "expected_yield_pct": "numeric(9, 4)"}, defaults={"id": "nextval", "created_at": "now"}), unique_constraints={"uq_snapshot_account_date": ("account_id", "snapshot_date")}, indexes={"ix_portfolio_snapshots_snapshot_at": ("snapshot_at",), "ix_portfolio_snapshots_snapshot_date": ("snapshot_date",)}),
    "portfolio_positions": Table(columns({"id": "integer", "snapshot_id": "integer", "figi": TEXT, "created_at": TIMESTAMP}, {"instrument_id": "integer", "instrument_uid": TEXT, "position_uid": TEXT, "asset_uid": TEXT, "ticker": TEXT, "name": TEXT, "instrument_type": TEXT, "quantity": "numeric(18, 6)", "currency": TEXT, "current_price": "numeric(18, 4)", "current_nkd": NUMERIC, "position_value": "numeric(18, 2)", "expected_yield": "numeric(18, 2)", "expected_yield_pct": "numeric(9, 4)", "weight_pct": "numeric(9, 4)", "raw_payload_json": TEXT}, defaults={"id": "nextval", "created_at": "now"}), foreign_keys={"portfolio_positions_snapshot_id_fkey": (("snapshot_id",), "portfolio_snapshots", ("id",)), "portfolio_positions_instrument_id_fkey": (("instrument_id",), "instruments", ("id",))}),
    "operations": Table(columns({"id": "bigint", "account_id": TEXT, "operation_id": TEXT, "operation_type": TEXT, "date": TIMESTAMP, "amount": "numeric(18, 2)", "currency": TEXT, "created_at": TIMESTAMP}, {"description": TEXT, "source": TEXT, "instrument_uid": TEXT, "figi": TEXT, "cursor": TEXT, "broker_account_id": TEXT, "parent_operation_id": TEXT, "name": TEXT, "state": TEXT, "instrument_type": TEXT, "instrument_kind": TEXT, "position_uid": TEXT, "asset_uid": TEXT, "price": NUMERIC, "commission": NUMERIC, "yield": NUMERIC, "yield_relative": NUMERIC, "accrued_int": NUMERIC, "quantity": "bigint", "quantity_rest": "bigint", "quantity_done": "bigint", "cancel_date_time": TIMESTAMP, "cancel_reason": TEXT, "cashflow_category": TEXT}, defaults={"id": "nextval", "operation_type": "operation_type_input", "created_at": "now"}), unique_constraints={"uq_operations_account_operation": ("account_id", "operation_id")}, indexes={"ix_operations_cashflow_category": ("account_id", "cashflow_category", "date")}),
    "income_events": Table(columns({"id": "integer", "account_id": "character varying", "figi": "character varying", "event_date": "date", "event_type": "character varying", "gross_amount": "numeric(18, 2)", "tax_amount": "numeric(18, 2)", "net_amount": "numeric(18, 2)", "net_yield_pct": "numeric(9, 4)", "notified": "boolean", "created_at": TIMESTAMP, "currency": TEXT}, defaults={"id": "nextval", "notified": "false", "created_at": "now"}), unique_constraints={"uq_income_events_account_figi_date_type_currency": ("account_id", "figi", "event_date", "event_type", "currency")}),
    "asset_aliases": Table(columns({"id": "bigint", "asset_uid": TEXT, "first_seen_at": TIMESTAMP, "last_seen_at": TIMESTAMP, "created_at": TIMESTAMP, "updated_at": TIMESTAMP}, {"instrument_uid": TEXT, "figi": TEXT, "ticker": TEXT, "name": TEXT}, defaults={"id": "nextval", "created_at": "now", "updated_at": "now"}), unique_constraints={"uq_asset_aliases_asset_instrument_figi": ("asset_uid", "instrument_uid", "figi")}, indexes={"ix_asset_aliases_asset_uid": ("asset_uid",), "ix_asset_aliases_instrument_uid": ("instrument_uid",), "ix_asset_aliases_figi": ("figi",)}),
    "rebalance_targets": Table(columns({"id": "integer", "account_id": TEXT, "asset_class": TEXT, "target_weight_pct": "numeric(9, 4)", "created_at": TIMESTAMP, "updated_at": TIMESTAMP}, defaults={"id": "nextval", "created_at": "now", "updated_at": "now"}), unique_constraints={"uq_rebalance_targets_account_class": ("account_id", "asset_class")}),
    "invest_notifications": Table(columns({"id": "integer", "account_id": TEXT, "operation_id": TEXT, "operation_date": TIMESTAMP, "amount": "numeric(18, 2)", "created_at": TIMESTAMP}, defaults={"id": "nextval", "created_at": "now"}), unique_constraints={"uq_invest_notifications_account_operation": ("account_id", "operation_id")}),
    "bot_daily_job_runs": Table(columns({"id": "integer", "job_name": TEXT, "run_date": "date", "status": TEXT, "created_at": TIMESTAMP, "attempt_id": TEXT, "claimed_at": TIMESTAMP, "heartbeat_at": TIMESTAMP}, {"completed_at": TIMESTAMP, "sent_total": "integer", "failed_total": "integer"}, defaults={"id": "nextval", "status": "started", "created_at": "now"}), unique_constraints={"uq_bot_daily_job_runs_job_date": ("job_name", "run_date")}),
    "bot_notification_deliveries": Table(columns({"id": "integer", "notification_kind": TEXT, "notification_key": TEXT, "chat_id": "bigint", "message_type": TEXT, "status": TEXT, "attempt_id": TEXT, "claimed_at": TIMESTAMP, "created_at": TIMESTAMP, "updated_at": TIMESTAMP}, {"delivered_at": TIMESTAMP}, defaults={"id": "nextval", "status": "started", "created_at": "now", "updated_at": "now"}), unique_constraints={"uq_bot_notification_deliveries_identity": ("notification_kind", "notification_key", "chat_id", "message_type")}, indexes={"ix_bot_notification_deliveries_status_claimed": ("status", "claimed_at")}),
    "payout_calendar_events": Table(columns({"id": "integer", "account_id": TEXT, "figi": TEXT, "event_type": TEXT, "event_uid": TEXT, "payment_date": "date", "quantity": "numeric(18, 6)", "fetched_at": TIMESTAMP, "created_at": TIMESTAMP, "updated_at": TIMESTAMP}, {"instrument_uid": TEXT, "ticker": TEXT, "name": TEXT, "instrument_type": TEXT, "record_date": "date", "last_buy_date": "date", "amount_per_unit": "numeric(18, 9)", "expected_amount": "numeric(18, 2)", "currency": TEXT, "source_event_type": TEXT, "coupon_start_date": "date", "coupon_end_date": "date", "coupon_period_days": "integer"}, defaults={"id": "nextval", "created_at": "now", "updated_at": "now"}), unique_constraints={"uq_payout_calendar_event_source": ("account_id", "figi", "event_type", "event_uid")}, indexes={"ix_payout_calendar_events_payment_date": ("payment_date",), "ix_payout_calendar_events_account_payment": ("account_id", "payment_date")}),
}
VIEWS: tuple[str, ...] = ("deposits",)


def manifest_digest(required_migrations: tuple[str, ...]) -> str:
    payload = {"schema_manifest_version": SCHEMA_MANIFEST_VERSION, "required_migrations": required_migrations, "tables": {name: {"columns": {column: vars(spec) for column, spec in table.columns.items()}, "primary_key": table.primary_key, "unique_constraints": table.unique_constraints or {}, "indexes": table.indexes or {}, "foreign_keys": table.foreign_keys or {}} for name, table in TABLES.items()}, "views": VIEWS}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _normalise_type(type_name: str) -> str:
    return type_name.lower().replace("timestamp without time zone", "timestamp").replace("character varying", "varchar")


def _column_type_name(column: dict) -> str:
    column_type = column["type"]
    if getattr(column_type, "timezone", False):
        return "timestamp with time zone"
    return _normalise_type(str(column_type))


def validate_schema_manifest(connection) -> list[str]:
    """Return all drift observations; migration runner fails closed on any one."""
    inspector = inspect(connection)
    failures: list[str] = []
    for table_name, expected in TABLES.items():
        if not inspector.has_table(table_name):
            failures.append(f"table {table_name} is missing")
            continue
        actual_columns = {column["name"]: column for column in inspector.get_columns(table_name)}
        for column_name, expected_column in expected.columns.items():
            actual = actual_columns.get(column_name)
            if actual is None:
                failures.append(f"column {table_name}.{column_name} is missing")
                continue
            actual_type = _column_type_name(actual)
            expected_type = _normalise_type(expected_column.type_name)
            if actual_type != expected_type:
                failures.append(f"column {table_name}.{column_name} has type {actual_type}, expected {expected_type}")
            if bool(actual["nullable"]) != expected_column.nullable:
                failures.append(f"column {table_name}.{column_name} has unexpected nullability")
            if expected_column.default and expected_column.default not in str(actual.get("default") or "").lower():
                failures.append(f"column {table_name}.{column_name} has unexpected default")
        if tuple(inspector.get_pk_constraint(table_name).get("constrained_columns") or ()) != expected.primary_key:
            failures.append(f"table {table_name} has unexpected primary key")
        actual_unique = {item["name"]: tuple(item.get("column_names") or ()) for item in inspector.get_unique_constraints(table_name) if item.get("name")}
        for name, expected_columns in (expected.unique_constraints or {}).items():
            if actual_unique.get(name) != expected_columns:
                failures.append(f"unique constraint {name} is missing or differs")
        actual_indexes = {item["name"]: tuple(item.get("column_names") or ()) for item in inspector.get_indexes(table_name) if item.get("name")}
        for name, expected_columns in (expected.indexes or {}).items():
            if actual_indexes.get(name) != expected_columns:
                failures.append(f"index {name} is missing or differs")
        actual_foreign_keys = {item["name"]: (tuple(item.get("constrained_columns") or ()), item.get("referred_table"), tuple(item.get("referred_columns") or ())) for item in inspector.get_foreign_keys(table_name) if item.get("name")}
        for name, expected_foreign_key in (expected.foreign_keys or {}).items():
            if actual_foreign_keys.get(name) != expected_foreign_key:
                failures.append(f"foreign key {name} is missing or differs")
    for view_name in VIEWS:
        relation_kind = connection.execute(
            text("SELECT relkind FROM pg_class WHERE oid = to_regclass(:view_name)"),
            {"view_name": f"public.{view_name}"},
        ).scalar_one_or_none()
        if relation_kind != "v":
            failures.append(f"view {view_name} is missing or differs")
    return failures
