import json
import os
import tempfile
import zipfile
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from common.text_utils import has_mojibake
from queries import (
    get_asset_alias_rows,
    get_daily_snapshot_rows,
    get_dataset_bounds,
    get_dataset_operations,
    get_income_events_for_period,
    get_latest_snapshot_with_id,
    get_positions_for_snapshot,
    resolve_reporting_account_id,
)
from runtime import (
    ACCOUNT_FRIENDLY_NAME,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TZ_NAME,
    db_session,
    decimal_to_str,
    json_default,
    normalize_decimal,
    to_iso_datetime,
    to_local_market_date,
    write_csv_file,
)
from services import (
    build_asset_alias_lookup,
    build_logical_asset_id,
    build_reconciliation_by_asset_type,
    classify_operation_group,
    compute_twr_timeseries,
    is_income_event_backed_tax_operation,
)


def build_dataset_export(session) -> tuple[dict, list[dict], list[dict], list[dict], list[dict]]:
    account_id = resolve_reporting_account_id(session)
    if account_id is None:
        raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

    bounds = get_dataset_bounds(session, account_id)
    min_date = bounds["min_date"]
    max_date = bounds["max_date"]
    if min_date is None or max_date is None:
        raise ValueError("Пока нет данных для экспорта датасета.")

    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None:
        raise ValueError("Пока нет данных для экспорта датасета.")

    daily_rows = get_daily_snapshot_rows(session, account_id)
    positions_rows = list(get_positions_for_snapshot(session, latest_snapshot["id"]))
    asset_alias_rows = list(get_asset_alias_rows(session))
    asset_alias_by_instrument_uid, asset_alias_by_figi = build_asset_alias_lookup(asset_alias_rows)
    operations_rows = list(
        get_dataset_operations(
            session,
            account_id=account_id,
            start_dt=datetime.combine(min_date, time.min),
            end_dt=datetime.combine(max_date + timedelta(days=1), time.min),
        )
    )
    income_rows = list(get_income_events_for_period(session, account_id, min_date, max_date))
    twr_data = compute_twr_timeseries(session, account_id)

    twr_by_date: dict[date, float] = {}
    if twr_data is not None:
        dates, _values, twr_series = twr_data
        twr_by_date = {dt: round(value * 100.0, 6) for dt, value in zip(dates, twr_series)}

    deposits_by_day: dict[date, Decimal] = {}
    withdrawals_by_day: dict[date, Decimal] = {}
    commissions_by_day: dict[date, Decimal] = {}
    taxes_by_day: dict[date, Decimal] = {}
    operations_csv_rows: list[dict] = []
    unknown_operation_groups = 0
    mojibake_detected_count = 0

    for row in operations_rows:
        dt = row["date"]
        local_date = to_local_market_date(dt)
        group = classify_operation_group(row["operation_type"])
        if group == "other":
            unknown_operation_groups += 1

        alias_row = None
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid:
            alias_row = asset_alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi:
            alias_row = asset_alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
        )
        description = row["description"]
        description_has_mojibake = has_mojibake(description)
        if description_has_mojibake:
            mojibake_detected_count += 1

        amount = normalize_decimal(row["amount"])
        amount_abs = abs(amount)
        if local_date is not None:
            if group == "deposit":
                deposits_by_day[local_date] = deposits_by_day.get(local_date, Decimal("0")) + amount
            elif group == "withdrawal":
                withdrawals_by_day[local_date] = withdrawals_by_day.get(local_date, Decimal("0")) + amount_abs
            elif group == "commission":
                commissions_by_day[local_date] = commissions_by_day.get(local_date, Decimal("0")) + amount_abs
            elif group == "income_tax" and not is_income_event_backed_tax_operation(row["operation_type"]):
                taxes_by_day[local_date] = taxes_by_day.get(local_date, Decimal("0")) + amount_abs

        operations_csv_rows.append(
            {
                "operation_id": row["operation_id"],
                "date_utc": to_iso_datetime(dt),
                "local_date": local_date.isoformat() if local_date is not None else None,
                "operation_type": row["operation_type"],
                "operation_group": group,
                "state": row["state"],
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "instrument_uid": instrument_uid,
                "figi": figi,
                "name": row["name"],
                "amount": amount,
                "currency": row["currency"],
                "price": row["price"],
                "quantity": row["quantity"],
                "commission": row["commission"],
                "yield_amount": row["yield"],
                "description": description,
                "description_has_mojibake": description_has_mojibake,
                "source": row["source"],
            }
        )

    income_net_by_day: dict[date, Decimal] = {}
    income_tax_by_day: dict[date, Decimal] = {}
    income_csv_rows: list[dict] = []
    for row in income_rows:
        event_date = row["event_date"]
        alias_row = asset_alias_by_figi.get(row["figi"]) if row.get("figi") else None
        asset_uid = alias_row.get("asset_uid") if alias_row is not None else None
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=alias_row.get("instrument_uid") if alias_row is not None else None,
            figi=row.get("figi"),
        )
        net_amount = normalize_decimal(row["net_amount"])
        tax_amount = normalize_decimal(row["tax_amount"])
        income_net_by_day[event_date] = income_net_by_day.get(event_date, Decimal("0")) + net_amount
        income_tax_by_day[event_date] = income_tax_by_day.get(event_date, Decimal("0")) + abs(tax_amount)
        income_csv_rows.append(
            {
                "event_date": event_date,
                "event_type": row["event_type"],
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "figi": row["figi"],
                "ticker": row["ticker"],
                "instrument_name": row["instrument_name"],
                "gross_amount": row["gross_amount"],
                "tax_amount": row["tax_amount"],
                "net_amount": row["net_amount"],
                "net_yield_pct": row["net_yield_pct"],
                "notified": row["notified"],
            }
        )

    daily_csv_rows: list[dict] = []
    previous_value: Decimal | None = None
    for row in daily_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = normalize_decimal(row["total_value"])
        deposits = deposits_by_day.get(snapshot_date, Decimal("0"))
        withdrawals = withdrawals_by_day.get(snapshot_date, Decimal("0"))
        income_net = income_net_by_day.get(snapshot_date, Decimal("0"))
        commissions = commissions_by_day.get(snapshot_date, Decimal("0"))
        taxes = taxes_by_day.get(snapshot_date, Decimal("0"))
        income_tax = income_tax_by_day.get(snapshot_date, Decimal("0"))
        net_cashflow = deposits - withdrawals + income_net - commissions - taxes
        day_pnl = Decimal("0")
        if previous_value is not None:
            day_pnl = portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value

        daily_csv_rows.append(
            {
                "date": snapshot_date,
                "snapshot_at_utc": to_iso_datetime(row["snapshot_at"]),
                "portfolio_value": portfolio_value,
                "expected_yield": row["expected_yield"],
                "expected_yield_pct": row["expected_yield_pct"],
                "deposits": deposits,
                "withdrawals": withdrawals,
                "income_net": income_net,
                "commissions": commissions,
                "operation_taxes": taxes,
                "income_taxes": income_tax,
                "net_cashflow": net_cashflow,
                "day_pnl": day_pnl,
                "twr_pct": twr_by_date.get(snapshot_date),
            }
        )

    positions_csv_rows: list[dict] = []
    for row in positions_rows:
        alias_row = None
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid:
            alias_row = asset_alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi:
            alias_row = asset_alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        logical_asset_id = build_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
        )
        positions_csv_rows.append(
            {
                "snapshot_date": latest_snapshot["snapshot_date"],
                "snapshot_at_utc": to_iso_datetime(latest_snapshot["snapshot_at"]),
                "logical_asset_id": logical_asset_id,
                "asset_uid": asset_uid,
                "instrument_uid": instrument_uid,
                "position_uid": row["position_uid"],
                "figi": figi,
                "ticker": row["ticker"],
                "name": row["name"],
                "instrument_type": row["instrument_type"],
                "quantity": row["quantity"],
                "currency": row["currency"],
                "current_price": row["current_price"],
                "current_nkd": row["current_nkd"],
                "position_value": row["position_value"],
                "expected_yield": row["expected_yield"],
                "expected_yield_pct": row["expected_yield_pct"],
                "weight_pct": row["weight_pct"],
                "value_source": "quantity_x_current_price",
            }
        )

    deposits_total = sum((row["deposits"] for row in daily_csv_rows), Decimal("0"))
    withdrawals_total = sum((row["withdrawals"] for row in daily_csv_rows), Decimal("0"))
    income_net_total = sum((row["income_net"] for row in daily_csv_rows), Decimal("0"))
    commissions_total = sum((row["commissions"] for row in daily_csv_rows), Decimal("0"))
    operation_taxes_total = sum((row["operation_taxes"] for row in daily_csv_rows), Decimal("0"))
    income_taxes_total = sum((row["income_taxes"] for row in daily_csv_rows), Decimal("0"))
    current_value = normalize_decimal(latest_snapshot["total_value"])
    net_contributions = deposits_total - withdrawals_total
    period_start_value = normalize_decimal(daily_csv_rows[0]["portfolio_value"])
    period_end_value = current_value
    period_net_cashflow = sum((row["net_cashflow"] for row in daily_csv_rows[1:]), Decimal("0"))
    period_pnl_abs = period_end_value - period_start_value - period_net_cashflow
    has_full_history_from_zero = period_start_value == Decimal("0")
    reconciliation_rows, positions_value_sum, reconciliation_gap_abs = build_reconciliation_by_asset_type(
        latest_snapshot,
        positions_rows,
    )
    alias_groups_count = len({row["asset_uid"] for row in asset_alias_rows if row.get("asset_uid")})

    positions_missing_labels = sum(1 for row in positions_csv_rows if not (row["ticker"] or row["name"]))

    dataset = {
        "meta": {
            "dataset_version": 2,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "timezone": TZ_NAME,
            "account_name": ACCOUNT_FRIENDLY_NAME,
            "period_start": min_date.isoformat(),
            "period_end": max_date.isoformat(),
            "base_currency": latest_snapshot["currency"],
            "latest_snapshot_at": to_iso_datetime(latest_snapshot["snapshot_at"]),
        },
        "summary": {
            "current_value": current_value,
            "net_contributions": net_contributions,
            "deposits_total": deposits_total,
            "withdrawals_total": withdrawals_total,
            "income_net_total": income_net_total,
            "commissions_total": commissions_total,
            "income_taxes_total": income_taxes_total,
            "operation_taxes_total": operation_taxes_total,
            "taxes_total": income_taxes_total + operation_taxes_total,
            "period_start_value": period_start_value,
            "period_end_value": period_end_value,
            "period_net_cashflow": period_net_cashflow,
            "period_pnl_abs": period_pnl_abs,
            "period_twr_pct": twr_by_date.get(max_date),
            "has_full_history_from_zero": has_full_history_from_zero,
            "positions_value_sum": positions_value_sum,
            "reconciliation_gap_abs": reconciliation_gap_abs,
            "reconciliation_by_asset_type": reconciliation_rows,
            "snapshot_count": len(daily_csv_rows),
            "positions_count": len(positions_csv_rows),
            "operations_count": len(operations_csv_rows),
            "income_events_count": len(income_csv_rows),
        },
        "timeseries_daily": daily_csv_rows,
        "positions_current": positions_csv_rows,
        "operations": operations_csv_rows,
        "income_events": income_csv_rows,
        "asset_aliases": [
            {
                "logical_asset_id": row["asset_uid"],
                "asset_uid": row["asset_uid"],
                "instrument_uid": row["instrument_uid"],
                "figi": row["figi"],
                "ticker": row["ticker"],
                "name": row["name"],
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
            }
            for row in asset_alias_rows
        ],
        "data_quality": {
            "unknown_operation_group_count": unknown_operation_groups,
            "mojibake_detected_count": mojibake_detected_count,
            "positions_missing_label_count": positions_missing_labels,
            "has_full_history_from_zero": has_full_history_from_zero,
            "alias_groups_count": alias_groups_count,
            "income_events_available": True,
        },
        "assumptions": [
            "В operations включены только исполненные операции после дедупликации по operation_id.",
            "Дневные cashflow-агрегаты привязаны к локальной дате Europe/Moscow.",
            "income_net в daily timeseries уже учитывает удержанный налог из income_events.",
            "operation_taxes_total не включает dividend/coupon tax, если тот же налог уже представлен в income_events.",
            "Архив считается period-first: lifetime return не вычисляется без полной истории с нуля.",
            "reconciliation_by_asset_type строится от snapshot totals по классам активов; нераскрытый остаток остаётся residual.",
        ],
    }

    return dataset, daily_csv_rows, positions_csv_rows, operations_csv_rows, income_csv_rows


def build_dataset_readme(dataset: dict) -> str:
    summary = dataset["summary"]
    meta = dataset["meta"]
    return (
        "# FinanceTracker AI Dataset\n\n"
        "Этот архив подготовлен командой `/dataset` для передачи ИИ-модели.\n\n"
        "## Контекст\n\n"
        f"- Счёт: {meta['account_name']}\n"
        f"- Таймзона агрегации: {meta['timezone']}\n"
        f"- Период: {meta['period_start']} .. {meta['period_end']}\n"
        f"- Базовая валюта: {meta['base_currency']}\n"
        f"- Сформировано: {meta['generated_at']}\n\n"
        "## Файлы\n\n"
        "- `dataset.json` — основной структурированный датасет для ИИ.\n"
        "- `daily_timeseries.csv` — дневной ряд стоимости и денежных потоков.\n"
        "- `positions_current.csv` — текущие позиции на последнем снапшоте.\n"
        "- `operations.csv` — исполненные операции после дедупликации.\n"
        "- `income_events.csv` — купоны и дивиденды в нормализованном виде.\n\n"
        "## Ключевые поля\n\n"
        f"- Current value: {decimal_to_str(summary['current_value'])} {meta['base_currency']}\n"
        f"- Period start value: {decimal_to_str(summary['period_start_value'])} {meta['base_currency']}\n"
        f"- Period end value: {decimal_to_str(summary['period_end_value'])} {meta['base_currency']}\n"
        f"- Period net cashflow: {decimal_to_str(summary['period_net_cashflow'])} {meta['base_currency']}\n"
        f"- Period pnl abs: {decimal_to_str(summary['period_pnl_abs'])} {meta['base_currency']}\n"
        f"- Period twr pct: {summary['period_twr_pct']}\n"
        f"- Positions value sum: {decimal_to_str(summary['positions_value_sum'])} {meta['base_currency']}\n"
        f"- Reconciliation gap abs: {decimal_to_str(summary['reconciliation_gap_abs'])} {meta['base_currency']}\n"
        f"- Full history from zero: {summary['has_full_history_from_zero']}\n\n"
        "## Правила интерпретации\n\n"
        "- Денежные значения в JSON сохраняются как строки, чтобы не терять точность.\n"
        "- `operation_group` нормализует сырые типы операций; налоги по операциям экспортируются как `income_tax`.\n"
        "- `logical_asset_id` строится из `asset_uid` и нужен для склейки бумаг при смене FIGI.\n"
        "- `income_net` в дневном ряду уже очищен от удержанного налога по income_events.\n"
        "- `taxes_total` дедуплицирован: dividend/coupon tax не суммируется второй раз из operations, если он уже попал в income_events.\n"
        "- Если `has_full_history_from_zero=false`, архив нельзя трактовать как полную lifetime-историю портфеля.\n"
        "- Если `reconciliation_gap_abs` не равен нулю, смотрите `reconciliation_by_asset_type`: это residual между snapshot totals и суммой позиционных оценок.\n"
        "- Для подробного анализа сначала читайте `dataset.json`, затем CSV-файлы как табличную детализацию.\n"
    )


def create_dataset_archive() -> tuple[str, str]:
    with db_session() as session:
        dataset, daily_rows, positions_rows, operations_rows, income_rows = build_dataset_export(session)

    archive_name = f"fintracker_dataset_{dataset['meta']['period_end']}.zip"
    archive_tmp = tempfile.NamedTemporaryFile(prefix="fintracker_dataset_", suffix=".zip", delete=False)
    archive_path = archive_tmp.name
    archive_tmp.close()

    json_text = json.dumps(dataset, ensure_ascii=False, indent=2, default=json_default)
    readme_text = build_dataset_readme(dataset)

    daily_fields = [
        "date",
        "snapshot_at_utc",
        "portfolio_value",
        "expected_yield",
        "expected_yield_pct",
        "deposits",
        "withdrawals",
        "income_net",
        "commissions",
        "operation_taxes",
        "income_taxes",
        "net_cashflow",
        "day_pnl",
        "twr_pct",
    ]
    positions_fields = [
        "snapshot_date",
        "snapshot_at_utc",
        "logical_asset_id",
        "asset_uid",
        "instrument_uid",
        "position_uid",
        "figi",
        "ticker",
        "name",
        "instrument_type",
        "quantity",
        "currency",
        "current_price",
        "current_nkd",
        "position_value",
        "expected_yield",
        "expected_yield_pct",
        "weight_pct",
        "value_source",
    ]
    operations_fields = [
        "operation_id",
        "date_utc",
        "local_date",
        "operation_type",
        "operation_group",
        "state",
        "logical_asset_id",
        "asset_uid",
        "instrument_uid",
        "figi",
        "name",
        "amount",
        "currency",
        "price",
        "quantity",
        "commission",
        "yield_amount",
        "description",
        "description_has_mojibake",
        "source",
    ]
    income_fields = [
        "event_date",
        "event_type",
        "logical_asset_id",
        "asset_uid",
        "figi",
        "ticker",
        "instrument_name",
        "gross_amount",
        "tax_amount",
        "net_amount",
        "net_yield_pct",
        "notified",
    ]

    with tempfile.TemporaryDirectory(prefix="fintracker_dataset_") as temp_dir:
        dataset_json_path = os.path.join(temp_dir, "dataset.json")
        with open(dataset_json_path, "w", encoding="utf-8") as f:
            f.write(json_text)

        readme_path = os.path.join(temp_dir, "README_AI.md")
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(readme_text)

        write_csv_file(os.path.join(temp_dir, "daily_timeseries.csv"), daily_fields, daily_rows)
        write_csv_file(os.path.join(temp_dir, "positions_current.csv"), positions_fields, positions_rows)
        write_csv_file(os.path.join(temp_dir, "operations.csv"), operations_fields, operations_rows)
        write_csv_file(os.path.join(temp_dir, "income_events.csv"), income_fields, income_rows)

        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(dataset_json_path, arcname="dataset.json")
            archive.write(readme_path, arcname="README_AI.md")
            archive.write(os.path.join(temp_dir, "daily_timeseries.csv"), arcname="daily_timeseries.csv")
            archive.write(os.path.join(temp_dir, "positions_current.csv"), arcname="positions_current.csv")
            archive.write(os.path.join(temp_dir, "operations.csv"), arcname="operations.csv")
            archive.write(os.path.join(temp_dir, "income_events.csv"), arcname="income_events.csv")

    return archive_path, archive_name
