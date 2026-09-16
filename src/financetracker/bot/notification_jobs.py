"""Income and investment notification use cases."""

from __future__ import annotations


def build_income_event_notification_text(
    row: dict, *, annualize_simple_yield_pct, fmt_signed_amount, fmt_plain_pct
) -> str:
    event_type = row["event_type"]
    icon = "💸" if event_type == "coupon" else "💰"
    action_line = "Купон зачислен" if event_type == "coupon" else "Дивиденды зачислены"
    net_amount = float(row["net_amount"])
    net_yield_pct = float(row["net_yield_pct"])
    lines = [
        f"{icon} {row['instrument_name']}",
        action_line,
        f"{fmt_signed_amount(net_amount)} ₽ ({fmt_plain_pct(net_yield_pct)} %)",
    ]
    annualized_pct = annualize_simple_yield_pct(net_yield_pct, row.get("coupon_period_days"))
    if event_type == "coupon" and annualized_pct is not None:
        lines.append(f"≈ {fmt_plain_pct(annualized_pct)} % годовых по этому купону")
    return "\n".join(lines)


async def check_income_events(
    context,
    *,
    db_session,
    resolve_reporting_account_id,
    get_unnotified_income_events,
    target_chat_ids,
    build_notification_text,
    send_tracked_notification,
    safe_send_message,
    deliveries_complete,
    mark_income_event_notified,
    logger,
) -> None:
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return
        rows = get_unnotified_income_events(session, account_id)

    for row in rows:
        event_type = row["event_type"]
        notification_key = str(row["id"])
        text_msg = build_notification_text(row)
        for chat_id in target_chat_ids:
            try:
                delivered_now = await send_tracked_notification(
                    notification_kind="income_event", notification_key=notification_key,
                    chat_id=chat_id, message_type="income",
                    send=lambda chat_id=chat_id: safe_send_message(
                        context.bot, chat_id, text_msg, parse_mode="Markdown"
                    ),
                )
                if delivered_now:
                    logger.info(
                        "income_event_notification_sent", "Income event notification sent.",
                        {"income_event_id": row["id"], "chat_id": chat_id,
                         "event_type": event_type, "figi": row["figi"]},
                    )
            except Exception:
                logger.exception(
                    "income_event_notification_failed", "Failed to send income event notification.",
                    {"income_event_id": row["id"], "chat_id": chat_id,
                     "event_type": event_type, "figi": row["figi"]},
                )

        if not deliveries_complete(
            notification_kind="income_event", notification_key=notification_key,
            message_types={"income"},
        ):
            continue
        with db_session() as session:
            mark_income_event_notified(session, row["id"])


async def check_invest_notifications(
    context,
    *,
    db_session,
    resolve_reporting_account_id,
    get_pending_invest_notifications,
    target_chat_ids,
    normalize_decimal,
    build_invest_text_for_account,
    fmt_decimal_rub,
    build_iis_tax_deduction_markup,
    iis_tax_deduction_category,
    send_tracked_notification,
    safe_send_message,
    deliveries_complete,
    mark_invest_notification_sent,
    decimal_to_str,
    logger,
) -> None:
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return
        rows = get_pending_invest_notifications(session, account_id)
    if rows is None:
        return

    for row in rows:
        amount = normalize_decimal(row["amount"])
        with db_session() as session:
            text_msg = build_invest_text_for_account(
                session, account_id, amount,
                header=f"💸 Получено пополнение: {fmt_decimal_rub(amount, precision=0)}",
            )
        notification_key = str(row["operation_id"])
        reply_markup = build_iis_tax_deduction_markup(
            notification_key,
            marked=row.get("cashflow_category") == iis_tax_deduction_category,
        )
        for chat_id in target_chat_ids:
            try:
                delivered_now = await send_tracked_notification(
                    notification_kind="invest_notification", notification_key=notification_key,
                    chat_id=chat_id, message_type="invest",
                    send=lambda chat_id=chat_id: safe_send_message(
                        context.bot, chat_id, text_msg, parse_mode="Markdown",
                        reply_markup=reply_markup,
                    ),
                )
                if delivered_now:
                    logger.info(
                        "invest_notification_sent", "Invest notification sent.",
                        {"operation_id": row["operation_id"], "chat_id": chat_id,
                         "amount": decimal_to_str(amount)},
                    )
            except Exception:
                logger.exception(
                    "invest_notification_failed", "Failed to send invest notification.",
                    {"operation_id": row["operation_id"], "chat_id": chat_id,
                     "amount": decimal_to_str(amount)},
                )

        if not deliveries_complete(
            notification_kind="invest_notification", notification_key=notification_key,
            message_types={"invest"},
        ):
            continue
        with db_session() as session:
            marked = mark_invest_notification_sent(
                session, account_id=account_id, operation_id=row["operation_id"],
                operation_date=row["date"], amount=amount,
            )
        if not marked:
            return
