import os
import tempfile
from datetime import datetime

from telegram import InputFile, Update
from telegram.ext import ContextTypes

from charts import (
    build_history_chart,
    build_year_chart,
    build_year_monthly_delta_chart,
    render_twr_chart,
)
from dataset import create_dataset_archive
from queries import resolve_reporting_account_id
from runtime import (
    INVEST_USAGE_TEXT,
    REBALANCE_FEATURE_UNAVAILABLE_TEXT,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TARGETS_USAGE_TEXT,
    TZ,
    db_session,
    fmt_decimal_rub,
    is_authorized,
    log_update_received,
    logger,
    safe_send_message,
)
from services import (
    build_help_text,
    build_invest_text_for_account,
    build_month_summary,
    build_rebalance_text_for_account,
    build_structure_text,
    build_targets_text_for_account,
    build_today_summary,
    build_week_summary,
    build_year_summary,
    compute_portfolio_xirr_and_run_rate,
    compute_twr_timeseries,
    get_year_period,
    parse_rebalance_targets_args,
    parse_decimal_input,
    quantize_ruble_amount,
    render_twr_summary_text,
    replace_rebalance_targets,
)


async def debug_command_probe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_name = None
    text = getattr(update.effective_message, "text", None) or ""
    if text.startswith("/"):
        command_name = text.split()[0]
    log_update_received(update, command_name=command_name)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/start")
    if not is_authorized(update):
        return
    text = (
        "Привет! Я слежу за вашим портфелем «Семейный капитал».\n\n"
        "Доступные команды можно посмотреть в /help."
    )
    logger.info(
        "bot_reply_text_started",
        "Sending reply_text response.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )
    await update.message.reply_text(text)
    logger.info(
        "bot_reply_text_succeeded",
        "reply_text response sent.",
        {"chat_id": getattr(update.effective_chat, "id", None), "command": "/start"},
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/help")
    if not is_authorized(update):
        return

    text = build_help_text()
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/today")
    if not is_authorized(update):
        return
    text = build_today_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/week")
    if not is_authorized(update):
        return
    text = build_week_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/month")
    if not is_authorized(update):
        return
    text = build_month_summary()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/year")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) > 1:
        await update.message.reply_text("Формат: /year или /year YYYY")
        return

    year: int | None = None
    if len(args) == 1:
        try:
            parsed_year = int(args[0])
            if parsed_year < 1900 or parsed_year > 2100:
                raise ValueError
            year = parsed_year
        except ValueError:
            await update.message.reply_text("Формат: /year или /year YYYY")
            return

    try:
        summary_text, diff_text, label = build_year_summary(year)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    _, period_end_dt_exclusive, _, _ = get_year_period(year)
    chart_year = year if year is not None else datetime.now(TZ).year
    temp_chart = tempfile.NamedTemporaryFile(prefix=f"year_{chart_year}_", suffix=".png", delete=False)
    chart_path = temp_chart.name
    temp_chart.close()
    try:
        chart = build_year_chart(
            chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(chart_path):
            os.remove(chart_path)
        await update.message.reply_text(str(exc))
        return
    if chart:
        try:
            with open(chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(chart):
                os.remove(chart)
    else:
        await update.message.reply_text(f"Недостаточно данных для графика за {label}.")

    temp_delta_chart = tempfile.NamedTemporaryFile(prefix=f"year_delta_{chart_year}_", suffix=".png", delete=False)
    delta_chart_path = temp_delta_chart.name
    temp_delta_chart.close()
    try:
        delta_chart = build_year_monthly_delta_chart(
            delta_chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
        )
    except ValueError as exc:
        if os.path.exists(delta_chart_path):
            os.remove(delta_chart_path)
        await update.message.reply_text(str(exc))
        return
    if delta_chart:
        try:
            with open(delta_chart, "rb") as f:
                await update.message.reply_photo(photo=InputFile(f))
        finally:
            if os.path.exists(delta_chart):
                os.remove(delta_chart)

    await safe_send_message(context.bot, update.effective_chat.id, diff_text, parse_mode="Markdown")


async def cmd_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/dataset")
    if not is_authorized(update):
        return

    if context.args:
        await update.message.reply_text("Формат: /dataset")
        return

    try:
        archive_path, archive_name = create_dataset_archive()
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    try:
        with open(archive_path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=archive_name),
                caption="Архив для AI-анализа: JSON, CSV и README с контекстом.",
            )
    finally:
        if os.path.exists(archive_path):
            os.remove(archive_path)


async def cmd_structure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/structure")
    if not is_authorized(update):
        return
    text = build_structure_text()
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/history")
    if not is_authorized(update):
        return

    path = "/tmp/history.png"
    try:
        p = build_history_chart(path)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    if not p:
        await update.message.reply_text(
            "Недостаточно данных для построения графика."
        )
        return

    with open(p, "rb") as f:
        # Caption убран по требованию
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_twr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/twr")
    if not is_authorized(update):
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return

        data = compute_twr_timeseries(session, account_id)
        xirr_value, projected_value, projection_date = compute_portfolio_xirr_and_run_rate(
            session,
            account_id,
        )

    if not data:
        await update.message.reply_text("Недостаточно данных")
        return

    dates, values, twr = data
    last_date = dates[-1]
    last_value = values[-1]
    last_twr_pct = twr[-1] * 100.0
    summary_text = render_twr_summary_text(
        last_date=last_date,
        last_value=last_value,
        last_twr_pct=last_twr_pct,
        xirr_value=xirr_value,
        projected_value=projected_value,
        projection_date=projection_date,
    )
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    path = "/tmp/twr.png"
    render_twr_chart(path, dates, values, twr)

    with open(path, "rb") as f:
        await update.message.reply_photo(
            photo=InputFile(f)
        )


async def cmd_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/targets")
    if not is_authorized(update):
        return

    args = context.args or []
    if not args:
        with db_session() as session:
            account_id = resolve_reporting_account_id(session)
            if account_id is None:
                await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
                return
            text = build_targets_text_for_account(session, account_id)
        await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")
        return

    if args[0].lower() != "set":
        await update.message.reply_text(TARGETS_USAGE_TEXT)
        return

    try:
        targets = parse_rebalance_targets_args(args[1:])
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        saved = replace_rebalance_targets(session, account_id, targets)
        if not saved:
            await update.message.reply_text(REBALANCE_FEATURE_UNAVAILABLE_TEXT)
            return

    with db_session() as session:
        text = build_targets_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/rebalance")
    if not is_authorized(update):
        return
    if context.args:
        await update.message.reply_text("Формат: /rebalance")
        return

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_rebalance_text_for_account(session, account_id)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_invest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/invest")
    if not is_authorized(update):
        return

    args = context.args or []
    if len(args) != 1:
        await update.message.reply_text(INVEST_USAGE_TEXT)
        return

    try:
        amount = parse_decimal_input(args[0], allow_zero=False)
        if quantize_ruble_amount(amount) <= 0:
            raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    rounded_amount = quantize_ruble_amount(amount)
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        text = build_invest_text_for_account(
            session,
            account_id,
            rounded_amount,
            header=f"💸 Как распределить пополнение {fmt_decimal_rub(rounded_amount, precision=0)}",
        )
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")
