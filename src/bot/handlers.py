import asyncio
import functools
import os
import tempfile
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from telegram import InputFile, Update
from telegram.ext import ContextTypes

from charts import (
    build_history_chart,
    build_year_chart,
    build_year_monthly_delta_chart,
    render_twr_chart,
)
from dataset import create_dataset_archive
from iis_tax_deduction import (
    CALLBACK_PREFIX,
    build_iis_tax_deduction_markup,
    render_iis_tax_deduction_message,
)
from queries import resolve_reporting_account_id, set_iis_tax_deduction_category
from report_client import ReporterClientError, request_monthly_report_pdf
from runtime import (
    INVEST_USAGE_TEXT,
    PAYOUT_CALENDAR_HORIZON_DAYS,
    REBALANCE_FEATURE_UNAVAILABLE_TEXT,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TARGETS_USAGE_TEXT,
    TZ,
    db_session,
    fmt_decimal_rub,
    get_authorization_denial_text,
    is_authorized,
    log_update_received,
    logger,
    safe_send_document,
    safe_send_message,
)


async def require_authorized_private_chat(update: Update) -> bool:
    if is_authorized(update):
        return True
    denial_text = get_authorization_denial_text(update)
    message = getattr(update, "effective_message", None)
    if denial_text and message is not None:
        await message.reply_text(denial_text)
    return False
from services import (
    build_help_text,
    build_invest_text_for_account,
    build_month_summary,
    build_payout_calendar_text_for_account,
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


BOT_COMMAND_MAX_CONCURRENCY = max(1, min(8, int(os.getenv("BOT_COMMAND_MAX_CONCURRENCY", "2"))))
BOT_COMMAND_TIMEOUT_SECONDS = max(1.0, float(os.getenv("BOT_COMMAND_TIMEOUT_SECONDS", "120")))
_BOT_COMMAND_EXECUTOR = ThreadPoolExecutor(
    max_workers=BOT_COMMAND_MAX_CONCURRENCY,
    thread_name_prefix="bot-command",
)
_BOT_COMMAND_SEMAPHORES: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _command_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = _BOT_COMMAND_SEMAPHORES.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(BOT_COMMAND_MAX_CONCURRENCY)
        _BOT_COMMAND_SEMAPHORES[loop] = semaphore
    return semaphore


async def run_blocking_command(
    function,
    /,
    *args,
    timeout: float | None = None,
    timeout_cleanup=None,
    **kwargs,
):
    semaphore = _command_semaphore()
    async with semaphore:
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(
            _BOT_COMMAND_EXECUTOR,
            functools.partial(function, *args, **kwargs),
        )
        try:
            return await asyncio.wait_for(
                asyncio.shield(future),
                timeout=timeout if timeout is not None else BOT_COMMAND_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            if timeout_cleanup is not None:
                future.add_done_callback(timeout_cleanup)
            raise


def _cleanup_path_when_done(path: str):
    def cleanup(_future) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    return cleanup


def _cleanup_returned_path(future) -> None:
    try:
        result = future.result()
    except Exception:
        return
    if isinstance(result, tuple) and result and isinstance(result[0], str):
        try:
            os.remove(result[0])
        except FileNotFoundError:
            pass


def _set_iis_tax_deduction(*, enabled: bool, operation_id: str):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return "account_unavailable"
        return set_iis_tax_deduction_category(
            session,
            account_id=account_id,
            operation_id=operation_id,
            enabled=enabled,
        )


def _build_calendar_text(*, start_date, end_date):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return None
        return build_payout_calendar_text_for_account(
            session,
            account_id,
            start_date=start_date,
            end_date=end_date,
            heading=f"💸 Календарь выплат на {PAYOUT_CALENDAR_HORIZON_DAYS} дней",
        )


def _build_targets_text():
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return None
        return build_targets_text_for_account(session, account_id)


def _replace_targets_and_build_text(targets):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return "account_unavailable", None
        saved = replace_rebalance_targets(session, account_id, targets)
        if not saved:
            return "unavailable", None
    with db_session() as session:
        return "ok", build_targets_text_for_account(session, account_id)


def _build_rebalance_text():
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return None
        return build_rebalance_text_for_account(session, account_id)


def _build_invest_text(rounded_amount):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return None
        return build_invest_text_for_account(
            session,
            account_id,
            rounded_amount,
            header=f"💸 Как распределить пополнение {fmt_decimal_rub(rounded_amount, precision=0)}",
        )


def _new_private_temp_path(*, prefix: str, suffix: str) -> str:
    handle = tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix, delete=False)
    path = handle.name
    handle.close()
    os.chmod(path, 0o600)
    return path


def _build_twr_artifact(path: str):
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return "account_unavailable", None
        data = compute_twr_timeseries(session, account_id)
        if not data:
            return "insufficient", None
        xirr_value, projected_value, projection_date = compute_portfolio_xirr_and_run_rate(
            session,
            account_id,
        )
    dates, values, twr = data
    summary_text = render_twr_summary_text(
        last_date=dates[-1],
        last_value=values[-1],
        last_twr_pct=twr[-1] * 100.0,
        xirr_value=xirr_value,
        projected_value=projected_value,
        projection_date=projection_date,
    )
    render_twr_chart(path, dates, values, twr)
    return "ok", summary_text


async def debug_command_probe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_name = None
    text = getattr(update.effective_message, "text", None) or ""
    if text.startswith("/"):
        command_name = text.split()[0]
    log_update_received(update, command_name=command_name)


async def handle_iis_tax_deduction_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    if not is_authorized(update):
        await query.answer("Недостаточно прав", show_alert=True)
        return

    parts = (query.data or "").split(":", 2)
    if len(parts) != 3 or parts[0] != CALLBACK_PREFIX or parts[1] not in {"set", "unset"}:
        await query.answer("Кнопка устарела", show_alert=True)
        return

    enabled = parts[1] == "set"
    operation_id = parts[2].strip()
    if not operation_id:
        await query.answer("Операция не найдена", show_alert=True)
        return

    result = await run_blocking_command(
        _set_iis_tax_deduction,
        enabled=enabled,
        operation_id=operation_id,
    )
    if result == "account_unavailable":
        await query.answer(REPORTING_ACCOUNT_UNAVAILABLE_TEXT, show_alert=True)
        return

    if result == "not_found":
        await query.answer("Исполненное пополнение не найдено", show_alert=True)
        return

    message = query.message
    source_text = message.text if message is not None else ""
    rendered_text = render_iis_tax_deduction_message(source_text, marked=enabled)
    markup = build_iis_tax_deduction_markup(operation_id, marked=enabled)
    if message is not None:
        try:
            await query.edit_message_text(rendered_text, parse_mode="Markdown", reply_markup=markup)
        except Exception:
            await query.edit_message_text(rendered_text, reply_markup=markup)
    await query.answer("Вычет отмечен" if enabled else "Отметка снята")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/start")
    if not await require_authorized_private_chat(update):
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
    if not await require_authorized_private_chat(update):
        return

    text = build_help_text()
    await update.message.reply_text(text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/today")
    if not await require_authorized_private_chat(update):
        return
    text = await run_blocking_command(build_today_summary)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/week")
    if not await require_authorized_private_chat(update):
        return
    text = await run_blocking_command(build_week_summary)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/month")
    if not await require_authorized_private_chat(update):
        return
    text = await run_blocking_command(build_month_summary)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/calendar")
    if not await require_authorized_private_chat(update):
        return
    if context.args:
        await update.message.reply_text("Формат: /calendar")
        return

    start_date = datetime.now(TZ).date()
    end_date = start_date + timedelta(days=PAYOUT_CALENDAR_HORIZON_DAYS - 1)
    text = await run_blocking_command(
        _build_calendar_text,
        start_date=start_date,
        end_date=end_date,
    )
    if text is None:
        await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
        return
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode=None)


def _parse_monthpdf_args(args):
    if not args:
        return None, None

    if len(args) == 1 and "-" in args[0]:
        year_str, month_str = args[0].split("-", 1)
    elif len(args) == 2:
        year_str, month_str = args
    else:
        raise ValueError("Формат: /monthpdf или /monthpdf YYYY MM")

    try:
        year = int(year_str)
        month = int(month_str)
    except ValueError as exc:
        raise ValueError("Формат: /monthpdf или /monthpdf YYYY MM") from exc

    if year < 1900 or year > 2100 or month < 1 or month > 12:
        raise ValueError("Формат: /monthpdf или /monthpdf YYYY MM")
    return year, month


async def cmd_monthpdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/monthpdf")
    if not await require_authorized_private_chat(update):
        return

    try:
        year, month = _parse_monthpdf_args(context.args or [])
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return

    logger.info(
        "bot_monthpdf_requested",
        "Received /monthpdf request.",
        {
            "chat_id": getattr(update.effective_chat, "id", None),
            "year": year,
            "month": month,
        },
    )
    status_message = await update.message.reply_text("Собираю PDF-отчёт. Это может занять до пары минут.")
    document_path = None
    try:
        document_path, filename = await run_blocking_command(
            request_monthly_report_pdf,
            year=year,
            month=month,
            timeout_cleanup=_cleanup_returned_path,
        )
        await safe_send_document(
            context.bot,
            update.effective_chat.id,
            file_path=document_path,
            filename=filename,
            caption="Monthly review в PDF.",
        )
        logger.info(
            "bot_monthpdf_succeeded",
            "Sent monthly PDF report to Telegram chat.",
            {
                "chat_id": getattr(update.effective_chat, "id", None),
                "filename": filename,
            },
        )
    except ReporterClientError as exc:
        logger.warning(
            "bot_monthpdf_failed",
            "Failed to fetch monthly PDF report from reporter.",
            {
                "chat_id": getattr(update.effective_chat, "id", None),
                "error_type": type(exc).__name__,
            },
        )
        await update.message.reply_text(str(exc))
    except Exception:
        logger.exception(
            "bot_monthpdf_send_failed",
            "Failed to deliver monthly PDF report to Telegram.",
            {
                "chat_id": getattr(update.effective_chat, "id", None),
            },
        )
        await update.message.reply_text("Не удалось отправить PDF-отчёт в Telegram.")
    finally:
        if document_path and os.path.exists(document_path):
            os.remove(document_path)
        try:
            await status_message.delete()
        except Exception:
            logger.warning(
                "bot_monthpdf_status_delete_failed",
                "Failed to delete temporary /monthpdf status message.",
                {
                    "chat_id": getattr(update.effective_chat, "id", None),
                },
            )


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/year")
    if not await require_authorized_private_chat(update):
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
        summary_text, diff_text, label = await run_blocking_command(build_year_summary, year)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    await safe_send_message(context.bot, update.effective_chat.id, summary_text, parse_mode="Markdown")

    _, period_end_dt_exclusive, _, _ = get_year_period(year)
    chart_year = year if year is not None else datetime.now(TZ).year
    chart_path = _new_private_temp_path(prefix=f"year_{chart_year}_", suffix=".png")
    try:
        chart = await run_blocking_command(
            build_year_chart,
            chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
            timeout_cleanup=_cleanup_path_when_done(chart_path),
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

    delta_chart_path = _new_private_temp_path(prefix=f"year_delta_{chart_year}_", suffix=".png")
    try:
        delta_chart = await run_blocking_command(
            build_year_monthly_delta_chart,
            delta_chart_path,
            year=chart_year,
            end_date_exclusive=period_end_dt_exclusive.date(),
            timeout_cleanup=_cleanup_path_when_done(delta_chart_path),
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
    if not await require_authorized_private_chat(update):
        return

    if context.args:
        await update.message.reply_text("Формат: /dataset")
        return

    try:
        archive_path, archive_name = await run_blocking_command(
            create_dataset_archive,
            timeout_cleanup=_cleanup_returned_path,
        )
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
    if not await require_authorized_private_chat(update):
        return
    text = await run_blocking_command(build_structure_text)
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/history")
    if not await require_authorized_private_chat(update):
        return

    path = _new_private_temp_path(prefix="history_", suffix=".png")
    p = None
    try:
        try:
            p = await run_blocking_command(
                build_history_chart,
                path,
                timeout_cleanup=_cleanup_path_when_done(path),
            )
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        if not p:
            await update.message.reply_text("Недостаточно данных для построения графика.")
            return
        with open(p, "rb") as f:
            await update.message.reply_photo(photo=InputFile(f))
    finally:
        for candidate in {path, p or ""}:
            if candidate and os.path.exists(candidate):
                os.remove(candidate)


async def cmd_twr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/twr")
    if not await require_authorized_private_chat(update):
        return

    path = _new_private_temp_path(prefix="twr_", suffix=".png")
    try:
        status, summary_text = await run_blocking_command(
            _build_twr_artifact,
            path,
            timeout_cleanup=_cleanup_path_when_done(path),
        )
        if status == "account_unavailable":
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
        if status == "insufficient":
            await update.message.reply_text("Недостаточно данных")
            return
        await safe_send_message(
            context.bot,
            update.effective_chat.id,
            summary_text,
            parse_mode="Markdown",
        )
        with open(path, "rb") as f:
            await update.message.reply_photo(photo=InputFile(f))
    finally:
        if os.path.exists(path):
            os.remove(path)


async def cmd_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/targets")
    if not await require_authorized_private_chat(update):
        return

    args = context.args or []
    if not args:
        text = await run_blocking_command(_build_targets_text)
        if text is None:
            await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
            return
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

    status, text = await run_blocking_command(_replace_targets_and_build_text, targets)
    if status == "account_unavailable":
        await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
        return
    if status == "unavailable":
        await update.message.reply_text(REBALANCE_FEATURE_UNAVAILABLE_TEXT)
        return
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_rebalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/rebalance")
    if not await require_authorized_private_chat(update):
        return
    if context.args:
        await update.message.reply_text("Формат: /rebalance")
        return

    text = await run_blocking_command(_build_rebalance_text)
    if text is None:
        await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
        return
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")


async def cmd_invest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, command_name="/invest")
    if not await require_authorized_private_chat(update):
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
    text = await run_blocking_command(_build_invest_text, rounded_amount)
    if text is None:
        await update.message.reply_text(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)
        return
    await safe_send_message(context.bot, update.effective_chat.id, text, parse_mode="Markdown")
