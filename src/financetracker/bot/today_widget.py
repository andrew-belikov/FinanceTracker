"""Opt-in delivery and periodic refresh of the latest portfolio widget."""

import asyncio
import hashlib
import json
import os
from pathlib import Path

from telegram.error import BadRequest, Forbidden, TelegramError

from financetracker.bot.runtime import TARGET_CHAT_IDS, logger
from financetracker.bot.today_widget_view import TODAY_WIDGET_HTML, build_today_widget_data


WIDGET_CAPTION = "FinanceTracker · Портфель сегодня"
STATE_PATH = Path(os.getenv("TODAY_WIDGET_STATE_FILE", "/var/lib/financetracker-bot/today-widgets.json"))


def _lock(context):
    return context.bot_data.setdefault("today_widget_lock", asyncio.Lock())


async def _find_legacy_pin(bot, chat_id):
    chat = await bot.do_api_request("getChat", {"chat_id": chat_id})
    pinned = chat.get("pinned_message") or {}
    if (
        chat.get("type") == "private"
        and (pinned.get("from") or {}).get("id") == bot.id
        and pinned.get("caption") == WIDGET_CAPTION
        and pinned.get("widget")
    ):
        return pinned
    return None


def _digest(data):
    return hashlib.sha256(data.encode()).hexdigest()


def _save_state(bot, state):
    temp_path = STATE_PATH.with_name(f".{STATE_PATH.name}.tmp")
    descriptor = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump({"bot_id": bot.id, "messages": state}, stream)
    temp_path.replace(STATE_PATH)


async def _load_state(bot):
    try:
        stored = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        stored = {}
    if stored.get("bot_id") == bot.id:
        return stored["messages"]

    # The previous version used Telegram pins to remember active messages.
    state = {}
    for chat_id in TARGET_CHAT_IDS:
        try:
            message = await _find_legacy_pin(bot, chat_id)
        except Forbidden:
            continue
        except BadRequest as exc:
            if "chat not found" in str(exc).casefold():
                continue
            raise
        if message:
            state[str(chat_id)] = [message["message_id"], _digest(message["widget"].get("chat_data") or "")]
    _save_state(bot, state)
    for chat_id, (message_id, _) in state.items():
        try:
            await bot.unpin_chat_message(int(chat_id), message_id)
        except TelegramError as exc:
            _log_failure(exc)
    return state


async def send_today_widget(context, chat_id):
    bot = context.bot
    async with _lock(context):
        state = await _load_state(bot)
        previous = state.get(str(chat_id))
        data = await asyncio.to_thread(build_today_widget_data)
        message = await bot.do_api_request("sendWidget", {
            "chat_id": chat_id,
            "html": TODAY_WIDGET_HTML,
            "chat_data": data,
            "caption": WIDGET_CAPTION,
            "disable_notification": True,
            "protect_content": True,
        })
        state[str(chat_id)] = [message["message_id"], _digest(data)]
        _save_state(bot, state)
        if previous and previous[0] != message["message_id"]:
            try:
                await bot.delete_message(chat_id, previous[0])
            except TelegramError as exc:
                logger.warning("today_widget_delete_failed", "Could not delete the previous widget.", {
                    "error_type": type(exc).__name__,
                })


async def refresh_today_widgets(context):
    async with _lock(context):
        state = await _load_state(context.bot)
        active = {str(chat_id): state[str(chat_id)] for chat_id in TARGET_CHAT_IDS if str(chat_id) in state}
        if not active and not state:
            return
        data = await asyncio.to_thread(build_today_widget_data) if active else ""
        digest = _digest(data)
        for chat_id, (message_id, previous_digest) in list(active.items()):
            if digest == previous_digest:
                continue
            try:
                await context.bot.do_api_request("editMessageWidgetData", {
                    "chat_id": int(chat_id),
                    "message_id": message_id,
                    "chat_data": data,
                })
            except Forbidden:
                del active[chat_id]
                continue
            except BadRequest as exc:
                reason = str(exc).casefold()
                if "message to edit not found" in reason or "message_id_invalid" in reason:
                    del active[chat_id]
                    continue
                if "message is not modified" not in reason:
                    _log_failure(exc)
                    continue
            except TelegramError as exc:
                _log_failure(exc)
                continue
            active[chat_id] = [message_id, digest]
        if active != state:
            _save_state(context.bot, active)


def _log_failure(exc):
    logger.warning("today_widget_request_failed", "Telegram widget request failed.", {
        "error_type": type(exc).__name__,
    })
