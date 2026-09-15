from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


CALLBACK_PREFIX = "iis_deduction"
MARKED_SUFFIX = "\n\n🏛 Пополнение помечено как налоговый вычет ИИС."


def build_iis_tax_deduction_markup(operation_id: str, *, marked: bool) -> InlineKeyboardMarkup:
    action = "unset" if marked else "set"
    label = "Отменить вычет" if marked else "Вычет"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=f"{CALLBACK_PREFIX}:{action}:{operation_id}")]]
    )


def render_iis_tax_deduction_message(text: str, *, marked: bool) -> str:
    base = text
    if base.endswith(MARKED_SUFFIX):
        base = base[: -len(MARKED_SUFFIX)]
    return f"{base}{MARKED_SUFFIX}" if marked else base
