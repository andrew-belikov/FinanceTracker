"""
Кнопочное меню поверх существующих команд.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

MENU_CALLBACK_PREFIX = "menu"


def build_menu_keyboard(command_specs) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for name, description, _handler in command_specs:
        if name in ("start", "help", "menu"):
            continue
        row.append(InlineKeyboardButton(description, callback_data=f"{MENU_CALLBACK_PREFIX}:{name}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from financetracker.bot.bot import COMMAND_SPECS

    await update.message.reply_text(
        "Выбери отчёт:",
        reply_markup=build_menu_keyboard(COMMAND_SPECS),
    )


async def on_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from financetracker.bot.bot import COMMAND_SPECS

    query = update.callback_query
    await query.answer()

    command_name = query.data.split(":", 1)[1]
    handlers_by_name = {name: handler for name, _desc, handler in COMMAND_SPECS}
    handler = handlers_by_name.get(command_name)
    if handler is None:
        await query.edit_message_text("Команда не найдена.")
        return

    await handler(update, context)