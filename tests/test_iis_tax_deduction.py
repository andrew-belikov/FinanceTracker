import sys
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import handlers  # noqa: E402
import runtime  # noqa: E402
from iis_tax_deduction import (  # noqa: E402
    build_iis_tax_deduction_markup,
    render_iis_tax_deduction_message,
)


class IisTaxDeductionHelpersTests(unittest.TestCase):
    def test_markup_switches_between_mark_and_unmark(self):
        mark = build_iis_tax_deduction_markup("123", marked=False).inline_keyboard[0][0]
        unmark = build_iis_tax_deduction_markup("123", marked=True).inline_keyboard[0][0]

        self.assertEqual(mark.text, "Вычет")
        self.assertEqual(mark.callback_data, "iis_deduction:set:123")
        self.assertEqual(unmark.text, "Отменить вычет")
        self.assertEqual(unmark.callback_data, "iis_deduction:unset:123")

    def test_message_suffix_is_idempotent_and_reversible(self):
        marked = render_iis_tax_deduction_message("Пополнение", marked=True)
        self.assertEqual(render_iis_tax_deduction_message(marked, marked=True), marked)
        self.assertEqual(render_iis_tax_deduction_message(marked, marked=False), "Пополнение")


class IisTaxDeductionCallbackTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def update(data="iis_deduction:set:123"):
        query = SimpleNamespace(
            data=data,
            message=SimpleNamespace(text="Пополнение"),
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        return SimpleNamespace(callback_query=query), query

    async def test_unauthorized_user_cannot_change_category(self):
        update, query = self.update()
        with mock.patch.object(handlers, "is_authorized", return_value=False):
            await handlers.handle_iis_tax_deduction_callback(update, SimpleNamespace())

        query.answer.assert_awaited_once_with("Недостаточно прав", show_alert=True)
        query.edit_message_text.assert_not_awaited()

    async def test_marks_operation_and_renders_reverse_button(self):
        update, query = self.update()
        with (
            mock.patch.object(handlers, "is_authorized", return_value=True),
            mock.patch.object(handlers, "db_session", return_value=nullcontext(object())),
            mock.patch.object(handlers, "resolve_reporting_account_id", return_value="account"),
            mock.patch.object(handlers, "set_iis_tax_deduction_category", return_value="marked") as setter,
        ):
            await handlers.handle_iis_tax_deduction_callback(update, SimpleNamespace())

        setter.assert_called_once_with(
            mock.ANY,
            account_id="account",
            operation_id="123",
            enabled=True,
        )
        kwargs = query.edit_message_text.await_args.kwargs
        self.assertIn("налоговый вычет ИИС", query.edit_message_text.await_args.args[0])
        self.assertEqual(kwargs["reply_markup"].inline_keyboard[0][0].text, "Отменить вычет")

    async def test_repeated_mark_is_idempotent_and_unknown_operation_is_rejected(self):
        for result, should_edit in (("unchanged", True), ("not_found", False)):
            with self.subTest(result=result):
                update, query = self.update()
                with (
                    mock.patch.object(handlers, "is_authorized", return_value=True),
                    mock.patch.object(handlers, "db_session", return_value=nullcontext(object())),
                    mock.patch.object(handlers, "resolve_reporting_account_id", return_value="account"),
                    mock.patch.object(handlers, "set_iis_tax_deduction_category", return_value=result),
                ):
                    await handlers.handle_iis_tax_deduction_callback(update, SimpleNamespace())
                self.assertEqual(query.edit_message_text.await_count, 1 if should_edit else 0)

    async def test_unsets_category(self):
        update, query = self.update("iis_deduction:unset:123")
        query.message.text = render_iis_tax_deduction_message("Пополнение", marked=True)
        with (
            mock.patch.object(handlers, "is_authorized", return_value=True),
            mock.patch.object(handlers, "db_session", return_value=nullcontext(object())),
            mock.patch.object(handlers, "resolve_reporting_account_id", return_value="account"),
            mock.patch.object(handlers, "set_iis_tax_deduction_category", return_value="unmarked"),
        ):
            await handlers.handle_iis_tax_deduction_callback(update, SimpleNamespace())

        self.assertEqual(query.edit_message_text.await_args.args[0], "Пополнение")
        self.assertEqual(
            query.edit_message_text.await_args.kwargs["reply_markup"].inline_keyboard[0][0].text,
            "Вычет",
        )


class SafeSendMessageMarkupTests(unittest.IsolatedAsyncioTestCase):
    async def test_markdown_fallback_preserves_inline_keyboard(self):
        markup = build_iis_tax_deduction_markup("123", marked=False)
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=[RuntimeError("bad markdown"), "sent"]))

        result = await runtime.safe_send_message(
            bot,
            42,
            "text",
            parse_mode="Markdown",
            reply_markup=markup,
        )

        self.assertEqual(result, "sent")
        self.assertEqual(bot.send_message.await_count, 2)
        self.assertIs(bot.send_message.await_args_list[1].kwargs["reply_markup"], markup)


if __name__ == "__main__":
    unittest.main()
