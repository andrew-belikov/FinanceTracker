import ast
import asyncio
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_FILE = PROJECT_ROOT / "src" / "bot" / "runtime.py"


class FakeLogger:
    def warning(self, *_args, **_kwargs):
        return None


def load_runtime_authorization():
    tree = ast.parse(RUNTIME_FILE.read_text(encoding="utf-8"), filename=str(RUNTIME_FILE))
    functions = []
    for node in tree.body:
        if getattr(node, "name", None) not in {
            "is_authorized",
            "get_authorization_denial_text",
        }:
            continue
        copied = deepcopy(node)
        copied.returns = None
        for arg in copied.args.args:
            arg.annotation = None
        functions.append(copied)
    namespace = {
        "ALLOWED_USER_IDS": frozenset({101}),
        "PRIVATE_CHAT_REQUIRED_TEXT": "private chat required",
        "logger": FakeLogger(),
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(RUNTIME_FILE), "exec"), namespace)
    return namespace


class PrivateChatAuthorizationTests(unittest.TestCase):
    authorization = load_runtime_authorization()
    is_authorized = staticmethod(authorization["is_authorized"])

    @staticmethod
    def update(*, chat_type: str):
        return SimpleNamespace(
            update_id=1,
            effective_user=SimpleNamespace(id=101, username="private-name"),
            effective_chat=SimpleNamespace(id=202, type=chat_type),
            effective_message=SimpleNamespace(text="/dataset private financial text"),
        )

    def test_allowed_user_is_rejected_outside_private_chat(self):
        update = self.update(chat_type="group")
        self.assertFalse(self.is_authorized(update))

    def test_allowed_user_is_accepted_in_private_chat(self):
        update = self.update(chat_type="private")
        self.assertTrue(self.is_authorized(update))

    def test_group_denial_is_explicit_without_starting_private_work(self):
        update = self.update(chat_type="group")
        update.effective_message.reply_text = AsyncMock()
        handlers_tree = ast.parse(
            (PROJECT_ROOT / "src" / "bot" / "handlers.py").read_text(encoding="utf-8")
        )
        helper = next(
            deepcopy(node)
            for node in handlers_tree.body
            if getattr(node, "name", None) == "require_authorized_private_chat"
        )
        helper.returns = None
        for arg in helper.args.args:
            arg.annotation = None
        namespace = {
            "is_authorized": self.is_authorized,
            "get_authorization_denial_text": self.authorization["get_authorization_denial_text"],
        }
        exec(
            compile(ast.Module(body=[helper], type_ignores=[]), "handlers.py", "exec"),
            namespace,
        )

        allowed = asyncio.run(namespace["require_authorized_private_chat"](update))

        self.assertFalse(allowed)
        update.effective_message.reply_text.assert_awaited_once_with("private chat required")

    def test_sensitive_handlers_guard_before_private_work(self):
        tree = ast.parse(
            (PROJECT_ROOT / "src" / "bot" / "handlers.py").read_text(encoding="utf-8")
        )
        sensitive = {
            "handle_iis_tax_deduction_callback",
            "cmd_today",
            "cmd_week",
            "cmd_month",
            "cmd_calendar",
            "cmd_monthpdf",
            "cmd_year",
            "cmd_dataset",
            "cmd_structure",
            "cmd_history",
            "cmd_twr",
            "cmd_targets",
            "cmd_rebalance",
            "cmd_invest",
        }
        functions = {
            node.name: node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in sensitive
        }
        self.assertEqual(set(functions), sensitive)
        for name, function in functions.items():
            with self.subTest(handler=name):
                guard_indexes = []
                for index, statement in enumerate(function.body[:3]):
                    if not isinstance(statement, ast.If):
                        continue
                    if any(
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id in {"is_authorized", "require_authorized_private_chat"}
                        for node in ast.walk(statement.test)
                    ):
                        guard_indexes.append(index)
                        self.assertTrue(
                            any(isinstance(node, (ast.Return, ast.Raise)) for node in ast.walk(statement)),
                            f"{name} authorization denial must exit",
                        )
                self.assertTrue(
                    guard_indexes,
                    f"{name} must reject before DB/PDF/dataset/chart work",
                )


if __name__ == "__main__":
    unittest.main()
