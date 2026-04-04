import ast
import os
import unittest
from copy import deepcopy
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BOT_FILE = PROJECT_ROOT / "src" / "bot" / "bot.py"
ENTRYPOINT_FILE = PROJECT_ROOT / "src" / "bot" / "entrypoint.py"


def load_selected_symbols(file_path: Path, wanted_assignments: set[str], wanted_functions: set[str], namespace=None):
    module_ast = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    selected_nodes = []
    for node in module_ast.body:
        if isinstance(node, ast.Assign):
            target_names = {
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            if target_names & wanted_assignments:
                selected_nodes.append(node)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id in wanted_assignments:
                selected_nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            copied = deepcopy(node)
            copied.returns = None
            for arg in copied.args.args:
                arg.annotation = None
            for arg in copied.args.kwonlyargs:
                arg.annotation = None
            selected_nodes.append(copied)

    loaded_namespace = {} if namespace is None else namespace
    isolated_module = ast.Module(body=selected_nodes, type_ignores=[])
    code = compile(isolated_module, filename=str(file_path), mode="exec")
    exec(code, loaded_namespace)
    return loaded_namespace


class DummyTimedOut(Exception):
    pass


class DummyNetworkError(Exception):
    pass


class DummyOtherError(Exception):
    pass


def load_bot_symbols():
    namespace = {
        "TimedOut": DummyTimedOut,
        "NetworkError": DummyNetworkError,
    }
    return load_selected_symbols(
        BOT_FILE,
        set(),
        {"is_retryable_telegram_transport_error"},
        namespace=namespace,
    )


def load_entrypoint_symbols():
    namespace = {"os": os}
    return load_selected_symbols(
        ENTRYPOINT_FILE,
        {"BOT_STARTUP_RETRY_EXIT_CODE"},
        {"get_bot_startup_retry_delay_seconds", "should_retry_bot_process"},
        namespace=namespace,
    )


class BotStartupResilienceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot_symbols = load_bot_symbols()
        cls.entrypoint_symbols = load_entrypoint_symbols()

    def test_retryable_transport_errors_include_timeout_and_network_error(self):
        is_retryable = self.bot_symbols["is_retryable_telegram_transport_error"]

        self.assertTrue(is_retryable(DummyTimedOut("timeout")))
        self.assertTrue(is_retryable(DummyNetworkError("network")))
        self.assertFalse(is_retryable(DummyOtherError("other")))

    def test_retry_delay_uses_env_and_is_clamped_to_zero(self):
        get_delay = self.entrypoint_symbols["get_bot_startup_retry_delay_seconds"]

        with mock.patch.dict(os.environ, {"BOT_STARTUP_RETRY_DELAY_SECONDS": "25"}, clear=False):
            self.assertEqual(get_delay(), 25)

        with mock.patch.dict(os.environ, {"BOT_STARTUP_RETRY_DELAY_SECONDS": "-7"}, clear=False):
            self.assertEqual(get_delay(), 0)

    def test_should_retry_bot_process_only_for_supervised_retry_exit_code(self):
        should_retry = self.entrypoint_symbols["should_retry_bot_process"]
        retry_exit_code = self.entrypoint_symbols["BOT_STARTUP_RETRY_EXIT_CODE"]

        self.assertTrue(should_retry(retry_exit_code))
        self.assertFalse(should_retry(1))


if __name__ == "__main__":
    unittest.main()
