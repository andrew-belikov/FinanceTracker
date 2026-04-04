import ast
import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUNTIME_FILES = [
    PROJECT_ROOT / "src" / "bot" / "bot.py",
    PROJECT_ROOT / "src" / "bot" / "entrypoint.py",
    PROJECT_ROOT / "src" / "bot" / "proxy_smoke.py",
    PROJECT_ROOT / "src" / "bot" / "runtime.py",
    PROJECT_ROOT / "src" / "tracker" / "app.py",
    PROJECT_ROOT / "src" / "tracker" / "repair_operations_description_encoding.py",
    PROJECT_ROOT / "src" / "xray_client" / "entrypoint.py",
    PROJECT_ROOT / "src" / "xray_client" / "healthcheck.py",
]

LOGGER_METHODS = {"debug", "info", "warning", "error", "exception", "critical"}
EVENT_RE = re.compile(r"^[a-z0-9_]+$")


class RuntimeLoggingGuardrailsTests(unittest.TestCase):
    def test_runtime_modules_do_not_use_raw_console_output_for_logging(self):
        for path in RUNTIME_FILES:
            with self.subTest(path=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print":
                        self.fail(f"{path} uses print() at line {node.lineno}")

                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and isinstance(node.func.value, ast.Attribute)
                        and isinstance(node.func.value.value, ast.Name)
                        and node.func.value.value.id == "sys"
                        and node.func.value.attr in {"stderr", "stdout"}
                        and node.func.attr == "write"
                    ):
                        self.fail(f"{path} writes directly to sys.{node.func.value.attr} at line {node.lineno}")

    def test_runtime_modules_use_explicit_snake_case_logger_events(self):
        for path in RUNTIME_FILES:
            with self.subTest(path=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    if not isinstance(node.func, ast.Attribute):
                        continue
                    if not isinstance(node.func.value, ast.Name) or node.func.value.id != "logger":
                        continue
                    if node.func.attr not in LOGGER_METHODS:
                        continue

                    self.assertGreaterEqual(
                        len(node.args),
                        2,
                        f"{path} logger.{node.func.attr} at line {node.lineno} must pass event and message",
                    )
                    event_arg = node.args[0]
                    self.assertIsInstance(
                        event_arg,
                        ast.Constant,
                        f"{path} logger.{node.func.attr} at line {node.lineno} must use a literal event name",
                    )
                    self.assertIsInstance(
                        event_arg.value,
                        str,
                        f"{path} logger.{node.func.attr} at line {node.lineno} must use a string event name",
                    )
                    self.assertRegex(
                        event_arg.value,
                        EVENT_RE,
                        f"{path} logger.{node.func.attr} at line {node.lineno} must use snake_case event names",
                    )

                    for keyword in node.keywords:
                        self.assertNotEqual(
                            keyword.arg,
                            "extra",
                            f"{path} logger.{node.func.attr} at line {node.lineno} must not pass raw extra",
                        )


if __name__ == "__main__":
    unittest.main()
