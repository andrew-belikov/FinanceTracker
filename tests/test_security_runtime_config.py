import ast
import os
import unittest
from copy import deepcopy
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_FILE = PROJECT_ROOT / "src" / "bot" / "runtime.py"
TRACKER_FILE = PROJECT_ROOT / "src" / "tracker" / "app.py"


def load_symbols(path: Path, names: set[str], namespace=None):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = []
    for node in tree.body:
        node_name = getattr(node, "name", None)
        if node_name not in names:
            continue
        copied = deepcopy(node)
        if isinstance(copied, (ast.FunctionDef, ast.AsyncFunctionDef)):
            copied.returns = None
            for arg in (*copied.args.args, *copied.args.kwonlyargs):
                arg.annotation = None
        selected.append(copied)
    loaded = {} if namespace is None else dict(namespace)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), loaded)
    return loaded


class RuntimeConfigurationTests(unittest.TestCase):
    def test_bot_main_validates_configuration_before_application_build(self):
        tree = ast.parse(
            (PROJECT_ROOT / "src" / "bot" / "bot.py").read_text(encoding="utf-8")
        )
        main = next(node for node in tree.body if getattr(node, "name", None) == "main")
        calls = [
            (node.func.id, node.lineno)
            for node in ast.walk(main)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]
        line_by_name = {name: line for name, line in calls}
        self.assertLess(
            line_by_name["validate_database_credentials"],
            line_by_name["build_application"],
        )
        self.assertIn("ALLOWLIST_CONFIGURATION_ERROR", ast.unparse(main))

    def test_allowlist_is_required_nonempty_and_numeric(self):
        symbols = load_symbols(
            RUNTIME_FILE,
            {"RuntimeConfigurationError", "parse_required_id_allowlist"},
        )
        parse = symbols["parse_required_id_allowlist"]
        error = symbols["RuntimeConfigurationError"]

        for raw in (None, "", "   ", "123,", "123,abc", "١٢٣", "0", "-7"):
            with self.subTest(raw=raw), self.assertRaises(error):
                parse(raw)

        self.assertEqual(parse("101, 202"), frozenset({101, 202}))

    def test_database_secret_is_required_without_exposing_value(self):
        symbols = load_symbols(
            RUNTIME_FILE,
            {"RuntimeConfigurationError", "validate_database_credentials"},
        )
        validate = symbols["validate_database_credentials"]
        error = symbols["RuntimeConfigurationError"]

        with self.assertRaises(error) as raised:
            validate(db_dsn="", db_password="")
        self.assertNotIn("postgresql", str(raised.exception).lower())

        validate(db_dsn="sqlite://", db_password="")
        validate(db_dsn="", db_password="synthetic-test-password")
        for path in (RUNTIME_FILE, TRACKER_FILE):
            source = path.read_text(encoding="utf-8")
            self.assertNotRegex(source, r'os\.getenv\("DB_PASSWORD",\s*"[^\"]+"\)')

    def test_tls_defaults_true_and_false_requires_explicit_test_break_glass(self):
        symbols = load_symbols(
            TRACKER_FILE,
            {"RuntimeConfigurationError", "parse_verify_ssl"},
        )
        parse = symbols["parse_verify_ssl"]
        error = symbols["RuntimeConfigurationError"]

        self.assertTrue(parse(None, app_env="prod", allow_insecure_test=False))
        self.assertTrue(parse("true", app_env="prod", allow_insecure_test=False))
        with self.assertRaises(error):
            parse("truthy", app_env="prod", allow_insecure_test=False)
        with self.assertRaises(error):
            parse("false", app_env="prod", allow_insecure_test=True)
        with self.assertRaises(error):
            parse("false", app_env="test", allow_insecure_test=False)
        self.assertFalse(parse("false", app_env="test", allow_insecure_test=True))


if __name__ == "__main__":
    unittest.main()
