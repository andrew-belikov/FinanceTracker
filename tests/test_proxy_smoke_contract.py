import ast
from copy import deepcopy
import json
import os
from pathlib import Path
import unittest
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROXY_SMOKE_FILE = PROJECT_ROOT / "src" / "bot" / "proxy_smoke.py"


def load_proxy_smoke_symbols():
    module_ast = ast.parse(PROXY_SMOKE_FILE.read_text(encoding="utf-8"))
    selected = []
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name in {
            "build_telegram_probe_url",
            "probe_telegram",
        }:
            copied = deepcopy(node)
            copied.returns = None
            for arg in copied.args.args:
                arg.annotation = None
            selected.append(copied)
    namespace = {"os": os, "httpx": mock.Mock()}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(PROXY_SMOKE_FILE), "exec"), namespace)
    return namespace


class FakeResponse:
    def __init__(self, status_code, payload=None, *, json_error=None):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class ProxySmokeContractTests(unittest.TestCase):
    def setUp(self):
        self.symbols = load_proxy_smoke_symbols()

    def run_probe(self, response):
        client = mock.MagicMock()
        client.__enter__.return_value.get.return_value = response
        self.symbols["httpx"].Client.return_value = client
        return self.symbols["probe_telegram"](timeout=1.0, proxy_url=None)

    def test_only_http_200_with_json_ok_true_is_success(self):
        self.assertEqual(self.run_probe(FakeResponse(200, {"ok": True}))[0], True)
        self.assertEqual(self.run_probe(FakeResponse(401, {"ok": False}))[0], False)
        self.assertEqual(self.run_probe(FakeResponse(200, {"ok": False}))[0], False)
        self.assertEqual(self.run_probe(FakeResponse(200, []))[0], False)

    def test_invalid_json_is_failure_without_response_body_in_details(self):
        ok, details = self.run_probe(
            FakeResponse(200, json_error=json.JSONDecodeError("bad", "private payload", 0))
        )

        self.assertFalse(ok)
        self.assertNotIn("private payload", details)

    def test_entrypoint_stops_before_polling_when_smoke_fails(self):
        entrypoint_file = PROJECT_ROOT / "src" / "bot" / "entrypoint.py"
        entrypoint_ast = ast.parse(entrypoint_file.read_text(encoding="utf-8"))
        main_node = next(
            node
            for node in entrypoint_ast.body
            if isinstance(node, ast.FunctionDef) and node.name == "main"
        )
        main_source = ast.get_source_segment(
            entrypoint_file.read_text(encoding="utf-8"),
            main_node,
        )

        self.assertIn("smoke_exit_code = run_startup_smoke()", main_source)
        self.assertRegex(main_source, r"if smoke_exit_code != 0:[\s\S]+return smoke_exit_code")
        self.assertLess(main_source.index("run_startup_smoke()"), main_source.index("run_bot_process()"))


if __name__ == "__main__":
    unittest.main()
