from __future__ import annotations

import os
import ast
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ReporterDependencyBoundaryTests(unittest.TestCase):
    def test_reporting_package_does_not_import_bot(self):
        reporting_dir = ROOT / "src" / "financetracker" / "reporting"
        for path in reporting_dir.glob("*.py"):
            source = path.read_text(encoding="utf-8")
            imported_modules = {
                node.module
                for node in ast.walk(ast.parse(source))
                if isinstance(node, ast.ImportFrom) and node.module
            }
            forbidden = sorted(
                module
                for module in imported_modules
                if module == "financetracker.bot" or module.startswith("financetracker.bot.")
            )
            self.assertEqual(forbidden, [], msg=f"{path.name}: {forbidden}")

    def test_renderer_does_not_import_bot_chart_helpers(self):
        source = (ROOT / "src" / "financetracker" / "reporting" / "report_render.py").read_text(
            encoding="utf-8"
        )
        imported_modules = {
            node.module
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom) and node.module
        }
        self.assertNotIn("financetracker.bot.charts", imported_modules)

    def test_reporter_payload_import_does_not_require_telegram_package(self):
        script = textwrap.dedent(
            """
            import builtins
            import sys

            real_import = builtins.__import__

            def blocking_import(name, *args, **kwargs):
                if name == "telegram" or name.startswith("telegram."):
                    raise ModuleNotFoundError("telegram is intentionally unavailable")
                return real_import(name, *args, **kwargs)

            builtins.__import__ = blocking_import
            from financetracker.reporting import report_payload
            """
        )
        env = os.environ.copy()
        env.update(
            {
                "ALLOWED_USER_IDS": "1",
                "DB_PASSWORD": "synthetic-db-password",
                "TIMEZONE": "Europe/Moscow",
            }
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            msg=(
                "reporter import crossed the Telegram-only dependency boundary; "
                f"stderr={completed.stderr}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
