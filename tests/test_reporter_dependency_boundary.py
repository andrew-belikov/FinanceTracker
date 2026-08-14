from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ReporterDependencyBoundaryTests(unittest.TestCase):
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
            sys.path.insert(0, "src/bot")
            sys.path.insert(0, "src")
            import report_payload
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
