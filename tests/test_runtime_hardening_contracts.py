import asyncio
import csv
import io
import os
import stat
import sys
import tempfile
import unittest
import weakref
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import json
import zipfile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import handlers  # noqa: E402
import runtime  # noqa: E402
import dataset  # noqa: E402
import debug_artifacts  # noqa: E402


class CsvSafetyTests(unittest.TestCase):
    def test_csv_neutralizes_formula_cells_but_preserves_plain_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "export.csv"
            runtime.write_csv_file(
                str(path),
                ["value"],
                [
                    {"value": value}
                    for value in ("=1+1", "  +SUM(A1:A2)", "\t@cmd", "-1", "safe")
                ],
            )
            values = [row["value"] for row in csv.DictReader(io.StringIO(path.read_text()))]
        self.assertEqual(values, ["'=1+1", "'  +SUM(A1:A2)", "'\t@cmd", "'-1", "safe"])

    def test_dataset_archive_neutralizes_csv_only_and_keeps_json_canonical(self):
        operation_fields = [
            "operation_id", "date_utc", "local_date", "operation_type", "operation_group",
            "cashflow_category", "state", "logical_asset_id", "asset_uid", "instrument_uid",
            "figi", "name", "amount", "currency", "price", "quantity", "commission",
            "yield_amount", "description", "description_has_mojibake", "source",
        ]
        operation = {key: "" for key in operation_fields}
        operation["name"] = " =synthetic-formula"
        exported = {
            "meta": {"period_end": "2026-08-14"},
            "operations": [dict(operation)],
        }
        with patch.object(
            dataset,
            "build_dataset_export",
            return_value=(exported, [], [], [operation], []),
        ), patch.object(dataset, "build_dataset_readme", return_value="synthetic"):
            archive_path, _ = dataset.create_dataset_archive()
        try:
            with zipfile.ZipFile(archive_path) as archive:
                raw_json = json.loads(archive.read("dataset.json"))
                csv_rows = list(
                    csv.DictReader(io.StringIO(archive.read("operations.csv").decode("utf-8")))
                )
            self.assertEqual(raw_json["operations"][0]["name"], " =synthetic-formula")
            self.assertEqual(csv_rows[0]["name"], "' =synthetic-formula")
        finally:
            Path(archive_path).unlink(missing_ok=True)


class HandlerBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_releases_command_budget_and_does_not_block_loop(self):
        def slow():
            import time
            time.sleep(0.2)
            return "slow"

        heartbeat = asyncio.create_task(asyncio.sleep(0.01, result="alive"))
        with patch.object(handlers, "BOT_COMMAND_TIMEOUT_SECONDS", 0.02), patch.object(
            handlers, "BOT_COMMAND_MAX_CONCURRENCY", 1
        ), patch.object(handlers, "_BOT_COMMAND_SEMAPHORES", weakref.WeakKeyDictionary()):
            with self.assertRaises(asyncio.TimeoutError):
                await handlers.run_blocking_command(slow)
            result = await handlers.run_blocking_command(lambda: "next")
        self.assertEqual(await heartbeat, "alive")
        self.assertEqual(result, "next")

    async def test_timeout_cleans_artifact_created_after_handler_returns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "late.zip"

            def build_late_artifact():
                import time
                time.sleep(0.05)
                path.write_bytes(b"private")
                return str(path), "late.zip"

            with self.assertRaises(asyncio.TimeoutError):
                await handlers.run_blocking_command(
                    build_late_artifact,
                    timeout=0.01,
                    timeout_cleanup=handlers._cleanup_returned_path,
                )
            await asyncio.sleep(0.1)
            self.assertFalse(path.exists())

    async def test_history_uses_unique_0600_paths_and_cleans_success_and_error(self):
        paths = []
        modes = []

        def build(path):
            paths.append(path)
            modes.append(stat.S_IMODE(Path(path).stat().st_mode))
            Path(path).write_bytes(b"png")
            return path

        def fail(path):
            paths.append(path)
            modes.append(stat.S_IMODE(Path(path).stat().st_mode))
            Path(path).write_bytes(b"private")
            raise ValueError("synthetic failure")

        def update():
            message = SimpleNamespace(reply_photo=AsyncMock(), reply_text=AsyncMock())
            return SimpleNamespace(
                message=message,
                effective_message=message,
                effective_chat=SimpleNamespace(id=101),
            )

        with patch.object(handlers, "is_authorized", return_value=True), patch.object(
            handlers, "log_update_received"
        ), patch.object(handlers, "build_history_chart", side_effect=build):
            await asyncio.gather(
                handlers.cmd_history(update(), SimpleNamespace(bot=object())),
                handlers.cmd_history(update(), SimpleNamespace(bot=object())),
            )
        self.assertEqual(len(set(paths)), 2)
        self.assertTrue(all(mode == 0o600 for mode in modes))
        self.assertTrue(all(not Path(path).exists() for path in paths))

        with patch.object(handlers, "is_authorized", return_value=True), patch.object(
            handlers, "log_update_received"
        ), patch.object(handlers, "build_history_chart", side_effect=fail):
            await handlers.cmd_history(update(), SimpleNamespace(bot=object()))
        self.assertFalse(Path(paths[-1]).exists())

    async def test_twr_uses_unique_0600_paths_and_cleans_success_and_error(self):
        paths = []
        modes = []

        def build(path):
            paths.append(path)
            modes.append(stat.S_IMODE(Path(path).stat().st_mode))
            Path(path).write_bytes(b"png")
            return "ok", "synthetic summary"

        def fail(path):
            paths.append(path)
            modes.append(stat.S_IMODE(Path(path).stat().st_mode))
            Path(path).write_bytes(b"private")
            raise ValueError("synthetic failure")

        def update():
            message = SimpleNamespace(reply_photo=AsyncMock(), reply_text=AsyncMock())
            return SimpleNamespace(
                message=message,
                effective_message=message,
                effective_chat=SimpleNamespace(id=101),
            )

        with patch.object(handlers, "is_authorized", return_value=True), patch.object(
            handlers, "log_update_received"
        ), patch.object(handlers, "safe_send_message", new=AsyncMock()), patch.object(
            handlers, "_build_twr_artifact", side_effect=build
        ):
            await asyncio.gather(
                handlers.cmd_twr(update(), SimpleNamespace(bot=object())),
                handlers.cmd_twr(update(), SimpleNamespace(bot=object())),
            )
        self.assertEqual(len(set(paths)), 2)
        self.assertTrue(all(mode == 0o600 for mode in modes))
        self.assertTrue(all(not Path(path).exists() for path in paths))

        with patch.object(handlers, "is_authorized", return_value=True), patch.object(
            handlers, "log_update_received"
        ), patch.object(handlers, "_build_twr_artifact", side_effect=fail):
            with self.assertRaises(ValueError):
                await handlers.cmd_twr(update(), SimpleNamespace(bot=object()))
        self.assertFalse(Path(paths[-1]).exists())


class DebugArtifactTests(unittest.TestCase):
    def test_debug_artifacts_require_explicit_protected_dir_and_are_bounded(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("REPORT_DEBUG_DIR", None)
            with self.assertRaises(ValueError):
                debug_artifacts.save_debug_text(kind="payload", suffix=".json", text="private")
        with patch.dict(os.environ, {"REPORT_DEBUG_DIR": "relative/debug"}):
            with self.assertRaises(ValueError):
                debug_artifacts.save_debug_text(kind="payload", suffix=".json", text="private")
        with tempfile.TemporaryDirectory() as parent:
            directory = Path(parent) / "debug"
            with patch.dict(
                os.environ,
                {
                    "REPORT_DEBUG_DIR": str(directory),
                    "REPORT_DEBUG_MAX_FILES": "2",
                    "REPORT_DEBUG_MAX_AGE_SECONDS": "3600",
                },
            ):
                paths = [
                    debug_artifacts.save_debug_text(kind="payload", suffix=".json", text=str(i))
                    for i in range(3)
                ]
                oldest = Path(paths[-1])
                os.utime(oldest, (0, 0))
                newest = debug_artifacts.save_debug_text(
                    kind="payload",
                    suffix=".json",
                    text="fresh",
                )
            existing = list(directory.iterdir())
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
            self.assertEqual(len(existing), 2)
            self.assertFalse(oldest.exists())
            self.assertTrue(Path(newest).exists())
            self.assertTrue(all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in existing))


class ComposeIsolationTests(unittest.TestCase):
    def test_proxy_and_reporter_use_dedicated_internal_networks(self):
        compose = (PROJECT_ROOT / "compose.yml").read_text(encoding="utf-8")
        xray = compose.split("  xray-client:", 1)[1].split("\n  migrate:", 1)[0]
        bot = compose.split("  bot:", 1)[1].split("\n  reporter:", 1)[0]
        reporter = compose.split("  reporter:", 1)[1].split("\nvolumes:", 1)[0]
        self.assertIn("bot_proxy_internal:", compose)
        self.assertIn("bot_reporter_internal:", compose)
        self.assertIn("internal: true", compose)
        self.assertIn("      - bot_proxy_internal", xray)
        self.assertIn("      - xray_egress", xray)
        self.assertNotIn("      - default", xray)
        self.assertNotIn("ports:", xray)
        self.assertIn("      - bot_proxy_internal", bot)
        self.assertIn("      - bot_reporter_internal", bot)
        self.assertIn("      - bot_reporter_internal", reporter)


if __name__ == "__main__":
    unittest.main()
