import ast
import os
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_FILE = PROJECT_ROOT / "src" / "bot" / "runtime.py"
SERVICES_FILE = PROJECT_ROOT / "src" / "bot" / "services.py"


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


def load_symbols():
    namespace = {
        "os": os,
        "TZ": ZoneInfo("Europe/Moscow"),
    }
    exec("from datetime import time\n", namespace)

    with mock.patch.dict(
        os.environ,
        {
            "TIMEZONE": "Europe/Moscow",
            "DAILY_SUMMARY_HOUR": "18",
            "DAILY_SUMMARY_MINUTE": "0",
            "YESTERDAY_PEAK_ALERT_HOUR": "8",
            "YESTERDAY_PEAK_ALERT_MINUTE": "0",
            "BOT_PROXY_ENABLED": "true",
            "BOT_PROXY_ENDPOINT": "socks5h://xray-client:1080",
        },
        clear=False,
    ):
        load_selected_symbols(
            RUNTIME_FILE,
            {
                "TZ_NAME",
                "DAILY_JOB_HOUR",
                "DAILY_JOB_MINUTE",
                "DAILY_JOB_SCHEDULE_LABEL",
                "YESTERDAY_PEAK_ALERT_HOUR",
                "YESTERDAY_PEAK_ALERT_MINUTE",
                "YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL",
                "BOT_PROXY_ENABLED",
                "BOT_PROXY_ENDPOINT",
                "POLLING_BACKLOG_PENDING_THRESHOLD",
                "POLLING_BACKLOG_STALL_THRESHOLD_SECONDS",
                "POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT",
            },
            {
                "build_daily_job_time",
                "build_yesterday_peak_alert_time",
                "format_daily_job_schedule",
                "format_yesterday_peak_alert_schedule",
                "resolve_telegram_proxy_url",
                "build_telegram_request_kwargs",
                "is_polling_backlog_detected",
                "next_polling_backlog_detection_streak",
                "should_trigger_polling_self_heal",
            },
            namespace=namespace,
        )
        load_selected_symbols(
            SERVICES_FILE,
            set(),
            {"build_help_text"},
            namespace=namespace,
        )
    return namespace


class BotScheduleConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.symbols = load_symbols()

    def test_build_daily_job_time_uses_timezone_env(self):
        job_time = self.symbols["build_daily_job_time"]()

        self.assertEqual(job_time.hour, 18)
        self.assertEqual(job_time.minute, 0)
        self.assertEqual(getattr(job_time.tzinfo, "key", None), "Europe/Moscow")

    def test_format_daily_job_schedule_includes_configured_timezone(self):
        self.assertEqual(
            self.symbols["format_daily_job_schedule"](),
            "18:00 (Europe/Moscow)",
        )

    def test_build_yesterday_peak_alert_time_uses_morning_default(self):
        job_time = self.symbols["build_yesterday_peak_alert_time"]()

        self.assertEqual(job_time.hour, 8)
        self.assertEqual(job_time.minute, 0)
        self.assertEqual(getattr(job_time.tzinfo, "key", None), "Europe/Moscow")

    def test_format_yesterday_peak_alert_schedule_includes_configured_timezone(self):
        self.assertEqual(
            self.symbols["format_yesterday_peak_alert_schedule"](),
            "08:00 (Europe/Moscow)",
        )

    def test_help_text_mentions_configured_schedule(self):
        help_text = self.symbols["build_help_text"]()

        self.assertIn("18:00 (Europe/Moscow)", help_text)

    def test_resolve_telegram_proxy_url_uses_proxy_env(self):
        self.assertEqual(
            self.symbols["resolve_telegram_proxy_url"](),
            "socks5h://xray-client:1080",
        )

    def test_build_telegram_request_kwargs_sets_explicit_proxy_and_disables_trust_env(self):
        kwargs = self.symbols["build_telegram_request_kwargs"](
            proxy_url="socks5h://xray-client:1080",
            connection_pool_size=2,
            connect_timeout=20.0,
            read_timeout=75.0,
            write_timeout=20.0,
            pool_timeout=30.0,
        )

        self.assertEqual(kwargs["proxy"], "socks5h://xray-client:1080")
        self.assertEqual(kwargs["connection_pool_size"], 2)
        self.assertEqual(kwargs["read_timeout"], 75.0)
        self.assertEqual(kwargs["httpx_kwargs"], {"trust_env": False})

    def test_is_polling_backlog_detected_when_updates_accumulate_without_recent_processing(self):
        now_utc = datetime(2026, 3, 26, 16, 50, tzinfo=timezone.utc)
        process_started_at = now_utc - timedelta(minutes=10)
        last_update_received_at = now_utc - timedelta(minutes=5)

        detected = self.symbols["is_polling_backlog_detected"](
            pending_update_count=3,
            last_update_received_at=last_update_received_at,
            process_started_at=process_started_at,
            now_utc=now_utc,
        )

        self.assertTrue(detected)

    def test_is_polling_backlog_detected_returns_false_without_backlog_or_stall(self):
        now_utc = datetime(2026, 3, 26, 16, 50, tzinfo=timezone.utc)
        process_started_at = now_utc - timedelta(minutes=1)

        detected = self.symbols["is_polling_backlog_detected"](
            pending_update_count=0,
            last_update_received_at=None,
            process_started_at=process_started_at,
            now_utc=now_utc,
        )

        self.assertFalse(detected)

    def test_next_polling_backlog_detection_streak_increments_only_while_backlog_persists(self):
        next_streak = self.symbols["next_polling_backlog_detection_streak"](
            backlog_detected=True,
            current_streak=1,
        )
        cleared_streak = self.symbols["next_polling_backlog_detection_streak"](
            backlog_detected=False,
            current_streak=next_streak,
        )

        self.assertEqual(next_streak, 2)
        self.assertEqual(cleared_streak, 0)

    def test_should_trigger_polling_self_heal_only_after_confirmed_backlog(self):
        recovery_confirmation_count = self.symbols["POLLING_BACKLOG_RECOVERY_CONFIRMATION_COUNT"]

        first_detection = self.symbols["should_trigger_polling_self_heal"](
            backlog_detected=True,
            detection_streak=recovery_confirmation_count - 1,
        )
        confirmed_detection = self.symbols["should_trigger_polling_self_heal"](
            backlog_detected=True,
            detection_streak=recovery_confirmation_count,
        )
        healthy_state = self.symbols["should_trigger_polling_self_heal"](
            backlog_detected=False,
            detection_streak=recovery_confirmation_count,
        )

        self.assertFalse(first_detection)
        self.assertTrue(confirmed_detection)
        self.assertFalse(healthy_state)


if __name__ == "__main__":
    unittest.main()
