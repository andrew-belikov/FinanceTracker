from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from financetracker.bot import entrypoint as bot_entrypoint
from financetracker.tracker.container_smoke import (
    ContainerSmokeConfigurationError,
    run_if_requested,
)


class TrackerContainerSmokeModeTests(unittest.TestCase):
    def test_normal_mode_does_not_write_ready_state(self):
        writes: list[str] = []

        self.assertFalse(
            run_if_requested(
                mode="",
                app_env="ci",
                write_ready_state=lambda: writes.append("ready"),
            )
        )
        self.assertEqual(writes, [])

    def test_container_smoke_is_rejected_outside_ci(self):
        with self.assertRaises(ContainerSmokeConfigurationError):
            run_if_requested(
                mode="container-smoke",
                app_env="production",
                write_ready_state=lambda: None,
            )

    def test_container_smoke_writes_ready_state_before_waiting(self):
        writes: list[str] = []

        def stop_after_first_wait(_seconds: float) -> None:
            raise RuntimeError("stop test loop")

        with self.assertRaisesRegex(RuntimeError, "stop test loop"):
            run_if_requested(
                mode="container-smoke",
                app_env="ci",
                write_ready_state=lambda: writes.append("ready"),
                sleep=stop_after_first_wait,
            )
        self.assertEqual(writes, ["ready"])


class BotContainerSmokeModeTests(unittest.TestCase):
    def test_container_smoke_is_enabled_only_in_ci(self):
        with mock.patch.dict("os.environ", {"BOT_STARTUP_MODE": "container-smoke", "APP_ENV": "ci"}, clear=True):
            self.assertTrue(bot_entrypoint.is_container_smoke_mode())

        with mock.patch.dict("os.environ", {"BOT_STARTUP_MODE": "container-smoke", "APP_ENV": "production"}, clear=True):
            with self.assertRaises(bot_entrypoint.RuntimeConfigurationError):
                bot_entrypoint.is_container_smoke_mode()

    def test_reporter_boundary_uses_synthetic_internal_request(self):
        response = mock.MagicMock()
        response.status = 200
        opener = mock.MagicMock()
        opener.open.return_value.__enter__.return_value = response
        with mock.patch.dict("os.environ", {"REPORTER_SERVICE_KEY": "x" * 32}, clear=True), mock.patch.object(
            bot_entrypoint.request, "build_opener", return_value=opener
        ) as build_opener:
            bot_entrypoint.verify_reporter_boundary()

        handler = build_opener.call_args.args[0]
        self.assertEqual(handler.proxies, {})
        probe = opener.open.call_args.args[0]
        self.assertEqual(probe.full_url, "http://reporter:8088/reports/monthly/pdf")
        self.assertEqual(probe.get_header("X-reporter-service-key"), "x" * 32)


class ComposeContainerSmokeContractTests(unittest.TestCase):
    def test_ci_override_is_hermetic_and_enables_each_ci_only_path(self):
        project_root = Path(__file__).resolve().parents[1]
        compose_ci = (project_root / "compose.ci.yml").read_text(encoding="utf-8")
        workflow = (project_root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

        for expected in (
            "APP_ENV: ci",
            "TRACKER_STARTUP_MODE: container-smoke",
            "BOT_STARTUP_MODE: container-smoke",
            'BOT_PROXY_ENABLED: "false"',
            'OLLAMA_ENABLED: "false"',
            "REPORTER_INTERNAL_URL: http://reporter:8088",
            "internal: true",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, compose_ci)
        self.assertIn("up -d --wait --wait-timeout 120 db migrate tracker reporter xray-client bot", workflow)
        self.assertIn('"${compose[@]}" run --rm migrate --check', workflow)


if __name__ == "__main__":
    unittest.main()
