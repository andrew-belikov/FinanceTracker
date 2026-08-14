from pathlib import Path
import re
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_TEXT = (PROJECT_ROOT / "compose.yml").read_text(encoding="utf-8")


class ComposeHealthcheckContractTests(unittest.TestCase):
    def service_block(self, service_name, next_service_name):
        pattern = rf"(?ms)^  {re.escape(service_name)}:\n(.*?)(?=^  {re.escape(next_service_name)}:\n)"
        match = re.search(pattern, COMPOSE_TEXT)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_tracker_has_process_healthcheck(self):
        tracker = self.service_block("tracker", "bot")
        self.assertIn("healthcheck:", tracker)
        self.assertIn("tracker_healthcheck", tracker)
        self.assertIn("TRACKER_READY_FILE", tracker)
        self.assertIn("TRACKER_READY_MAX_AGE_SECONDS", tracker)

    def test_bot_has_process_healthcheck_and_waits_for_healthy_proxy(self):
        bot = self.service_block("bot", "reporter")
        self.assertIn("healthcheck:", bot)
        self.assertIn("bot_healthcheck", bot)
        self.assertIn("BOT_READY_FILE", bot)
        self.assertIn("BOT_READY_MAX_AGE_SECONDS", bot)
        self.assertRegex(bot, r"(?ms)xray-client:\s*\n\s+condition:\s+service_healthy")


if __name__ == "__main__":
    unittest.main()
