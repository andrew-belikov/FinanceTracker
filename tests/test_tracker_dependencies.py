from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TrackerDependencyTests(unittest.TestCase):
    def test_tracker_runtime_has_no_telegram_sdk_or_compiler_toolchain(self):
        requirements = (ROOT / "requirements" / "tracker.in").read_text(encoding="utf-8")
        dockerfile = (ROOT / "docker" / "Dockerfile.tracker").read_text(encoding="utf-8")

        self.assertNotIn("python-telegram-bot", requirements)
        self.assertNotIn("build-essential", dockerfile)
        self.assertNotIn("libpq-dev", dockerfile)
