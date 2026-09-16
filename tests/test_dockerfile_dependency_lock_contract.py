from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DockerfileDependencyLockContractTests(unittest.TestCase):
    def test_production_images_require_hashes_for_locked_dependencies(self):
        for name in ("bot", "tracker", "reporter"):
            with self.subTest(name=name):
                text = (ROOT / "docker" / f"Dockerfile.{name}").read_text(encoding="utf-8")
                self.assertIn("RUN pip install --require-hashes -r requirements.txt", text)
