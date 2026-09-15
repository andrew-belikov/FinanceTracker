from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DockerignoreContractTests(unittest.TestCase):
    def test_build_context_is_default_deny_with_explicit_runtime_allowlist(self):
        lines = {
            line.strip()
            for line in (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }

        self.assertIn("*", lines)
        self.assertTrue(
            {
                "!src/",
                "!requirements/",
                "!migrations/",
                "!docker/",
            }.issubset(lines)
        )

    def test_secret_and_local_paths_are_not_reintroduced_into_build_context(self):
        lines = {
            line.strip()
            for line in (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }

        forbidden_allowlist_entries = {
            "!.env",
            "!.env.*",
            "!.git/",
            "!.venv/",
            "!tests/",
        }
        self.assertTrue(lines.isdisjoint(forbidden_allowlist_entries))


if __name__ == "__main__":
    unittest.main()
