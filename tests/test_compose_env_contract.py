from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ComposeEnvContractTests(unittest.TestCase):
    def test_synthetic_verifier_checks_resolved_postgres_values_without_logging_config(self):
        script_path = PROJECT_ROOT / "scripts" / "verify_compose_env.py"
        self.assertTrue(script_path.exists(), "synthetic Compose env verifier is missing")
        text = script_path.read_text(encoding="utf-8")
        self.assertIn("--env-file", text)
        self.assertIn("POSTGRES_DB", text)
        self.assertIn("POSTGRES_USER", text)
        self.assertIn("POSTGRES_PASSWORD", text)
        self.assertIn("capture_output=True", text)
        self.assertNotIn("completed.stdout.decode", text)
        self.assertNotIn("print(completed.stdout", text)


if __name__ == "__main__":
    unittest.main()
