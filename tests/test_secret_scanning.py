import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCANNER_PATH = PROJECT_ROOT / "scripts" / "scan_secrets.py"


def load_scanner():
    spec = importlib.util.spec_from_file_location("scan_secrets_under_test", SCANNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class SecretScanningTests(unittest.TestCase):
    def test_current_tracked_tree_is_clean(self):
        scanner = load_scanner()
        findings = scanner.scan_current_tree(PROJECT_ROOT)
        self.assertEqual(findings, [])

    def test_history_scan_detects_deleted_secret_without_printing_value(self):
        scanner = load_scanner()
        secret = "vless:" + "//11111111-2222-4333-8444-555555555555@8.8.8.8:443"
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "security@example.invalid"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Security Test"], cwd=repo, check=True)
            fixture = repo / "fixture.txt"
            fixture.write_text(secret, encoding="utf-8")
            subprocess.run(["git", "add", "fixture.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "add fixture"], cwd=repo, check=True)
            fixture.write_text("sanitized\n", encoding="utf-8")
            subprocess.run(["git", "commit", "-qam", "sanitize fixture"], cwd=repo, check=True)

            findings = scanner.scan_git_history(repo)

        self.assertTrue(any(finding.rule == "vless_credential_uri" for finding in findings))
        rendered = "\n".join(scanner.format_finding(finding) for finding in findings)
        self.assertNotIn(secret, rendered)


if __name__ == "__main__":
    unittest.main()
