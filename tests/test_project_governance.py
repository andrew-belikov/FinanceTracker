from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ProjectGovernanceTests(unittest.TestCase):
    def test_mit_license_is_present(self):
        license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")
        self.assertIn("MIT License", license_text)
        self.assertIn("Permission is hereby granted", license_text)

    def test_release_policy_is_documented(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
        self.assertIn("SemVer", readme)
        self.assertIn("annotated Git tag", readme)
        self.assertIn("annotated tag", contributing)


if __name__ == "__main__":
    unittest.main()
