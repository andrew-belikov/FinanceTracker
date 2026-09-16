from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ReleaseMetadataTests(unittest.TestCase):
    def test_project_version_matches_latest_changelog_release(self):
        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")

        project_version = re.search(r'^version = "([^"]+)"$', pyproject, re.MULTILINE)
        changelog_version = re.search(r"^## \[([^]]+)\]", changelog, re.MULTILINE)

        self.assertIsNotNone(project_version)
        self.assertIsNotNone(changelog_version)
        self.assertEqual(project_version.group(1), changelog_version.group(1))
