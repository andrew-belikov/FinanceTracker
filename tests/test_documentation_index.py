from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


class DocumentationIndexTests(unittest.TestCase):
    def test_no_documentation_uses_machine_local_absolute_paths(self):
        for path in [*DOCS.rglob("*.md"), ROOT / "README.md", ROOT / "CONTRIBUTING.md"]:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertNotIn("/Users/andrew/Dev/FinanceTracker", path.read_text(encoding="utf-8"))

    def test_active_index_separates_historical_plans(self):
        index = (DOCS / "README.md").read_text(encoding="utf-8")
        self.assertIn("archive/PDF_REPORT_ROADMAP.md", index)
        self.assertIn("archive/PDF_REPORT_TECH.md", index)
        self.assertIn("archive/bot-decomposition-plan.md", index)
        for name in ("PDF_REPORT_ROADMAP.md", "PDF_REPORT_TECH.md", "bot-decomposition-plan.md"):
            text = (DOCS / "archive" / name).read_text(encoding="utf-8")
            self.assertTrue("Истор" in text or "Архив" in text)

    def test_active_index_links_to_architecture_decisions(self):
        index = (DOCS / "README.md").read_text(encoding="utf-8")
        adr_index = DOCS / "adr" / "README.md"

        self.assertIn("adr/README.md", index)
        self.assertTrue(adr_index.is_file())
        self.assertIn("ADR-0004", adr_index.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
