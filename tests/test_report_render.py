import os
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_render  # noqa: E402
from tests.test_report_payload import MonthlyReportPayloadBuilderTests  # noqa: E402


def build_sample_payload():
    return MonthlyReportPayloadBuilderTests()._build_payload()


class ReportRenderTests(unittest.TestCase):
    def test_build_deterministic_monthly_narrative_returns_expected_sections(self):
        payload = build_sample_payload()

        narrative = report_render.build_deterministic_monthly_narrative(payload)

        self.assertEqual(narrative["schema_version"], "monthly_fallback_narrative.v1")
        self.assertIn("обзор портфеля", narrative["report_title"])
        self.assertTrue(narrative["executive_summary"])
        self.assertTrue(narrative["performance_commentary"])

    def test_build_monthly_report_html_renders_five_pages_and_embeds_charts(self):
        payload = build_sample_payload()
        charts = report_render.build_monthly_report_charts(payload)

        html = report_render.build_monthly_report_html(payload, charts=charts)

        self.assertGreaterEqual(html.count('<section class="page">'), 5)
        self.assertIn("Динамика за месяц", html)
        self.assertIn("Структура на конец месяца", html)
        self.assertIn("Инструменты за месяц", html)
        self.assertIn("Операции, доходы и качество", html)
        self.assertNotIn("Executive Summary", html)
        self.assertIn("data:image/png;base64,", html)

    def test_build_monthly_report_pdf_bytes_uses_injected_renderer(self):
        payload = build_sample_payload()
        captured = {}

        def fake_renderer(html: str) -> bytes:
            captured["html"] = html
            return b"%PDF-mock"

        pdf_bytes = report_render.build_monthly_report_pdf_bytes(
            payload,
            pdf_renderer=fake_renderer,
        )

        self.assertEqual(pdf_bytes, b"%PDF-mock")
        self.assertIn("обзор портфеля", captured["html"])

    def test_save_debug_report_html_writes_file(self):
        html = "<html><body>test</body></html>"
        path = report_render.save_debug_report_html(html)
        try:
            self.assertTrue(Path(path).exists())
            self.assertIn("test", Path(path).read_text(encoding="utf-8"))
        finally:
            Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
