import sys
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_pipeline  # noqa: E402
from tests.test_report_payload import MonthlyReportPayloadBuilderTests  # noqa: E402


def build_sample_payload():
    return MonthlyReportPayloadBuilderTests()._build_payload()


class ReportPipelineTests(unittest.TestCase):
    def test_build_monthly_report_artifact_for_request_uses_payload_builder(self):
        payload = build_sample_payload()
        ai_narrative = {
            "schema_version": "monthly_ai_output.v1",
            "report_title": "Тестовый обзор портфеля",
            "executive_summary": ["Портфель закрыл месяц в плюсе."],
            "performance_commentary": ["Главный график показал ровную траекторию."],
            "instrument_takeaways": ["Крупнейшая позиция сохранила лидерство."],
            "cashflow_notes": [],
            "quality_notes": [],
            "risk_notes": [],
            "warnings": [],
        }

        with mock.patch.object(report_pipeline, "create_monthly_report_payload", return_value=payload), \
             mock.patch.object(
                 report_pipeline,
                 "build_monthly_report_narrative",
                 return_value={
                     "source": "ollama",
                     "narrative": ai_narrative,
                     "ai_input": {"schema_version": "monthly_ai_input.v1"},
                     "attempts": 1,
                     "telemetry": {"total_duration": 123},
                     "errors": [],
                 },
             ):
            artifact = report_pipeline.build_monthly_report_artifact_for_request(
                {"year": 2026, "month": 4},
                pdf_renderer=lambda html: b"%PDF-pipeline",
            )

        self.assertEqual(artifact["schema_version"], "monthly_report_artifact.v1")
        self.assertEqual(artifact["period"], "2026-04")
        self.assertEqual(artifact["filename"], "fintracker_monthly_2026-04.pdf")
        self.assertEqual(artifact["pdf_bytes"], b"%PDF-pipeline")
        self.assertEqual(artifact["narrative_source"], "ollama")
        self.assertTrue(artifact["payload"]["meta"]["has_ai_narrative"])
        self.assertEqual(artifact["narrative"]["report_title"], "Тестовый обзор портфеля")

    def test_build_monthly_report_artifact_for_request_validates_month_type(self):
        with self.assertRaises(report_pipeline.ReportRequestError):
            report_pipeline.build_monthly_report_artifact_for_request({"month": "04"})


if __name__ == "__main__":
    unittest.main()
