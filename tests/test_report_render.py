import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_render  # noqa: E402
from tests.test_report_payload import MonthlyReportPayloadBuilderTests  # noqa: E402


def build_sample_payload():
    return MonthlyReportPayloadBuilderTests()._build_payload()


class ReportRenderTests(unittest.TestCase):
    def test_classify_day_pnl_rows_excludes_first_synthetic_zero(self):
        stats = report_render._classify_day_pnl_rows(
            [
                {"date": "2026-04-01", "day_pnl": "0"},
                {"date": "2026-04-02", "day_pnl": "12"},
                {"date": "2026-04-03", "day_pnl": "-4"},
                {"date": "2026-04-04", "day_pnl": "0"},
            ]
        )

        self.assertEqual(stats["positive"]["count"], 1)
        self.assertEqual(stats["negative"]["count"], 1)
        self.assertEqual(stats["neutral"]["count"], 1)
        self.assertEqual(stats["positive"]["total"], 3)

    def test_compute_average_day_pnl_by_sign_uses_relevant_rows(self):
        rows = [
            {"date": "2026-04-01", "day_pnl": "0"},
            {"date": "2026-04-02", "day_pnl": "12"},
            {"date": "2026-04-03", "day_pnl": "6"},
            {"date": "2026-04-04", "day_pnl": "-9"},
            {"date": "2026-04-05", "day_pnl": "-3"},
        ]

        self.assertEqual(report_render._compute_average_day_pnl(rows, positive=True), Decimal("9"))
        self.assertEqual(report_render._compute_average_day_pnl(rows, positive=False), Decimal("-6"))

    def test_build_plan_pace_fact_prefers_target_to_date(self):
        primary, secondary = report_render._build_plan_pace_fact(
            {
                "deposits_ytd": "120000",
                "target_to_date": "100000",
                "plan_progress_pct": "30.0",
                "plan_annual_contrib": "400000",
            }
        )

        self.assertEqual(primary, "120,0%")
        self.assertEqual(secondary, "цель к дате: 100 000 ₽")

    def test_build_plan_pace_fact_falls_back_to_plan_progress_pct(self):
        primary, secondary = report_render._build_plan_pace_fact(
            {
                "deposits_ytd": "120000",
                "target_to_date": None,
                "plan_progress_pct": "30.0",
                "plan_annual_contrib": "400000",
            }
        )

        self.assertEqual(primary, "30,0%")
        self.assertEqual(secondary, "из плана 400 000 ₽")

    def test_build_weight_transition_map_formats_start_to_end(self):
        transitions = report_render._build_weight_transition_map(
            [
                {"logical_asset_id": "asset-1", "weight_pct": "5.0"},
            ],
            [
                {"logical_asset_id": "asset-1", "weight_pct": "2.5"},
                {"logical_asset_id": "asset-2", "weight_pct": "4.0"},
            ],
        )

        self.assertEqual(transitions["asset-1"], "5,0% → 2,5%")
        self.assertEqual(transitions["asset-2"], "0,0% → 4,0%")

    def test_build_asset_class_breakdown_adds_other_bucket(self):
        breakdown = report_render._build_asset_class_breakdown(
            [
                {"instrument_type": "share", "position_value": Decimal("100")},
                {"instrument_type": "bond", "position_value": Decimal("50")},
                {"instrument_type": "future", "position_value": Decimal("20")},
            ]
        )

        self.assertEqual(
            breakdown,
            [
                {"key": "stocks", "label": "Акции", "value": Decimal("100")},
                {"key": "bonds", "label": "Облигации", "value": Decimal("50")},
                {"key": "other", "label": "Другое", "value": Decimal("20")},
            ],
        )

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
        page_count = html.count('<section class="page">') + html.count('<section class="page page--cover">')
        pages = [segment for segment in html.split("<section") if 'class="page' in segment]

        self.assertGreaterEqual(page_count, 5)
        self.assertIn("Динамика за месяц", html)
        self.assertIn("Структура на конец месяца", html)
        self.assertIn("Инструменты за месяц", html)
        self.assertIn("Операции, доходы и качество", html)
        self.assertNotIn("Executive Summary", html)
        self.assertIn("data:image/png;base64,", html)
        self.assertNotIn("Крупнейшая позиция", pages[0])
        self.assertNotIn("Расхождение", pages[0])
        self.assertNotIn("Детерминированный месячный отчёт", pages[0])
        self.assertIn("Факты месяца", pages[0])
        self.assertIn("Дни месяца", pages[0])
        self.assertIn("Диапазон стоимости", pages[0])
        self.assertIn("Денежный поток", pages[0])
        self.assertIn("&nbsp;₽", pages[0])
        self.assertIn("Ритм месяца", pages[1])
        self.assertIn("Баланс дней", pages[1])
        self.assertIn("Сила движения", pages[1])
        self.assertIn("Годовой план", pages[1])
        self.assertIn("Ростовых дней", pages[1])
        self.assertIn("Снижающихся дней", pages[1])
        self.assertIn("Средний плюс-день", pages[1])
        self.assertIn("Средний минус-день", pages[1])
        self.assertIn("Внесено с начала года", pages[1])
        self.assertIn("Темп к дате", pages[1])
        self.assertNotIn("Лучший день", pages[1])
        self.assertNotIn("Худший день", pages[1])
        self.assertNotIn("Пик месяца", pages[1])
        self.assertNotIn("Минимум месяца", pages[1])
        self.assertNotIn("Нейтральных дней", pages[1])
        self.assertIn("Классы активов", pages[2])
        self.assertIn("Изм. доли", pages[2])
        self.assertIn("Нереализованный результат", pages[2])
        self.assertIn("status-dot", pages[2])

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
