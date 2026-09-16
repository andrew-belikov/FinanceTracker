import json
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]



from financetracker.reporting import report_ai  # noqa: E402
from financetracker.reporting.report_render import build_deterministic_monthly_narrative  # noqa: E402
from tests.test_report_payload import MonthlyReportPayloadBuilderTests  # noqa: E402


class FakeOllamaResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def build_sample_payload():
    return MonthlyReportPayloadBuilderTests()._build_payload()


def build_valid_ai_output(payload: dict) -> dict:
    narrative = build_deterministic_monthly_narrative(payload)
    narrative["schema_version"] = report_ai.MONTHLY_AI_OUTPUT_SCHEMA_VERSION
    return narrative


class ReportAITests(unittest.TestCase):
    def test_build_monthly_report_narrative_returns_fallback_when_disabled(self):
        payload = build_sample_payload()

        with mock.patch.object(report_ai, "OLLAMA_ENABLED", False):
            result = report_ai.build_monthly_report_narrative(payload)

        self.assertEqual(result["source"], "fallback")
        self.assertEqual(result["attempts"], 0)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["narrative"]["schema_version"], "monthly_fallback_narrative.v1")

    def test_build_monthly_report_narrative_uses_ollama_output_when_valid(self):
        payload = build_sample_payload()
        ai_output = build_valid_ai_output(payload)
        response = FakeOllamaResponse(
            {
                "message": {"role": "assistant", "content": json.dumps(ai_output, ensure_ascii=False)},
                "done": True,
                "total_duration": 123,
                "prompt_eval_count": 45,
                "eval_count": 67,
            }
        )

        with mock.patch.object(report_ai, "OLLAMA_ENABLED", True), \
             mock.patch.object(report_ai.request, "urlopen", return_value=response):
            result = report_ai.build_monthly_report_narrative(payload)

        self.assertEqual(result["source"], "ollama")
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["narrative"]["schema_version"], report_ai.MONTHLY_AI_OUTPUT_SCHEMA_VERSION)
        self.assertEqual(result["telemetry"]["total_duration"], 123)

    def test_build_monthly_report_narrative_retries_after_validation_failure(self):
        payload = build_sample_payload()
        invalid_output = build_valid_ai_output(payload)
        invalid_output["executive_summary"][0] = "На конец месяца портфель оценён в 999 999 ₽."
        valid_output = build_valid_ai_output(payload)
        responses = [
            FakeOllamaResponse({"message": {"role": "assistant", "content": json.dumps(invalid_output, ensure_ascii=False)}}),
            FakeOllamaResponse({"message": {"role": "assistant", "content": json.dumps(valid_output, ensure_ascii=False)}}),
        ]

        with mock.patch.object(report_ai, "OLLAMA_ENABLED", True), \
             mock.patch.object(report_ai.request, "urlopen", side_effect=responses) as urlopen_mock:
            result = report_ai.build_monthly_report_narrative(payload)

        self.assertEqual(result["source"], "ollama")
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(urlopen_mock.call_count, 2)

    def test_build_monthly_report_narrative_falls_back_after_repeated_invalid_json(self):
        payload = build_sample_payload()
        responses = [
            FakeOllamaResponse({"message": {"role": "assistant", "content": "not json"}}),
            FakeOllamaResponse({"message": {"role": "assistant", "content": "still not json"}}),
        ]

        with mock.patch.object(report_ai, "OLLAMA_ENABLED", True), \
             mock.patch.object(report_ai.request, "urlopen", side_effect=responses):
            result = report_ai.build_monthly_report_narrative(payload)

        self.assertEqual(result["source"], "fallback")
        self.assertEqual(result["attempts"], 2)
        self.assertTrue(result["errors"])

    def test_normalize_monthly_ai_output_rejects_english_report_title(self):
        payload = build_valid_ai_output(build_sample_payload())
        payload["report_title"] = "April 2026 monthly review"

        with self.assertRaises(report_ai.ReportAIValidationError):
            report_ai.normalize_monthly_ai_output(payload)

    def test_ollama_url_rejects_non_http_schemes_and_credentials(self):
        for value in ("file:///tmp/model", "unix:///var/run/ollama.sock", "http://user:pass@ollama:11434"):
            with self.subTest(value=value):
                with self.assertRaises(report_ai.ReportAIError):
                    report_ai.build_ollama_chat_url(value)

    def test_ollama_url_keeps_only_the_chat_endpoint(self):
        self.assertEqual(
            report_ai.build_ollama_chat_url("https://ollama.example/api?ignored=yes#ignored"),
            "https://ollama.example/api/api/chat",
        )


if __name__ == "__main__":
    unittest.main()
