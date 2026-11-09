from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class IntradayDailyPushDocumentationTests(unittest.TestCase):
    def test_operator_configuration_declares_weekday_moscow_defaults(self):
        env_example = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
        config = (PROJECT_ROOT / "docs" / "CONFIG.md").read_text(encoding="utf-8")
        contract = (
            PROJECT_ROOT / "docs" / "contracts" / "configuration.md"
        ).read_text(encoding="utf-8")

        for text in (env_example, config):
            with self.subTest(document=text[:40]):
                self.assertIn("INTRADAY_DAILY_PUSH_HOUR", text)
                self.assertIn("INTRADAY_DAILY_PUSH_MINUTE", text)

        self.assertIn("INTRADAY_DAILY_PUSH_HOUR=19", env_example)
        self.assertIn("INTRADAY_DAILY_PUSH_MINUTE=10", env_example)
        self.assertIn("`INTRADAY_DAILY_PUSH_HOUR` / `_MINUTE`", contract)
        self.assertIn("понедельника по пятницу", config)
        self.assertIn("`19:10`", config)
        self.assertIn("Monday–Friday", contract)

    def test_behavior_preserves_eod_and_requires_confirmed_two_part_delivery(self):
        behavior = (PROJECT_ROOT / "docs" / "BEHAVIOR.md").read_text(encoding="utf-8")

        for required in (
            "`intraday_daily_push(...)`",
            "с понедельника по пятницу",
            "`19:10 Europe/Moscow`",
            "не меняют источники и формулы",
            "При двух и более наблюдениях",
            "вкладу, а не по проценту изменения",
            "текст и график доставлены",
            "отдельный календарь торговых сессий и\nоценка пропусков",
        ):
            with self.subTest(required=required):
                self.assertIn(required, behavior)

    def test_database_contract_separates_temporary_observations_from_durable_summary(self):
        schema = (
            PROJECT_ROOT / "docs" / "contracts" / "database-schema.md"
        ).read_text(encoding="utf-8")

        for required in (
            "### `intraday_portfolio_snapshots`",
            "### `intraday_portfolio_positions`",
            "### `intraday_daily_summaries`",
            "`20260916_intraday_portfolio_snapshots.sql`",
            "raw observations удаляются",
            "`delivered_at` заполняется только после подтверждённой доставки",
        ):
            with self.subTest(required=required):
                self.assertIn(required, schema)


if __name__ == "__main__":
    unittest.main()
