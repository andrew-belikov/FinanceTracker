from __future__ import annotations

from pathlib import Path
import json
import re
import unittest

import jsonschema


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


class DocumentationIndexTests(unittest.TestCase):
    def test_no_documentation_uses_machine_local_absolute_paths(self):
        for path in [*DOCS.rglob("*.md"), ROOT / "README.md", ROOT / "CONTRIBUTING.md"]:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertNotIn("/Users/andrew/Dev/FinanceTracker", path.read_text(encoding="utf-8"))

    def test_active_index_lists_only_current_product_documentation(self):
        index = (DOCS / "README.md").read_text(encoding="utf-8")
        self.assertIn("ARCHITECTURE.md", index)
        self.assertIn("PROJECT_MAP.md", index)
        self.assertIn("BEHAVIOR.md", index)
        self.assertIn("RUNBOOK.md", index)
        self.assertIn("contracts/README.md", index)
        for historical_name in (
            "PROJECT_AUDIT.md",
            "TARGET_ARCHITECTURE.md",
            "CONTRACTS.md",
            "archive/",
            "CODE_REVIEW",
        ):
            with self.subTest(historical_name=historical_name):
                self.assertNotIn(historical_name, index)

    def test_completed_work_artifacts_are_not_kept_as_active_documentation(self):
        self.assertFalse((DOCS / "audits").exists())
        self.assertFalse((DOCS / "acceptance").exists())
        self.assertFalse((DOCS / "MODULE_DECOMPOSITION_PLAN.md").exists())
        self.assertFalse((DOCS / "MODULE_EXTRACTION_REGISTER.md").exists())

    def test_contract_catalog_and_machine_readable_contracts_exist(self):
        contracts = DOCS / "contracts"
        required = {
            "README.md",
            "database-schema.md",
            "reporter-http-api.md",
            "monthly-report-payload.md",
            "configuration.md",
            "external-integrations.md",
            "versioning.md",
            "reporter.openapi.json",
            "monthly-report-payload.schema.json",
        }
        self.assertTrue(required.issubset(path.name for path in contracts.iterdir()))

        for path in contracts.glob("*.json"):
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertIsInstance(json.loads(path.read_text(encoding="utf-8")), dict)

        for path in (
            DOCS / "logging.schema.json",
            contracts / "monthly-report-payload.schema.json",
        ):
            with self.subTest(path=path.relative_to(ROOT)):
                jsonschema.Draft202012Validator.check_schema(
                    json.loads(path.read_text(encoding="utf-8"))
                )

    def test_active_index_links_to_architecture_decisions(self):
        index = (DOCS / "README.md").read_text(encoding="utf-8")
        adr_index = DOCS / "adr" / "README.md"

        self.assertIn("adr/README.md", index)
        self.assertTrue(adr_index.is_file())
        self.assertIn("ADR-0004", adr_index.read_text(encoding="utf-8"))

    def test_operator_configuration_documents_tracker_timezone_and_safe_auto_account(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        config = (DOCS / "CONFIG.md").read_text(encoding="utf-8")

        self.assertIn("SCHED_TZ=Europe/Moscow", env_example)
        self.assertNotIn('"auto" to pick the first available', env_example)
        self.assertIn("`SCHED_TZ`", config)
        self.assertIn("ровно один открытый счёт", config)

    def test_every_example_environment_variable_is_documented(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        documentation = "\n".join(
            (
                (DOCS / "CONFIG.md").read_text(encoding="utf-8"),
                (DOCS / "contracts" / "configuration.md").read_text(
                    encoding="utf-8"
                ),
            )
        )

        names = re.findall(r"^([A-Z][A-Z0-9_]*)=", env_example, re.MULTILINE)
        for name in names:
            with self.subTest(name=name):
                self.assertIn(name, documentation)

    def test_operator_templates_and_contracts_use_canonical_startup_inputs(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        integrations = (DOCS / "contracts" / "external-integrations.md").read_text(encoding="utf-8")
        runbook = (DOCS / "RUNBOOK.md").read_text(encoding="utf-8")
        ollama_compose = (ROOT / "compose.ollama.yml").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("REPORTER_SERVICE_KEY=CHANGE_ME", env_example)
        self.assertIn("SNAPSHOT_MODE=interval", env_example)
        self.assertIn("BOT_STARTUP_RETRY_DELAY_SECONDS=15", env_example)
        self.assertIn("TINVEST_API_TOKEN", integrations)
        self.assertIn("TINVEST_BASE_URL", integrations)
        self.assertNotIn("TINKOFF_API_TOKEN", integrations)
        self.assertNotIn("TINKOFF_BASE_URL", integrations)
        self.assertIn("${APP_ENV_FILE:-.env}", runbook)
        self.assertIn('--env-file "$APP_ENV_FILE"', runbook)
        for text in (ollama_compose, readme):
            with self.subTest(text=text[:30]):
                self.assertIn("-f compose.yml -f compose.ollama.yml", text)

    def test_project_map_covers_repository_and_runtime_owners(self):
        project_map = (DOCS / "PROJECT_MAP.md").read_text(encoding="utf-8")

        for path in (
            "src/financetracker/",
            "migrations/",
            "docker/",
            "requirements/",
            "tests/",
            "docs/",
            "scripts/",
            ".github/workflows/",
        ):
            with self.subTest(path=path):
                self.assertIn(f"`{path}`", project_map)

        for package in (
            "common",
            "domain",
            "config",
            "database",
            "tracker",
            "bot",
            "reporting",
            "xray",
        ):
            with self.subTest(package=package):
                self.assertIn(f"`{package}`", project_map)


if __name__ == "__main__":
    unittest.main()
