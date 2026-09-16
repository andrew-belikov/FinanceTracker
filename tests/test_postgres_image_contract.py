import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "compose.yml").read_text(encoding="utf-8")
CI = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
DEPLOY = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
POSTGRES_REF = re.compile(r"postgres:16@sha256:[0-9a-f]{64}")


class PostgresImageContractTests(unittest.TestCase):
    def test_compose_and_ci_use_the_same_pinned_postgres_digest(self):
        compose_refs = POSTGRES_REF.findall(COMPOSE)
        ci_refs = POSTGRES_REF.findall(CI)
        self.assertEqual(len(compose_refs), 1)
        self.assertEqual(ci_refs, compose_refs)

    def test_deploy_receipt_captures_database_reference_and_image_identity(self):
        self.assertIn("ACTUAL_DB_IMAGE_REF", DEPLOY)
        self.assertIn("ACTUAL_DB_IMAGE_ID", DEPLOY)
        self.assertIn("Database image", DEPLOY)
        self.assertIn("^postgres:16@sha256:[0-9a-f]{64}$", DEPLOY)
