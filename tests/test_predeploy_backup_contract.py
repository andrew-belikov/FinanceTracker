from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "scripts" / "predeploy_backup.sh").read_text(encoding="utf-8")


class PredeployBackupContractTests(unittest.TestCase):
    def test_backup_requires_deploy_identity_and_existing_destination(self):
        for required in ("APP_ENV_FILE", "BACKUP_DIR", "DEPLOY_SHA", "CI_RUN_ID"):
            with self.subTest(required=required):
                self.assertIn(f"require {required}", SCRIPT)
        self.assertIn('mkdir "$bundle"', SCRIPT)
        self.assertIn('[[ -d "$BACKUP_DIR" && ! -L "$BACKUP_DIR" ]]', SCRIPT)

    def test_backup_captures_hashes_and_proves_restore_before_success(self):
        for required in (
            "pg_dump --format=custom --no-owner --no-privileges",
            "schema_migrations.txt",
            "sha256sum",
            "createdb -U",
            "pg_restore -U",
            "migrate --check",
            '"restore_drill":"passed"',
        ):
            with self.subTest(required=required):
                self.assertIn(required, SCRIPT)
        self.assertLess(SCRIPT.index("pg_restore -U"), SCRIPT.index('"restore_drill":"passed"'))
        self.assertNotIn("pg_restore --clean", SCRIPT)


if __name__ == "__main__":
    unittest.main()
