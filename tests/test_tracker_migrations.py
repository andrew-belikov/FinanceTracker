import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRACKER_DIR = PROJECT_ROOT / "src" / "tracker"
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(TRACKER_DIR))

SPEC = importlib.util.spec_from_file_location(
    "tracker_migrate_under_test",
    TRACKER_DIR / "migrate.py",
)
migrate = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
with mock.patch.dict(
    "os.environ",
    {
        "DB_DSN": "sqlite://",
        "TINVEST_API_TOKEN": "test-token",
    },
):
    SPEC.loader.exec_module(migrate)


class TrackerMigrationTests(unittest.TestCase):
    def test_discovers_forward_migrations_and_excludes_rollbacks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "20260101_first.sql").write_text("SELECT 1;", encoding="utf-8")
            (root / "20260101_first.rollback.sql").write_text(
                "SELECT 2;",
                encoding="utf-8",
            )
            (root / "README.md").write_text("ignored", encoding="utf-8")

            paths = migrate.discover_migrations(root)

        self.assertEqual([path.name for path in paths], ["20260101_first.sql"])

    def test_strips_outer_transaction_wrapper(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "20260101_first.sql"
            path.write_text(
                "-- comment\nBEGIN;\nSELECT 1;\nCOMMIT;\n",
                encoding="utf-8",
            )

            sql = migrate.migration_sql(path)

        self.assertEqual(sql, "-- comment\n\nSELECT 1;")

    def test_rejects_changed_applied_migration(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "20260101_first.sql"
            path.write_text("SELECT 1;", encoding="utf-8")

            with self.assertRaisesRegex(migrate.MigrationError, "Checksum mismatch"):
                migrate._validate_applied_checksums(
                    [path],
                    {path.name: "different"},
                )

    def test_rejects_missing_applied_migration_file(self):
        with self.assertRaisesRegex(migrate.MigrationError, "missing"):
            migrate._validate_applied_checksums(
                [],
                {"20260101_missing.sql": "checksum"},
            )

    def test_asset_alias_backfill_produces_one_row_per_conflict_key(self):
        sql = (
            PROJECT_ROOT / "migrations" / "20260324_dataset_source_fields.sql"
        ).read_text(encoding="utf-8")
        backfill = sql.split("INSERT INTO public.asset_aliases", 1)[1]
        group_by = backfill.split("GROUP BY", 1)[1].split("ON CONFLICT", 1)[0]

        self.assertIn("MAX(i.ticker)", backfill)
        self.assertIn("MAX(COALESCE(", backfill)
        self.assertIn("NOW() AS created_at", backfill)
        self.assertIn("NOW() AS updated_at", backfill)
        self.assertNotIn("i.ticker", group_by)
        self.assertNotIn("o.name", group_by)

    def test_iis_tax_deduction_migration_is_idempotent(self):
        sql = (
            PROJECT_ROOT / "migrations" / "20260805_operations_cashflow_category.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("ADD COLUMN IF NOT EXISTS cashflow_category TEXT", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS ix_operations_cashflow_category", sql)


if __name__ == "__main__":
    unittest.main()
