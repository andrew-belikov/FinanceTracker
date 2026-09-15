from pathlib import Path
import tempfile
import unittest
from unittest import mock

from financetracker.tracker import migrate

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRACKER_DIR = PROJECT_ROOT / "src" / "financetracker" / "tracker"


class TrackerMigrationTests(unittest.TestCase):
    def test_discovers_forward_migrations_and_excludes_rollbacks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "20260101_first.sql").write_text("SELECT 1;", encoding="utf-8")
            (root / "20260101_first.rollback.sql").write_text(
                "SELECT 2;",
                encoding="utf-8",
            )
            (root / "._20260101_first.sql").write_bytes(b"\x00\xa3resource-fork")
            (root / "README.md").write_text("ignored", encoding="utf-8")

            paths = migrate.discover_migrations(root)

        self.assertEqual([path.name for path in paths], ["20260101_first.sql"])

    def test_versioned_baseline_replaces_orm_create_all(self):
        baseline = (PROJECT_ROOT / "migrations" / "20260220_portfolio_baseline.sql").read_text(encoding="utf-8")
        runner = (TRACKER_DIR / "migrate.py").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS portfolio_snapshots", baseline)
        self.assertIn("CREATE TABLE IF NOT EXISTS portfolio_positions", baseline)
        self.assertNotIn("Base.metadata.create_all", runner)

    def test_migration_runner_does_not_import_tracker_application(self):
        runner = (TRACKER_DIR / "migrate.py").read_text(encoding="utf-8")
        self.assertNotIn("financetracker.tracker.app", runner)

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

    def test_timezone_migration_explicitly_interprets_legacy_values_as_utc(self):
        sql = (
            PROJECT_ROOT / "migrations" / "20260915_timezone_aware_utc.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("BEGIN;", sql)
        self.assertIn("COMMIT;", sql)
        self.assertIn("TYPE TIMESTAMPTZ", sql)
        self.assertIn("AT TIME ZONE 'UTC'", sql)
        self.assertIn("DROP VIEW IF EXISTS public.deposits", sql)
        self.assertIn("CREATE VIEW public.deposits AS", sql)
        self.assertIn("ALTER TABLE public.operations", sql)
        self.assertIn("ALTER TABLE public.bot_notification_deliveries", sql)

    def test_schema_contract_alignment_migrates_legacy_orm_column_types(self):
        sql = (
            PROJECT_ROOT / "migrations" / "20260915_schema_contract_alignment.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("ALTER COLUMN figi TYPE TEXT", sql)
        self.assertNotIn("ALTER COLUMN current_nkd TYPE", sql)
        self.assertIn("ALTER COLUMN id TYPE BIGINT", sql)
        self.assertIn("ALTER SEQUENCE IF EXISTS public.asset_aliases_id_seq AS BIGINT", sql)

    def test_currency_migration_uses_explicit_unknown_sentinel(self):
        sql = (
            PROJECT_ROOT / "migrations" / "20260915_currency_unknown_sentinel.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("SET currency = 'UNKNOWN'", sql)
        self.assertIn("public.payout_calendar_events", sql)

    def test_schema_manifest_is_versioned_and_covers_versioned_tables(self):
        from financetracker.database import schema_manifest

        self.assertGreater(schema_manifest.SCHEMA_MANIFEST_VERSION, 0)
        self.assertIn("operations", schema_manifest.TABLES)
        self.assertIn("income_events", schema_manifest.TABLES)
        self.assertIn("payout_calendar_events", schema_manifest.TABLES)
        self.assertIn("deposits", schema_manifest.VIEWS)
        self.assertIn(
            "uq_operations_account_operation",
            schema_manifest.TABLES["operations"].unique_constraints,
        )

    def test_schema_manifest_digest_includes_migration_order(self):
        from financetracker.database.schema_manifest import manifest_digest

        self.assertNotEqual(
            manifest_digest(("20260101_first.sql",)),
            manifest_digest(("20260102_second.sql", "20260101_first.sql")),
        )

    def test_schema_manifest_reports_missing_column_type_and_index(self):
        from financetracker.database import schema_manifest

        class Inspector:
            def has_table(self, _table):
                return True

            def get_columns(self, table):
                return [
                    {
                        "name": name,
                        "type": spec.type_name,
                        "nullable": spec.nullable,
                        "default": spec.default or "",
                    }
                    for name, spec in schema_manifest.TABLES[table].columns.items()
                    if not (table == "operations" and name == "cashflow_category")
                ]

            def get_pk_constraint(self, table):
                return {"constrained_columns": schema_manifest.TABLES[table].primary_key}

            def get_unique_constraints(self, table):
                return [
                    {"name": name, "column_names": columns}
                    for name, columns in (schema_manifest.TABLES[table].unique_constraints or {}).items()
                ]

            def get_indexes(self, table):
                return [
                    {"name": name, "column_names": columns}
                    for name, columns in (schema_manifest.TABLES[table].indexes or {}).items()
                    if name != "ix_operations_cashflow_category"
                ]

            def get_foreign_keys(self, table):
                return [
                    {
                        "name": name,
                        "constrained_columns": columns,
                        "referred_table": remote_table,
                        "referred_columns": remote_columns,
                    }
                    for name, (columns, remote_table, remote_columns) in (schema_manifest.TABLES[table].foreign_keys or {}).items()
                ]

        class Connection:
            def execute(self, _query, _params):
                class Result:
                    @staticmethod
                    def scalar_one_or_none():
                        return "v"

                return Result()

        with mock.patch.object(schema_manifest, "inspect", return_value=Inspector()):
            failures = schema_manifest.validate_schema_manifest(Connection())

        self.assertIn("column operations.cashflow_category is missing", failures)
        self.assertIn("index ix_operations_cashflow_category is missing or differs", failures)


if __name__ == "__main__":
    unittest.main()
