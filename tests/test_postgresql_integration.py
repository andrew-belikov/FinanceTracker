"""Opt-in PostgreSQL 16 checks for the production SQL and migration path."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import unittest
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_INTEGRATION = os.getenv("FINANCETRACKER_POSTGRES_INTEGRATION") == "1"


@unittest.skipUnless(RUN_INTEGRATION, "requires PostgreSQL 16 integration environment")
class PostgreSQLIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.environment = os.environ.copy()
        cls.environment.update(
            {
                "MIGRATIONS_DIR": str(PROJECT_ROOT / "migrations"),
                "PYTHONPATH": os.pathsep.join(
                    [
                        str(PROJECT_ROOT / "src"),
                        str(PROJECT_ROOT / "src" / "financetracker" / "tracker"),
                        str(PROJECT_ROOT / "src" / "financetracker" / "bot"),
                        cls.environment.get("PYTHONPATH", ""),
                    ]
                ),
            }
        )
        for arguments in ((), (), ("--check",)):
            subprocess.run(
                [sys.executable, "-m", "financetracker.tracker.migrate", *arguments],
                cwd=PROJECT_ROOT,
                env=cls.environment,
                check=True,
            )

    def setUp(self) -> None:
        from sqlalchemy import text

        from financetracker.tracker import app as tracker_app

        with tracker_app.SessionLocal() as session:
            session.execute(
                text(
                    """
                    TRUNCATE TABLE
                        portfolio_positions,
                        portfolio_snapshots,
                        instruments,
                        operations,
                        income_events,
                        asset_aliases,
                        rebalance_targets,
                        invest_notifications,
                        bot_daily_job_runs,
                        bot_notification_deliveries,
                        payout_calendar_events
                    RESTART IDENTITY CASCADE
                    """
                )
            )
            session.commit()

    def test_tracker_bot_and_reporter_queries_run_against_postgresql(self):
        from financetracker.bot import queries as bot_queries
        from financetracker.reporting import report_payload
        from financetracker.reporting import repository as reporting_repository
        from financetracker.bot import runtime as bot_runtime
        from financetracker.tracker import app as tracker_app

        with tracker_app.SessionLocal() as session:
            self.assertEqual(session.query(tracker_app.PortfolioSnapshot).count(), 0)

        with bot_runtime.db_session() as session:
            self.assertIsNone(bot_queries.resolve_reporting_account_id(session))
            self.assertIsNone(reporting_repository.resolve_reporting_account_id(session))

        self.assertEqual(
            report_payload.resolve_monthly_report_period(year=2026, month=1),
            (2026, 1),
        )

    def test_timezone_migration_uses_timestamptz_and_returns_aware_instants(self):
        from sqlalchemy import text

        from financetracker.tracker import app as tracker_app

        with tracker_app.SessionLocal() as session:
            legacy_columns = session.execute(
                text(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND data_type = 'timestamp without time zone'
                    """
                )
            ).all()
            self.assertEqual(legacy_columns, [])

            snapshot = tracker_app.PortfolioSnapshot(
                account_id="timezone-integration",
                snapshot_at=datetime(2026, 9, 15, 6, 0, tzinfo=timezone.utc),
                snapshot_date=datetime(2026, 9, 15, tzinfo=timezone.utc).date(),
                currency="RUB",
            )
            session.add(snapshot)
            session.commit()
            snapshot_id = snapshot.id

        with tracker_app.SessionLocal() as session:
            restored = session.get(tracker_app.PortfolioSnapshot, snapshot_id)
            self.assertIsNotNone(restored)
            self.assertIsNotNone(restored.snapshot_at.tzinfo)
            self.assertEqual(restored.snapshot_at.utcoffset(), timezone.utc.utcoffset(None))


if __name__ == "__main__":
    unittest.main()
