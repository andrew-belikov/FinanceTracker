from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from sqlalchemy import text

from app import Base, engine
from common.logging_setup import configure_logging, get_logger


MIGRATIONS_DIR = Path(os.getenv("MIGRATIONS_DIR", "/app/migrations"))
MIGRATION_LOCK_ID = 1_731_904_221
SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    checksum_sha256 TEXT NOT NULL,
    applied_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
)
"""

configure_logging()
logger = get_logger("tracker_migrations")


class MigrationError(RuntimeError):
    pass


def discover_migrations(migrations_dir: Path) -> list[Path]:
    if not migrations_dir.is_dir():
        raise MigrationError(f"Migrations directory does not exist: {migrations_dir}")
    return sorted(
        path
        for path in migrations_dir.glob("*.sql")
        if not path.name.endswith(".rollback.sql")
    )


def migration_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def migration_sql(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    meaningful_indexes = [
        index
        for index, line in enumerate(lines)
        if line.strip() and not line.lstrip().startswith("--")
    ]
    if meaningful_indexes and lines[meaningful_indexes[0]].strip().upper() == "BEGIN;":
        lines[meaningful_indexes[0]] = ""
    if meaningful_indexes and lines[meaningful_indexes[-1]].strip().upper() == "COMMIT;":
        lines[meaningful_indexes[-1]] = ""
    return "\n".join(lines).strip()


def _load_applied_migrations(connection) -> dict[str, str]:
    rows = connection.execute(
        text("SELECT filename, checksum_sha256 FROM schema_migrations")
    )
    return {str(row.filename): str(row.checksum_sha256) for row in rows}


def _validate_applied_checksums(
    migrations: list[Path],
    applied: dict[str, str],
) -> None:
    available_names = {path.name for path in migrations}
    missing_files = sorted(set(applied) - available_names)
    if missing_files:
        raise MigrationError(
            "Applied migration files are missing from the repository: "
            + ", ".join(missing_files)
        )

    for path in migrations:
        recorded_checksum = applied.get(path.name)
        if recorded_checksum is None:
            continue
        current_checksum = migration_checksum(path)
        if recorded_checksum != current_checksum:
            raise MigrationError(
                f"Checksum mismatch for already applied migration: {path.name}"
            )


def run_migrations(*, check_only: bool = False) -> int:
    migrations = discover_migrations(MIGRATIONS_DIR)

    applied_count = 0
    with engine.connect() as connection:
        connection.execute(text(SCHEMA_MIGRATIONS_DDL))
        connection.commit()
        connection.execute(
            text("SELECT pg_advisory_lock(:lock_id)"),
            {"lock_id": MIGRATION_LOCK_ID},
        )
        try:
            # Fresh installations get the current ORM baseline. Existing
            # installations are changed only by versioned SQL migrations.
            Base.metadata.create_all(bind=connection)
            applied = _load_applied_migrations(connection)
            _validate_applied_checksums(migrations, applied)
            pending = [path for path in migrations if path.name not in applied]

            if check_only:
                if pending:
                    raise MigrationError(
                        "Pending migrations: " + ", ".join(path.name for path in pending)
                    )
                logger.info(
                    "database_migrations_verified",
                    "Database migrations are up to date.",
                    {"applied_total": len(applied)},
                )
                return 0

            for path in pending:
                checksum = migration_checksum(path)
                sql = migration_sql(path)
                logger.info(
                    "database_migration_started",
                    "Applying database migration.",
                    {"filename": path.name},
                )
                try:
                    if sql:
                        connection.exec_driver_sql(sql)
                    connection.execute(
                        text(
                            """
                            INSERT INTO schema_migrations (filename, checksum_sha256)
                            VALUES (:filename, :checksum)
                            """
                        ),
                        {"filename": path.name, "checksum": checksum},
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    logger.exception(
                        "database_migration_failed",
                        "Database migration failed.",
                        {"filename": path.name},
                    )
                    raise
                applied_count += 1
                logger.info(
                    "database_migration_completed",
                    "Database migration completed.",
                    {"filename": path.name},
                )

            logger.info(
                "database_migrations_completed",
                "Database migrations are up to date.",
                {
                    "applied_now": applied_count,
                    "applied_total": len(applied) + applied_count,
                },
            )
            return 0
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": MIGRATION_LOCK_ID},
            )
            connection.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply FinanceTracker SQL migrations.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail when a migration is pending or its checksum changed.",
    )
    args = parser.parse_args()
    try:
        return run_migrations(check_only=args.check)
    except Exception:
        logger.exception(
            "database_migrations_aborted",
            "Database migrations were not completed.",
            {"check_only": args.check},
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
