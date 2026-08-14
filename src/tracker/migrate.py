from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from common.logging_setup import configure_logging, get_logger


MIGRATIONS_DIR = Path(os.getenv("MIGRATIONS_DIR", "/app/migrations"))
MIGRATION_LOCK_ID = 1_731_904_221
MIGRATION_TIMEZONE = (
    os.getenv("TIMEZONE") or os.getenv("SCHED_TZ") or "Europe/Moscow"
).strip()
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


class RuntimeConfigurationError(ValueError):
    """Raised when migration database credentials are absent or malformed."""


def validate_database_credentials() -> None:
    db_dsn = os.getenv("DB_DSN", "").strip()
    db_password = os.getenv("DB_PASSWORD", "").strip()
    if not db_dsn:
        if not db_password:
            raise RuntimeConfigurationError(
                "Database credentials must be explicitly configured"
            )
        return
    try:
        parsed = make_url(db_dsn)
    except (ArgumentError, ValueError):
        raise RuntimeConfigurationError("Database DSN is malformed") from None
    if parsed.get_backend_name() in {"postgres", "postgresql"} and (
        not parsed.host
        or not parsed.username
        or not parsed.database
        or parsed.password is None
        or not str(parsed.password).strip()
    ):
        raise RuntimeConfigurationError("Database DSN is malformed")


def validate_migration_timezone() -> None:
    if not MIGRATION_TIMEZONE:
        raise RuntimeConfigurationError("Migration timezone must be explicitly configured")
    try:
        ZoneInfo(MIGRATION_TIMEZONE)
    except (ZoneInfoNotFoundError, ValueError):
        raise RuntimeConfigurationError("Migration timezone is invalid") from None


def _load_database_runtime():
    from app import Base, engine

    return Base, engine


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


def _run_check_only(connection, Base, migrations: list[Path]) -> int:
    ledger_exists = connection.execute(
        text("SELECT to_regclass('public.schema_migrations')")
    ).scalar_one_or_none()
    if ledger_exists is None:
        raise MigrationError("Migration ledger is missing")

    missing_tables = sorted(
        table.name
        for table in Base.metadata.sorted_tables
        if not inspect(connection).has_table(table.name)
    )
    if missing_tables:
        raise MigrationError("Required tables are missing: " + ", ".join(missing_tables))

    applied = _load_applied_migrations(connection)
    _validate_applied_checksums(migrations, applied)
    pending = [path for path in migrations if path.name not in applied]
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


def run_migrations(*, check_only: bool = False) -> int:
    validate_database_credentials()
    validate_migration_timezone()
    Base, engine = _load_database_runtime()
    migrations = discover_migrations(MIGRATIONS_DIR)

    applied_count = 0
    with engine.connect() as connection:
        if check_only:
            return _run_check_only(connection, Base, migrations)

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

            for path in pending:
                checksum = migration_checksum(path)
                sql = migration_sql(path)
                logger.info(
                    "database_migration_started",
                    "Applying database migration.",
                    {"filename": path.name},
                )
                try:
                    connection.execute(
                        text("SELECT set_config('TimeZone', :timezone, true)"),
                        {"timezone": MIGRATION_TIMEZONE},
                    )
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply FinanceTracker SQL migrations.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail when a migration is pending or its checksum changed.",
    )
    args = parser.parse_args(argv)
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
