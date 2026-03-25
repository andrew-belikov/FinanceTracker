import os

from sqlalchemy import create_engine, text

from common.logging_setup import configure_logging, get_logger
from common.text_utils import has_mojibake, try_repair_cp866_utf8


os.environ.setdefault("APP_SERVICE", "tracker_maintenance")
configure_logging()
logger = get_logger(__name__)


def build_dsn() -> str:
    return (
        os.getenv("DB_DSN")
        or (
            "postgresql+psycopg2://"
            f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
    )


def main() -> int:
    engine = create_engine(build_dsn(), future=True)

    updated = 0
    skipped = 0

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, description
                FROM operations
                WHERE description IS NOT NULL
                """
            )
        ).mappings().all()

        for row in rows:
            description = row["description"]
            if not has_mojibake(description):
                continue

            repaired = try_repair_cp866_utf8(description)
            if repaired == description or has_mojibake(repaired):
                skipped += 1
                continue

            conn.execute(
                text(
                    """
                    UPDATE operations
                    SET description = :description
                    WHERE id = :id
                    """
                ),
                {"id": row["id"], "description": repaired},
            )
            updated += 1

    logger.info(
        "repair_operations_description_encoding_completed",
        "Operations description encoding repair completed.",
        {"updated_rows": updated, "skipped_rows": skipped},
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception(
            "repair_operations_description_encoding_failed",
            "Operations description encoding repair failed.",
        )
        raise SystemExit(1)
