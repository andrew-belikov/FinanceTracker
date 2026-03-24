import os

from sqlalchemy import create_engine, text

from common.text_utils import has_mojibake, try_repair_cp866_utf8


def build_dsn() -> str:
    return (
        os.getenv("DB_DSN")
        or (
            "postgresql+psycopg2://"
            f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
    )


def main():
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

    print(f"updated rows: {updated}")
    print(f"skipped rows: {skipped}")


if __name__ == "__main__":
    main()
