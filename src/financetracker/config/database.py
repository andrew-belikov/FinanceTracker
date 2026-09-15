"""Database connection settings without SQLAlchemy side effects."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    name: str
    user: str
    password: str
    explicit_dsn: str

    @property
    def dsn(self) -> str:
        return self.explicit_dsn or (
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
        )


def read_database_settings() -> DatabaseSettings:
    return DatabaseSettings(
        host=os.getenv("DB_HOST", "db"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "fintracker"),
        user=os.getenv("DB_USER", "aqua4"),
        password=os.getenv("DB_PASSWORD", "").strip(),
        explicit_dsn=os.getenv("DB_DSN", "").strip(),
    )
