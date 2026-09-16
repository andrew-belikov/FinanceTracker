"""Database connection settings without SQLAlchemy side effects."""

import os
from dataclasses import dataclass

from sqlalchemy.engine import URL


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
        if self.explicit_dsn:
            return self.explicit_dsn
        return URL.create(
            "postgresql+psycopg2",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name,
        ).render_as_string(hide_password=False)


def read_database_settings() -> DatabaseSettings:
    return DatabaseSettings(
        host=os.getenv("DB_HOST", "db"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "fintracker"),
        user=os.getenv("DB_USER", "aqua4"),
        password=os.getenv("DB_PASSWORD", "").strip(),
        explicit_dsn=os.getenv("DB_DSN", "").strip(),
    )
