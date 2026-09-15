"""Engine and session-factory construction without environment access."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def create_session_factory(dsn: str, *, echo: bool = False):
    """Create the synchronous PostgreSQL engine and its session factory."""
    engine = create_engine(dsn, echo=echo, future=True)
    return engine, sessionmaker(bind=engine, expire_on_commit=False)
