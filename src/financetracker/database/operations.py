"""Shared, database-owned SQL fragments for canonical operation reads."""

from __future__ import annotations

from typing import LiteralString

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


OPERATIONS_DEDUP_CTE = """
WITH operations_dedup AS (
    SELECT DISTINCT ON (account_id, COALESCE(operation_id, id::text))
        id, account_id, operation_id, date, amount, currency, operation_type,
        cashflow_category, state, figi, name, commission, yield
    FROM operations
    ORDER BY account_id, COALESCE(operation_id, id::text), id DESC
)
"""


def operations_dedup_statement(statement: LiteralString) -> TextClause:
    """Combine the canonical CTE with static SQL that binds all runtime values."""
    return text("".join((OPERATIONS_DEDUP_CTE, statement)))
