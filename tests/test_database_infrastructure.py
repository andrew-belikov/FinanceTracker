import ast
import unittest
from pathlib import Path

from financetracker.database.base import Base
from sqlalchemy import BigInteger

from financetracker.database.models import AssetAlias, Operation
from financetracker.database.session import create_session_factory
from financetracker.tracker import app as tracker_app


class DatabaseInfrastructureTests(unittest.TestCase):
    def test_database_models_use_shared_metadata(self):
        self.assertIs(tracker_app.Base, Base)
        self.assertIs(tracker_app.Operation, Operation)
        self.assertIn("operations", Base.metadata.tables)

    def test_bigint_primary_keys_match_database_schema_contract(self):
        for model in (Operation, AssetAlias):
            with self.subTest(model=model.__name__):
                self.assertIsInstance(model.__table__.c.id.type, BigInteger)

    def test_tracker_orchestrator_does_not_declare_database_models(self):
        tree = ast.parse(Path(tracker_app.__file__).read_text(encoding="utf-8"))
        declared_classes = {
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        }

        self.assertTrue(
            {
                "Instrument",
                "PortfolioSnapshot",
                "PortfolioPosition",
                "Operation",
                "IncomeEvent",
                "PayoutCalendarEvent",
                "AssetAlias",
                "RebalanceTarget",
                "InvestNotification",
            }.isdisjoint(declared_classes)
        )

    def test_session_factory_does_not_read_environment(self):
        engine, session_local = create_session_factory("sqlite:///:memory:")
        try:
            with session_local() as session:
                self.assertIsNotNone(session.connection())
        finally:
            engine.dispose()
