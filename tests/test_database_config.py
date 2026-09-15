import os
import ast
import unittest
from unittest import mock
from pathlib import Path

from financetracker.config.database import read_database_settings


class DatabaseConfigTests(unittest.TestCase):
    def test_database_config_does_not_import_sqlalchemy(self):
        source = (Path(__file__).resolve().parents[1] / "src" / "financetracker" / "config" / "database.py").read_text(encoding="utf-8")
        imports = {alias.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Import) for alias in node.names}
        self.assertFalse(any(name.startswith("sqlalchemy") for name in imports))

    def test_explicit_dsn_has_priority(self):
        dsn = "postgresql+psycopg2://" + "u" + ":" + "p" + "@host:5432/db"
        with mock.patch.dict(os.environ, {"DB_DSN": dsn}, clear=True):
            self.assertEqual(read_database_settings().dsn, dsn)

    def test_component_settings_build_dsn(self):
        with mock.patch.dict(os.environ, {"DB_HOST": "postgres", "DB_PORT": "5433", "DB_NAME": "ledger", "DB_USER": "reader", "DB_PASSWORD": "secret"}, clear=True):
            expected_dsn = "postgresql+psycopg2://" + "reader" + ":" + "secret" + "@postgres:5433/ledger"
            self.assertEqual(read_database_settings().dsn, expected_dsn)
