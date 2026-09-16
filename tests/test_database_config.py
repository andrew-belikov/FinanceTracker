import os
import ast
import unittest
from unittest import mock
from pathlib import Path

from sqlalchemy.engine import make_url

from financetracker.config.database import read_database_settings


class DatabaseConfigTests(unittest.TestCase):
    def test_database_config_does_not_create_sqlalchemy_side_effects(self):
        source = (Path(__file__).resolve().parents[1] / "src" / "financetracker" / "config" / "database.py").read_text(encoding="utf-8")
        imports = {alias.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Import) for alias in node.names}
        self.assertFalse(any(name.startswith("sqlalchemy.orm") for name in imports))
        self.assertNotIn("create_engine", source)

    def test_explicit_dsn_has_priority(self):
        dsn = "postgresql+psycopg2://" + "u" + ":" + "p" + "@host:5432/db"
        with mock.patch.dict(os.environ, {"DB_DSN": dsn}, clear=True):
            self.assertEqual(read_database_settings().dsn, dsn)

    def test_component_settings_build_dsn(self):
        with mock.patch.dict(os.environ, {"DB_HOST": "postgres", "DB_PORT": "5433", "DB_NAME": "ledger", "DB_USER": "reader", "DB_PASSWORD": "secret"}, clear=True):
            expected_dsn = "postgresql+psycopg2://" + "reader" + ":" + "secret" + "@postgres:5433/ledger"
            self.assertEqual(read_database_settings().dsn, expected_dsn)

    def test_component_settings_escape_reserved_uri_characters(self):
        password = "p@ss:/#%word"
        user = "read@er"
        with mock.patch.dict(
            os.environ,
            {
                "DB_HOST": "postgres",
                "DB_PORT": "5433",
                "DB_NAME": "ledger",
                "DB_USER": user,
                "DB_PASSWORD": password,
            },
            clear=True,
        ):
            parsed = make_url(read_database_settings().dsn)

        self.assertEqual(parsed.host, "postgres")
        self.assertEqual(parsed.port, 5433)
        self.assertEqual(parsed.database, "ledger")
        self.assertEqual(parsed.username, user)
        self.assertEqual(parsed.password, password)
