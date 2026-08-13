import importlib.util
import os
from pathlib import Path
import sys
import unittest
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BOT_DIR = PROJECT_ROOT / "src" / "bot"
TRACKER_DIR = PROJECT_ROOT / "src" / "tracker"
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(BOT_DIR))
sys.path.insert(0, str(TRACKER_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class DatabaseCredentialEntrypointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot_entrypoint = load_module(
            "security_bot_entrypoint_under_test",
            BOT_DIR / "entrypoint.py",
        )
        cls.migrate = load_module(
            "security_tracker_migrate_under_test",
            TRACKER_DIR / "migrate.py",
        )

    def test_bot_entrypoint_rejects_missing_credentials_before_proxy_smoke_or_network(self):
        env = {"DB_DSN": "", "DB_PASSWORD": ""}
        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(self.bot_entrypoint, "configure_proxy_env") as configure_proxy,
            mock.patch.object(self.bot_entrypoint, "run_startup_smoke") as smoke,
            mock.patch.object(self.bot_entrypoint, "run_bot_process") as bot_process,
        ):
            exit_code = self.bot_entrypoint.main()

        self.assertEqual(exit_code, 1)
        configure_proxy.assert_not_called()
        smoke.assert_not_called()
        bot_process.assert_not_called()

    def test_bot_entrypoint_rejects_malformed_dsn_without_logging_it(self):
        malformed = "postgresql://user:" + "token-like-value" + "@"
        with (
            mock.patch.dict(os.environ, {"DB_DSN": malformed, "DB_PASSWORD": ""}, clear=False),
            mock.patch.object(self.bot_entrypoint, "configure_proxy_env") as configure_proxy,
            mock.patch.object(self.bot_entrypoint.logger, "error") as log_error,
        ):
            exit_code = self.bot_entrypoint.main()

        self.assertEqual(exit_code, 1)
        configure_proxy.assert_not_called()
        self.assertNotIn(malformed, repr(log_error.call_args))

    def test_proxy_smoke_itself_rejects_missing_credentials_before_tcp_or_telegram(self):
        proxy_smoke = sys.modules[self.bot_entrypoint.run_startup_smoke.__module__]
        with (
            mock.patch.dict(os.environ, {"DB_DSN": "", "DB_PASSWORD": ""}, clear=False),
            mock.patch.object(proxy_smoke, "probe_tcp") as probe_tcp,
            mock.patch.object(proxy_smoke, "probe_telegram") as probe_telegram,
        ):
            exit_code = proxy_smoke.run_startup_smoke()

        self.assertEqual(exit_code, 1)
        probe_tcp.assert_not_called()
        probe_telegram.assert_not_called()

    def test_migrate_rejects_missing_credentials_before_loading_engine_or_ddl(self):
        with (
            mock.patch.dict(os.environ, {"DB_DSN": "", "DB_PASSWORD": ""}, clear=False),
            mock.patch.object(self.migrate, "_load_database_runtime") as load_runtime,
        ):
            exit_code = self.migrate.main([])

        self.assertEqual(exit_code, 1)
        load_runtime.assert_not_called()

    def test_migrate_rejects_malformed_dsn_without_loading_or_logging_it(self):
        malformed = "postgresql://user:" + "migration-secret" + "@"
        with (
            mock.patch.dict(os.environ, {"DB_DSN": malformed, "DB_PASSWORD": ""}, clear=False),
            mock.patch.object(self.migrate, "_load_database_runtime") as load_runtime,
            mock.patch.object(self.migrate.logger, "exception") as log_exception,
        ):
            exit_code = self.migrate.main([])

        self.assertEqual(exit_code, 1)
        load_runtime.assert_not_called()
        self.assertNotIn(malformed, repr(log_exception.call_args))


if __name__ == "__main__":
    unittest.main()
