import ast
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(0, str(SRC_ROOT / "bot"))
sys.path.insert(0, str(SRC_ROOT / "tracker"))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_function(path: Path, name: str, namespace: dict):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name
    )
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(module, str(path), "exec"), namespace)
    return namespace[name]


def write_proc_entry(root: Path, pid: int, command: str, *, start_ticks: str = "123") -> None:
    process_dir = root / str(pid)
    process_dir.mkdir(parents=True, exist_ok=True)
    (process_dir / "cmdline").write_bytes(command.replace(" ", "\0").encode() + b"\0")
    stat_tail = ["S", *(["0"] * 18), start_ticks]
    (process_dir / "stat").write_text(
        f"{pid} (python worker) " + " ".join(stat_tail),
        encoding="utf-8",
    )


class ReadyStateContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module_path = SRC_ROOT / "common" / "readiness_state.py"
        if not cls.module_path.exists():
            raise AssertionError("shared readiness state module is missing")
        cls.readiness = load_module("readiness_state_under_test", cls.module_path)

    def test_atomic_ready_state_is_private_and_owner_bound(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proc_root = root / "proc"
            write_proc_entry(proc_root, 1, "python -u app.py")
            ready_path = root / "tracker.ready"

            self.readiness.write_ready_state(
                ready_path,
                pid=1,
                now=100.0,
                proc_root=proc_root,
            )

            self.assertEqual(stat.S_IMODE(ready_path.stat().st_mode), 0o600)
            self.assertTrue(
                self.readiness.validate_ready_state(
                    ready_path,
                    expected_pid=1,
                    max_age_seconds=30,
                    now=110.0,
                    proc_root=proc_root,
                )
            )

    def test_stale_wrong_owner_and_insecure_state_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proc_root = root / "proc"
            write_proc_entry(proc_root, 1, "python -u app.py", start_ticks="123")
            ready_path = root / "tracker.ready"
            payload = {"pid": 1, "process_start_ticks": "123", "updated_at": 100.0}
            ready_path.write_text(json.dumps(payload), encoding="utf-8")
            ready_path.chmod(0o600)

            self.assertFalse(
                self.readiness.validate_ready_state(
                    ready_path,
                    expected_pid=1,
                    max_age_seconds=30,
                    now=131.0,
                    proc_root=proc_root,
                )
            )
            payload["pid"] = 2
            ready_path.write_text(json.dumps(payload), encoding="utf-8")
            ready_path.chmod(0o600)
            self.assertFalse(
                self.readiness.validate_ready_state(
                    ready_path,
                    expected_pid=1,
                    max_age_seconds=30,
                    now=110.0,
                    proc_root=proc_root,
                )
            )
            payload["pid"] = 1
            ready_path.write_text(json.dumps(payload), encoding="utf-8")
            ready_path.chmod(0o644)
            self.assertFalse(
                self.readiness.validate_ready_state(
                    ready_path,
                    expected_pid=1,
                    max_age_seconds=30,
                    now=110.0,
                    proc_root=proc_root,
                )
            )

    def test_healthchecks_require_ready_state_owned_by_live_service(self):
        tracker_health = load_module(
            "tracker_healthcheck_under_test",
            SRC_ROOT / "tracker" / "tracker_healthcheck.py",
        )
        bot_health = load_module(
            "bot_healthcheck_under_test",
            SRC_ROOT / "bot" / "bot_healthcheck.py",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proc_root = root / "proc"
            write_proc_entry(proc_root, 1, "python -u app.py")
            tracker_ready = root / "tracker.ready"
            self.readiness.write_ready_state(
                tracker_ready,
                pid=1,
                now=100.0,
                proc_root=proc_root,
            )
            self.assertEqual(
                tracker_health.main(
                    state_path=tracker_ready,
                    proc_root=proc_root,
                    now=110.0,
                    max_age_seconds=30,
                ),
                0,
            )

            write_proc_entry(proc_root, 1, "python -u entrypoint.py")
            write_proc_entry(proc_root, 42, "python -u bot.py", start_ticks="456")
            bot_ready = root / "bot.ready"
            self.readiness.write_ready_state(
                bot_ready,
                pid=1,
                now=100.0,
                proc_root=proc_root,
            )
            self.assertEqual(
                bot_health.main(
                    state_path=bot_ready,
                    proc_root=proc_root,
                    now=110.0,
                    max_age_seconds=30,
                ),
                0,
            )
            self.assertEqual(
                bot_health.main(
                    state_path=bot_ready,
                    proc_root=proc_root,
                    now=131.0,
                    max_age_seconds=30,
                ),
                1,
            )


class StartupReadinessContractTests(unittest.TestCase):
    def test_tracker_job_marks_ready_only_after_complete_sync(self):
        class Logger:
            def info(self, *_args, **_kwargs):
                pass

            def error(self, *_args, **_kwargs):
                pass

            def exception(self, *_args, **_kwargs):
                pass

        writes: list[str] = []
        namespace = {
            "logger": Logger(),
            "run_snapshot_and_operations_once": lambda: False,
            "write_tracker_ready_state": lambda: writes.append("ready"),
        }
        job_with_retry = load_function(
            SRC_ROOT / "tracker" / "app.py",
            "job_with_retry",
            namespace,
        )
        self.assertFalse(job_with_retry())
        self.assertEqual(writes, [])

        namespace["run_snapshot_and_operations_once"] = lambda: True
        self.assertTrue(job_with_retry())
        self.assertEqual(writes, ["ready"])

    def test_tracker_initial_sync_failure_exits_before_scheduler(self):
        class Logger:
            def error(self, *_args, **_kwargs):
                pass

        calls: list[str] = []

        class RuntimeConfigurationError(ValueError):
            pass

        namespace = {
            "API_TOKEN": "synthetic-token",
            "EXPLICIT_DB_DSN": "postgresql://synthetic.invalid/finance",
            "DB_PASSWORD": "",
            "RuntimeConfigurationError": RuntimeConfigurationError,
            "validate_database_credentials": lambda **_kwargs: calls.append("validate"),
            "logger": Logger(),
            "clear_tracker_ready_state": lambda: calls.append("clear"),
            "init_db": lambda: calls.append("init"),
            "job_with_retry": lambda: False,
        }
        main = load_function(SRC_ROOT / "tracker" / "app.py", "main", namespace)
        self.assertEqual(main(), 1)
        self.assertEqual(calls, ["clear", "validate", "init"])

    def test_tracker_initial_sync_failure_cannot_mark_ready(self):
        text = (SRC_ROOT / "tracker" / "app.py").read_text(encoding="utf-8")
        self.assertIn("clear_tracker_ready_state()", text)
        self.assertRegex(text, r"if not job_with_retry\(\):\s+return 1")
        self.assertIn("write_tracker_ready_state()", text)

    def test_bot_marks_ready_only_after_startup_smoke(self):
        text = (SRC_ROOT / "bot" / "entrypoint.py").read_text(encoding="utf-8")
        smoke_index = text.index("smoke_exit_code = run_startup_smoke()")
        ready_index = text.index("write_bot_ready_state()", smoke_index)
        process_index = text.index("run_bot_process()", ready_index)
        self.assertLess(smoke_index, ready_index)
        self.assertLess(ready_index, process_index)
        self.assertIn("clear_bot_ready_state()", text)


if __name__ == "__main__":
    unittest.main()
