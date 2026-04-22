import ast
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_FILE = PROJECT_ROOT / "src" / "bot" / "jobs.py"


def load_selected_symbols(file_path: Path, wanted_functions: set[str], namespace=None):
    module_ast = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    selected_nodes = []
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            copied = deepcopy(node)
            copied.returns = None
            for arg in copied.args.args:
                arg.annotation = None
            for arg in copied.args.kwonlyargs:
                arg.annotation = None
            selected_nodes.append(copied)

    loaded_namespace = {} if namespace is None else dict(namespace)
    isolated_module = ast.Module(body=selected_nodes, type_ignores=[])
    code = compile(isolated_module, filename=str(file_path), mode="exec")
    exec(code, loaded_namespace)
    return loaded_namespace


SYMBOLS = load_selected_symbols(
    JOBS_FILE,
    {"is_daily_job_catchup_due", "is_yesterday_peak_alert_catchup_due", "should_release_daily_job_run"},
    namespace={
        "datetime": datetime,
        "TZ": ZoneInfo("Europe/Moscow"),
        "DAILY_JOB_HOUR": 18,
        "DAILY_JOB_MINUTE": 0,
        "YESTERDAY_PEAK_ALERT_HOUR": 8,
        "YESTERDAY_PEAK_ALERT_MINUTE": 0,
    },
)

is_daily_job_catchup_due = SYMBOLS["is_daily_job_catchup_due"]
is_yesterday_peak_alert_catchup_due = SYMBOLS["is_yesterday_peak_alert_catchup_due"]
should_release_daily_job_run = SYMBOLS["should_release_daily_job_run"]


class DailyJobCatchupTests(unittest.TestCase):
    def test_catchup_is_not_due_before_configured_time(self):
        now_local = datetime(2026, 4, 4, 17, 59, 59, tzinfo=ZoneInfo("Europe/Moscow"))

        self.assertFalse(is_daily_job_catchup_due(now_local))

    def test_catchup_is_due_at_configured_time(self):
        now_local = datetime(2026, 4, 4, 18, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

        self.assertTrue(is_daily_job_catchup_due(now_local))

    def test_catchup_is_due_after_configured_time(self):
        now_local = datetime(2026, 4, 4, 21, 37, 0, tzinfo=ZoneInfo("Europe/Moscow"))

        self.assertTrue(is_daily_job_catchup_due(now_local))

    def test_yesterday_peak_alert_catchup_uses_morning_schedule(self):
        before_alert = datetime(2026, 4, 4, 7, 59, 59, tzinfo=ZoneInfo("Europe/Moscow"))
        at_alert = datetime(2026, 4, 4, 8, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

        self.assertFalse(is_yesterday_peak_alert_catchup_due(before_alert))
        self.assertTrue(is_yesterday_peak_alert_catchup_due(at_alert))

    def test_release_claim_only_when_every_send_failed(self):
        self.assertTrue(should_release_daily_job_run(sent_total=0, failed_total=2))
        self.assertFalse(should_release_daily_job_run(sent_total=1, failed_total=1))
        self.assertFalse(should_release_daily_job_run(sent_total=0, failed_total=0))


if __name__ == "__main__":
    unittest.main()
