#!/usr/bin/env python3
"""Prevent incidental growth of legacy multi-purpose modules."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
# Intentional baseline: lowering a value is encouraged after a focused extraction.
MAX_LINES = {
    "src/financetracker/tracker/app.py": 250,
    "src/financetracker/bot/services.py": 142,
    "src/financetracker/bot/jobs.py": 178,
    "src/financetracker/bot/queries.py": 1262,
    "src/financetracker/bot/scheduled_jobs.py": 994,
    "src/financetracker/bot/summary_service.py": 1016,
    "src/financetracker/reporting/report_render.py": 100,
    "src/financetracker/reporting/report_payload.py": 508,
    "src/financetracker/reporting/report_html.py": 1037,
}


def line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def main() -> int:
    violations = []
    for relative_path, maximum in MAX_LINES.items():
        actual = line_count(ROOT / relative_path)
        print(f"{relative_path}: {actual}/{maximum} lines")
        if actual > maximum:
            violations.append(f"{relative_path} grew from its approved baseline {maximum} to {actual}")
    if violations:
        print("\n".join(violations), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
