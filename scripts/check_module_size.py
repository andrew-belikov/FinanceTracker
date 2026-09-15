#!/usr/bin/env python3
"""Prevent incidental growth of legacy multi-purpose modules."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
# Intentional baseline: lowering a value is encouraged after a focused extraction.
MAX_LINES = {
    "src/financetracker/tracker/app.py": 1767,
    "src/financetracker/bot/services.py": 1813,
    "src/financetracker/bot/jobs.py": 1323,
    "src/financetracker/bot/queries.py": 2009,
    "src/financetracker/reporting/report_render.py": 1785,
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
