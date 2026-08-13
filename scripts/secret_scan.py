#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import sys


PATTERNS = (
    re.compile(rb"vless://[0-9a-fA-F-]{36}@"),
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(rb"(?i)(?:api[_-]?token|bot[_-]?token|password)\s*[:=]\s*['\"][^'\"\s]{16,}"),
)
SKIP_PARTS = {".git", ".venv", "__pycache__"}


def contains_secret(data: bytes) -> bool:
    return any(pattern.search(data) for pattern in PATTERNS)


def scan_worktree(root: Path) -> list[str]:
    matches: list[str] = []
    for path in root.rglob("*"):
        if not path.is_file() or SKIP_PARTS.intersection(path.parts):
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        if b"\0" not in data and contains_secret(data):
            matches.append(str(path.relative_to(root)))
    return matches


def scan_history(root: Path) -> int:
    completed = subprocess.run(
        ["git", "log", "-p", "--all", "--no-ext-diff", "--no-textconv", "--format=commit:%H"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    return int(contains_secret(completed.stdout))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-closed repository secret-pattern scan.")
    parser.add_argument("--history", action="store_true", help="scan reachable Git history")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    worktree_matches = scan_worktree(root)
    history_match = scan_history(root) if args.history else 0
    total = len(worktree_matches) + history_match
    if total:
        print(
            f"secret scan failed: worktree_files={len(worktree_matches)} history_matches={history_match}",
            file=sys.stderr,
        )
        return 1
    print("secret scan passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
