#!/usr/bin/env python3
"""High-confidence repository secret scan with value-safe findings."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlsplit


class Finding(NamedTuple):
    rule: str
    source: str
    line: int | None = None


VLESS_RE = re.compile(
    r"vless://[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"@[^\s\"'<>]+",
    re.I,
)
PRIVATE_KEY_RE = re.compile(r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----")
TELEGRAM_TOKEN_RE = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b")
PASSWORD_DSN_RE = re.compile(
    r"postgres(?:ql)?(?:\+[a-z0-9_]+)?://[^\s:/]+:(?P<password>[^\s@/]+)@",
    re.I,
)
DB_FALLBACK_RE = re.compile(
    r"os\.getenv\(\s*[\"']DB_PASSWORD[\"']\s*,\s*[\"']([^\"']+)[\"']"
)
PLACEHOLDERS = {"", "change_me", "changeme", "example", "placeholder", "test"}
DOCUMENTATION_NETWORKS = tuple(
    ipaddress.ip_network(network)
    for network in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")
)


def _is_synthetic_vless(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        host = ipaddress.ip_address(parsed.hostname or "")
    except ValueError:
        return False
    return any(host in network for network in DOCUMENTATION_NETWORKS)


def _line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def scan_text(text: str, *, source: str) -> list[Finding]:
    findings: list[Finding] = []
    for match in VLESS_RE.finditer(text):
        if not _is_synthetic_vless(match.group(0)):
            findings.append(Finding("vless_credential_uri", source, _line_number(text, match.start())))
    for rule, pattern in (
        ("private_key", PRIVATE_KEY_RE),
        ("telegram_bot_token", TELEGRAM_TOKEN_RE),
    ):
        findings.extend(
            Finding(rule, source, _line_number(text, match.start()))
            for match in pattern.finditer(text)
        )
    for match in PASSWORD_DSN_RE.finditer(text):
        password = match.group("password")
        if "{" not in password and password.strip().lower() not in PLACEHOLDERS:
            findings.append(
                Finding("password_in_database_dsn", source, _line_number(text, match.start()))
            )
    for match in DB_FALLBACK_RE.finditer(text):
        if match.group(1).strip().lower() not in PLACEHOLDERS:
            findings.append(
                Finding("database_password_fallback", source, _line_number(text, match.start()))
            )
    return findings


def _git(repo: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def scan_current_tree(repo: Path) -> list[Finding]:
    paths = _git(repo, "ls-files", "-z").decode("utf-8", errors="surrogateescape").split("\0")
    findings: list[Finding] = []
    for relative in paths:
        if not relative:
            continue
        data = (repo / relative).read_bytes()
        if b"\x00" in data:
            continue
        findings.extend(scan_text(data.decode("utf-8", errors="replace"), source=relative))
    return findings


def scan_git_history(repo: Path) -> list[Finding]:
    patch = _git(repo, "log", "--all", "--format=commit:%H", "-p", "--no-ext-diff", "--no-textconv")
    return scan_text(patch.decode("utf-8", errors="replace"), source="git_history")


def format_finding(finding: Finding) -> str:
    source_digest = hashlib.sha256(
        finding.source.encode("utf-8", errors="surrogatepass")
    ).hexdigest()[:16]
    line = f" line={finding.line}" if finding.line is not None else ""
    return (
        f"SECRET_SCAN finding rule={finding.rule} "
        f"source_sha256={source_digest}{line} value=[REDACTED]"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args(argv)
    findings = scan_git_history(args.repo) if args.history else scan_current_tree(args.repo)
    for finding in findings:
        print(format_finding(finding))
    scope = "history" if args.history else "tracked-tree"
    print(f"SECRET_SCAN scope={scope} findings={len(findings)}")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
