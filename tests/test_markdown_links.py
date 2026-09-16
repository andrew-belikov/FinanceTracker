from __future__ import annotations

import re
from pathlib import Path
import unittest
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
ROOT_ACTIVE_DOCUMENTS = [
    ROOT / name
    for name in ("README.md", "CONTRIBUTING.md", "CHANGELOG.md", "SECURITY.md")
]
EXCLUDED_HISTORICAL_DIRECTORIES = {"archive", "audits", "acceptance"}
ACTIVE_DOCUMENTS = [
    *ROOT_ACTIVE_DOCUMENTS,
    *(
        path
        for path in DOCS.rglob("*.md")
        if not (set(path.relative_to(DOCS).parts) & EXCLUDED_HISTORICAL_DIRECTORIES)
    ),
]
INLINE_LINK = re.compile(r"(?<!!)\[[^]]*]\(([^)\s]+)(?:\s+['\"][^)]*)?\)")


class MarkdownLinkTests(unittest.TestCase):
    def test_active_local_markdown_links_resolve(self):
        for document in ACTIVE_DOCUMENTS:
            text = document.read_text(encoding="utf-8")
            for raw_target in INLINE_LINK.findall(text):
                target = unquote(raw_target.split("#", 1)[0])
                if not target or target.startswith(("http://", "https://", "mailto:", "#")):
                    continue
                with self.subTest(document=document.relative_to(ROOT), target=target):
                    self.assertTrue((document.parent / target).resolve().is_file())
