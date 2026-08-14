import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = PROJECT_ROOT / "docker" / "Dockerfile.xray-client"


class XrayChecksumContractTests(unittest.TestCase):
    def test_supported_archives_have_pinned_sha256_checked_before_unzip(self):
        source = DOCKERFILE.read_text(encoding="utf-8")
        self.assertRegex(source, r"ARG XRAY_SHA256_AMD64=[0-9a-f]{64}")
        self.assertRegex(source, r"ARG XRAY_SHA256_ARM64=[0-9a-f]{64}")
        self.assertIn('echo "${archive_sha256}  /tmp/xray.zip" | sha256sum -c -', source)
        self.assertLess(source.index("sha256sum -c -"), source.index("unzip /tmp/xray.zip"))
        self.assertNotRegex(source, r"ARG XRAY_SHA256_(?:AMD64|ARM64)=(?:0{64}|CHANGE_ME)")


if __name__ == "__main__":
    unittest.main()
