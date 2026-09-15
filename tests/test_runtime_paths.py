from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from financetracker.common.runtime_paths import configured_runtime_file, default_runtime_file


class RuntimePathTests(unittest.TestCase):
    def test_default_path_uses_platform_tempdir(self):
        self.assertEqual(
            default_runtime_file("service.ready"),
            os.path.join(tempfile.gettempdir(), "service.ready"),
        )

    def test_explicit_path_overrides_default(self):
        with patch.dict(os.environ, {"TEST_RUNTIME_PATH": "/state/service.ready"}):
            self.assertEqual(
                configured_runtime_file("TEST_RUNTIME_PATH", "service.ready"),
                "/state/service.ready",
            )

    def test_blank_override_falls_back_to_default(self):
        with patch.dict(os.environ, {"TEST_RUNTIME_PATH": "  "}):
            self.assertEqual(
                configured_runtime_file("TEST_RUNTIME_PATH", "service.ready"),
                default_runtime_file("service.ready"),
            )


if __name__ == "__main__":
    unittest.main()
