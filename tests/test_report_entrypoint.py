import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ReporterEntrypointTests(unittest.TestCase):
    def test_entrypoint_import_keeps_healthcheck_path_light(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join(
            [
                str(PROJECT_ROOT / "src"),
                str(PROJECT_ROOT / "src" / "bot"),
                env.get("PYTHONPATH", ""),
            ]
        )
        code = textwrap.dedent(
            """
            import sys
            import report_entrypoint

            assert "report_server" not in sys.modules
            assert "report_render" not in sys.modules
            """
        )

        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
