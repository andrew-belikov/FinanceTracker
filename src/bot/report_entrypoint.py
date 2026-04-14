from __future__ import annotations

import argparse
import json
import os
from urllib.request import urlopen

from common.logging_setup import configure_logging

from report_server import main as run_reporter_server


configure_logging()


REPORTER_PORT = int(os.getenv("REPORTER_PORT", "8088"))


def _run_healthcheck() -> int:
    with urlopen(f"http://127.0.0.1:{REPORTER_PORT}/healthz", timeout=2) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if response.status != 200 or payload.get("status") != "ok":
        raise RuntimeError("Reporter healthcheck failed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="FinanceTracker reporter entrypoint")
    parser.add_argument("--healthcheck", action="store_true", help="Run local reporter healthcheck and exit")
    args = parser.parse_args()

    if args.healthcheck:
        return _run_healthcheck()
    return run_reporter_server()


if __name__ == "__main__":
    raise SystemExit(main())
