from __future__ import annotations

import json
import os
import socket
import sys


STATUS_FILE = "/tmp/xray-client-status.json"


def load_status() -> dict:
    with open(STATUS_FILE, "r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> int:
    if not os.path.exists(STATUS_FILE):
        return 1

    try:
        status = load_status()
    except Exception:
        return 1

    mode = status.get("mode")
    if mode == "disabled":
        return 0

    if mode != "enabled":
        return 1

    port = int(status.get("port", 0))
    if port <= 0:
        return 1

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3.0):
            return 0
    except OSError:
        return 1


if __name__ == "__main__":
    sys.exit(main())
