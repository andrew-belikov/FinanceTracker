#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "POSTGRES_DB": "compose_contract_db",
    "POSTGRES_USER": "compose_contract_user",
    "POSTGRES_PASSWORD": "compose_contract_password",
    "REPORTER_SERVICE_KEY": "compose_contract_reporter_key",
}


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="financetracker-compose-env-") as temp_dir:
        env_path = Path(temp_dir) / "synthetic.env"
        lines = [f"APP_ENV_FILE={env_path}"]
        lines.extend(f"{key}={value}" for key, value in EXPECTED.items())
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        env_path.chmod(0o600)

        compose_command = ["docker", "compose", "--env-file", str(env_path), "config"]
        quiet = subprocess.run(
            [*compose_command, "--quiet"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
        )
        if quiet.returncode != 0:
            print("compose env verification failed: quiet config returned nonzero")
            return 1

        completed = subprocess.run(
            [
                *compose_command,
                "--format",
                "json",
            ],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            print("compose env verification failed: config command returned nonzero")
            return 1
        try:
            rendered = json.loads(completed.stdout)
        except (json.JSONDecodeError, UnicodeDecodeError):
            print("compose env verification failed: config output was not valid JSON")
            return 1

    services = rendered.get("services") or {}
    required = {
        "db": {
            "POSTGRES_DB": EXPECTED["POSTGRES_DB"],
            "POSTGRES_USER": EXPECTED["POSTGRES_USER"],
            "POSTGRES_PASSWORD": EXPECTED["POSTGRES_PASSWORD"],
        },
        **{
            service: {
                "DB_NAME": EXPECTED["POSTGRES_DB"],
                "DB_USER": EXPECTED["POSTGRES_USER"],
                "DB_PASSWORD": EXPECTED["POSTGRES_PASSWORD"],
            }
            for service in ("migrate", "tracker", "bot", "reporter")
        },
        "bot": {
            "DB_NAME": EXPECTED["POSTGRES_DB"],
            "DB_USER": EXPECTED["POSTGRES_USER"],
            "DB_PASSWORD": EXPECTED["POSTGRES_PASSWORD"],
            "REPORTER_SERVICE_KEY": EXPECTED["REPORTER_SERVICE_KEY"],
        },
        "reporter": {
            "DB_NAME": EXPECTED["POSTGRES_DB"],
            "DB_USER": EXPECTED["POSTGRES_USER"],
            "DB_PASSWORD": EXPECTED["POSTGRES_PASSWORD"],
            "REPORTER_SERVICE_KEY": EXPECTED["REPORTER_SERVICE_KEY"],
        },
    }
    failures: list[str] = []
    for service_name, expected_environment in required.items():
        actual_environment = (services.get(service_name) or {}).get("environment") or {}
        for key, expected_value in expected_environment.items():
            if not actual_environment.get(key) or actual_environment.get(key) != expected_value:
                failures.append(f"{service_name}.{key}")
    if failures:
        print("compose env verification failed for keys: " + ", ".join(sorted(failures)))
        return 1
    print("compose env verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
