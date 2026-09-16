"""CI-only tracker startup mode used by the hermetic Compose smoke test."""

from __future__ import annotations

import time
from collections.abc import Callable


class ContainerSmokeConfigurationError(ValueError):
    """Raised when the CI-only startup mode is enabled outside CI."""


def run_if_requested(
    *,
    mode: str,
    app_env: str,
    write_ready_state: Callable[[], None],
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Run the no-egress heartbeat when explicitly enabled for CI.

    Returns ``False`` when normal tracker startup must continue.  The infinite
    loop is deliberate: Compose verifies the same readiness contract as the
    production service without invoking the external ingestion API.
    """
    if mode.strip() != "container-smoke":
        return False
    if app_env.strip().lower() != "ci":
        raise ContainerSmokeConfigurationError(
            "TRACKER_STARTUP_MODE=container-smoke requires APP_ENV=ci"
        )
    while True:
        write_ready_state()
        sleep(10)
