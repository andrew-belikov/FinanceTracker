"""Runtime-file paths backed by the container's writable temporary directory."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def default_runtime_file(filename: str) -> str:
    """Build a predictable per-service filename under the platform temp directory."""
    return str(Path(tempfile.gettempdir()) / filename)


def configured_runtime_file(env_name: str, filename: str) -> str:
    """Use an explicit runtime-file override, otherwise the platform temp directory."""
    return os.getenv(env_name, default_runtime_file(filename)).strip() or default_runtime_file(filename)
