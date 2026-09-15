from __future__ import annotations

import contextlib
import errno
import os
import secrets
import stat
import threading
import time
from pathlib import Path


DEBUG_PREFIX = "fintracker_debug_"
_DEBUG_LOCK = threading.RLock()
_DIRECTORY_OPEN_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)


def _positive_int(name: str, default: int, maximum: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1 or value > maximum:
        raise ValueError(f"{name} is outside the allowed range")
    return value


def _configured_debug_directory() -> Path:
    raw_path = os.getenv("REPORT_DEBUG_DIR", "").strip()
    if not raw_path:
        raise ValueError("REPORT_DEBUG_DIR is required when report debug artifacts are enabled")
    directory = Path(raw_path)
    if not directory.is_absolute():
        raise ValueError("REPORT_DEBUG_DIR must be an absolute path")
    if directory == Path("/"):
        raise ValueError("REPORT_DEBUG_DIR must not be the filesystem root")
    if any(part in {".", ".."} for part in directory.parts):
        raise ValueError("REPORT_DEBUG_DIR must not contain traversal components")
    return directory


@contextlib.contextmanager
def _open_protected_directory(directory: Path):
    descriptor = os.open("/", _DIRECTORY_OPEN_FLAGS)
    try:
        for component in directory.parts[1:]:
            try:
                child = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=descriptor)
            except FileNotFoundError:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
                child = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise ValueError("REPORT_DEBUG_DIR must not contain symlinks") from exc
                raise
            os.close(descriptor)
            descriptor = child
        os.fchmod(descriptor, 0o700)
        if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o700:
            raise ValueError("REPORT_DEBUG_DIR must have mode 0700")
        yield descriptor
    finally:
        os.close(descriptor)


def protected_debug_directory() -> Path:
    directory = _configured_debug_directory()
    with _DEBUG_LOCK, _open_protected_directory(directory):
        return directory


def _cleanup_open_directory(directory_fd: int, *, now: float | None = None) -> None:
    now = time.time() if now is None else now
    max_age = _positive_int("REPORT_DEBUG_MAX_AGE_SECONDS", 86400, 31 * 86400)
    retain = _positive_int("REPORT_DEBUG_MAX_FILES", 10, 100)
    files = []
    for name in os.listdir(directory_fd):
        if not name.startswith(DEBUG_PREFIX):
            continue
        try:
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(metadata.st_mode):
            continue
        files.append((metadata.st_mtime, name))
    files.sort(reverse=True)
    for index, (modified_at, name) in enumerate(files):
        if now - modified_at <= max_age and index < retain:
            continue
        try:
            os.unlink(name, dir_fd=directory_fd)
        except FileNotFoundError:
            continue


def cleanup_debug_artifacts(directory: Path, *, now: float | None = None) -> None:
    with _DEBUG_LOCK, _open_protected_directory(directory) as directory_fd:
        _cleanup_open_directory(directory_fd, now=now)


def _validate_filename_part(*, kind: str, suffix: str) -> None:
    if not kind or any(not (character.isalnum() or character in "_-") for character in kind):
        raise ValueError("Debug artifact kind contains unsafe characters")
    if len(suffix) < 2 or not suffix.startswith(".") or any(
        not (character.isalnum() or character in "._-") for character in suffix[1:]
    ):
        raise ValueError("Debug artifact suffix contains unsafe characters")


def save_debug_text(*, kind: str, suffix: str, text: str) -> str:
    _validate_filename_part(kind=kind, suffix=suffix)
    directory = _configured_debug_directory()
    with _DEBUG_LOCK, _open_protected_directory(directory) as directory_fd:
        _cleanup_open_directory(directory_fd)
        descriptor = -1
        filename = ""
        for _attempt in range(10):
            filename = f"{DEBUG_PREFIX}{kind}_{secrets.token_hex(8)}{suffix}"
            try:
                descriptor = os.open(
                    filename,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=directory_fd,
                )
                break
            except FileExistsError:
                continue
        if descriptor < 0:
            raise FileExistsError("Unable to allocate a unique debug artifact")
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as file_obj:
                descriptor = -1
                file_obj.write(text)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        _cleanup_open_directory(directory_fd)
    return str(directory / filename)
