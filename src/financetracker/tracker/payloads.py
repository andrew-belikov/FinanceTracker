"""Side-effect-free conversion of T-Invest JSON payload values."""

from __future__ import annotations

from typing import Any


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def quotation_to_float(value: dict[str, Any] | None) -> float | None:
    if not value:
        return None
    return to_int(value.get("units")) + to_int(value.get("nano")) / 1e9


def money_to_float(value: dict[str, Any] | None) -> float | None:
    return quotation_to_float(value)


def json_value(payload: dict[str, Any], snake_name: str) -> Any:
    camel_name = "".join(
        part.capitalize() if index else part
        for index, part in enumerate(snake_name.split("_"))
    )
    return payload.get(camel_name, payload.get(snake_name))


def url_host(url: str) -> str:
    return url.split("//", 1)[1].split("/", 1)[0]


def url_path(url: str) -> str:
    parts = url.split("/", 3)
    return "/" + parts[3] if len(parts) > 3 else "/"
