from __future__ import annotations


MOJIBAKE_MARKERS = ("╨", "╤", "Ð", "Ñ")


def has_mojibake(value: str | None) -> bool:
    if not value:
        return False
    return any(marker in value for marker in MOJIBAKE_MARKERS)


def try_repair_cp866_utf8(value: str | None) -> str | None:
    if not value:
        return value
    if not has_mojibake(value):
        return value
    try:
        repaired = value.encode("cp866").decode("utf-8")
    except Exception:
        return value
    return repaired
