from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on", "enabled"}


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"cannot parse decimal value {value!r}") from exc


def format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def bool_or_none(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
    raise ValueError(f"cannot parse boolean value {value!r}")


def redact_secret(value: str, visible: int = 4) -> str:
    value = value or ""
    if len(value) <= visible:
        return "*" * len(value)
    return f"{value[:visible]}{'*' * (len(value) - visible)}"
