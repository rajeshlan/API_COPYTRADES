from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import TypeVar

from loguru import logger

T = TypeVar("T")


@dataclass
class ExponentialBackoff:
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.initial_delay_seconds <= 0:
            raise ValueError("initial delay must be positive")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max delay must be >= initial delay")
        if self.jitter_ratio < 0:
            raise ValueError("jitter ratio must not be negative")
        self._attempt = 0

    def reset(self) -> None:
        self._attempt = 0

    def next_delay(self) -> float:
        raw_delay = min(self.max_delay_seconds, self.initial_delay_seconds * (2**self._attempt))
        self._attempt += 1

        if self.jitter_ratio == 0:
            return raw_delay

        jitter = raw_delay * self.jitter_ratio
        return max(0.0, raw_delay + random.uniform(-jitter, jitter))


class BybitAPIError(RuntimeError):
    def __init__(self, ret_code: int | str, ret_msg: str) -> None:
        self.ret_code = ret_code
        self.ret_msg = ret_msg
        super().__init__(f"Bybit API error retCode={ret_code} retMsg={ret_msg}")


RETRYABLE_BYBIT_RET_CODES = {10000, 10006, 10016, "10000", "10006", "10016"}


def is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, BybitAPIError):
        return exc.ret_code in RETRYABLE_BYBIT_RET_CODES
    return isinstance(exc, (TimeoutError, ConnectionError, OSError))


async def with_retry(
    operation_name: str,
    operation,
    *,
    attempts: int,
    initial_delay_seconds: float,
    max_delay_seconds: float,
    jitter_ratio: float,
    log_context: dict[str, object] | None = None,
) -> T:
    if attempts <= 0:
        raise ValueError("attempts must be positive")

    backoff = ExponentialBackoff(
        initial_delay_seconds=initial_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        jitter_ratio=jitter_ratio,
    )
    context = log_context or {}
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts or not is_retryable_exception(exc):
                raise
            delay = backoff.next_delay()
            logger.bind(
                action=operation_name,
                result="retry_scheduled",
                attempt=attempt,
                next_attempt=attempt + 1,
                delay_seconds=round(delay, 3),
                **context,
            ).warning("retrying Bybit API action after transient failure")
            await asyncio.sleep(delay)

    raise RuntimeError(f"{operation_name} failed without exception") from last_exc
