from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


def configure_logging(log_level: str, project_root: Path) -> None:
    logger.remove()

    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "{message} | {extra}"
    )

    logger.add(
        sys.stderr,
        level=log_level,
        format=log_format,
        backtrace=False,
        diagnose=False,
        enqueue=True,
    )
    logger.add(
        log_dir / "copy_trader.log",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message} | {extra}",
        rotation="25 MB",
        retention=10,
        compression="zip",
        backtrace=False,
        diagnose=False,
        enqueue=True,
    )


def bind_account(account: str, **extra: object):
    return logger.bind(account=account, **extra)

