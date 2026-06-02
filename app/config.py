from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from app.constants import (
    DEFAULT_ACCOUNTS_FILE,
    DEFAULT_PRIVATE_TOPICS,
    DEFAULT_RUNTIME_STATE_FILE,
    SUPPORTED_PRIVATE_TOPICS,
)
from app.utils.helpers import parse_bool


class AccountCredentials(BaseModel):
    name: str
    api_key: str = ""
    api_secret: str = ""

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("account name must not be blank")
        return value

    def has_credentials(self) -> bool:
        return bool(self.api_key.strip() and self.api_secret.strip())


class AccountsConfig(BaseModel):
    master: AccountCredentials
    followers: list[AccountCredentials] = Field(default_factory=list)


class AppSettings(BaseModel):
    project_root: Path
    accounts_file: Path
    enabled: bool
    testnet: bool
    log_level: str
    print_normalized_events: bool
    validate_master_on_startup: bool
    ws_topics: tuple[str, ...]
    ws_max_active_time: str
    ws_ping_interval_seconds: float
    ws_auth_timeout_seconds: float
    ws_connect_timeout_seconds: float
    reconnect_initial_delay_seconds: float
    reconnect_max_delay_seconds: float
    reconnect_jitter_ratio: float
    dedupe_ttl_seconds: float
    dedupe_max_items: int
    runtime_state_file: Path
    persist_runtime_state: bool
    block_if_follower_position_exists: bool
    follower_replication_concurrency: int
    mirror_order_link_prefix: str
    sync_leverage: bool
    sync_tpsl: bool
    sync_empty_tpsl_to_cancel: bool
    default_tp_trigger_by: str | None
    default_sl_trigger_by: str | None
    follower_position_sync_attempts: int
    follower_position_sync_retry_delay_seconds: float
    clip_close_qty_to_follower_position: bool
    api_call_attempts: int
    api_retry_initial_delay_seconds: float
    api_retry_max_delay_seconds: float
    api_retry_jitter_ratio: float
    normalize_order_quantities: bool
    instrument_cache_ttl_seconds: float

    @field_validator("follower_replication_concurrency")
    @classmethod
    def concurrency_must_be_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("FOLLOWER_REPLICATION_CONCURRENCY must be positive")
        return value

    @field_validator("mirror_order_link_prefix")
    @classmethod
    def order_link_prefix_must_fit_bybit(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("MIRROR_ORDER_LINK_PREFIX must not be blank")
        if len(value) > 5:
            raise ValueError("MIRROR_ORDER_LINK_PREFIX must be 5 characters or fewer")
        return value

    @field_validator("default_tp_trigger_by", "default_sl_trigger_by")
    @classmethod
    def trigger_by_must_be_valid(cls, value: str | None) -> str | None:
        if value is None or value == "":
            return None
        if value not in {"LastPrice", "MarkPrice", "IndexPrice"}:
            raise ValueError("trigger type must be LastPrice, MarkPrice, or IndexPrice")
        return value

    @field_validator("follower_position_sync_attempts")
    @classmethod
    def sync_attempts_must_be_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("FOLLOWER_POSITION_SYNC_ATTEMPTS must be positive")
        return value

    @field_validator("api_call_attempts")
    @classmethod
    def api_attempts_must_be_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("API_CALL_ATTEMPTS must be positive")
        return value

    @property
    def env_name(self) -> str:
        return "testnet" if self.testnet else "mainnet"


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_settings() -> AppSettings:
    root = project_root()
    load_dotenv(root / ".env", override=False)

    topics = _parse_topics(os.getenv("BYBIT_WS_TOPICS", ",".join(DEFAULT_PRIVATE_TOPICS)))
    accounts_file = Path(os.getenv("ACCOUNTS_FILE", DEFAULT_ACCOUNTS_FILE))
    if not accounts_file.is_absolute():
        accounts_file = root / accounts_file
    runtime_state_file = Path(os.getenv("RUNTIME_STATE_FILE", DEFAULT_RUNTIME_STATE_FILE))
    if not runtime_state_file.is_absolute():
        runtime_state_file = root / runtime_state_file

    return AppSettings(
        project_root=root,
        accounts_file=accounts_file,
        enabled=parse_bool(os.getenv("COPY_TRADER_ENABLED"), default=False),
        testnet=parse_bool(os.getenv("BYBIT_TESTNET"), default=True),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        print_normalized_events=parse_bool(os.getenv("PRINT_NORMALIZED_EVENTS"), default=True),
        validate_master_on_startup=parse_bool(os.getenv("VALIDATE_MASTER_ON_STARTUP"), default=True),
        ws_topics=topics,
        ws_max_active_time=os.getenv("BYBIT_WS_MAX_ACTIVE_TIME", "1m"),
        ws_ping_interval_seconds=_env_float("BYBIT_WS_PING_INTERVAL_SECONDS", 20.0),
        ws_auth_timeout_seconds=_env_float("BYBIT_WS_AUTH_TIMEOUT_SECONDS", 10.0),
        ws_connect_timeout_seconds=_env_float("BYBIT_WS_CONNECT_TIMEOUT_SECONDS", 10.0),
        reconnect_initial_delay_seconds=_env_float("RECONNECT_INITIAL_DELAY_SECONDS", 1.0),
        reconnect_max_delay_seconds=_env_float("RECONNECT_MAX_DELAY_SECONDS", 30.0),
        reconnect_jitter_ratio=_env_float("RECONNECT_JITTER_RATIO", 0.2),
        dedupe_ttl_seconds=_env_float("DEDUPE_TTL_SECONDS", 86_400.0),
        dedupe_max_items=_env_int("DEDUPE_MAX_ITEMS", 50_000),
        runtime_state_file=runtime_state_file,
        persist_runtime_state=parse_bool(os.getenv("PERSIST_RUNTIME_STATE"), default=True),
        block_if_follower_position_exists=parse_bool(os.getenv("BLOCK_IF_FOLLOWER_POSITION_EXISTS"), default=True),
        follower_replication_concurrency=_env_int("FOLLOWER_REPLICATION_CONCURRENCY", 10),
        mirror_order_link_prefix=os.getenv("MIRROR_ORDER_LINK_PREFIX", "ct"),
        sync_leverage=parse_bool(os.getenv("SYNC_LEVERAGE"), default=True),
        sync_tpsl=parse_bool(os.getenv("SYNC_TPSL"), default=True),
        sync_empty_tpsl_to_cancel=parse_bool(os.getenv("SYNC_EMPTY_TPSL_TO_CANCEL"), default=True),
        default_tp_trigger_by=os.getenv("DEFAULT_TP_TRIGGER_BY", "MarkPrice"),
        default_sl_trigger_by=os.getenv("DEFAULT_SL_TRIGGER_BY", "MarkPrice"),
        follower_position_sync_attempts=_env_int("FOLLOWER_POSITION_SYNC_ATTEMPTS", 3),
        follower_position_sync_retry_delay_seconds=_env_float("FOLLOWER_POSITION_SYNC_RETRY_DELAY_SECONDS", 0.5),
        clip_close_qty_to_follower_position=parse_bool(os.getenv("CLIP_CLOSE_QTY_TO_FOLLOWER_POSITION"), default=True),
        api_call_attempts=_env_int("API_CALL_ATTEMPTS", 3),
        api_retry_initial_delay_seconds=_env_float("API_RETRY_INITIAL_DELAY_SECONDS", 0.25),
        api_retry_max_delay_seconds=_env_float("API_RETRY_MAX_DELAY_SECONDS", 2.0),
        api_retry_jitter_ratio=_env_float("API_RETRY_JITTER_RATIO", 0.2),
        normalize_order_quantities=parse_bool(os.getenv("NORMALIZE_ORDER_QUANTITIES"), default=True),
        instrument_cache_ttl_seconds=_env_float("INSTRUMENT_CACHE_TTL_SECONDS", 3600.0),
    )


def load_accounts(accounts_file: Path) -> AccountsConfig:
    if not accounts_file.exists():
        raise FileNotFoundError(f"accounts file not found: {accounts_file}")

    raw = json.loads(accounts_file.read_text(encoding="utf-8"))
    return AccountsConfig.model_validate(raw)


def validate_accounts_for_enabled_engine(accounts: AccountsConfig) -> list[str]:
    errors: list[str] = []
    if not accounts.master.has_credentials():
        errors.append("master account api_key/api_secret are required in accounts.json")

    if not accounts.followers:
        errors.append("at least one follower account is required in accounts.json")

    for follower in accounts.followers:
        if not follower.has_credentials():
            errors.append(f"follower account {follower.name!r} api_key/api_secret are required in accounts.json")
    return errors


def validate_runtime_prerequisites(settings: AppSettings, accounts: AccountsConfig) -> list[str]:
    if not settings.enabled:
        return []
    return validate_accounts_for_enabled_engine(accounts)


def _parse_topics(raw_topics: str) -> tuple[str, ...]:
    topics = tuple(topic.strip() for topic in raw_topics.split(",") if topic.strip())
    invalid = sorted(set(topics) - SUPPORTED_PRIVATE_TOPICS)
    if invalid:
        raise ValueError(
            f"unsupported websocket topics {invalid}; supported topics: {sorted(SUPPORTED_PRIVATE_TOPICS)}"
        )
    if not topics:
        raise ValueError("at least one private websocket topic must be configured")
    return topics


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def public_settings_snapshot(settings: AppSettings) -> dict[str, Any]:
    return {
        "enabled": settings.enabled,
        "environment": settings.env_name,
        "accounts_file": str(settings.accounts_file),
        "ws_topics": list(settings.ws_topics),
        "ws_max_active_time": settings.ws_max_active_time,
        "dedupe_ttl_seconds": settings.dedupe_ttl_seconds,
        "dedupe_max_items": settings.dedupe_max_items,
        "runtime_state_file": str(settings.runtime_state_file),
        "persist_runtime_state": settings.persist_runtime_state,
        "block_if_follower_position_exists": settings.block_if_follower_position_exists,
        "follower_replication_concurrency": settings.follower_replication_concurrency,
        "sync_leverage": settings.sync_leverage,
        "sync_tpsl": settings.sync_tpsl,
        "sync_empty_tpsl_to_cancel": settings.sync_empty_tpsl_to_cancel,
        "clip_close_qty_to_follower_position": settings.clip_close_qty_to_follower_position,
        "api_call_attempts": settings.api_call_attempts,
        "normalize_order_quantities": settings.normalize_order_quantities,
    }
