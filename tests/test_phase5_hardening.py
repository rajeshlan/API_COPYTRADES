import asyncio
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.config import AccountCredentials, AccountsConfig, validate_accounts_for_enabled_engine
from app.core.dedupe import EventDeduplicator
from app.core.event_models import EventKind, ExecutionEvent
from app.core.replication_models import MirrorAction, MirrorOrderRequest
from app.core.state_manager import RuntimeStateManager
from app.exchanges.instrument_cache import InstrumentInfoCache
from app.exchanges.order_executor import BybitOrderExecutor
from app.utils.precision import InstrumentSpec, normalize_order_quantity
from app.utils.retry import BybitAPIError, with_retry


def test_quantity_normalization_rounds_down_to_qty_step() -> None:
    spec = InstrumentSpec(
        category="linear",
        symbol="BTCUSDT",
        qty_step=Decimal("0.001"),
        min_order_qty=Decimal("0.001"),
        max_market_order_qty=Decimal("100"),
    )

    result = normalize_order_quantity(Decimal("0.0019"), spec)

    assert result.valid is True
    assert result.normalized_qty == Decimal("0.001")


def test_quantity_normalization_rejects_below_minimum() -> None:
    spec = InstrumentSpec(
        category="linear",
        symbol="BTCUSDT",
        qty_step=Decimal("0.001"),
        min_order_qty=Decimal("0.01"),
        max_market_order_qty=None,
    )

    result = normalize_order_quantity(Decimal("0.009"), spec)

    assert result.valid is False
    assert result.reason == "below_min_order_qty"


def test_order_executor_uses_instrument_precision_cache() -> None:
    client = FakeBybitClient(
        positions=[],
        instrument_info={
            "qtyStep": "0.001",
            "minOrderQty": "0.001",
            "maxMktOrderQty": "10",
        },
    )
    cache = InstrumentInfoCache(client, ttl_seconds=60)
    executor = BybitOrderExecutor(
        client,
        block_if_position_exists=True,
        order_link_prefix="ct",
        instrument_cache=cache,
        normalize_quantities=True,
    )

    result = asyncio.run(executor.mirror_market_open(_open_request(qty=Decimal("0.0019"))))

    assert result.success is True
    assert result.qty == Decimal("0.001")
    assert client.orders[0]["qty"] == "0.001"
    assert client.instrument_calls == 1


def test_order_executor_skips_when_normalized_qty_is_invalid() -> None:
    client = FakeBybitClient(
        positions=[],
        instrument_info={
            "qtyStep": "0.001",
            "minOrderQty": "0.01",
            "maxMktOrderQty": "10",
        },
    )
    cache = InstrumentInfoCache(client, ttl_seconds=60)
    executor = BybitOrderExecutor(
        client,
        block_if_position_exists=True,
        order_link_prefix="ct",
        instrument_cache=cache,
        normalize_quantities=True,
    )

    result = asyncio.run(executor.mirror_market_open(_open_request(qty=Decimal("0.0019"))))

    assert result.skipped is True
    assert result.result == "skipped_below_min_order_qty"
    assert client.orders == []


def test_runtime_state_persists_dedupe_keys(tmp_path: Path) -> None:
    state_path = tmp_path / "runtime_state.json"
    manager = RuntimeStateManager(state_path, enabled=True, max_event_keys=10)
    event = _execution_event()

    async def run() -> list[str]:
        await manager.load()
        await manager.record_master_event(event)
        reloaded = RuntimeStateManager(state_path, enabled=True, max_event_keys=10)
        await reloaded.load()
        return [entry.key for entry in reloaded.persisted_event_keys(ttl_seconds=60)]

    keys = asyncio.run(run())

    assert event.fingerprint() in keys
    raw = json.loads(state_path.read_text(encoding="utf-8"))
    assert raw["last_master_event"]["dedupe_key"] == event.fingerprint()


def test_deduper_hydrates_from_runtime_state(tmp_path: Path) -> None:
    state_path = tmp_path / "runtime_state.json"
    manager = RuntimeStateManager(state_path, enabled=True, max_event_keys=10)
    event = _execution_event()

    async def run() -> bool:
        await manager.load()
        await manager.record_master_event(event)
        dedupe = EventDeduplicator(
            ttl_seconds=60,
            max_items=10,
            initial_entries=manager.persisted_event_keys(ttl_seconds=60),
        )
        return await dedupe.check_and_mark(event.fingerprint())

    assert asyncio.run(run()) is False


def test_retry_retries_rate_limit_error() -> None:
    attempts = 0

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BybitAPIError(10006, "Too many visits")
        return "ok"

    result = asyncio.run(
        with_retry(
            "place_order",
            operation,
            attempts=2,
            initial_delay_seconds=0.001,
            max_delay_seconds=0.001,
            jitter_ratio=0,
        )
    )

    assert result == "ok"
    assert attempts == 2


def test_retry_does_not_retry_non_retryable_error() -> None:
    attempts = 0

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        raise BybitAPIError(10001, "Params error")

    with pytest.raises(BybitAPIError):
        asyncio.run(
            with_retry(
                "place_order",
                operation,
                attempts=3,
                initial_delay_seconds=0.001,
                max_delay_seconds=0.001,
                jitter_ratio=0,
            )
        )

    assert attempts == 1


def test_enabled_account_validation_rejects_blank_master_and_follower_credentials() -> None:
    accounts = AccountsConfig(
        master=AccountCredentials(name="master", api_key="", api_secret=""),
        followers=[AccountCredentials(name="copy_1", api_key="", api_secret="")],
    )

    errors = validate_accounts_for_enabled_engine(accounts)

    assert "master account api_key/api_secret are required in accounts.json" in errors
    assert "follower account 'copy_1' api_key/api_secret are required in accounts.json" in errors


def test_enabled_account_validation_accepts_complete_accounts() -> None:
    accounts = AccountsConfig(
        master=AccountCredentials(name="master", api_key="key", api_secret="secret"),
        followers=[AccountCredentials(name="copy_1", api_key="key", api_secret="secret")],
    )

    assert validate_accounts_for_enabled_engine(accounts) == []


class FakeBybitClient:
    def __init__(self, positions: list[dict[str, Any]], instrument_info: dict[str, str]) -> None:
        self.account = AccountCredentials(name="copy_1", api_key="key", api_secret="secret")
        self.positions = positions
        self.instrument_info = instrument_info
        self.instrument_calls = 0
        self.orders: list[dict[str, Any]] = []

    async def get_positions(self, **kwargs: Any) -> dict[str, Any]:
        return {"retCode": 0, "retMsg": "OK", "result": {"list": self.positions}}

    async def get_instruments_info(self, **kwargs: Any) -> dict[str, Any]:
        self.instrument_calls += 1
        return {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": [{"lotSizeFilter": self.instrument_info}]},
        }

    async def place_order(self, **kwargs: Any) -> dict[str, Any]:
        self.orders.append(kwargs)
        return {"retCode": 0, "retMsg": "OK", "result": {"orderId": "order-1"}}


def _open_request(qty: Decimal) -> MirrorOrderRequest:
    return MirrorOrderRequest(
        action=MirrorAction.OPEN_MARKET,
        source_account="master",
        master_exec_id="master-exec-1",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        qty=qty,
        position_idx=0,
    )


def _execution_event() -> ExecutionEvent:
    return ExecutionEvent(
        event_kind=EventKind.EXECUTION,
        account_name="master",
        raw_topic="execution",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        received_time_ms=1,
        order_id="master-order-1",
        order_type="Market",
        exec_id="master-exec-1",
        exec_qty=Decimal("0.001"),
        exec_type="Trade",
    )
