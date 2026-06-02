import asyncio
from decimal import Decimal
from typing import Any

from app.config import AccountCredentials
from app.core.event_models import EventKind, OrderEvent, PositionEvent
from app.core.replication_models import PositionSettingsSyncRequest
from app.core.risk_engine import Phase3PositionSyncPolicy
from app.core.state_manager import PositionSyncStateManager
from app.exchanges.order_executor import BybitOrderExecutor


def test_phase3_policy_builds_position_settings_request() -> None:
    event = _position_event(take_profit=Decimal("30000"), stop_loss=Decimal("25000"))

    decision = _policy().evaluate_position(event, tp_trigger_by="LastPrice", sl_trigger_by="IndexPrice")

    assert decision.should_sync is True
    assert decision.request is not None
    assert decision.request.symbol == "BTCUSDT"
    assert decision.request.leverage == Decimal("10")
    assert decision.request.take_profit == Decimal("30000")
    assert decision.request.stop_loss == Decimal("25000")
    assert decision.request.tp_trigger_by == "LastPrice"
    assert decision.request.sl_trigger_by == "IndexPrice"
    assert decision.request.tpsl_mode == "Full"


def test_phase3_policy_converts_empty_tpsl_to_cancel_zeroes() -> None:
    event = _position_event(take_profit=None, stop_loss=None)

    decision = _policy().evaluate_position(event)

    assert decision.should_sync is True
    assert decision.request is not None
    assert decision.request.take_profit == Decimal("0")
    assert decision.request.stop_loss == Decimal("0")


def test_position_sync_state_suppresses_unchanged_settings() -> None:
    state = PositionSyncStateManager()
    request = _sync_request(stop_loss=Decimal("25000"))
    changed_request = _sync_request(stop_loss=Decimal("24000"))

    async def run() -> tuple[bool, bool, bool]:
        first = await state.should_dispatch(request)
        second = await state.should_dispatch(request)
        third = await state.should_dispatch(changed_request)
        return first, second, third

    assert asyncio.run(run()) == (True, False, True)


def test_position_sync_state_remembers_order_trigger_types() -> None:
    state = PositionSyncStateManager()
    order = OrderEvent(
        event_kind=EventKind.ORDER,
        account_name="master",
        raw_topic="order",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        received_time_ms=1,
        order_id="order-1",
        position_idx=0,
        tp_trigger_by="LastPrice",
        sl_trigger_by="IndexPrice",
    )

    async def run() -> tuple[str | None, str | None]:
        await state.observe_order(order)
        snapshot = await state.trigger_by_for("linear", "BTCUSDT", 0)
        return snapshot.tp_trigger_by, snapshot.sl_trigger_by

    assert asyncio.run(run()) == ("LastPrice", "IndexPrice")


def test_order_executor_syncs_leverage_and_tpsl() -> None:
    client = FakeBybitClient(
        positions=[
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "positionIdx": 0,
                "side": "Buy",
                "size": "0.01",
            }
        ]
    )
    executor = BybitOrderExecutor(
        client,
        block_if_position_exists=True,
        order_link_prefix="ct",
        position_sync_attempts=1,
        position_sync_retry_delay_seconds=0,
    )

    result = asyncio.run(executor.sync_position_settings(_sync_request(stop_loss=Decimal("25000"))))

    assert result.success is True
    assert result.leverage_synced is True
    assert result.tpsl_synced is True
    assert client.leverage_calls == [
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "buyLeverage": "10",
            "sellLeverage": "10",
        }
    ]
    assert client.trading_stop_calls == [
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "tpslMode": "Full",
            "positionIdx": 0,
            "tpOrderType": "Market",
            "slOrderType": "Market",
            "takeProfit": "30000",
            "stopLoss": "25000",
            "tpTriggerBy": "MarkPrice",
            "slTriggerBy": "MarkPrice",
        }
    ]


def test_order_executor_skips_tpsl_when_follower_side_mismatches() -> None:
    client = FakeBybitClient(
        positions=[
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "positionIdx": 0,
                "side": "Sell",
                "size": "0.01",
            }
        ]
    )
    executor = BybitOrderExecutor(
        client,
        block_if_position_exists=True,
        order_link_prefix="ct",
        position_sync_attempts=1,
        position_sync_retry_delay_seconds=0,
    )

    result = asyncio.run(executor.sync_position_settings(_sync_request(stop_loss=Decimal("25000"))))

    assert result.success is False
    assert result.leverage_synced is True
    assert result.tpsl_synced is False
    assert result.result == "partial_error"
    assert client.trading_stop_calls == []


class FakeBybitClient:
    def __init__(self, positions: list[dict[str, Any]]) -> None:
        self.account = AccountCredentials(name="copy_1", api_key="key", api_secret="secret")
        self.positions = positions
        self.leverage_calls: list[dict[str, Any]] = []
        self.trading_stop_calls: list[dict[str, Any]] = []

    async def get_positions(self, **kwargs: Any) -> dict[str, Any]:
        return {"retCode": 0, "retMsg": "OK", "result": {"list": self.positions}}

    async def set_leverage(self, **kwargs: Any) -> dict[str, Any]:
        self.leverage_calls.append(kwargs)
        return {"retCode": 0, "retMsg": "OK", "result": {}}

    async def set_trading_stop(self, **kwargs: Any) -> dict[str, Any]:
        self.trading_stop_calls.append(kwargs)
        return {"retCode": 0, "retMsg": "OK", "result": {}}


def _policy() -> Phase3PositionSyncPolicy:
    return Phase3PositionSyncPolicy(
        sync_leverage=True,
        sync_tpsl=True,
        sync_empty_tpsl_to_cancel=True,
        default_tp_trigger_by="MarkPrice",
        default_sl_trigger_by="MarkPrice",
    )


def _position_event(take_profit: Decimal | None, stop_loss: Decimal | None) -> PositionEvent:
    return PositionEvent(
        event_kind=EventKind.POSITION,
        account_name="master",
        raw_topic="position",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        received_time_ms=1,
        sequence=10,
        position_idx=0,
        size=Decimal("0.01"),
        leverage=Decimal("10"),
        take_profit=take_profit,
        stop_loss=stop_loss,
        updated_time_ms=2,
    )


def _sync_request(stop_loss: Decimal) -> PositionSettingsSyncRequest:
    return PositionSettingsSyncRequest(
        source_account="master",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        position_idx=0,
        leverage=Decimal("10"),
        take_profit=Decimal("30000"),
        stop_loss=stop_loss,
        tp_trigger_by="MarkPrice",
        sl_trigger_by="MarkPrice",
    )
