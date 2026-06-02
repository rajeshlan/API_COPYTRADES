import asyncio
from decimal import Decimal
from typing import Any

from app.config import AccountCredentials
from app.core.event_models import EventKind, ExecutionEvent
from app.core.replication_models import MirrorAction, MirrorOrderRequest
from app.core.risk_engine import Phase4ExecutionMirrorPolicy
from app.exchanges.order_executor import BybitOrderExecutor


def test_phase4_policy_builds_reduce_only_close_request() -> None:
    event = _execution_event(side="Sell", exec_qty=Decimal("0.03"), closed_size=Decimal("0.01"))

    decision = Phase4ExecutionMirrorPolicy().evaluate_execution(event)

    assert decision.should_mirror is True
    assert decision.request is not None
    assert decision.request.action == MirrorAction.CLOSE_MARKET
    assert decision.request.side == "Sell"
    assert decision.request.qty == Decimal("0.01")
    assert decision.request.reduce_only is True
    assert decision.request.source_exec_qty == Decimal("0.03")
    assert decision.request.source_closed_size == Decimal("0.01")


def test_phase4_policy_still_builds_open_request() -> None:
    event = _execution_event(side="Buy", exec_qty=Decimal("0.02"), closed_size=None)

    decision = Phase4ExecutionMirrorPolicy().evaluate_execution(event)

    assert decision.should_mirror is True
    assert decision.request is not None
    assert decision.request.action == MirrorAction.OPEN_MARKET
    assert decision.request.qty == Decimal("0.02")
    assert decision.request.reduce_only is False


def test_order_executor_submits_reduce_only_partial_close() -> None:
    client = FakeBybitClient(
        positions=[
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "positionIdx": 0,
                "side": "Buy",
                "size": "0.05",
            }
        ]
    )
    executor = BybitOrderExecutor(client, block_if_position_exists=True, order_link_prefix="ct")

    result = asyncio.run(executor.mirror_market_close(_close_request(side="Sell", qty=Decimal("0.02"))))

    assert result.success is True
    assert result.qty == Decimal("0.02")
    assert client.orders == [
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "side": "Sell",
            "orderType": "Market",
            "qty": "0.02",
            "positionIdx": 0,
            "reduceOnly": True,
            "orderLinkId": result.order_link_id,
        }
    ]


def test_order_executor_skips_close_when_no_follower_position() -> None:
    client = FakeBybitClient(
        positions=[
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "positionIdx": 0,
                "side": "",
                "size": "0",
            }
        ]
    )
    executor = BybitOrderExecutor(client, block_if_position_exists=True, order_link_prefix="ct")

    result = asyncio.run(executor.mirror_market_close(_close_request(side="Sell", qty=Decimal("0.02"))))

    assert result.skipped is True
    assert result.result == "skipped_no_position"
    assert client.orders == []


def test_order_executor_skips_close_when_follower_side_mismatches() -> None:
    client = FakeBybitClient(
        positions=[
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "positionIdx": 0,
                "side": "Sell",
                "size": "0.05",
            }
        ]
    )
    executor = BybitOrderExecutor(client, block_if_position_exists=True, order_link_prefix="ct")

    result = asyncio.run(executor.mirror_market_close(_close_request(side="Sell", qty=Decimal("0.02"))))

    assert result.skipped is True
    assert result.result == "skipped_side_mismatch"
    assert client.orders == []


def test_order_executor_clips_close_qty_to_follower_size() -> None:
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
        clip_close_qty_to_follower_position=True,
    )

    result = asyncio.run(executor.mirror_market_close(_close_request(side="Sell", qty=Decimal("0.02"))))

    assert result.success is True
    assert result.qty == Decimal("0.01")
    assert client.orders[0]["qty"] == "0.01"
    assert client.orders[0]["reduceOnly"] is True


def test_order_executor_can_skip_instead_of_clipping_close_qty() -> None:
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
        clip_close_qty_to_follower_position=False,
    )

    result = asyncio.run(executor.mirror_market_close(_close_request(side="Sell", qty=Decimal("0.02"))))

    assert result.skipped is True
    assert result.result == "skipped_insufficient_position"
    assert client.orders == []


class FakeBybitClient:
    def __init__(self, positions: list[dict[str, Any]]) -> None:
        self.account = AccountCredentials(name="copy_1", api_key="key", api_secret="secret")
        self.positions = positions
        self.orders: list[dict[str, Any]] = []

    async def get_positions(self, **kwargs: Any) -> dict[str, Any]:
        return {"retCode": 0, "retMsg": "OK", "result": {"list": self.positions}}

    async def place_order(self, **kwargs: Any) -> dict[str, Any]:
        self.orders.append(kwargs)
        return {"retCode": 0, "retMsg": "OK", "result": {"orderId": "close-order-1"}}


def _execution_event(side: str, exec_qty: Decimal, closed_size: Decimal | None) -> ExecutionEvent:
    return ExecutionEvent(
        event_kind=EventKind.EXECUTION,
        account_name="master",
        raw_topic="execution",
        category="linear",
        symbol="BTCUSDT",
        side=side,
        received_time_ms=1,
        order_id="master-order-1",
        order_type="Market",
        exec_id="master-exec-1",
        exec_qty=exec_qty,
        exec_type="Trade",
        closed_size=closed_size,
    )


def _close_request(side: str, qty: Decimal) -> MirrorOrderRequest:
    return MirrorOrderRequest(
        action=MirrorAction.CLOSE_MARKET,
        source_account="master",
        master_exec_id="master-exec-1",
        category="linear",
        symbol="BTCUSDT",
        side=side,
        qty=qty,
        position_idx=0,
        reduce_only=True,
        source_closed_size=qty,
        source_exec_qty=qty,
    )
