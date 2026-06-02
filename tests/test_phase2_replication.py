import asyncio
from decimal import Decimal
from typing import Any

from app.config import AccountCredentials
from app.constants import ONE_WAY_POSITION_IDX
from app.core.event_models import ExecutionEvent, EventKind
from app.core.replication_models import MirrorOrderRequest
from app.core.risk_engine import Phase2MirrorPolicy
from app.exchanges.order_executor import BybitOrderExecutor


def test_phase2_policy_builds_market_open_request() -> None:
    event = _execution_event(closed_size=None)

    decision = Phase2MirrorPolicy().evaluate_execution(event)

    assert decision.should_mirror is True
    assert decision.request is not None
    assert decision.request.symbol == "BTCUSDT"
    assert decision.request.side == "Buy"
    assert decision.request.qty == Decimal("0.01")
    assert decision.request.position_idx == ONE_WAY_POSITION_IDX
    assert decision.request.reduce_only is False


def test_phase2_policy_skips_close_or_reduce() -> None:
    event = _execution_event(closed_size=Decimal("0.01"))

    decision = Phase2MirrorPolicy().evaluate_execution(event)

    assert decision.should_mirror is False
    assert decision.reason == "close_or_reduce_deferred_to_phase4"


def test_order_executor_blocks_duplicate_follower_position() -> None:
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
    executor = BybitOrderExecutor(client, block_if_position_exists=True, order_link_prefix="ct")

    result = asyncio.run(executor.mirror_market_open(_mirror_request()))

    assert result.skipped is True
    assert result.result == "skipped_existing_position"
    assert client.orders == []


def test_order_executor_places_market_order_when_follower_is_flat() -> None:
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

    result = asyncio.run(executor.mirror_market_open(_mirror_request()))

    assert result.success is True
    assert result.order_id == "follower-order-1"
    assert len(result.order_link_id or "") <= 36
    assert client.orders == [
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "side": "Buy",
            "orderType": "Market",
            "qty": "0.01",
            "positionIdx": 0,
            "reduceOnly": False,
            "orderLinkId": result.order_link_id,
        }
    ]


class FakeBybitClient:
    def __init__(self, positions: list[dict[str, Any]]) -> None:
        self.account = AccountCredentials(name="copy_1", api_key="key", api_secret="secret")
        self.positions = positions
        self.orders: list[dict[str, Any]] = []

    async def get_positions(self, **kwargs: Any) -> dict[str, Any]:
        return {"retCode": 0, "retMsg": "OK", "result": {"list": self.positions}}

    async def place_order(self, **kwargs: Any) -> dict[str, Any]:
        self.orders.append(kwargs)
        return {"retCode": 0, "retMsg": "OK", "result": {"orderId": "follower-order-1"}}


def _execution_event(closed_size: Decimal | None) -> ExecutionEvent:
    return ExecutionEvent(
        event_kind=EventKind.EXECUTION,
        account_name="master",
        raw_topic="execution",
        message_id="msg-1",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        creation_time_ms=1,
        received_time_ms=2,
        sequence=3,
        order_id="master-order-1",
        order_link_id=None,
        order_type="Limit",
        exec_id="master-exec-1",
        exec_qty=Decimal("0.01"),
        exec_type="Trade",
        exec_time_ms=4,
        closed_size=closed_size,
    )


def _mirror_request() -> MirrorOrderRequest:
    return MirrorOrderRequest(
        source_account="master",
        master_exec_id="master-exec-1",
        category="linear",
        symbol="BTCUSDT",
        side="Buy",
        qty=Decimal("0.01"),
        position_idx=0,
        reduce_only=False,
    )
