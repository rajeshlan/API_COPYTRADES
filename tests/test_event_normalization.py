import asyncio
from decimal import Decimal

from app.core.dedupe import EventDeduplicator
from app.core.event_models import ExecutionEvent, OrderEvent, PositionEvent, normalize_ws_message


def test_execution_event_normalizes_and_fingerprints() -> None:
    message = {
        "topic": "execution",
        "id": "386825804_BTCUSDT_140612148849382",
        "creationTime": 1746270400355,
        "data": [
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "closedSize": "0.5",
                "execFee": "26.3725275",
                "execId": "0ab1bdf7-4219-438b-b30a-32ec863018f7",
                "execPrice": "95900.1",
                "execQty": "0.5",
                "execType": "Trade",
                "execValue": "47950.05",
                "feeRate": "0.00055",
                "leavesQty": "0",
                "orderId": "9aac161b-8ed6-450d-9cab-c5cc67c21784",
                "orderLinkId": "",
                "orderPrice": "94942.5",
                "orderQty": "0.5",
                "orderType": "Market",
                "stopOrderType": "UNKNOWN",
                "side": "Sell",
                "execTime": "1746270400353",
                "isMaker": False,
                "seq": 140612148849382,
                "execPnl": "0.05",
                "createType": "CreateByUser",
                "feeCurrency": "USDT",
            }
        ],
    }

    events = normalize_ws_message("master", message)

    assert len(events) == 1
    assert isinstance(events[0], ExecutionEvent)
    assert events[0].symbol == "BTCUSDT"
    assert events[0].exec_qty == Decimal("0.5")
    assert events[0].action() == "execution_close_or_reduce"
    assert events[0].fingerprint() == "execution:master:0ab1bdf7-4219-438b-b30a-32ec863018f7"


def test_order_event_keeps_cancel_race_fields_in_fingerprint() -> None:
    message = {
        "id": "order-msg",
        "topic": "order",
        "creationTime": 1672364262474,
        "data": [
            {
                "category": "linear",
                "symbol": "ETHUSDT",
                "orderId": "order-1",
                "side": "Buy",
                "orderType": "Market",
                "cancelType": "UNKNOWN",
                "price": "0",
                "qty": "1",
                "timeInForce": "IOC",
                "orderStatus": "Filled",
                "orderLinkId": "",
                "reduceOnly": False,
                "closeOnTrigger": False,
                "leavesQty": "0",
                "cumExecQty": "1",
                "cumExecValue": "2500",
                "avgPrice": "2500",
                "positionIdx": 0,
                "createdTime": "1672364262444",
                "updatedTime": "1672364262457",
                "rejectReason": "EC_NoError",
            }
        ],
    }

    events = normalize_ws_message("master", message)

    assert isinstance(events[0], OrderEvent)
    assert "EC_NoError" in events[0].fingerprint()
    assert events[0].quantity() == Decimal("1")


def test_position_event_normalizes_flat_side_safely() -> None:
    message = {
        "id": "position-msg",
        "topic": "position",
        "creationTime": 1746270400356,
        "data": [
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "side": "",
                "size": "0",
                "positionIdx": 0,
                "positionValue": "0",
                "entryPrice": "",
                "markPrice": "95901",
                "leverage": "10",
                "takeProfit": "",
                "stopLoss": "",
                "positionStatus": "Normal",
                "isReduceOnly": False,
                "createdTime": "1746270300000",
                "updatedTime": "1746270400356",
                "seq": 140612148849382,
            }
        ],
    }

    events = normalize_ws_message("master", message)

    assert isinstance(events[0], PositionEvent)
    assert events[0].side is None
    assert events[0].action() == "position_flat"


def test_deduper_suppresses_exact_replay() -> None:
    asyncio.run(_assert_deduper_suppresses_exact_replay())


async def _assert_deduper_suppresses_exact_replay() -> None:
    dedupe = EventDeduplicator(ttl_seconds=60, max_items=100)

    assert await dedupe.check_and_mark("execution:master:abc") is True
    assert await dedupe.check_and_mark("execution:master:abc") is False
    assert await dedupe.check_and_mark("execution:master:def") is True
