from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.utils.helpers import bool_or_none, decimal_or_none, int_or_none, now_ms


class EventKind(StrEnum):
    EXECUTION = "execution"
    ORDER = "order"
    POSITION = "position"


class NormalizedEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    event_kind: EventKind
    account_name: str
    raw_topic: str
    message_id: str | None = None
    category: str | None = None
    symbol: str | None = None
    side: str | None = None
    creation_time_ms: int | None = None
    received_time_ms: int
    sequence: int | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict, repr=False)

    def fingerprint(self) -> str:
        raise NotImplementedError

    def action(self) -> str:
        return self.event_kind.value

    def quantity(self) -> Decimal | None:
        return None

    def to_log_dict(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude={"raw_payload"})
        payload["dedupe_key"] = self.fingerprint()
        payload["action"] = self.action()
        qty = self.quantity()
        payload["quantity"] = str(qty) if qty is not None else None
        return payload


class ExecutionEvent(NormalizedEvent):
    event_kind: EventKind = EventKind.EXECUTION
    order_id: str | None = None
    order_link_id: str | None = None
    create_type: str | None = None
    order_type: str | None = None
    stop_order_type: str | None = None
    order_price: Decimal | None = None
    order_qty: Decimal | None = None
    leaves_qty: Decimal | None = None
    exec_id: str
    exec_price: Decimal | None = None
    exec_qty: Decimal
    exec_value: Decimal | None = None
    exec_pnl: Decimal | None = None
    exec_type: str | None = None
    exec_time_ms: int | None = None
    closed_size: Decimal | None = None
    is_maker: bool | None = None
    fee_rate: Decimal | None = None
    fee_currency: str | None = None

    def fingerprint(self) -> str:
        return f"{self.event_kind}:{self.account_name}:{self.exec_id}"

    def action(self) -> str:
        if self.closed_size is not None and self.closed_size > 0:
            return "execution_close_or_reduce"
        return "execution_fill"

    def quantity(self) -> Decimal | None:
        return self.exec_qty


class OrderEvent(NormalizedEvent):
    event_kind: EventKind = EventKind.ORDER
    order_id: str
    order_link_id: str | None = None
    parent_order_link_id: str | None = None
    position_idx: int | None = None
    order_status: str | None = None
    create_type: str | None = None
    cancel_type: str | None = None
    reject_reason: str | None = None
    price: Decimal | None = None
    qty: Decimal | None = None
    avg_price: Decimal | None = None
    leaves_qty: Decimal | None = None
    cum_exec_qty: Decimal | None = None
    cum_exec_value: Decimal | None = None
    closed_pnl: Decimal | None = None
    time_in_force: str | None = None
    order_type: str | None = None
    stop_order_type: str | None = None
    trigger_price: Decimal | None = None
    take_profit: Decimal | None = None
    stop_loss: Decimal | None = None
    tp_trigger_by: str | None = None
    sl_trigger_by: str | None = None
    reduce_only: bool | None = None
    close_on_trigger: bool | None = None
    created_time_ms: int | None = None
    updated_time_ms: int | None = None

    def fingerprint(self) -> str:
        return ":".join(
            [
                self.event_kind,
                self.account_name,
                self.order_id,
                self.order_status or "",
                str(self.updated_time_ms or ""),
                self.cancel_type or "",
                self.reject_reason or "",
                self.message_id or "",
            ]
        )

    def action(self) -> str:
        status = (self.order_status or "unknown").lower()
        return f"order_{status}"

    def quantity(self) -> Decimal | None:
        return self.qty


class PositionEvent(NormalizedEvent):
    event_kind: EventKind = EventKind.POSITION
    position_idx: int | None = None
    size: Decimal | None = None
    position_value: Decimal | None = None
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    leverage: Decimal | None = None
    break_even_price: Decimal | None = None
    auto_add_margin: int | None = None
    liq_price: Decimal | None = None
    take_profit: Decimal | None = None
    stop_loss: Decimal | None = None
    trailing_stop: Decimal | None = None
    unrealised_pnl: Decimal | None = None
    cur_realised_pnl: Decimal | None = None
    cum_realised_pnl: Decimal | None = None
    position_status: str | None = None
    is_reduce_only: bool | None = None
    created_time_ms: int | None = None
    updated_time_ms: int | None = None
    open_time_ms: int | None = None

    def fingerprint(self) -> str:
        return ":".join(
            [
                self.event_kind,
                self.account_name,
                self.symbol or "",
                str(self.position_idx or 0),
                str(self.sequence or ""),
                str(self.updated_time_ms or ""),
                str(self.size) if self.size is not None else "",
                self.side or "",
                str(self.take_profit) if self.take_profit is not None else "",
                str(self.stop_loss) if self.stop_loss is not None else "",
                str(self.leverage) if self.leverage is not None else "",
            ]
        )

    def action(self) -> str:
        if self.size is None or self.size == 0:
            return "position_flat"
        return "position_update"

    def quantity(self) -> Decimal | None:
        return self.size


NormalizedEventUnion = ExecutionEvent | OrderEvent | PositionEvent


def normalize_ws_message(account_name: str, message: dict[str, Any]) -> list[NormalizedEventUnion]:
    raw_topic = message.get("topic")
    if not isinstance(raw_topic, str):
        return []

    event_kind = raw_topic.split(".", maxsplit=1)[0]
    data = message.get("data", [])
    if not isinstance(data, list):
        raise ValueError(f"expected websocket data to be a list for topic {raw_topic!r}")

    received_time_ms = now_ms()
    normalized: list[NormalizedEventUnion] = []
    for row in data:
        if not isinstance(row, dict):
            raise ValueError(f"expected websocket data rows to be objects for topic {raw_topic!r}")
        if event_kind == EventKind.EXECUTION:
            normalized.append(_normalize_execution(account_name, message, row, received_time_ms))
        elif event_kind == EventKind.ORDER:
            normalized.append(_normalize_order(account_name, message, row, received_time_ms))
        elif event_kind == EventKind.POSITION:
            normalized.append(_normalize_position(account_name, message, row, received_time_ms))
    return normalized


def _base_kwargs(
    account_name: str,
    message: dict[str, Any],
    row: dict[str, Any],
    received_time_ms: int,
) -> dict[str, Any]:
    return {
        "account_name": account_name,
        "raw_topic": str(message.get("topic", "")),
        "message_id": _empty_to_none(message.get("id")),
        "category": _empty_to_none(row.get("category")),
        "symbol": _empty_to_none(row.get("symbol")),
        "side": _empty_to_none(row.get("side")),
        "creation_time_ms": int_or_none(message.get("creationTime")),
        "received_time_ms": received_time_ms,
        "sequence": int_or_none(row.get("seq")),
        "raw_payload": row,
    }


def _normalize_execution(
    account_name: str,
    message: dict[str, Any],
    row: dict[str, Any],
    received_time_ms: int,
) -> ExecutionEvent:
    return ExecutionEvent(
        **_base_kwargs(account_name, message, row, received_time_ms),
        order_id=_empty_to_none(row.get("orderId")),
        order_link_id=_empty_to_none(row.get("orderLinkId")),
        create_type=_empty_to_none(row.get("createType")),
        order_type=_empty_to_none(row.get("orderType")),
        stop_order_type=_empty_to_none(row.get("stopOrderType")),
        order_price=decimal_or_none(row.get("orderPrice")),
        order_qty=decimal_or_none(row.get("orderQty")),
        leaves_qty=decimal_or_none(row.get("leavesQty")),
        exec_id=str(row["execId"]),
        exec_price=decimal_or_none(row.get("execPrice")),
        exec_qty=decimal_or_none(row.get("execQty")) or Decimal("0"),
        exec_value=decimal_or_none(row.get("execValue")),
        exec_pnl=decimal_or_none(row.get("execPnl")),
        exec_type=_empty_to_none(row.get("execType")),
        exec_time_ms=int_or_none(row.get("execTime")),
        closed_size=decimal_or_none(row.get("closedSize")),
        is_maker=bool_or_none(row.get("isMaker")),
        fee_rate=decimal_or_none(row.get("feeRate")),
        fee_currency=_empty_to_none(row.get("feeCurrency")),
    )


def _normalize_order(
    account_name: str,
    message: dict[str, Any],
    row: dict[str, Any],
    received_time_ms: int,
) -> OrderEvent:
    return OrderEvent(
        **_base_kwargs(account_name, message, row, received_time_ms),
        order_id=str(row["orderId"]),
        order_link_id=_empty_to_none(row.get("orderLinkId")),
        parent_order_link_id=_empty_to_none(row.get("parentOrderLinkId")),
        position_idx=int_or_none(row.get("positionIdx")),
        order_status=_empty_to_none(row.get("orderStatus")),
        create_type=_empty_to_none(row.get("createType")),
        cancel_type=_empty_to_none(row.get("cancelType")),
        reject_reason=_empty_to_none(row.get("rejectReason")),
        price=decimal_or_none(row.get("price")),
        qty=decimal_or_none(row.get("qty")),
        avg_price=decimal_or_none(row.get("avgPrice")),
        leaves_qty=decimal_or_none(row.get("leavesQty")),
        cum_exec_qty=decimal_or_none(row.get("cumExecQty")),
        cum_exec_value=decimal_or_none(row.get("cumExecValue")),
        closed_pnl=decimal_or_none(row.get("closedPnl")),
        time_in_force=_empty_to_none(row.get("timeInForce")),
        order_type=_empty_to_none(row.get("orderType")),
        stop_order_type=_empty_to_none(row.get("stopOrderType")),
        trigger_price=decimal_or_none(row.get("triggerPrice")),
        take_profit=decimal_or_none(row.get("takeProfit")),
        stop_loss=decimal_or_none(row.get("stopLoss")),
        tp_trigger_by=_empty_to_none(row.get("tpTriggerBy")),
        sl_trigger_by=_empty_to_none(row.get("slTriggerBy")),
        reduce_only=bool_or_none(row.get("reduceOnly")),
        close_on_trigger=bool_or_none(row.get("closeOnTrigger")),
        created_time_ms=int_or_none(row.get("createdTime")),
        updated_time_ms=int_or_none(row.get("updatedTime")),
    )


def _normalize_position(
    account_name: str,
    message: dict[str, Any],
    row: dict[str, Any],
    received_time_ms: int,
) -> PositionEvent:
    return PositionEvent(
        **_base_kwargs(account_name, message, row, received_time_ms),
        position_idx=int_or_none(row.get("positionIdx")),
        size=decimal_or_none(row.get("size")),
        position_value=decimal_or_none(row.get("positionValue")),
        entry_price=decimal_or_none(row.get("entryPrice")),
        mark_price=decimal_or_none(row.get("markPrice")),
        leverage=decimal_or_none(row.get("leverage")),
        break_even_price=decimal_or_none(row.get("breakEvenPrice")),
        auto_add_margin=int_or_none(row.get("autoAddMargin")),
        liq_price=decimal_or_none(row.get("liqPrice")),
        take_profit=decimal_or_none(row.get("takeProfit")),
        stop_loss=decimal_or_none(row.get("stopLoss")),
        trailing_stop=decimal_or_none(row.get("trailingStop")),
        unrealised_pnl=decimal_or_none(row.get("unrealisedPnl")),
        cur_realised_pnl=decimal_or_none(row.get("curRealisedPnl")),
        cum_realised_pnl=decimal_or_none(row.get("cumRealisedPnl")),
        position_status=_empty_to_none(row.get("positionStatus")),
        is_reduce_only=bool_or_none(row.get("isReduceOnly")),
        created_time_ms=int_or_none(row.get("createdTime")),
        updated_time_ms=int_or_none(row.get("updatedTime")),
        open_time_ms=int_or_none(row.get("openTime")),
    )


def _empty_to_none(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)
