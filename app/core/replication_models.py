from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class MirrorAction(StrEnum):
    OPEN_MARKET = "open_market"
    CLOSE_MARKET = "close_market"
    SYNC_POSITION_SETTINGS = "sync_position_settings"


class MirrorOrderRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    action: MirrorAction = MirrorAction.OPEN_MARKET
    source_account: str
    master_exec_id: str
    category: str
    symbol: str
    side: str
    qty: Decimal
    position_idx: int
    reduce_only: bool = False
    source_order_id: str | None = None
    source_order_link_id: str | None = None
    source_order_type: str | None = None
    source_exec_time_ms: int | None = None
    source_closed_size: Decimal | None = None
    source_exec_qty: Decimal | None = None

    @property
    def is_close(self) -> bool:
        return self.action == MirrorAction.CLOSE_MARKET

    @property
    def is_open(self) -> bool:
        return self.action == MirrorAction.OPEN_MARKET


class FollowerPositionSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_name: str
    category: str
    symbol: str
    position_idx: int
    side: str | None
    size: Decimal

    @property
    def is_open(self) -> bool:
        return self.size > 0


class FollowerOrderResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_name: str
    symbol: str
    action: MirrorAction
    qty: Decimal
    success: bool
    skipped: bool = False
    result: str
    order_id: str | None = None
    order_link_id: str | None = None
    ret_code: int | str | None = None
    ret_msg: str | None = None
    error: str | None = None


class PositionSettingsSyncRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    action: MirrorAction = MirrorAction.SYNC_POSITION_SETTINGS
    source_account: str
    category: str
    symbol: str
    side: str
    position_idx: int
    leverage: Decimal | None = None
    take_profit: Decimal | None = None
    stop_loss: Decimal | None = None
    tp_trigger_by: str | None = None
    sl_trigger_by: str | None = None
    tpsl_mode: str = "Full"
    source_updated_time_ms: int | None = None
    source_sequence: int | None = None

    @property
    def has_leverage(self) -> bool:
        return self.leverage is not None and self.leverage > 0

    @property
    def has_tpsl_state(self) -> bool:
        return self.take_profit is not None or self.stop_loss is not None

    def desired_state_fingerprint(self) -> str:
        return "|".join(
            [
                self.category,
                self.symbol,
                self.side,
                str(self.position_idx),
                str(self.leverage) if self.leverage is not None else "",
                str(self.take_profit) if self.take_profit is not None else "",
                str(self.stop_loss) if self.stop_loss is not None else "",
                self.tp_trigger_by or "",
                self.sl_trigger_by or "",
                self.tpsl_mode,
            ]
        )


class FollowerSyncResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_name: str
    symbol: str
    action: MirrorAction
    success: bool
    skipped: bool = False
    result: str
    leverage_synced: bool = False
    tpsl_synced: bool = False
    ret_code: int | str | None = None
    ret_msg: str | None = None
    error: str | None = None
