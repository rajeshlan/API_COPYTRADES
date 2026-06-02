from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.constants import ONE_WAY_POSITION_IDX, SUPPORTED_COPY_CATEGORIES, SUPPORTED_POSITION_SYNC_CATEGORIES
from app.core.event_models import ExecutionEvent, PositionEvent
from app.core.replication_models import MirrorAction, MirrorOrderRequest, PositionSettingsSyncRequest


@dataclass(frozen=True)
class MirrorDecision:
    should_mirror: bool
    reason: str
    request: MirrorOrderRequest | None = None


@dataclass(frozen=True)
class PositionSyncDecision:
    should_sync: bool
    reason: str
    request: PositionSettingsSyncRequest | None = None


class Phase2MirrorPolicy:
    """Conservative Phase 2 gate for follower market opens.

    Closes, reductions, reversals, TP/SL, leverage, and order amendments are
    intentionally deferred to later phases because they need full state sync.
    """

    def evaluate_execution(self, event: ExecutionEvent) -> MirrorDecision:
        if event.exec_type and event.exec_type != "Trade":
            return MirrorDecision(False, f"unsupported_exec_type:{event.exec_type}")

        if event.category not in SUPPORTED_COPY_CATEGORIES:
            return MirrorDecision(False, f"unsupported_category:{event.category}")

        if not event.symbol:
            return MirrorDecision(False, "missing_symbol")

        if event.side not in {"Buy", "Sell"}:
            return MirrorDecision(False, f"unsupported_side:{event.side}")

        if event.exec_qty <= 0:
            return MirrorDecision(False, "non_positive_exec_qty")

        if event.closed_size is not None and event.closed_size > Decimal("0"):
            return MirrorDecision(False, "close_or_reduce_deferred_to_phase4")

        request = MirrorOrderRequest(
            source_account=event.account_name,
            master_exec_id=event.exec_id,
            category=event.category,
            symbol=event.symbol,
            side=event.side,
            qty=event.exec_qty,
            position_idx=ONE_WAY_POSITION_IDX,
            reduce_only=False,
            source_order_id=event.order_id,
            source_order_link_id=event.order_link_id,
            source_order_type=event.order_type,
            source_exec_time_ms=event.exec_time_ms,
        )
        return MirrorDecision(True, "mirror_open_market", request)


class Phase4ExecutionMirrorPolicy:
    """Phase 4 gate for opening executions and reduce-only close executions."""

    def evaluate_execution(self, event: ExecutionEvent) -> MirrorDecision:
        if event.exec_type and event.exec_type != "Trade":
            return MirrorDecision(False, f"unsupported_exec_type:{event.exec_type}")

        if event.category not in SUPPORTED_COPY_CATEGORIES:
            return MirrorDecision(False, f"unsupported_category:{event.category}")

        if not event.symbol:
            return MirrorDecision(False, "missing_symbol")

        if event.side not in {"Buy", "Sell"}:
            return MirrorDecision(False, f"unsupported_side:{event.side}")

        if event.exec_qty <= 0:
            return MirrorDecision(False, "non_positive_exec_qty")

        if event.closed_size is not None and event.closed_size > Decimal("0"):
            request = MirrorOrderRequest(
                action=MirrorAction.CLOSE_MARKET,
                source_account=event.account_name,
                master_exec_id=event.exec_id,
                category=event.category,
                symbol=event.symbol,
                side=event.side,
                qty=event.closed_size,
                position_idx=ONE_WAY_POSITION_IDX,
                reduce_only=True,
                source_order_id=event.order_id,
                source_order_link_id=event.order_link_id,
                source_order_type=event.order_type,
                source_exec_time_ms=event.exec_time_ms,
                source_closed_size=event.closed_size,
                source_exec_qty=event.exec_qty,
            )
            return MirrorDecision(True, "mirror_reduce_only_close_market", request)

        request = MirrorOrderRequest(
            action=MirrorAction.OPEN_MARKET,
            source_account=event.account_name,
            master_exec_id=event.exec_id,
            category=event.category,
            symbol=event.symbol,
            side=event.side,
            qty=event.exec_qty,
            position_idx=ONE_WAY_POSITION_IDX,
            reduce_only=False,
            source_order_id=event.order_id,
            source_order_link_id=event.order_link_id,
            source_order_type=event.order_type,
            source_exec_time_ms=event.exec_time_ms,
            source_exec_qty=event.exec_qty,
        )
        return MirrorDecision(True, "mirror_open_market", request)


class Phase3PositionSyncPolicy:
    """Conservative Phase 3 gate for master leverage and TP/SL state."""

    def __init__(
        self,
        *,
        sync_leverage: bool,
        sync_tpsl: bool,
        sync_empty_tpsl_to_cancel: bool,
        default_tp_trigger_by: str | None,
        default_sl_trigger_by: str | None,
    ) -> None:
        self._sync_leverage = sync_leverage
        self._sync_tpsl = sync_tpsl
        self._sync_empty_tpsl_to_cancel = sync_empty_tpsl_to_cancel
        self._default_tp_trigger_by = default_tp_trigger_by
        self._default_sl_trigger_by = default_sl_trigger_by

    def evaluate_position(
        self,
        event: PositionEvent,
        *,
        tp_trigger_by: str | None = None,
        sl_trigger_by: str | None = None,
    ) -> PositionSyncDecision:
        if event.category not in SUPPORTED_POSITION_SYNC_CATEGORIES:
            return PositionSyncDecision(False, f"unsupported_category:{event.category}")

        if not event.symbol:
            return PositionSyncDecision(False, "missing_symbol")

        if event.position_idx not in (None, ONE_WAY_POSITION_IDX):
            return PositionSyncDecision(False, "hedge_mode_deferred")

        if event.side not in {"Buy", "Sell"}:
            return PositionSyncDecision(False, "flat_or_missing_side")

        if event.size is None or event.size <= 0:
            return PositionSyncDecision(False, "flat_position")

        leverage = event.leverage if self._sync_leverage else None
        take_profit = event.take_profit if self._sync_tpsl else None
        stop_loss = event.stop_loss if self._sync_tpsl else None

        if self._sync_tpsl and self._sync_empty_tpsl_to_cancel:
            take_profit = take_profit or Decimal("0")
            stop_loss = stop_loss or Decimal("0")

        if (leverage is None or leverage <= 0) and not self._sync_tpsl:
            return PositionSyncDecision(False, "nothing_enabled")

        if (leverage is None or leverage <= 0) and take_profit is None and stop_loss is None:
            return PositionSyncDecision(False, "no_syncable_state")

        request = PositionSettingsSyncRequest(
            source_account=event.account_name,
            category=event.category,
            symbol=event.symbol,
            side=event.side,
            position_idx=event.position_idx or ONE_WAY_POSITION_IDX,
            leverage=leverage if leverage is not None and leverage > 0 else None,
            take_profit=take_profit,
            stop_loss=stop_loss,
            tp_trigger_by=tp_trigger_by or self._default_tp_trigger_by,
            sl_trigger_by=sl_trigger_by or self._default_sl_trigger_by,
            source_updated_time_ms=event.updated_time_ms,
            source_sequence=event.sequence,
        )
        return PositionSyncDecision(True, "sync_position_settings", request)
