from __future__ import annotations

from loguru import logger

from app.core.event_bus import EventBus
from app.core.event_models import EventKind, ExecutionEvent, NormalizedEventUnion, OrderEvent, PositionEvent
from app.core.follower_manager import FollowerManager
from app.core.risk_engine import Phase3PositionSyncPolicy, Phase4ExecutionMirrorPolicy
from app.core.state_manager import PositionSyncStateManager


class SyncEngine:
    def __init__(
        self,
        follower_manager: FollowerManager,
        mirror_policy: Phase4ExecutionMirrorPolicy,
        position_sync_policy: Phase3PositionSyncPolicy,
        position_sync_state: PositionSyncStateManager,
    ) -> None:
        self._follower_manager = follower_manager
        self._mirror_policy = mirror_policy
        self._position_sync_policy = position_sync_policy
        self._position_sync_state = position_sync_state

    def register(self, event_bus: EventBus) -> None:
        event_bus.subscribe(EventKind.EXECUTION, self.handle_event)
        event_bus.subscribe(EventKind.ORDER, self.handle_event)
        event_bus.subscribe(EventKind.POSITION, self.handle_event)

    async def handle_event(self, event: NormalizedEventUnion) -> None:
        if isinstance(event, ExecutionEvent):
            await self._handle_execution(event)
            return

        if isinstance(event, OrderEvent):
            await self._position_sync_state.observe_order(event)
            return

        if isinstance(event, PositionEvent):
            await self._handle_position(event)

    async def _handle_execution(self, event: ExecutionEvent) -> None:

        decision = self._mirror_policy.evaluate_execution(event)
        if not decision.should_mirror or decision.request is None:
            logger.bind(
                account=event.account_name,
                symbol=event.symbol,
                action="phase2_mirror",
                quantity=str(event.exec_qty),
                result="skipped",
                reason=decision.reason,
                master_exec_id=event.exec_id,
            ).info("master execution not mirrored in phase 2")
            return

        logger.bind(
            account=event.account_name,
            symbol=decision.request.symbol,
            action=decision.request.action,
            quantity=str(decision.request.qty),
            result="dispatching",
            master_exec_id=decision.request.master_exec_id,
            follower_count=self._follower_manager.active_count,
        ).info("dispatching follower market replication")

        if decision.request.is_close:
            await self._follower_manager.mirror_market_close(decision.request)
            return

        await self._follower_manager.mirror_market_open(decision.request)

    async def _handle_position(self, event: PositionEvent) -> None:
        if not event.category or not event.symbol:
            return

        trigger_by = await self._position_sync_state.trigger_by_for(
            event.category,
            event.symbol,
            event.position_idx or 0,
        )
        decision = self._position_sync_policy.evaluate_position(
            event,
            tp_trigger_by=trigger_by.tp_trigger_by,
            sl_trigger_by=trigger_by.sl_trigger_by,
        )
        if not decision.should_sync or decision.request is None:
            logger.bind(
                account=event.account_name,
                symbol=event.symbol,
                action="phase3_position_sync",
                quantity=str(event.size) if event.size is not None else None,
                result="skipped",
                reason=decision.reason,
            ).debug("master position not synced in phase 3")
            return

        if not await self._position_sync_state.should_dispatch(decision.request):
            logger.bind(
                account=event.account_name,
                symbol=event.symbol,
                action="phase3_position_sync",
                quantity=str(event.size) if event.size is not None else None,
                result="unchanged_suppressed",
            ).debug("unchanged master position settings suppressed")
            return

        logger.bind(
            account=event.account_name,
            symbol=decision.request.symbol,
            action=decision.request.action,
            quantity=str(event.size) if event.size is not None else None,
            result="dispatching",
            leverage=str(decision.request.leverage) if decision.request.leverage is not None else None,
            take_profit=str(decision.request.take_profit) if decision.request.take_profit is not None else None,
            stop_loss=str(decision.request.stop_loss) if decision.request.stop_loss is not None else None,
            follower_count=self._follower_manager.active_count,
        ).info("dispatching follower position settings sync")

        await self._follower_manager.sync_position_settings(decision.request)
