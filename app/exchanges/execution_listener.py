from __future__ import annotations

from typing import Any

from loguru import logger
from rich.console import Console

from app.config import AppSettings
from app.core.dedupe import EventDeduplicator
from app.core.event_bus import EventBus
from app.core.event_models import NormalizedEventUnion, normalize_ws_message
from app.core.state_manager import RuntimeStateManager


class MasterExecutionListener:
    def __init__(
        self,
        *,
        account_name: str,
        settings: AppSettings,
        dedupe: EventDeduplicator,
        event_bus: EventBus,
        runtime_state: RuntimeStateManager | None = None,
    ) -> None:
        self._account_name = account_name
        self._settings = settings
        self._dedupe = dedupe
        self._event_bus = event_bus
        self._runtime_state = runtime_state
        self._console = Console()

    async def handle_message(self, message: dict[str, Any]) -> None:
        try:
            events = normalize_ws_message(self._account_name, message)
        except Exception as exc:
            logger.bind(
                account=self._account_name,
                action="normalize_message",
                result="error",
                topic=message.get("topic"),
            ).opt(exception=exc).error("failed to normalize websocket message")
            return

        for event in events:
            is_new = await self._dedupe.check_and_mark(event.fingerprint())
            if not is_new:
                logger.bind(
                    account=event.account_name,
                    symbol=event.symbol,
                    action=event.action(),
                    quantity=str(event.quantity()) if event.quantity() is not None else None,
                    result="duplicate_suppressed",
                    dedupe_key=event.fingerprint(),
                ).warning("duplicate websocket event suppressed")
                continue

            self._log_normalized_event(event)
            if self._runtime_state is not None:
                await self._runtime_state.record_master_event(event)
            await self._event_bus.publish(event)

    def _log_normalized_event(self, event: NormalizedEventUnion) -> None:
        payload = event.to_log_dict()
        logger.bind(
            account=event.account_name,
            symbol=event.symbol,
            action=event.action(),
            quantity=str(event.quantity()) if event.quantity() is not None else None,
            result="normalized",
            dedupe_key=event.fingerprint(),
        ).info("normalized master event")

        if self._settings.print_normalized_events:
            self._console.print_json(data=payload)
