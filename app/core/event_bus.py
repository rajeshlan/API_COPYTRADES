from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable

from loguru import logger

from app.core.event_models import EventKind, NormalizedEventUnion

EventHandler = Callable[[NormalizedEventUnion], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._subscribers: dict[EventKind | str, list[EventHandler]] = defaultdict(list)

    def subscribe(self, event_kind: EventKind | str, handler: EventHandler) -> None:
        self._subscribers[event_kind].append(handler)

    async def publish(self, event: NormalizedEventUnion) -> None:
        handlers = [*self._subscribers.get(event.event_kind, []), *self._subscribers.get("*", [])]
        if not handlers:
            return

        results = await asyncio.gather(*(handler(event) for handler in handlers), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.bind(
                    account=event.account_name,
                    symbol=event.symbol,
                    action=event.action(),
                    quantity=str(event.quantity()) if event.quantity() is not None else None,
                    result="handler_error",
                ).opt(exception=result).error("event handler failed")
