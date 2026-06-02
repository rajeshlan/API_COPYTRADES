from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from app.constants import DEFAULT_RUNTIME_STATE_FILE, DEFAULT_RUNTIME_STATE_VERSION, PHASE
from app.core.event_models import NormalizedEventUnion, OrderEvent
from app.core.replication_models import FollowerOrderResult, FollowerSyncResult
from app.core.replication_models import PositionSettingsSyncRequest


class PersistedEventKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    inserted_at_epoch: float


class FollowerHealthSnapshot(BaseModel):
    account_name: str
    status: str
    success_count: int = 0
    failure_count: int = 0
    skipped_count: int = 0
    last_result: str | None = None
    last_error: str | None = None
    last_seen_epoch: float | None = None


class RuntimeState(BaseModel):
    version: int = DEFAULT_RUNTIME_STATE_VERSION
    phase: str = PHASE
    updated_at_epoch: float = Field(default_factory=time.time)
    seen_event_keys: list[PersistedEventKey] = Field(default_factory=list)
    last_master_event: dict[str, Any] | None = None
    follower_health: dict[str, FollowerHealthSnapshot] = Field(default_factory=dict)


class RuntimeStateManager:
    """Atomic JSON runtime state for replay-safe restarts and health snapshots."""

    def __init__(
        self,
        path: Path,
        *,
        enabled: bool = True,
        max_event_keys: int = 50_000,
    ) -> None:
        self.path = path
        self.enabled = enabled
        self._max_event_keys = max_event_keys
        self._state = RuntimeState()
        self._lock = asyncio.Lock()

    @classmethod
    def from_project_root(
        cls,
        project_root: Path,
        runtime_state_file: str = DEFAULT_RUNTIME_STATE_FILE,
        *,
        enabled: bool = True,
        max_event_keys: int = 50_000,
    ) -> "RuntimeStateManager":
        return cls(project_root / runtime_state_file, enabled=enabled, max_event_keys=max_event_keys)

    async def load(self) -> RuntimeState:
        if not self.enabled:
            return self._state
        if not self.path.exists():
            await self.flush()
            return self._state

        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self._state = RuntimeState.model_validate(raw)
            logger.bind(action="runtime_state_load", result="ok", path=str(self.path)).info("runtime state loaded")
        except Exception as exc:
            logger.bind(action="runtime_state_load", result="error", path=str(self.path)).opt(exception=exc).error(
                "runtime state load failed; starting with empty state"
            )
            self._state = RuntimeState()
        return self._state

    def persisted_event_keys(self, ttl_seconds: float) -> list[PersistedEventKey]:
        cutoff = time.time() - ttl_seconds
        return [entry for entry in self._state.seen_event_keys if entry.inserted_at_epoch >= cutoff]

    async def record_master_event(self, event: NormalizedEventUnion) -> None:
        if not self.enabled:
            return
        async with self._lock:
            now = time.time()
            self._state.seen_event_keys.append(PersistedEventKey(key=event.fingerprint(), inserted_at_epoch=now))
            self._state.seen_event_keys = self._state.seen_event_keys[-self._max_event_keys :]
            self._state.last_master_event = {
                "event_kind": event.event_kind,
                "account_name": event.account_name,
                "symbol": event.symbol,
                "action": event.action(),
                "quantity": str(event.quantity()) if event.quantity() is not None else None,
                "dedupe_key": event.fingerprint(),
                "received_time_ms": event.received_time_ms,
            }
            await self._flush_locked()

    async def record_follower_result(self, result: FollowerOrderResult | FollowerSyncResult) -> None:
        if not self.enabled:
            return
        async with self._lock:
            existing = self._state.follower_health.get(result.account_name) or FollowerHealthSnapshot(
                account_name=result.account_name,
                status="unknown",
            )
            snapshot = FollowerHealthSnapshot(
                account_name=result.account_name,
                status="healthy" if result.success or result.skipped else "degraded",
                success_count=existing.success_count + (1 if result.success else 0),
                failure_count=existing.failure_count + (0 if result.success or result.skipped else 1),
                skipped_count=existing.skipped_count + (1 if result.skipped else 0),
                last_result=result.result,
                last_error=result.error,
                last_seen_epoch=time.time(),
            )
            self._state.follower_health[result.account_name] = snapshot
            await self._flush_locked()

    async def record_follower_startup(self, account_name: str, *, success: bool, error: str | None = None) -> None:
        if not self.enabled:
            return
        async with self._lock:
            self._state.follower_health[account_name] = FollowerHealthSnapshot(
                account_name=account_name,
                status="healthy" if success else "unavailable",
                success_count=1 if success else 0,
                failure_count=0 if success else 1,
                last_result="startup_validation",
                last_error=error,
                last_seen_epoch=time.time(),
            )
            await self._flush_locked()

    async def flush(self) -> None:
        if not self.enabled:
            return
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        self._state.phase = PHASE
        self._state.updated_at_epoch = time.time()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(self._state.model_dump(mode="json"), indent=2, sort_keys=True)
        with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=self.path.parent) as temp_file:
            temp_file.write(data)
            temp_path = Path(temp_file.name)
        temp_path.replace(self.path)


class TriggerBySnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    tp_trigger_by: str | None = None
    sl_trigger_by: str | None = None


class PositionSyncStateManager:
    """In-memory desired-state cache for Phase 3.

    The position websocket can emit updates for order activity that does not
    change TP/SL or leverage. This cache suppresses repeated follower API calls
    for unchanged desired state while keeping restart behavior simple.
    """

    def __init__(self) -> None:
        self._trigger_by: dict[tuple[str, str, int], TriggerBySnapshot] = {}
        self._last_desired: dict[tuple[str, str, int], str] = {}
        self._lock = asyncio.Lock()

    async def observe_order(self, event: OrderEvent) -> None:
        if not event.category or not event.symbol:
            return
        position_idx = event.position_idx or 0
        if not event.tp_trigger_by and not event.sl_trigger_by:
            return

        key = (event.category, event.symbol, position_idx)
        async with self._lock:
            previous = self._trigger_by.get(key, TriggerBySnapshot())
            self._trigger_by[key] = TriggerBySnapshot(
                tp_trigger_by=event.tp_trigger_by or previous.tp_trigger_by,
                sl_trigger_by=event.sl_trigger_by or previous.sl_trigger_by,
            )

    async def trigger_by_for(self, category: str, symbol: str, position_idx: int) -> TriggerBySnapshot:
        key = (category, symbol, position_idx)
        async with self._lock:
            return self._trigger_by.get(key, TriggerBySnapshot())

    async def should_dispatch(self, request: PositionSettingsSyncRequest) -> bool:
        key = (request.category, request.symbol, request.position_idx)
        fingerprint = request.desired_state_fingerprint()
        async with self._lock:
            if self._last_desired.get(key) == fingerprint:
                return False
            self._last_desired[key] = fingerprint
            return True
