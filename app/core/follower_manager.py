from __future__ import annotations

import asyncio

from loguru import logger

from app.config import AccountCredentials, AppSettings
from app.core.replication_models import (
    FollowerOrderResult,
    FollowerSyncResult,
    MirrorOrderRequest,
    PositionSettingsSyncRequest,
)
from app.core.state_manager import RuntimeStateManager
from app.exchanges.bybit_client import BybitHTTPClient
from app.exchanges.instrument_cache import InstrumentInfoCache
from app.exchanges.order_executor import BybitOrderExecutor


class FollowerManager:
    def __init__(
        self,
        followers: list[AccountCredentials],
        settings: AppSettings,
        *,
        runtime_state: RuntimeStateManager | None = None,
    ) -> None:
        self._followers = followers
        self._settings = settings
        self._runtime_state = runtime_state
        self._executors: list[BybitOrderExecutor] = []
        self._semaphore = asyncio.Semaphore(settings.follower_replication_concurrency)

    @property
    def active_count(self) -> int:
        return len(self._executors)

    async def initialize(self) -> None:
        validation_tasks = [self._build_executor(account) for account in self._followers]
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, BybitOrderExecutor):
                self._executors.append(result)
                if self._runtime_state is not None:
                    await self._runtime_state.record_follower_startup(result.account_name, success=True)
            elif isinstance(result, Exception):
                logger.bind(action="follower_initialize", result="error").opt(exception=result).error(
                    "follower initialization failed"
                )

        if not self._executors:
            raise RuntimeError("no follower accounts are available for Phase 2 replication")

        logger.bind(
            action="follower_initialize",
            result="ok",
            active_followers=len(self._executors),
            configured_followers=len(self._followers),
        ).info("follower executors initialized")

    async def mirror_market_open(self, request: MirrorOrderRequest) -> list[FollowerOrderResult]:
        tasks = [self._mirror_one(executor, request) for executor in self._executors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        normalized: list[FollowerOrderResult] = []
        for result in results:
            if isinstance(result, FollowerOrderResult):
                normalized.append(result)
                await self._record_follower_result(result)
            elif isinstance(result, Exception):
                logger.bind(
                    symbol=request.symbol,
                    action=request.action,
                    quantity=str(request.qty),
                    result="unexpected_follower_error",
                ).opt(exception=result).error("unexpected follower replication failure")
        return normalized

    async def mirror_market_close(self, request: MirrorOrderRequest) -> list[FollowerOrderResult]:
        tasks = [self._mirror_one(executor, request) for executor in self._executors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        normalized: list[FollowerOrderResult] = []
        for result in results:
            if isinstance(result, FollowerOrderResult):
                normalized.append(result)
                await self._record_follower_result(result)
            elif isinstance(result, Exception):
                logger.bind(
                    symbol=request.symbol,
                    action=request.action,
                    quantity=str(request.qty),
                    result="unexpected_follower_error",
                ).opt(exception=result).error("unexpected follower close replication failure")
        return normalized

    async def sync_position_settings(self, request: PositionSettingsSyncRequest) -> list[FollowerSyncResult]:
        tasks = [self._sync_one(executor, request) for executor in self._executors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        normalized: list[FollowerSyncResult] = []
        for result in results:
            if isinstance(result, FollowerSyncResult):
                normalized.append(result)
                await self._record_follower_result(result)
            elif isinstance(result, Exception):
                logger.bind(
                    symbol=request.symbol,
                    action=request.action,
                    result="unexpected_follower_error",
                ).opt(exception=result).error("unexpected follower settings sync failure")
        return normalized

    async def _build_executor(self, account: AccountCredentials) -> BybitOrderExecutor:
        if not account.has_credentials():
            logger.bind(account=account.name, action="follower_initialize", result="missing_credentials").error(
                "follower api_key/api_secret are required in accounts.json"
            )
            if self._runtime_state is not None:
                await self._runtime_state.record_follower_startup(
                    account.name,
                    success=False,
                    error="missing_credentials",
                )
            raise ValueError(f"follower {account.name!r} is missing api_key/api_secret")

        client = BybitHTTPClient(
            account,
            testnet=self._settings.testnet,
            api_call_attempts=self._settings.api_call_attempts,
            api_retry_initial_delay_seconds=self._settings.api_retry_initial_delay_seconds,
            api_retry_max_delay_seconds=self._settings.api_retry_max_delay_seconds,
            api_retry_jitter_ratio=self._settings.api_retry_jitter_ratio,
        )
        try:
            await client.validate_credentials()
        except Exception as exc:
            if self._runtime_state is not None:
                await self._runtime_state.record_follower_startup(account.name, success=False, error=str(exc))
            raise
        instrument_cache = InstrumentInfoCache(client, ttl_seconds=self._settings.instrument_cache_ttl_seconds)
        return BybitOrderExecutor(
            client,
            block_if_position_exists=self._settings.block_if_follower_position_exists,
            order_link_prefix=self._settings.mirror_order_link_prefix,
            clip_close_qty_to_follower_position=self._settings.clip_close_qty_to_follower_position,
            instrument_cache=instrument_cache,
            normalize_quantities=self._settings.normalize_order_quantities,
            position_sync_attempts=self._settings.follower_position_sync_attempts,
            position_sync_retry_delay_seconds=self._settings.follower_position_sync_retry_delay_seconds,
        )

    async def _mirror_one(
        self,
        executor: BybitOrderExecutor,
        request: MirrorOrderRequest,
    ) -> FollowerOrderResult:
        async with self._semaphore:
            try:
                if request.is_close:
                    return await executor.mirror_market_close(request)
                return await executor.mirror_market_open(request)
            except Exception as exc:
                logger.bind(
                    account=executor.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    quantity=str(request.qty),
                    result="unexpected_follower_error",
                    master_exec_id=request.master_exec_id,
                ).opt(exception=exc).error("unexpected follower replication failure")
                return FollowerOrderResult(
                    account_name=executor.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    qty=request.qty,
                    success=False,
                    result="unexpected_follower_error",
                    error=str(exc),
                )

    async def _sync_one(
        self,
        executor: BybitOrderExecutor,
        request: PositionSettingsSyncRequest,
    ) -> FollowerSyncResult:
        async with self._semaphore:
            try:
                return await executor.sync_position_settings(request)
            except Exception as exc:
                logger.bind(
                    account=executor.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    result="unexpected_follower_error",
                ).opt(exception=exc).error("unexpected follower settings sync failure")
                return FollowerSyncResult(
                    account_name=executor.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    success=False,
                    result="unexpected_follower_error",
                    error=str(exc),
                )

    async def _record_follower_result(self, result: FollowerOrderResult | FollowerSyncResult) -> None:
        if self._runtime_state is None:
            return
        await self._runtime_state.record_follower_result(result)
