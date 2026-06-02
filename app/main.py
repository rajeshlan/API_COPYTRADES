from __future__ import annotations

import argparse
import asyncio
import signal

from loguru import logger

from app.config import load_accounts, load_settings, public_settings_snapshot, validate_runtime_prerequisites
from app.constants import APP_NAME, PHASE
from app.core.dedupe import EventDeduplicator
from app.core.event_bus import EventBus
from app.core.follower_manager import FollowerManager
from app.core.risk_engine import Phase3PositionSyncPolicy, Phase4ExecutionMirrorPolicy
from app.core.state_manager import PositionSyncStateManager, RuntimeStateManager
from app.core.sync_engine import SyncEngine
from app.exchanges.bybit_client import BybitHTTPClient
from app.exchanges.execution_listener import MasterExecutionListener
from app.exchanges.websocket_manager import BybitPrivateWebSocketManager
from app.logger import configure_logging


async def async_main() -> int:
    parser = argparse.ArgumentParser(description="Bybit V5 event-driven copy trader")
    parser.add_argument(
        "--config-check",
        action="store_true",
        help="load configuration and accounts, then exit before network connections",
    )
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level, settings.project_root)

    logger.bind(app=APP_NAME, phase=PHASE, **public_settings_snapshot(settings)).info("engine starting")

    accounts = load_accounts(settings.accounts_file)
    logger.bind(
        account=accounts.master.name,
        followers_configured=len(accounts.followers),
        phase=PHASE,
    ).info("accounts configuration loaded")

    prerequisite_errors = validate_runtime_prerequisites(settings, accounts)
    if args.config_check:
        if prerequisite_errors:
            for error in prerequisite_errors:
                logger.bind(result="configuration_error").error(error)
            return 2
        logger.bind(result="ok").info("configuration check completed")
        return 0

    if not settings.enabled:
        logger.bind(action="kill_switch", result="disabled").warning(
            "COPY_TRADER_ENABLED is false; engine will not connect"
        )
        return 0

    if prerequisite_errors:
        for error in prerequisite_errors:
            logger.bind(result="configuration_error").error(error)
        return 2

    runtime_state = RuntimeStateManager(
        settings.runtime_state_file,
        enabled=settings.persist_runtime_state,
        max_event_keys=settings.dedupe_max_items,
    )
    await runtime_state.load()

    if settings.validate_master_on_startup:
        await BybitHTTPClient(
            accounts.master,
            testnet=settings.testnet,
            api_call_attempts=settings.api_call_attempts,
            api_retry_initial_delay_seconds=settings.api_retry_initial_delay_seconds,
            api_retry_max_delay_seconds=settings.api_retry_max_delay_seconds,
            api_retry_jitter_ratio=settings.api_retry_jitter_ratio,
        ).validate_credentials()

    event_bus = EventBus()
    follower_manager = FollowerManager(accounts.followers, settings, runtime_state=runtime_state)
    try:
        await follower_manager.initialize()
    except Exception as exc:
        logger.bind(action="follower_initialize", result="fatal").opt(exception=exc).error(
            "no usable follower execution path"
        )
        return 2

    sync_engine = SyncEngine(
        follower_manager=follower_manager,
        mirror_policy=Phase4ExecutionMirrorPolicy(),
        position_sync_policy=Phase3PositionSyncPolicy(
            sync_leverage=settings.sync_leverage,
            sync_tpsl=settings.sync_tpsl,
            sync_empty_tpsl_to_cancel=settings.sync_empty_tpsl_to_cancel,
            default_tp_trigger_by=settings.default_tp_trigger_by,
            default_sl_trigger_by=settings.default_sl_trigger_by,
        ),
        position_sync_state=PositionSyncStateManager(),
    )
    sync_engine.register(event_bus)

    dedupe = EventDeduplicator(
        ttl_seconds=settings.dedupe_ttl_seconds,
        max_items=settings.dedupe_max_items,
        initial_entries=runtime_state.persisted_event_keys(settings.dedupe_ttl_seconds),
    )
    listener = MasterExecutionListener(
        account_name=accounts.master.name,
        settings=settings,
        dedupe=dedupe,
        event_bus=event_bus,
        runtime_state=runtime_state,
    )
    websocket_manager = BybitPrivateWebSocketManager(accounts.master, settings)

    _install_signal_handlers(websocket_manager)
    await websocket_manager.run_forever(listener.handle_message)
    return 0


def _install_signal_handlers(websocket_manager: BybitPrivateWebSocketManager) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(websocket_manager.stop()))
        except NotImplementedError:
            # Windows uses KeyboardInterrupt handling around asyncio.run.
            return


def main() -> None:
    try:
        raise SystemExit(asyncio.run(async_main()))
    except KeyboardInterrupt:
        logger.bind(result="interrupted").warning("engine stopped by user")
        raise SystemExit(130)


if __name__ == "__main__":
    main()
