from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlencode

import websockets
from loguru import logger

from app.config import AccountCredentials, AppSettings
from app.constants import BYBIT_PRIVATE_WS_MAINNET, BYBIT_PRIVATE_WS_TESTNET
from app.utils.retry import ExponentialBackoff

RawMessageHandler = Callable[[dict[str, Any]], Awaitable[None]]


class BybitPrivateWebSocketManager:
    """Async Bybit V5 private websocket connection with explicit reconnect control."""

    def __init__(self, account: AccountCredentials, settings: AppSettings) -> None:
        self._account = account
        self._settings = settings
        self._stop_event = asyncio.Event()
        self._backoff = ExponentialBackoff(
            initial_delay_seconds=settings.reconnect_initial_delay_seconds,
            max_delay_seconds=settings.reconnect_max_delay_seconds,
            jitter_ratio=settings.reconnect_jitter_ratio,
        )

    async def run_forever(self, on_message: RawMessageHandler) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_once(on_message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                delay = self._backoff.next_delay()
                logger.bind(
                    account=self._account.name,
                    action="websocket_reconnect",
                    result="scheduled",
                    delay_seconds=round(delay, 3),
                ).opt(exception=exc).error("private websocket disconnected")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                except TimeoutError:
                    continue

    async def stop(self) -> None:
        self._stop_event.set()

    async def _connect_once(self, on_message: RawMessageHandler) -> None:
        url = self._ws_url()
        logger.bind(
            account=self._account.name,
            action="websocket_connect",
            result="starting",
            environment=self._settings.env_name,
            topics=list(self._settings.ws_topics),
        ).info("connecting Bybit private websocket")

        async with websockets.connect(
            url,
            open_timeout=self._settings.ws_connect_timeout_seconds,
            ping_interval=None,
            close_timeout=5,
            max_queue=1024,
        ) as websocket:
            await self._authenticate(websocket)
            await self._subscribe(websocket)
            self._backoff.reset()

            heartbeat_task = asyncio.create_task(self._heartbeat(websocket))
            try:
                await self._read_loop(websocket, on_message)
            finally:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def _authenticate(self, websocket: Any) -> None:
        expires = int((time.time() + self._settings.ws_auth_timeout_seconds) * 1000)
        signature = hmac.new(
            self._account.api_secret.encode("utf-8"),
            f"GET/realtime{expires}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        req_id = f"auth-{uuid.uuid4()}"
        await websocket.send(
            json.dumps(
                {
                    "req_id": req_id,
                    "op": "auth",
                    "args": [self._account.api_key, expires, signature],
                }
            )
        )
        response = await self._recv_json(websocket, timeout=self._settings.ws_auth_timeout_seconds)
        if response.get("op") != "auth" or not response.get("success", False):
            raise RuntimeError(f"Bybit websocket authentication failed: {response}")

        logger.bind(
            account=self._account.name,
            action="websocket_auth",
            result="ok",
            conn_id=response.get("conn_id"),
        ).info("private websocket authenticated")

    async def _subscribe(self, websocket: Any) -> None:
        req_id = f"sub-{uuid.uuid4()}"
        await websocket.send(json.dumps({"req_id": req_id, "op": "subscribe", "args": list(self._settings.ws_topics)}))
        response = await self._recv_json(websocket, timeout=self._settings.ws_auth_timeout_seconds)
        if response.get("op") != "subscribe" or not response.get("success", False):
            raise RuntimeError(f"Bybit websocket subscription failed: {response}")

        logger.bind(
            account=self._account.name,
            action="websocket_subscribe",
            result="ok",
            topics=list(self._settings.ws_topics),
            conn_id=response.get("conn_id"),
        ).info("private websocket subscribed")

    async def _heartbeat(self, websocket: Any) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(self._settings.ws_ping_interval_seconds)
            payload = {"req_id": f"ping-{uuid.uuid4()}", "op": "ping"}
            await websocket.send(json.dumps(payload))
            logger.bind(account=self._account.name, action="websocket_ping", result="sent").debug(
                "private websocket heartbeat sent"
            )

    async def _read_loop(self, websocket: Any, on_message: RawMessageHandler) -> None:
        async for raw in websocket:
            message = self._loads(raw)
            if self._is_control_message(message):
                self._handle_control_message(message)
                continue
            if "topic" not in message:
                logger.bind(
                    account=self._account.name,
                    action="websocket_message",
                    result="ignored",
                    message=message,
                ).debug("ignored websocket message without topic")
                continue
            await on_message(message)

    async def _recv_json(self, websocket: Any, timeout: float) -> dict[str, Any]:
        raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
        return self._loads(raw)

    @staticmethod
    def _loads(raw: str | bytes) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        message = json.loads(raw)
        if not isinstance(message, dict):
            raise ValueError(f"expected websocket message object, got {type(message)!r}")
        return message

    @staticmethod
    def _is_control_message(message: dict[str, Any]) -> bool:
        return message.get("op") in {"auth", "subscribe", "pong", "ping"} or "success" in message

    def _handle_control_message(self, message: dict[str, Any]) -> None:
        op = message.get("op", "unknown")
        success = message.get("success")
        if success is False:
            logger.bind(
                account=self._account.name,
                action=f"websocket_{op}",
                result="error",
                message=message,
            ).error("websocket control message reported failure")
            return
        logger.bind(
            account=self._account.name,
            action=f"websocket_{op}",
            result="ok",
            message=message,
        ).debug("websocket control message received")

    def _ws_url(self) -> str:
        base_url = BYBIT_PRIVATE_WS_TESTNET if self._settings.testnet else BYBIT_PRIVATE_WS_MAINNET
        if not self._settings.ws_max_active_time:
            return base_url
        return f"{base_url}?{urlencode({'max_active_time': self._settings.ws_max_active_time})}"
