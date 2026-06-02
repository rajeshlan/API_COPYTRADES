from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger
from pybit.unified_trading import HTTP

from app.config import AccountCredentials
from app.utils.helpers import redact_secret
from app.utils.retry import BybitAPIError, with_retry


class BybitHTTPClient:
    """Small async boundary around pybit's synchronous HTTP client."""

    def __init__(
        self,
        account: AccountCredentials,
        *,
        testnet: bool,
        api_call_attempts: int = 3,
        api_retry_initial_delay_seconds: float = 0.25,
        api_retry_max_delay_seconds: float = 2.0,
        api_retry_jitter_ratio: float = 0.2,
    ) -> None:
        self.account = account
        self.testnet = testnet
        self._api_call_attempts = api_call_attempts
        self._api_retry_initial_delay_seconds = api_retry_initial_delay_seconds
        self._api_retry_max_delay_seconds = api_retry_max_delay_seconds
        self._api_retry_jitter_ratio = api_retry_jitter_ratio
        self._session = HTTP(
            testnet=testnet,
            api_key=account.api_key,
            api_secret=account.api_secret,
        )

    async def validate_credentials(self) -> None:
        logger.bind(
            account=self.account.name,
            api_key=redact_secret(self.account.api_key),
            environment="testnet" if self.testnet else "mainnet",
        ).info("validating account connectivity")

        try:
            response = await self._call("get_wallet_balance", self._session.get_wallet_balance, accountType="UNIFIED")
        except Exception as exc:
            logger.bind(account=self.account.name, result="error").opt(exception=exc).error(
                "account validation failed"
            )
            raise

        self._raise_if_bybit_error(response)
        logger.bind(account=self.account.name, result="ok").info("account validation succeeded")

    async def get_positions(self, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._call("get_positions", self._session.get_positions, **kwargs)
        except Exception as exc:
            logger.bind(
                account=self.account.name,
                symbol=kwargs.get("symbol"),
                action="get_positions",
                result="error",
            ).opt(exception=exc).error("Bybit get_positions failed")
            raise

        self._raise_if_bybit_error(response)
        return response

    async def place_order(self, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._call("place_order", self._session.place_order, **kwargs)
        except Exception as exc:
            logger.bind(
                account=self.account.name,
                symbol=kwargs.get("symbol"),
                action="place_order",
                quantity=kwargs.get("qty"),
                result="error",
                order_link_id=kwargs.get("orderLinkId"),
            ).opt(exception=exc).error("Bybit place_order failed")
            raise

        self._raise_if_bybit_error(response)
        return response

    async def set_leverage(self, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._call("set_leverage", self._session.set_leverage, **kwargs)
        except Exception as exc:
            logger.bind(
                account=self.account.name,
                symbol=kwargs.get("symbol"),
                action="set_leverage",
                result="error",
            ).opt(exception=exc).error("Bybit set_leverage failed")
            raise

        self._raise_if_bybit_error(response)
        return response

    async def set_trading_stop(self, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._call("set_trading_stop", self._session.set_trading_stop, **kwargs)
        except Exception as exc:
            logger.bind(
                account=self.account.name,
                symbol=kwargs.get("symbol"),
                action="set_trading_stop",
                result="error",
            ).opt(exception=exc).error("Bybit set_trading_stop failed")
            raise

        self._raise_if_bybit_error(response)
        return response

    async def get_instruments_info(self, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._call("get_instruments_info", self._session.get_instruments_info, **kwargs)
        except Exception as exc:
            logger.bind(
                account=self.account.name,
                symbol=kwargs.get("symbol"),
                action="get_instruments_info",
                result="error",
            ).opt(exception=exc).error("Bybit get_instruments_info failed")
            raise

        self._raise_if_bybit_error(response)
        return response

    async def _call(self, operation_name: str, func, **kwargs: Any) -> dict[str, Any]:
        async def operation() -> dict[str, Any]:
            response = await asyncio.to_thread(func, **kwargs)
            self._raise_if_bybit_error(response)
            return response

        return await with_retry(
            operation_name,
            operation,
            attempts=self._api_call_attempts,
            initial_delay_seconds=self._api_retry_initial_delay_seconds,
            max_delay_seconds=self._api_retry_max_delay_seconds,
            jitter_ratio=self._api_retry_jitter_ratio,
            log_context={"account": self.account.name, "symbol": kwargs.get("symbol")},
        )

    @staticmethod
    def _raise_if_bybit_error(response: dict[str, Any]) -> None:
        ret_code = response.get("retCode")
        if ret_code not in (0, "0", None):
            ret_msg = response.get("retMsg", "unknown Bybit error")
            raise BybitAPIError(ret_code, ret_msg)
