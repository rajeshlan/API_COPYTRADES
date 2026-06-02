from __future__ import annotations

import asyncio
import hashlib
from decimal import Decimal
from typing import Any

from loguru import logger

from app.constants import ONE_WAY_POSITION_IDX
from app.core.replication_models import (
    FollowerOrderResult,
    FollowerPositionSnapshot,
    FollowerSyncResult,
    MirrorOrderRequest,
    PositionSettingsSyncRequest,
)
from app.exchanges.bybit_client import BybitHTTPClient
from app.exchanges.instrument_cache import InstrumentInfoCache
from app.utils.helpers import decimal_or_none, format_decimal
from app.utils.precision import normalize_order_quantity


class BybitOrderExecutor:
    def __init__(
        self,
        client: BybitHTTPClient,
        *,
        block_if_position_exists: bool,
        order_link_prefix: str,
        clip_close_qty_to_follower_position: bool = True,
        instrument_cache: InstrumentInfoCache | None = None,
        normalize_quantities: bool = False,
        position_sync_attempts: int = 3,
        position_sync_retry_delay_seconds: float = 0.5,
    ) -> None:
        self._client = client
        self._block_if_position_exists = block_if_position_exists
        self._order_link_prefix = order_link_prefix
        self._clip_close_qty_to_follower_position = clip_close_qty_to_follower_position
        self._instrument_cache = instrument_cache
        self._normalize_quantities = normalize_quantities
        self._position_sync_attempts = position_sync_attempts
        self._position_sync_retry_delay_seconds = position_sync_retry_delay_seconds

    @property
    def account_name(self) -> str:
        return self._client.account.name

    async def mirror_market_open(self, request: MirrorOrderRequest) -> FollowerOrderResult:
        order_link_id = self._build_order_link_id(request.master_exec_id)

        existing_position_result = await self._check_existing_position(request, order_link_id)
        if existing_position_result is not None:
            return existing_position_result

        qty_result = await self._normalize_quantity_for_order(request, request.qty, order_link_id)
        if isinstance(qty_result, FollowerOrderResult):
            return qty_result

        order_qty = qty_result
        try:
            response = await self._client.place_order(
                category=request.category,
                symbol=request.symbol,
                side=request.side,
                orderType="Market",
                qty=format_decimal(order_qty),
                positionIdx=request.position_idx,
                reduceOnly=False,
                orderLinkId=order_link_id,
            )
        except Exception as exc:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(order_qty),
                result="error",
                order_link_id=order_link_id,
                master_exec_id=request.master_exec_id,
            ).opt(exception=exc).error("follower market order failed")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=order_qty,
                success=False,
                result="order_error",
                order_link_id=order_link_id,
                error=str(exc),
            )

        result = response.get("result", {}) if isinstance(response, dict) else {}
        order_id = result.get("orderId")
        ret_code = response.get("retCode") if isinstance(response, dict) else None
        ret_msg = response.get("retMsg") if isinstance(response, dict) else None
        logger.bind(
            account=self.account_name,
            symbol=request.symbol,
            action=request.action,
            quantity=str(order_qty),
            result="submitted",
            order_id=order_id,
            order_link_id=order_link_id,
            ret_code=ret_code,
            ret_msg=ret_msg,
            master_exec_id=request.master_exec_id,
        ).info("follower market order submitted")
        return FollowerOrderResult(
            account_name=self.account_name,
            symbol=request.symbol,
            action=request.action,
            qty=order_qty,
            success=True,
            result="submitted",
            order_id=order_id,
            order_link_id=order_link_id,
            ret_code=ret_code,
            ret_msg=ret_msg,
        )

    async def mirror_market_close(self, request: MirrorOrderRequest) -> FollowerOrderResult:
        order_link_id = self._build_order_link_id(request.master_exec_id)
        close_qty_result = await self._resolve_close_quantity(request, order_link_id)
        if isinstance(close_qty_result, FollowerOrderResult):
            return close_qty_result

        close_qty = close_qty_result
        qty_result = await self._normalize_quantity_for_order(request, close_qty, order_link_id)
        if isinstance(qty_result, FollowerOrderResult):
            return qty_result
        close_qty = qty_result
        try:
            response = await self._client.place_order(
                category=request.category,
                symbol=request.symbol,
                side=request.side,
                orderType="Market",
                qty=format_decimal(close_qty),
                positionIdx=request.position_idx,
                reduceOnly=True,
                orderLinkId=order_link_id,
            )
        except Exception as exc:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(close_qty),
                result="error",
                order_link_id=order_link_id,
                master_exec_id=request.master_exec_id,
            ).opt(exception=exc).error("follower reduce-only close order failed")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=close_qty,
                success=False,
                result="close_order_error",
                order_link_id=order_link_id,
                error=str(exc),
            )

        result = response.get("result", {}) if isinstance(response, dict) else {}
        order_id = result.get("orderId")
        ret_code = response.get("retCode") if isinstance(response, dict) else None
        ret_msg = response.get("retMsg") if isinstance(response, dict) else None
        logger.bind(
            account=self.account_name,
            symbol=request.symbol,
            action=request.action,
            quantity=str(close_qty),
            result="submitted",
            order_id=order_id,
            order_link_id=order_link_id,
            reduce_only=True,
            ret_code=ret_code,
            ret_msg=ret_msg,
            master_exec_id=request.master_exec_id,
        ).info("follower reduce-only close order submitted")
        return FollowerOrderResult(
            account_name=self.account_name,
            symbol=request.symbol,
            action=request.action,
            qty=close_qty,
            success=True,
            result="submitted",
            order_id=order_id,
            order_link_id=order_link_id,
            ret_code=ret_code,
            ret_msg=ret_msg,
        )

    async def sync_position_settings(self, request: PositionSettingsSyncRequest) -> FollowerSyncResult:
        errors: list[str] = []
        leverage_synced = False
        tpsl_synced = False

        if request.has_leverage:
            try:
                response = await self._client.set_leverage(
                    category=request.category,
                    symbol=request.symbol,
                    buyLeverage=format_decimal(request.leverage),
                    sellLeverage=format_decimal(request.leverage),
                )
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action="sync_leverage",
                    result="synced",
                    leverage=str(request.leverage),
                    ret_code=response.get("retCode"),
                    ret_msg=response.get("retMsg"),
                ).info("follower leverage synced")
                leverage_synced = True
            except Exception as exc:
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action="sync_leverage",
                    result="error",
                    leverage=str(request.leverage),
                ).opt(exception=exc).error("follower leverage sync failed")
                errors.append(f"leverage:{exc}")

        if request.has_tpsl_state:
            position = await self._wait_for_matching_position(request)
            if position is None:
                result = "partial_error" if errors or leverage_synced else "skipped_no_matching_position"
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action="sync_trading_stop",
                    result=result,
                    master_side=request.side,
                ).warning("follower matching position not available for TP/SL sync")
                return FollowerSyncResult(
                    account_name=self.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    success=False,
                    skipped=not errors,
                    result=result,
                    leverage_synced=leverage_synced,
                    tpsl_synced=False,
                    error="; ".join(errors) if errors else None,
                )

            try:
                response = await self._client.set_trading_stop(**self._trading_stop_params(request))
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action="sync_trading_stop",
                    result="synced",
                    take_profit=str(request.take_profit) if request.take_profit is not None else None,
                    stop_loss=str(request.stop_loss) if request.stop_loss is not None else None,
                    ret_code=response.get("retCode"),
                    ret_msg=response.get("retMsg"),
                ).info("follower TP/SL synced")
                tpsl_synced = True
            except Exception as exc:
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action="sync_trading_stop",
                    result="error",
                    take_profit=str(request.take_profit) if request.take_profit is not None else None,
                    stop_loss=str(request.stop_loss) if request.stop_loss is not None else None,
                ).opt(exception=exc).error("follower TP/SL sync failed")
                errors.append(f"tpsl:{exc}")

        success = not errors and (leverage_synced or tpsl_synced)
        return FollowerSyncResult(
            account_name=self.account_name,
            symbol=request.symbol,
            action=request.action,
            success=success,
            skipped=False,
            result="synced" if success else "partial_error",
            leverage_synced=leverage_synced,
            tpsl_synced=tpsl_synced,
            error="; ".join(errors) if errors else None,
        )

    async def _check_existing_position(
        self,
        request: MirrorOrderRequest,
        order_link_id: str,
    ) -> FollowerOrderResult | None:
        if not self._block_if_position_exists:
            return None

        try:
            position = await self.get_one_way_position(request.category, request.symbol)
        except Exception as exc:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(request.qty),
                result="position_check_error",
                master_exec_id=request.master_exec_id,
            ).opt(exception=exc).error("follower position safety check failed")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=request.qty,
                success=False,
                result="position_check_error",
                order_link_id=order_link_id,
                error=str(exc),
            )

        if position is None or not position.is_open:
            return None

        logger.bind(
            account=self.account_name,
            symbol=request.symbol,
            action=request.action,
            quantity=str(request.qty),
            result="skipped_existing_position",
            follower_position_side=position.side,
            follower_position_size=str(position.size),
            master_exec_id=request.master_exec_id,
        ).warning("follower already has an open one-way position; mirror open skipped")
        return FollowerOrderResult(
            account_name=self.account_name,
            symbol=request.symbol,
            action=request.action,
            qty=request.qty,
            success=False,
            skipped=True,
            result="skipped_existing_position",
            order_link_id=order_link_id,
        )

    async def _normalize_quantity_for_order(
        self,
        request: MirrorOrderRequest,
        qty: Decimal,
        order_link_id: str,
    ) -> Decimal | FollowerOrderResult:
        if not self._normalize_quantities or self._instrument_cache is None:
            return qty

        try:
            spec = await self._instrument_cache.get_spec(request.category, request.symbol)
            normalized = normalize_order_quantity(qty, spec)
        except Exception as exc:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(qty),
                result="quantity_normalization_error",
                order_link_id=order_link_id,
            ).opt(exception=exc).error("quantity normalization failed")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=qty,
                success=False,
                result="quantity_normalization_error",
                order_link_id=order_link_id,
                error=str(exc),
            )

        if not normalized.valid:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(qty),
                normalized_quantity=str(normalized.normalized_qty),
                result=f"skipped_{normalized.reason}",
                order_link_id=order_link_id,
            ).warning("order quantity invalid after precision normalization")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=normalized.normalized_qty,
                success=False,
                skipped=True,
                result=f"skipped_{normalized.reason}",
                order_link_id=order_link_id,
            )

        if normalized.normalized_qty != qty or normalized.clipped_to_max:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(qty),
                normalized_quantity=str(normalized.normalized_qty),
                clipped_to_max=normalized.clipped_to_max,
                result="quantity_normalized",
                order_link_id=order_link_id,
            ).info("order quantity normalized to instrument precision")
        return normalized.normalized_qty

    async def _resolve_close_quantity(
        self,
        request: MirrorOrderRequest,
        order_link_id: str,
    ) -> Decimal | FollowerOrderResult:
        try:
            position = await self.get_one_way_position(request.category, request.symbol)
        except Exception as exc:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(request.qty),
                result="position_check_error",
                master_exec_id=request.master_exec_id,
            ).opt(exception=exc).error("follower close position safety check failed")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=request.qty,
                success=False,
                result="position_check_error",
                order_link_id=order_link_id,
                error=str(exc),
            )

        if position is None or not position.is_open:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(request.qty),
                result="skipped_no_position",
                master_exec_id=request.master_exec_id,
            ).warning("follower has no open position; reduce-only close skipped")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=request.qty,
                success=False,
                skipped=True,
                result="skipped_no_position",
                order_link_id=order_link_id,
            )

        expected_side = _position_side_reduced_by_order_side(request.side)
        if position.side != expected_side:
            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(request.qty),
                result="skipped_side_mismatch",
                follower_side=position.side,
                expected_side=expected_side,
                follower_size=str(position.size),
                master_exec_id=request.master_exec_id,
            ).warning("follower position side does not match master close direction; reduce-only close skipped")
            return FollowerOrderResult(
                account_name=self.account_name,
                symbol=request.symbol,
                action=request.action,
                qty=request.qty,
                success=False,
                skipped=True,
                result="skipped_side_mismatch",
                order_link_id=order_link_id,
            )

        if position.size < request.qty:
            if not self._clip_close_qty_to_follower_position:
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    quantity=str(request.qty),
                    result="skipped_insufficient_position",
                    follower_size=str(position.size),
                    master_exec_id=request.master_exec_id,
                ).warning("follower position smaller than master close; reduce-only close skipped")
                return FollowerOrderResult(
                    account_name=self.account_name,
                    symbol=request.symbol,
                    action=request.action,
                    qty=request.qty,
                    success=False,
                    skipped=True,
                    result="skipped_insufficient_position",
                    order_link_id=order_link_id,
                )

            logger.bind(
                account=self.account_name,
                symbol=request.symbol,
                action=request.action,
                quantity=str(request.qty),
                result="close_qty_clipped",
                clipped_quantity=str(position.size),
                follower_size=str(position.size),
                master_exec_id=request.master_exec_id,
            ).warning("follower close quantity clipped to available position size")
            return position.size

        return request.qty

    async def get_one_way_position(self, category: str, symbol: str) -> FollowerPositionSnapshot | None:
        response = await self._client.get_positions(category=category, symbol=symbol)
        rows = response.get("result", {}).get("list", [])
        if not isinstance(rows, list):
            raise RuntimeError(f"unexpected get_positions response shape: {response}")

        for row in rows:
            if not isinstance(row, dict):
                continue
            position_idx = int(row.get("positionIdx", ONE_WAY_POSITION_IDX))
            if position_idx != ONE_WAY_POSITION_IDX:
                continue
            size = decimal_or_none(row.get("size")) or Decimal("0")
            return FollowerPositionSnapshot(
                account_name=self.account_name,
                category=str(row.get("category") or category),
                symbol=str(row.get("symbol") or symbol),
                position_idx=position_idx,
                side=str(row.get("side")) if row.get("side") else None,
                size=size,
            )
        return None

    async def _wait_for_matching_position(
        self,
        request: PositionSettingsSyncRequest,
    ) -> FollowerPositionSnapshot | None:
        for attempt in range(1, self._position_sync_attempts + 1):
            position = await self.get_one_way_position(request.category, request.symbol)
            if position is not None and position.is_open and position.side == request.side:
                return position
            if position is not None and position.is_open and position.side != request.side:
                logger.bind(
                    account=self.account_name,
                    symbol=request.symbol,
                    action="sync_trading_stop",
                    result="skipped_side_mismatch",
                    follower_side=position.side,
                    master_side=request.side,
                    follower_size=str(position.size),
                ).warning("follower side differs from master; TP/SL sync skipped")
                return None
            if attempt < self._position_sync_attempts:
                await asyncio.sleep(self._position_sync_retry_delay_seconds)
        return None

    @staticmethod
    def _trading_stop_params(request: PositionSettingsSyncRequest) -> dict[str, Any]:
        params: dict[str, Any] = {
            "category": request.category,
            "symbol": request.symbol,
            "tpslMode": request.tpsl_mode,
            "positionIdx": request.position_idx,
            "tpOrderType": "Market",
            "slOrderType": "Market",
        }
        if request.take_profit is not None:
            params["takeProfit"] = format_decimal(request.take_profit)
        if request.stop_loss is not None:
            params["stopLoss"] = format_decimal(request.stop_loss)
        if request.tp_trigger_by:
            params["tpTriggerBy"] = request.tp_trigger_by
        if request.sl_trigger_by:
            params["slTriggerBy"] = request.sl_trigger_by
        return params

    def _build_order_link_id(self, master_exec_id: str) -> str:
        digest = hashlib.sha256(f"{self.account_name}:{master_exec_id}".encode("utf-8")).hexdigest()
        return f"{self._order_link_prefix}-{digest[:29]}"


def _position_side_reduced_by_order_side(order_side: str) -> str | None:
    if order_side == "Sell":
        return "Buy"
    if order_side == "Buy":
        return "Sell"
    return None
