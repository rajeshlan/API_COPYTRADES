from __future__ import annotations

import time
from decimal import Decimal

from loguru import logger

from app.exchanges.bybit_client import BybitHTTPClient
from app.utils.helpers import decimal_or_none
from app.utils.precision import InstrumentSpec


class InstrumentInfoCache:
    def __init__(self, client: BybitHTTPClient, *, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise ValueError("instrument cache ttl must be positive")
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._cache: dict[tuple[str, str], tuple[float, InstrumentSpec]] = {}

    async def get_spec(self, category: str, symbol: str) -> InstrumentSpec:
        key = (category, symbol)
        cached = self._cache.get(key)
        now = time.time()
        if cached is not None and now - cached[0] <= self._ttl_seconds:
            return cached[1]

        response = await self._client.get_instruments_info(category=category, symbol=symbol)
        rows = response.get("result", {}).get("list", [])
        if not isinstance(rows, list) or not rows:
            raise RuntimeError(f"instrument info not found for {category}:{symbol}")

        row = rows[0]
        lot_size = row.get("lotSizeFilter", {})
        if not isinstance(lot_size, dict):
            raise RuntimeError(f"instrument info missing lotSizeFilter for {category}:{symbol}")

        qty_step = decimal_or_none(lot_size.get("qtyStep"))
        min_order_qty = decimal_or_none(lot_size.get("minOrderQty"))
        max_market_order_qty = decimal_or_none(lot_size.get("maxMktOrderQty")) or decimal_or_none(
            lot_size.get("maxMarketOrderQty")
        )
        if qty_step is None or min_order_qty is None:
            raise RuntimeError(f"instrument info missing qtyStep/minOrderQty for {category}:{symbol}")

        spec = InstrumentSpec(
            category=category,
            symbol=symbol,
            qty_step=qty_step,
            min_order_qty=min_order_qty,
            max_market_order_qty=max_market_order_qty,
        )
        self._cache[key] = (now, spec)
        logger.bind(
            action="instrument_info_cache",
            result="loaded",
            category=category,
            symbol=symbol,
            qty_step=str(spec.qty_step),
            min_order_qty=str(spec.min_order_qty),
            max_market_order_qty=str(spec.max_market_order_qty) if spec.max_market_order_qty else None,
        ).info("instrument precision metadata loaded")
        return spec
