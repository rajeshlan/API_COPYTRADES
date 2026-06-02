from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from pydantic import BaseModel, ConfigDict


def round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


class InstrumentSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    category: str
    symbol: str
    qty_step: Decimal
    min_order_qty: Decimal
    max_market_order_qty: Decimal | None = None


class QuantityNormalizationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    original_qty: Decimal
    normalized_qty: Decimal
    clipped_to_max: bool = False
    valid: bool
    reason: str = "ok"


def normalize_order_quantity(qty: Decimal, spec: InstrumentSpec) -> QuantityNormalizationResult:
    clipped_to_max = False
    working_qty = qty
    if spec.max_market_order_qty is not None and working_qty > spec.max_market_order_qty:
        working_qty = spec.max_market_order_qty
        clipped_to_max = True

    normalized_qty = round_down_to_step(working_qty, spec.qty_step)
    if normalized_qty <= 0:
        return QuantityNormalizationResult(
            original_qty=qty,
            normalized_qty=normalized_qty,
            clipped_to_max=clipped_to_max,
            valid=False,
            reason="rounded_to_zero",
        )

    if normalized_qty < spec.min_order_qty:
        return QuantityNormalizationResult(
            original_qty=qty,
            normalized_qty=normalized_qty,
            clipped_to_max=clipped_to_max,
            valid=False,
            reason="below_min_order_qty",
        )

    return QuantityNormalizationResult(
        original_qty=qty,
        normalized_qty=normalized_qty,
        clipped_to_max=clipped_to_max,
        valid=True,
    )
