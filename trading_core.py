"""
Core trading math and close-reason helpers.
Centralizes PnL calculations to keep behavior consistent.
"""

from typing import Optional


def calculate_trade_pnl(
    *,
    entry_price: float,
    exit_price: float,
    direction: str,
    amount: float,
    leverage: float,
    commission: float = 0.0,
) -> float:
    """Calculate net PnL for a trade closure."""
    entry = float(entry_price or 0.0)
    exit_ = float(exit_price or 0.0)
    qty_usd = float(amount or 0.0)
    lev = float(leverage or 0.0)
    comm = float(commission or 0.0)

    if entry <= 0 or qty_usd <= 0 or lev <= 0:
        # For invalid or already-drained positions, only commission is relevant.
        return -comm

    direction_upper = str(direction or "").upper()
    if direction_upper == "LONG":
        pnl_percent = (exit_ - entry) / entry
    else:
        pnl_percent = (entry - exit_) / entry

    return qty_usd * lev * pnl_percent - comm


def combine_realized_pnl(current_pnl: float, realized_pnl: Optional[float]) -> float:
    """Combine current closure PnL with accumulated realized PnL."""
    return float(current_pnl or 0.0) + float(realized_pnl or 0.0)


def classify_pnl_reason(base_reason: str, pnl_value: float) -> str:
    """
    Keep explicit reason when provided, otherwise infer TP/SL from pnl sign.
    Useful for exchange sync closures.
    """
    reason = (base_reason or "").strip().upper()
    if reason:
        return reason
    return "TP" if float(pnl_value or 0.0) > 0 else "SL"

