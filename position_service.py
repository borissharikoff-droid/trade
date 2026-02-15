"""
Position service helpers extracted from bot logic.
"""

from typing import Dict

from trading_core import calculate_trade_pnl, combine_realized_pnl


def build_auto_position(
    *,
    symbol: str,
    direction: str,
    entry: float,
    tp1: float,
    tp2: float,
    tp3: float,
    sl: float,
    amount: float,
    commission: float,
    bybit_qty: float,
) -> Dict:
    """Build normalized auto-trade position payload."""
    return {
        "symbol": symbol,
        "direction": direction,
        "entry": float(entry),
        "current": float(entry),
        "amount": float(amount),
        "tp": float(tp1),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tp3": float(tp3),
        "tp1_hit": False,
        "tp2_hit": False,
        "sl": float(sl),
        "commission": float(commission),
        "pnl": float(-commission),
        "bybit_qty": float(bybit_qty or 0),
        "original_amount": float(amount),
        "is_auto": True,
    }


def calculate_close_outcome(pos: Dict, close_price: float, leverage: float) -> Dict:
    """Calculate closure outcome for a position with standardized math."""
    amount = float(pos.get("amount", 0) or 0)
    pnl = calculate_trade_pnl(
        entry_price=float(pos.get("entry", 0) or 0),
        exit_price=float(close_price or 0),
        direction=str(pos.get("direction", "")),
        amount=amount,
        leverage=float(leverage or 0),
        commission=float(pos.get("commission", 0) or 0),
    )
    total_pnl_for_history = combine_realized_pnl(pnl, pos.get("realized_pnl", 0))
    returned = amount + pnl
    return {
        "pnl": pnl,
        "returned": returned,
        "total_pnl_for_history": total_pnl_for_history,
    }

