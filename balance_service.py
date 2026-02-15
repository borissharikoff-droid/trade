"""
Balance service helpers for atomic-like updates around trade closures.
"""

from typing import Dict


def apply_close_delta(user: Dict, returned: float, pnl: float, sanitize_balance) -> Dict:
    """
    Apply close-trade balance/profit deltas to user dict.
    Returns same dict for chaining.
    """
    user["balance"] = sanitize_balance(float(user.get("balance", 0) or 0) + float(returned or 0))
    user["total_profit"] = float(user.get("total_profit", 0) or 0) + float(pnl or 0)
    return user

