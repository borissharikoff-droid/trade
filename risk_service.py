"""
Risk service primitives to unify common trading checks.
"""

from typing import Tuple


def validate_position_open(
    *,
    balance: float,
    bet_amount: float,
    min_balance_reserve: float,
    min_bet: float,
) -> Tuple[bool, str]:
    """Return (is_valid, reason) for position opening constraints."""
    b = float(balance or 0)
    bet = float(bet_amount or 0)
    if bet < float(min_bet or 0):
        return False, "bet_below_minimum"
    if b - bet < float(min_balance_reserve or 0):
        return False, "insufficient_reserve_after_open"
    return True, "ok"


def validate_daily_limit(today_count: int, max_daily: int) -> Tuple[bool, str]:
    if int(today_count or 0) >= int(max_daily or 0):
        return False, "daily_limit_reached"
    return True, "ok"


def validate_risk_per_trade(amount: float, balance: float, risk_percent: float) -> Tuple[bool, str]:
    amt = float(amount or 0)
    bal = float(balance or 0)
    risk = float(risk_percent or 0)
    if risk <= 0:
        return False, "risk_percent_invalid"
    max_amount = bal * risk
    if amt > max_amount:
        return False, "amount_exceeds_risk_limit"
    return True, "ok"

