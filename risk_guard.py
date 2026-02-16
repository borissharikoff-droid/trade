"""
Risk-guard checks and counters for user-level trading protection.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional, Tuple

from db_core import run_sql
from execution_config import MAX_DIRECTIONAL_EXPOSURE_PCT, MAX_EQUITY_DRAWDOWN_PCT

DEFAULT_RISK_PER_TRADE = 0.015
DEFAULT_DAILY_STOP_LOSS = -0.03
DEFAULT_MAX_CONSECUTIVE_LOSSES = 5


def _today_iso() -> str:
    return datetime.utcnow().date().isoformat()


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _get_risk_row(user_id: int) -> Dict:
    row = run_sql(
        """
        SELECT user_id, balance, total_deposit, risk_per_trade, daily_stop_loss, max_consecutive_losses,
               daily_pnl, consecutive_losses, last_trade_date
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
        fetch="one",
    )
    return row or {}


def _reset_daily_if_needed(user_id: int, row: Dict) -> Dict:
    today = _today_iso()
    last_trade_date = (row.get("last_trade_date") or "").strip()
    if last_trade_date and last_trade_date == today:
        return row

    # Keep losing streak across days; reset only daily PnL window.
    run_sql(
        "UPDATE users SET daily_pnl = 0, last_trade_date = ? WHERE user_id = ?",
        (today, user_id),
    )
    row["daily_pnl"] = 0.0
    row["last_trade_date"] = today
    return row


def check_risk_per_trade(user_id: int, amount: float, balance: Optional[float] = None) -> Tuple[bool, str]:
    row = _get_risk_row(user_id)
    if not row:
        return False, "user_not_found"

    risk_per_trade = _to_float(row.get("risk_per_trade"), DEFAULT_RISK_PER_TRADE)
    if risk_per_trade <= 0:
        risk_per_trade = DEFAULT_RISK_PER_TRADE

    current_balance = _to_float(balance, _to_float(row.get("balance"), 0.0))
    max_amount = current_balance * risk_per_trade
    if _to_float(amount, 0.0) > max_amount:
        return (
            False,
            f"risk_per_trade_limit_exceeded amount={amount:.2f} max={max_amount:.2f}",
        )
    return True, "ok"


def check_daily_stop_loss(user_id: int) -> Tuple[bool, str]:
    row = _get_risk_row(user_id)
    if not row:
        return False, "user_not_found"
    row = _reset_daily_if_needed(user_id, row)

    daily_stop_loss = _to_float(row.get("daily_stop_loss"), DEFAULT_DAILY_STOP_LOSS)
    if daily_stop_loss == 0:
        return True, "ok"
    threshold_ratio = abs(daily_stop_loss)

    balance = max(_to_float(row.get("balance"), 0.0), 1.0)
    max_daily_loss_abs = balance * threshold_ratio
    daily_pnl = _to_float(row.get("daily_pnl"), 0.0)
    if daily_pnl <= -max_daily_loss_abs:
        return (
            False,
            f"daily_stop_loss_hit daily_pnl={daily_pnl:.2f} limit=-{max_daily_loss_abs:.2f}",
        )
    return True, "ok"


def check_consecutive_losses(user_id: int, max_losses: Optional[int] = None) -> Tuple[bool, str]:
    row = _get_risk_row(user_id)
    if not row:
        return False, "user_not_found"

    configured_limit = _to_int(row.get("max_consecutive_losses"), DEFAULT_MAX_CONSECUTIVE_LOSSES)
    limit = _to_int(max_losses, configured_limit) if max_losses is not None else configured_limit
    if limit <= 0:
        limit = DEFAULT_MAX_CONSECUTIVE_LOSSES

    consecutive = _to_int(row.get("consecutive_losses"), 0)
    if consecutive >= limit:
        return False, f"consecutive_losses_limit_hit current={consecutive} limit={limit}"
    return True, "ok"


def update_daily_pnl(user_id: int, pnl: float) -> None:
    row = _get_risk_row(user_id)
    if not row:
        return
    row = _reset_daily_if_needed(user_id, row)

    today = _today_iso()
    daily_pnl = _to_float(row.get("daily_pnl"), 0.0) + _to_float(pnl, 0.0)
    consecutive = _to_int(row.get("consecutive_losses"), 0)
    if _to_float(pnl, 0.0) < 0:
        consecutive += 1
    else:
        consecutive = 0

    run_sql(
        """
        UPDATE users
        SET daily_pnl = ?, consecutive_losses = ?, last_trade_date = ?
        WHERE user_id = ?
        """,
        (daily_pnl, consecutive, today, user_id),
    )


def get_risk_status(user_id: int) -> Dict:
    row = _get_risk_row(user_id)
    if not row:
        return {"user_id": user_id, "error": "user_not_found"}
    row = _reset_daily_if_needed(user_id, row)

    daily_ok, daily_reason = check_daily_stop_loss(user_id)
    streak_ok, streak_reason = check_consecutive_losses(user_id)
    return {
        "user_id": user_id,
        "risk_per_trade": _to_float(row.get("risk_per_trade"), DEFAULT_RISK_PER_TRADE),
        "daily_stop_loss": _to_float(row.get("daily_stop_loss"), DEFAULT_DAILY_STOP_LOSS),
        "max_consecutive_losses": _to_int(row.get("max_consecutive_losses"), DEFAULT_MAX_CONSECUTIVE_LOSSES),
        "daily_pnl": _to_float(row.get("daily_pnl"), 0.0),
        "consecutive_losses": _to_int(row.get("consecutive_losses"), 0),
        "last_trade_date": row.get("last_trade_date"),
        "blocked": (not daily_ok) or (not streak_ok),
        "daily_stop_ok": daily_ok,
        "daily_stop_reason": daily_reason,
        "loss_streak_ok": streak_ok,
        "loss_streak_reason": streak_reason,
    }


def check_total_directional_exposure(
    user_id: int,
    *,
    direction: str,
    new_amount: float,
    max_exposure_percent: float = MAX_DIRECTIONAL_EXPOSURE_PCT,
) -> Tuple[bool, str]:
    row = _get_risk_row(user_id)
    if not row:
        return False, "user_not_found"
    balance = max(_to_float(row.get("balance"), 0.0), 1.0)
    current = run_sql(
        """
        SELECT COALESCE(SUM(amount), 0) AS exposure
        FROM positions
        WHERE user_id = ? AND UPPER(direction) = ?
        """,
        (user_id, str(direction or "").upper()),
        fetch="one",
    ) or {}
    total = _to_float(current.get("exposure"), 0.0) + _to_float(new_amount, 0.0)
    pct = (total / balance) * 100.0
    if pct > float(max_exposure_percent):
        return False, f"directional_exposure_limit_hit direction={direction} exposure_pct={pct:.2f} limit_pct={max_exposure_percent:.2f}"
    return True, "ok"


def check_portfolio_drawdown(user_id: int, max_drawdown_percent: float = MAX_EQUITY_DRAWDOWN_PCT) -> Tuple[bool, str]:
    row = _get_risk_row(user_id)
    if not row:
        return False, "user_not_found"
    balance = _to_float(row.get("balance"), 0.0)
    deposit = _to_float(row.get("total_deposit"), 0.0)
    if deposit <= 0:
        return True, "ok"
    drawdown_pct = max(0.0, ((deposit - balance) / deposit) * 100.0)
    if drawdown_pct >= float(max_drawdown_percent):
        return False, f"max_drawdown_hit drawdown_pct={drawdown_pct:.2f} limit_pct={max_drawdown_percent:.2f}"
    return True, "ok"
