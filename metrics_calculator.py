"""
Trade metrics calculator and persistence helpers.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from db_core import run_sql


def ensure_user_metrics_table() -> None:
    run_sql(
        """
        CREATE TABLE IF NOT EXISTS user_metrics (
            user_id BIGINT PRIMARY KEY,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            winrate REAL DEFAULT 0,
            avg_win REAL DEFAULT 0,
            avg_loss REAL DEFAULT 0,
            expectancy REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0,
            max_drawdown_pct REAL DEFAULT 0,
            profit_factor REAL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _fetch_history(user_id: int, since_iso: Optional[str] = None) -> List[Dict]:
    if since_iso:
        return run_sql(
            """
            SELECT pnl, closed_at
            FROM history
            WHERE user_id = ? AND closed_at >= ?
            ORDER BY closed_at ASC
            """,
            (user_id, since_iso),
            fetch="all",
        ) or []
    return run_sql(
        """
        SELECT pnl, closed_at
        FROM history
        WHERE user_id = ?
        ORDER BY closed_at ASC
        """,
        (user_id,),
        fetch="all",
    ) or []


def calculate_winrate(trades: List[Dict]) -> float:
    if not trades:
        return 0.0
    wins = sum(1 for trade in trades if _to_float(trade.get("pnl"), 0.0) > 0)
    return round((wins / len(trades)) * 100, 4)


def calculate_avg_win_loss(trades: List[Dict]) -> Tuple[float, float]:
    wins = [_to_float(trade.get("pnl"), 0.0) for trade in trades if _to_float(trade.get("pnl"), 0.0) > 0]
    losses = [_to_float(trade.get("pnl"), 0.0) for trade in trades if _to_float(trade.get("pnl"), 0.0) < 0]
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    return round(avg_win, 8), round(avg_loss, 8)


def calculate_expectancy(trades: List[Dict]) -> float:
    if not trades:
        return 0.0
    total_pnl = sum(_to_float(trade.get("pnl"), 0.0) for trade in trades)
    return round(total_pnl / len(trades), 8)


def calculate_max_drawdown(trades: List[Dict]) -> Tuple[float, float]:
    if not trades:
        return 0.0, 0.0
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    max_dd_pct = 0.0
    for trade in trades:
        equity += _to_float(trade.get("pnl"), 0.0)
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_dd:
            max_dd = drawdown
            if peak > 0:
                max_dd_pct = (drawdown / peak) * 100
            else:
                max_dd_pct = 0.0
    return round(max_dd, 8), round(max_dd_pct, 8)


def calculate_metrics_from_trades(trades: List[Dict]) -> Dict:
    total_trades = len(trades)
    wins = sum(1 for trade in trades if _to_float(trade.get("pnl"), 0.0) > 0)
    losses = sum(1 for trade in trades if _to_float(trade.get("pnl"), 0.0) < 0)
    total_pnl = sum(_to_float(trade.get("pnl"), 0.0) for trade in trades)
    winrate = calculate_winrate(trades)
    avg_win, avg_loss = calculate_avg_win_loss(trades)
    expectancy = calculate_expectancy(trades)
    max_drawdown, max_drawdown_pct = calculate_max_drawdown(trades)

    gross_profit = sum(_to_float(trade.get("pnl"), 0.0) for trade in trades if _to_float(trade.get("pnl"), 0.0) > 0)
    gross_loss = abs(sum(_to_float(trade.get("pnl"), 0.0) for trade in trades if _to_float(trade.get("pnl"), 0.0) < 0))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    return {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "total_pnl": round(total_pnl, 8),
        "winrate": round(winrate, 4),
        "avg_win": round(avg_win, 8),
        "avg_loss": round(avg_loss, 8),
        "expectancy": round(expectancy, 8),
        "max_drawdown": round(max_drawdown, 8),
        "max_drawdown_pct": round(max_drawdown_pct, 8),
        "profit_factor": round(profit_factor, 8),
    }


def update_user_metrics(user_id: int) -> Dict:
    ensure_user_metrics_table()
    trades = _fetch_history(user_id=user_id)
    metrics = calculate_metrics_from_trades(trades)
    run_sql(
        """
        INSERT INTO user_metrics (
            user_id, total_trades, wins, losses, total_pnl, winrate,
            avg_win, avg_loss, expectancy, max_drawdown, max_drawdown_pct,
            profit_factor, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            total_trades=excluded.total_trades,
            wins=excluded.wins,
            losses=excluded.losses,
            total_pnl=excluded.total_pnl,
            winrate=excluded.winrate,
            avg_win=excluded.avg_win,
            avg_loss=excluded.avg_loss,
            expectancy=excluded.expectancy,
            max_drawdown=excluded.max_drawdown,
            max_drawdown_pct=excluded.max_drawdown_pct,
            profit_factor=excluded.profit_factor,
            updated_at=excluded.updated_at
        """,
        (
            user_id,
            metrics["total_trades"],
            metrics["wins"],
            metrics["losses"],
            metrics["total_pnl"],
            metrics["winrate"],
            metrics["avg_win"],
            metrics["avg_loss"],
            metrics["expectancy"],
            metrics["max_drawdown"],
            metrics["max_drawdown_pct"],
            metrics["profit_factor"],
            datetime.utcnow().isoformat(),
        ),
    )
    return metrics


def get_trade_metrics(user_id: int, period_days: int = 30) -> Dict:
    ensure_user_metrics_table()
    period = max(int(period_days or 30), 1)
    since = datetime.utcnow() - timedelta(days=period)
    trades = _fetch_history(user_id=user_id, since_iso=since.isoformat())
    metrics = calculate_metrics_from_trades(trades)
    metrics["user_id"] = user_id
    metrics["period_days"] = period
    return metrics
