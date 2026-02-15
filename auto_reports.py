"""
Calendar weekly/monthly report generation and persistence.
Dashboard-only delivery.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Dict, Tuple

from db_core import run_sql
from metrics_calculator import calculate_metrics_from_trades


def ensure_reports_table() -> None:
    run_sql(
        """
        CREATE TABLE IF NOT EXISTS reports (
            user_id BIGINT NOT NULL,
            period TEXT NOT NULL,
            report_date TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, period, report_date)
        )
        """
    )


def _period_bounds(period: str, now: datetime) -> Tuple[datetime, datetime, str]:
    period = (period or "").strip().lower()
    if period == "weekly":
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_start + timedelta(days=7)
        report_date = week_start.date().isoformat()
        return week_start, week_end, report_date
    if period == "monthly":
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1)
        report_date = month_start.date().isoformat()
        return month_start, month_end, report_date
    raise ValueError("period must be weekly or monthly")


def _fetch_trades_for_period(user_id: int, start_dt: datetime, end_dt: datetime):
    return run_sql(
        """
        SELECT pnl, symbol, direction, reason, closed_at
        FROM history
        WHERE user_id = ? AND closed_at >= ? AND closed_at < ?
        ORDER BY closed_at ASC
        """,
        (user_id, start_dt.isoformat(), end_dt.isoformat()),
        fetch="all",
    ) or []


def _compose_report(user_id: int, period: str, start_dt: datetime, end_dt: datetime) -> Dict:
    trades = _fetch_trades_for_period(user_id, start_dt, end_dt)
    metrics = calculate_metrics_from_trades(trades)

    by_reason = {}
    for trade in trades:
        reason = (trade.get("reason") or "UNKNOWN").strip() or "UNKNOWN"
        by_reason[reason] = by_reason.get(reason, 0) + 1
    top_reasons = sorted(by_reason.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "user_id": user_id,
        "period": period,
        "period_start": start_dt.isoformat(),
        "period_end": end_dt.isoformat(),
        "generated_at": datetime.utcnow().isoformat(),
        "metrics": metrics,
        "top_close_reasons": [{"reason": reason, "count": count} for reason, count in top_reasons],
    }


def _save_report(user_id: int, period: str, report_date: str, report_data: Dict) -> None:
    ensure_reports_table()
    run_sql(
        """
        INSERT INTO reports (user_id, period, report_date, data, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id, period, report_date) DO UPDATE SET
            data = excluded.data,
            created_at = excluded.created_at
        """,
        (
            user_id,
            period,
            report_date,
            json.dumps(report_data, ensure_ascii=True),
            datetime.utcnow().isoformat(),
        ),
    )


def generate_report(user_id: int, period: str, as_of: datetime | None = None, persist: bool = True) -> Dict:
    now = as_of or datetime.utcnow()
    start_dt, end_dt, report_date = _period_bounds(period, now)
    report = _compose_report(user_id, period, start_dt, end_dt)
    report["report_date"] = report_date
    if persist:
        _save_report(user_id, period, report_date, report)
    return report


def generate_weekly_report(user_id: int) -> Dict:
    return generate_report(user_id=user_id, period="weekly", persist=True)


def generate_monthly_report(user_id: int) -> Dict:
    return generate_report(user_id=user_id, period="monthly", persist=True)


def refresh_reports_for_all_users() -> Dict:
    ensure_reports_table()
    users = run_sql("SELECT user_id FROM users", fetch="all") or []
    refreshed = 0
    for row in users:
        user_id = int(row.get("user_id"))
        generate_weekly_report(user_id)
        generate_monthly_report(user_id)
        refreshed += 1
    return {"refreshed_users": refreshed, "timestamp": datetime.utcnow().isoformat()}
