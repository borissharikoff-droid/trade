"""
Offline-gated strategy validation and versioning helpers.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from db_core import run_sql, USE_POSTGRES
from execution_config import (
    LEARNING_MAX_DRAWDOWN_PCT,
    LEARNING_MIN_PROFIT_FACTOR,
    LEARNING_MIN_TRADES,
    LEARNING_MIN_WINRATE_PCT,
)

logger = logging.getLogger(__name__)


def ensure_strategy_versions_table() -> None:
    try:
        if USE_POSTGRES:
            run_sql(
                """
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id SERIAL PRIMARY KEY,
                    version_tag TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    validation_json TEXT NOT NULL,
                    status TEXT DEFAULT 'CANDIDATE',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        else:
            run_sql(
                """
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_tag TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    validation_json TEXT NOT NULL,
                    status TEXT DEFAULT 'CANDIDATE',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
    except Exception as exc:
        logger.error(f"[LEARNING] Ensure strategy_versions failed: {exc}")


def validate_candidate_metrics(metrics: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    trades = int(metrics.get("total_trades", 0) or 0)
    win_rate = float(metrics.get("win_rate", 0) or 0)
    profit_factor = float(metrics.get("profit_factor", 0) or 0)
    max_drawdown_pct = float(metrics.get("max_drawdown_pct", 1000) or 1000)

    checks = {
        "min_trades_ok": trades >= LEARNING_MIN_TRADES,
        "win_rate_ok": win_rate >= LEARNING_MIN_WINRATE_PCT,
        "profit_factor_ok": profit_factor >= LEARNING_MIN_PROFIT_FACTOR,
        "drawdown_ok": max_drawdown_pct <= LEARNING_MAX_DRAWDOWN_PCT,
    }
    checks["passed"] = all(checks.values())
    checks["thresholds"] = {
        "min_trades": LEARNING_MIN_TRADES,
        "min_win_rate_pct": LEARNING_MIN_WINRATE_PCT,
        "min_profit_factor": LEARNING_MIN_PROFIT_FACTOR,
        "max_drawdown_pct": LEARNING_MAX_DRAWDOWN_PCT,
    }
    return checks["passed"], checks


def save_strategy_candidate(version_tag: str, params: Dict[str, Any], validation: Dict[str, Any]) -> Optional[int]:
    try:
        run_sql(
            """
            INSERT INTO strategy_versions (version_tag, params_json, validation_json, status)
            VALUES (?, ?, ?, ?)
            """,
            (
                version_tag,
                json.dumps(params, ensure_ascii=False),
                json.dumps(validation, ensure_ascii=False),
                "APPROVED" if validation.get("passed") else "REJECTED",
            ),
        )
        row = run_sql("SELECT MAX(id) AS id FROM strategy_versions", fetch="one")
        return int(row["id"]) if row and row.get("id") is not None else None
    except Exception as exc:
        logger.error(f"[LEARNING] Save strategy candidate failed: {exc}")
        return None


def get_latest_approved_strategy() -> Optional[Dict[str, Any]]:
    try:
        row = run_sql(
            """
            SELECT id, version_tag, params_json, validation_json, status, created_at
            FROM strategy_versions
            WHERE status = 'APPROVED'
            ORDER BY id DESC
            LIMIT 1
            """,
            fetch="one",
        )
        if not row:
            return None
        row["params"] = json.loads(row.get("params_json") or "{}")
        row["validation"] = json.loads(row.get("validation_json") or "{}")
        return row
    except Exception as exc:
        logger.error(f"[LEARNING] Fetch latest approved strategy failed: {exc}")
        return None
