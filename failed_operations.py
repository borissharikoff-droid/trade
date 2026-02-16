"""
Dead-letter queue for failed execution operations.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from db_core import run_sql, USE_POSTGRES

logger = logging.getLogger(__name__)
_failed_ops_table_checked = False


def ensure_failed_operations_table() -> None:
    global _failed_ops_table_checked
    try:
        if USE_POSTGRES:
            run_sql(
                """
                CREATE TABLE IF NOT EXISTS failed_operations (
                    id SERIAL PRIMARY KEY,
                    operation_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    error TEXT,
                    status TEXT DEFAULT 'FAILED',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_retry_at TIMESTAMP
                )
                """
            )
        else:
            run_sql(
                """
                CREATE TABLE IF NOT EXISTS failed_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    error TEXT,
                    status TEXT DEFAULT 'FAILED',
                    retry_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_retry_at TEXT
                )
                """
            )
    except Exception as exc:
        logger.error(f"[FAILED_OPS] Ensure table failed: {exc}")
    finally:
        _failed_ops_table_checked = True


def add_failed_operation(operation_type: str, payload: Dict[str, Any], error: str) -> Optional[int]:
    try:
        if not _failed_ops_table_checked:
            ensure_failed_operations_table()
        payload_json = json.dumps(payload, ensure_ascii=False)
        run_sql(
            """
            INSERT INTO failed_operations (operation_type, payload, error, status, retry_count)
            VALUES (?, ?, ?, 'FAILED', 0)
            """,
            (operation_type, payload_json, str(error)[:500]),
        )
        row = run_sql("SELECT MAX(id) AS id FROM failed_operations", fetch="one")
        return int(row["id"]) if row and row.get("id") is not None else None
    except Exception as exc:
        if "failed_operations" in str(exc).lower():
            return None
        logger.error(f"[FAILED_OPS] Insert failed: {exc}")
        return None


def list_failed_operations(limit: int = 100) -> List[Dict[str, Any]]:
    try:
        if not _failed_ops_table_checked:
            ensure_failed_operations_table()
        rows = run_sql(
            """
            SELECT id, operation_type, payload, error, status, retry_count, created_at, last_retry_at
            FROM failed_operations
            WHERE status IN ('FAILED', 'RETRYING')
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
            fetch="all",
        ) or []
        result: List[Dict[str, Any]] = []
        for row in rows:
            payload = row.get("payload")
            try:
                row["payload"] = json.loads(payload) if isinstance(payload, str) else payload
            except Exception:
                row["payload"] = {"raw": payload}
            result.append(row)
        return result
    except Exception as exc:
        if "failed_operations" in str(exc).lower():
            return []
        logger.error(f"[FAILED_OPS] List failed: {exc}")
        return []


def mark_failed_operation_retried(operation_id: int, success: bool, error: str = "") -> None:
    try:
        if not _failed_ops_table_checked:
            ensure_failed_operations_table()
        now = datetime.utcnow().isoformat()
        if success:
            run_sql(
                """
                UPDATE failed_operations
                SET status = 'RESOLVED',
                    retry_count = retry_count + 1,
                    error = ?,
                    last_retry_at = ?
                WHERE id = ?
                """,
                (str(error)[:500], now, int(operation_id)),
            )
        else:
            run_sql(
                """
                UPDATE failed_operations
                SET status = 'FAILED',
                    retry_count = retry_count + 1,
                    error = ?,
                    last_retry_at = ?
                WHERE id = ?
                """,
                (str(error)[:500], now, int(operation_id)),
            )
    except Exception as exc:
        if "failed_operations" in str(exc).lower():
            return
        logger.error(f"[FAILED_OPS] Retry status update failed: {exc}")
