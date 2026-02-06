"""
Central database layer: run_sql, transactions, placeholder conversion.
Single place for ? -> %s (PostgreSQL) and parameterized queries.
"""

import os
import logging
from typing import Tuple, List, Any, Optional

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
DB_PATH = os.environ.get("DB_PATH", "bot_data.db")
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
else:
    import sqlite3


def sql_now() -> str:
    """SQL expression for current time (PostgreSQL/SQLite compatible)"""
    return "CURRENT_TIMESTAMP" if USE_POSTGRES else "datetime('now')"


def sql_interval(field: str, interval: str) -> str:
    """SQL expression for interval (e.g. field > now - interval)"""
    if USE_POSTGRES:
        return f"{field} > NOW() - INTERVAL '{interval}'"
    return f"{field} > datetime('now', '-{interval}')"


def _sql_prepare(query: str, params: tuple) -> Tuple[str, tuple]:
    """
    Single place for placeholder conversion: ? -> %s for PostgreSQL.
    Params must always be tuple/list (SQL injection protection).
    """
    if not isinstance(params, (tuple, list)):
        raise TypeError("SQL params must be tuple or list, never string-format")
    if USE_POSTGRES:
        return query.replace("?", "%s"), params
    return query, params


def run_sql(query: str, params: tuple = (), fetch: str = None):
    """
    Execute SQL with parameterized queries. Use ? in query and pass params as tuple.
    fetch: None, 'one', 'all', 'id'
    """
    from connection_pool import get_pooled_connection, return_pooled_connection

    query, params = _sql_prepare(query, params)
    conn = None
    c = None
    try:
        conn = get_pooled_connection()
        if USE_POSTGRES:
            if fetch in ('all', 'one'):
                c = conn.cursor(cursor_factory=RealDictCursor)
            else:
                c = conn.cursor()
        else:
            c = conn.cursor()
        c.execute(query, params)
        result = None
        if fetch == "one":
            row = c.fetchone()
            result = dict(row) if row else None
        elif fetch == "all":
            rows = c.fetchall()
            result = [dict(r) for r in rows] if rows else []
        elif fetch == "id":
            if USE_POSTGRES:
                row = c.fetchone()
                result = row[0] if row and "RETURNING" in query.upper() else None
            else:
                result = c.lastrowid
        conn.commit()
        return result
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error("[DB] SQL error: %s, query: %s", e, query[:100])
        raise
    finally:
        if c:
            try:
                c.close()
            except Exception:
                pass
        if conn:
            return_pooled_connection(conn)


def run_sql_transaction(operations: List[Tuple[str, tuple, Optional[str]]]) -> List[Any]:
    """Execute multiple SQL operations in one transaction."""
    from connection_pool import get_pooled_connection, return_pooled_connection

    conn = None
    c = None
    results = []
    try:
        conn = get_pooled_connection()
        if USE_POSTGRES:
            conn.autocommit = False
        else:
            conn.execute("BEGIN TRANSACTION")
        for query, params, fetch in operations:
            query, params = _sql_prepare(query, params)
            if USE_POSTGRES:
                c = conn.cursor(cursor_factory=RealDictCursor) if fetch in ('all', 'one') else conn.cursor()
            else:
                c = conn.cursor()
            c.execute(query, params)
            result = None
            if fetch == "one":
                row = c.fetchone()
                result = dict(row) if row else None
            elif fetch == "all":
                rows = c.fetchall()
                result = [dict(r) for r in rows] if rows else []
            elif fetch == "id":
                if USE_POSTGRES:
                    row = c.fetchone()
                    result = row[0] if row and "RETURNING" in query.upper() else None
                else:
                    result = c.lastrowid
            results.append(result)
            if c:
                c.close()
                c = None
        conn.commit()
        return results
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error("[DB] Transaction error: %s", e)
        raise
    finally:
        if c:
            try:
                c.close()
            except Exception:
                pass
        if conn:
            return_pooled_connection(conn)


def get_connection():
    """Get DB connection (prefer get_pooled_connection from connection_pool)."""
    from connection_pool import get_pooled_connection
    try:
        return get_pooled_connection()
    except Exception as e:
        logger.warning("[DB] Pool failed, fallback: %s", e)
        if USE_POSTGRES:
            return psycopg2.connect(DATABASE_URL)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
