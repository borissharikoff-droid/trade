#!/usr/bin/env python3
"""
Подсчёт автотрейдов за последние 24 часа.
Запуск: python scripts/count_auto_trades.py
Требует: DATABASE_URL (PostgreSQL) или bot_data.db (SQLite)
"""
import os
import sys
from datetime import datetime, timezone, timedelta

# Добавляем корень проекта в path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from connection_pool import init_connection_pool
from db_core import run_sql, DATABASE_URL, DB_PATH


def main():
    # Инициализация пула подключений
    init_connection_pool(DATABASE_URL, DB_PATH)
    
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    
    row = run_sql(
        """
        SELECT COUNT(*) as cnt
        FROM history h
        JOIN users u ON h.user_id = u.user_id
        WHERE u.auto_trade = 1
          AND h.closed_at >= ?
        """,
        (since,),
        fetch="one"
    )
    
    count = int(row['cnt']) if row else 0
    print(f"Автотрейдов за последние 24 часа: {count}")


if __name__ == "__main__":
    main()
