"""
Скрипт для анализа логов из базы данных
Показывает ошибки, проблемы с /start, открытием и закрытием сделок
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# Определяем тип БД
DATABASE_URL = os.environ.get("DATABASE_URL")
DB_PATH = os.environ.get("DB_PATH", "bot_data.db")
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    print("[DB] Using PostgreSQL")
else:
    import sqlite3
    print(f"[DB] Using SQLite: {DB_PATH}")

def get_connection():
    """Получить подключение к БД"""
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def run_sql(query: str, params: tuple = (), fetch: str = None):
    """Выполнить SQL запрос"""
    conn = None
    c = None
    try:
        conn = get_connection()
        
        if USE_POSTGRES:
            query = query.replace("?", "%s")
            if fetch == 'all' or fetch == 'one':
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
                result = row[0] if row and 'RETURNING' in query.upper() else None
            else:
                result = c.lastrowid
        
        conn.commit()
        return result
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        print(f"[ERROR] SQL error: {e}")
        raise
    finally:
        if c:
            try:
                c.close()
            except:
                pass
        if conn:
            conn.close()

def check_table_exists():
    """Проверить существование таблицы trading_logs"""
    try:
        if USE_POSTGRES:
            result = run_sql("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'trading_logs'
                )
            """, fetch="one")
            return result['exists'] if result else False
        else:
            result = run_sql("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='trading_logs'
            """, fetch="one")
            return result is not None
    except Exception as e:
        print(f"[ERROR] Failed to check table: {e}")
        return False

def get_recent_errors(hours=24, limit=50):
    """Получить недавние ошибки"""
    if not check_table_exists():
        print("[WARNING] Table trading_logs does not exist. Logs may not be initialized.")
        return []
    
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        errors = run_sql("""
            SELECT timestamp, category, level, user_id, symbol, direction, message, 
                   data, error_traceback, session_id
            FROM trading_logs 
            WHERE timestamp > ? AND (level = 'ERROR' OR level = 'CRITICAL' OR category = 'ERROR')
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (cutoff, limit), fetch="all")
        
        return errors or []
    except Exception as e:
        print(f"[ERROR] Failed to get errors: {e}")
        return []

def get_start_command_errors(hours=24):
    """Получить ошибки связанные с /start"""
    if not check_table_exists():
        return []
    
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        errors = run_sql("""
            SELECT timestamp, user_id, message, error_traceback, data
            FROM trading_logs 
            WHERE timestamp > ? 
            AND (message LIKE '%START%' OR message LIKE '%start%' OR message LIKE '%/start%')
            AND (level = 'ERROR' OR level = 'CRITICAL' OR category = 'ERROR')
            ORDER BY timestamp DESC
        """, (cutoff,), fetch="all")
        
        return errors or []
    except Exception as e:
        print(f"[ERROR] Failed to get /start errors: {e}")
        return []

def get_trade_errors(hours=24):
    """Получить ошибки связанные с открытием/закрытием сделок"""
    if not check_table_exists():
        return []
    
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        errors = run_sql("""
            SELECT timestamp, category, user_id, symbol, direction, message, 
                   error_traceback, data
            FROM trading_logs 
            WHERE timestamp > ? 
            AND (category IN ('TRADE_OPEN', 'TRADE_CLOSE', 'ERROR'))
            AND (level = 'ERROR' OR level = 'CRITICAL')
            ORDER BY timestamp DESC
        """, (cutoff,), fetch="all")
        
        return errors or []
    except Exception as e:
        print(f"[ERROR] Failed to get trade errors: {e}")
        return []

def print_error_summary():
    """Вывести сводку ошибок"""
    print("\n" + "="*80)
    print("АНАЛИЗ ЛОГОВ - ПОСЛЕДНИЕ 24 ЧАСА")
    print("="*80)
    
    # Проверяем существование таблицы
    if not check_table_exists():
        print("\n[КРИТИЧНО] Таблица trading_logs не существует!")
        print("Это означает, что логирование не инициализировано.")
        print("Нужно запустить бота хотя бы раз для создания таблицы.")
        return
    
    # Общие ошибки
    print("\n1. ВСЕ ОШИБКИ (последние 20):")
    print("-" * 80)
    errors = get_recent_errors(hours=24, limit=20)
    if errors:
        for i, err in enumerate(errors, 1):
            timestamp = err.get('timestamp', 'N/A')
            level = err.get('level', 'ERROR')
            message = err.get('message', 'No message')[:100]
            user_id = err.get('user_id', 'N/A')
            print(f"{i}. [{timestamp}] [{level}] User: {user_id}")
            print(f"   {message}")
            if err.get('error_traceback'):
                traceback = err['error_traceback'][:200]
                print(f"   Traceback: {traceback}...")
            print()
    else:
        print("   Ошибок не найдено!")
    
    # Ошибки /start
    print("\n2. ОШИБКИ КОМАНДЫ /START:")
    print("-" * 80)
    start_errors = get_start_command_errors(hours=24)
    if start_errors:
        for i, err in enumerate(start_errors, 1):
            timestamp = err.get('timestamp', 'N/A')
            user_id = err.get('user_id', 'N/A')
            message = err.get('message', 'No message')
            print(f"{i}. [{timestamp}] User: {user_id}")
            print(f"   {message}")
            if err.get('error_traceback'):
                traceback = err['error_traceback'][:300]
                print(f"   {traceback}")
            print()
    else:
        print("   Ошибок /start не найдено!")
    
    # Ошибки сделок
    print("\n3. ОШИБКИ ОТКРЫТИЯ/ЗАКРЫТИЯ СДЕЛОК:")
    print("-" * 80)
    trade_errors = get_trade_errors(hours=24)
    if trade_errors:
        for i, err in enumerate(trade_errors, 1):
            timestamp = err.get('timestamp', 'N/A')
            category = err.get('category', 'N/A')
            user_id = err.get('user_id', 'N/A')
            symbol = err.get('symbol', 'N/A')
            message = err.get('message', 'No message')[:150]
            print(f"{i}. [{timestamp}] [{category}] User: {user_id}, Symbol: {symbol}")
            print(f"   {message}")
            if err.get('error_traceback'):
                traceback = err['error_traceback'][:200]
                print(f"   Traceback: {traceback}...")
            print()
    else:
        print("   Ошибок сделок не найдено!")
    
    # Статистика
    print("\n4. СТАТИСТИКА:")
    print("-" * 80)
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        
        total_errors = run_sql("""
            SELECT COUNT(*) as cnt FROM trading_logs 
            WHERE timestamp > ? AND (level = 'ERROR' OR level = 'CRITICAL')
        """, (cutoff,), fetch="one")
        
        error_by_category = run_sql("""
            SELECT category, COUNT(*) as cnt FROM trading_logs 
            WHERE timestamp > ? AND (level = 'ERROR' OR level = 'CRITICAL')
            GROUP BY category
            ORDER BY cnt DESC
        """, (cutoff,), fetch="all")
        
        print(f"Всего ошибок за 24 часа: {total_errors['cnt'] if total_errors else 0}")
        print("\nОшибки по категориям:")
        if error_by_category:
            for cat in error_by_category:
                print(f"  {cat['category']}: {cat['cnt']}")
        else:
            print("  Нет данных")
            
    except Exception as e:
        print(f"   Ошибка получения статистики: {e}")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    print_error_summary()
