import logging
import os
import random
import asyncio
import aiohttp
import json
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, PreCheckoutQueryHandler, MessageHandler, filters
from telegram.error import BadRequest, Conflict

from hedger import hedge_open, hedge_close, is_hedging_enabled, hedger
from smart_analyzer import (
    SmartAnalyzer, find_best_setup, record_trade_result, get_trading_state,
    TradeSetup, SetupQuality, MarketRegime, get_signal_stats, reset_signal_stats,
    increment_bybit_opened, increment_accepted
)
from rate_limiter import rate_limit, rate_limiter, init_rate_limiter, configure_rate_limiter
from connection_pool import init_connection_pool, get_pooled_connection, return_pooled_connection
from cache_manager import users_cache, positions_cache, price_cache, stats_cache, cleanup_caches, invalidate_stats_cache
from trade_logger import trade_logger, init_trade_logger, LogCategory, LogLevel
from auto_optimizer import auto_optimizer, init_auto_optimizer
from dashboard import init_dashboard, start_dashboard_thread

load_dotenv()

# Настройка логирования ДО импорта модулей, которые используют logger
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Умный анализатор v2.0 - единственный режим
smart = SmartAnalyzer()

# Новые модули для максимизации прибыли
try:
    from trailing_stop import trailing_manager
    from smart_scaling import should_scale_in_smart, calculate_scale_in_size
    from pyramid_trading import should_pyramid, calculate_pyramid_size
    from adaptive_exit import detect_reversal_signals, adjust_tp_dynamically, should_exit_early
    from position_manager import calculate_partial_close_amount, check_correlation_risk
    ADVANCED_POSITION_MANAGEMENT = True
    logger.info("[INIT] Advanced position management loaded")
except ImportError as e:
    ADVANCED_POSITION_MANAGEMENT = False
    logger.warning(f"[INIT] Advanced position management disabled: {e}")

# Продвинутые модули (после создания logger)
try:
    from whale_tracker import get_whale_signal, get_combined_whale_analysis, WhaleSignal
    from advanced_signals import (
        get_best_coins_to_trade, get_meme_opportunities, get_market_context,
        COIN_CATEGORIES, ALL_TRADEABLE
    )
    ADVANCED_FEATURES = True
    logger.info("[INIT] Advanced features loaded: whale tracker, meme scanner")
except ImportError as e:
    ADVANCED_FEATURES = False
    logger.warning(f"[INIT] Advanced features disabled: {e}")

# News Analyzer - отслеживание новостей, Twitter, макро-событий
try:
    from news_analyzer import (
        news_analyzer, get_news_signals, get_market_sentiment,
        should_trade_now, detect_manipulations, get_news_trading_opportunities,
        get_upcoming_events, NewsImpact, NewsSentiment
    )
    NEWS_FEATURES = True
    logger.info("[INIT] News analyzer loaded: Twitter, macro events, sentiment")
except ImportError as e:
    NEWS_FEATURES = False
    logger.warning(f"[INIT] News analyzer disabled: {e}")

# ==================== DATABASE ====================
DATABASE_URL = os.environ.get("DATABASE_URL")
DB_PATH = os.environ.get("DB_PATH", "bot_data.db")

# Определяем тип БД
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    logger.info("[DB] Using PostgreSQL")
else:
    import sqlite3
    logger.info("[DB] Using SQLite")

def get_connection():
    """Получить подключение к БД (legacy - use get_pooled_connection instead)"""
    # Try to use pool first
    try:
        return get_pooled_connection()
    except Exception as e:
        logger.warning(f"[DB] Pool connection failed, using fallback: {e}")
        # Fallback to direct connection
        if USE_POSTGRES:
            logger.debug("[DB] Creating direct PostgreSQL connection")
            return psycopg2.connect(DATABASE_URL)
        else:
            logger.debug(f"[DB] Creating direct SQLite connection to {DB_PATH}")
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            return conn

def run_sql(query: str, params: tuple = (), fetch: str = None):
    """
    Выполнить SQL запрос с автоматической конвертацией placeholder'ов
    fetch: None, 'one', 'all', 'id' (lastrowid)
    Uses connection pool for better performance
    """
    conn = None
    c = None
    try:
        conn = get_pooled_connection()
        
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
                # Для PostgreSQL используем RETURNING id
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
            except Exception:
                pass
        logger.error(f"[DB] SQL error: {e}, query: {query[:100]}")
        raise
    finally:
        # Always close cursor first
        if c:
            try:
                c.close()
            except Exception:
                pass
        # Return connection to pool
        if conn:
            return_pooled_connection(conn)

def init_db():
    """Инициализация базы данных"""
    conn = get_connection()
    c = conn.cursor()
    
    if USE_POSTGRES:
        # PostgreSQL синтаксис
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance REAL DEFAULT 0.0,
            total_deposit REAL DEFAULT 0.0,
            total_profit REAL DEFAULT 0.0,
            trading INTEGER DEFAULT 0,
            referrer_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS positions (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            current REAL,
            sl REAL,
            tp REAL,
            amount REAL,
            commission REAL,
            pnl REAL DEFAULT 0,
            bybit_qty REAL DEFAULT 0,
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS history (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            exit_price REAL,
            sl REAL,
            tp REAL,
            amount REAL,
            commission REAL,
            pnl REAL,
            reason TEXT,
            opened_at TIMESTAMP,
            closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS alerts (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            symbol TEXT,
            target_price REAL,
            direction TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            triggered INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
    else:
        # SQLite синтаксис
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0.0,
            total_deposit REAL DEFAULT 0.0,
            total_profit REAL DEFAULT 0.0,
            trading INTEGER DEFAULT 0,
            referrer_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            current REAL,
            sl REAL,
            tp REAL,
            amount REAL,
            commission REAL,
            pnl REAL DEFAULT 0,
            bybit_qty REAL DEFAULT 0,
            opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            exit_price REAL,
            sl REAL,
            tp REAL,
            amount REAL,
            commission REAL,
            pnl REAL,
            reason TEXT,
            opened_at TEXT,
            closed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            symbol TEXT,
            target_price REAL,
            direction TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            triggered INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )''')
    
    conn.commit()
    
    # Миграция: добавляем bybit_qty если колонки нет
    try:
        if USE_POSTGRES:
            c.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS bybit_qty REAL DEFAULT 0")
        else:
            # SQLite не поддерживает IF NOT EXISTS для ALTER, проверяем вручную
            c.execute("PRAGMA table_info(positions)")
            columns = [col[1] for col in c.fetchall()]
            if 'bybit_qty' not in columns:
                c.execute("ALTER TABLE positions ADD COLUMN bybit_qty REAL DEFAULT 0")
        conn.commit()
        logger.info("[DB] Migration: bybit_qty column ensured")
    except Exception as e:
        logger.warning(f"[DB] Migration warning: {e}")
    
    # Миграция: добавляем realized_pnl если колонки нет
    try:
        if USE_POSTGRES:
            c.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_pnl REAL DEFAULT 0")
        else:
            c.execute("PRAGMA table_info(positions)")
            columns = [col[1] for col in c.fetchall()]
            if 'realized_pnl' not in columns:
                c.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL DEFAULT 0")
        conn.commit()
        logger.info("[DB] Migration: realized_pnl column ensured")
    except Exception as e:
        logger.warning(f"[DB] Migration warning (realized_pnl): {e}")
    
    # Миграция: добавляем поля для авто-трейда пользователя
    try:
        if USE_POSTGRES:
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_trade INTEGER DEFAULT 0")
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_trade_max_daily INTEGER DEFAULT 10")
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_trade_min_winrate INTEGER DEFAULT 70")
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_trade_today INTEGER DEFAULT 0")
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_trade_last_reset TEXT")
        else:
            c.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in c.fetchall()]
            if 'auto_trade' not in columns:
                c.execute("ALTER TABLE users ADD COLUMN auto_trade INTEGER DEFAULT 0")
            if 'auto_trade_max_daily' not in columns:
                c.execute("ALTER TABLE users ADD COLUMN auto_trade_max_daily INTEGER DEFAULT 10")
            if 'auto_trade_min_winrate' not in columns:
                c.execute("ALTER TABLE users ADD COLUMN auto_trade_min_winrate INTEGER DEFAULT 70")
            if 'auto_trade_today' not in columns:
                c.execute("ALTER TABLE users ADD COLUMN auto_trade_today INTEGER DEFAULT 0")
            if 'auto_trade_last_reset' not in columns:
                c.execute("ALTER TABLE users ADD COLUMN auto_trade_last_reset TEXT")
        conn.commit()
        logger.info("[DB] Migration: auto_trade columns ensured")
    except Exception as e:
        logger.warning(f"[DB] Migration warning (auto_trade): {e}")
    
    # Миграция: создаём таблицу для системных настроек (pending_commission, invoices)
    try:
        if USE_POSTGRES:
            c.execute('''CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS pending_invoices (
                invoice_id BIGINT PRIMARY KEY,
                user_id BIGINT,
                amount REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )''')
            # Таблица для отслеживания реферальных начислений
            c.execute('''CREATE TABLE IF NOT EXISTS referral_earnings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                from_user_id BIGINT NOT NULL,
                amount REAL NOT NULL,
                level INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
        else:
            c.execute('''CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS pending_invoices (
                invoice_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                expires_at TEXT
            )''')
            # Таблица для отслеживания реферальных начислений
            c.execute('''CREATE TABLE IF NOT EXISTS referral_earnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                from_user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                level INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )''')
        conn.commit()
        logger.info("[DB] Migration: system_settings, pending_invoices, referral_earnings tables ensured")
    except Exception as e:
        logger.warning(f"[DB] Migration warning (system_settings): {e}")
    
    # Add database indexes for performance BEFORE closing connection
    try:
        if USE_POSTGRES:
            # PostgreSQL indexes
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_user_id ON positions(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_history_closed_at ON history(closed_at)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_history_user_pnl ON history(user_id, pnl)")  # Composite index for stats
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_user_id ON alerts(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered)")
            # Indexes for referral_earnings
            c.execute("CREATE INDEX IF NOT EXISTS idx_ref_earn_user ON referral_earnings(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_ref_earn_from ON referral_earnings(from_user_id)")
        else:
            # SQLite indexes
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_user_id ON positions(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_history_closed_at ON history(closed_at)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_history_user_pnl ON history(user_id, pnl)")  # Composite index for stats
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_user_id ON alerts(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered)")
            # Indexes for referral_earnings
            c.execute("CREATE INDEX IF NOT EXISTS idx_ref_earn_user ON referral_earnings(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_ref_earn_from ON referral_earnings(from_user_id)")
        conn.commit()
        logger.info("[DB] Indexes created/verified")
    except Exception as e:
        logger.warning(f"[DB] Index creation warning: {e}")
    
    # Close cursor and connection
    try:
        c.close()
    except Exception:
        pass
    conn.close()
    
    # Initialize connection pool
    init_connection_pool(DATABASE_URL, DB_PATH, min_connections=2, max_connections=10)
    logger.info("[DB] Connection pool initialized")
    
    # Initialize rate limiter tables
    init_rate_limiter()
    
    # Initialize trade logger for comprehensive logging
    init_trade_logger(run_sql, USE_POSTGRES)
    logger.info("[DB] Trade logger initialized")
    
    # Initialize auto optimizer for continuous learning
    init_auto_optimizer(run_sql, trade_logger)
    logger.info("[DB] Auto optimizer initialized")
    
    db_type = "PostgreSQL" if USE_POSTGRES else f"SQLite ({DB_PATH})"
    logger.info(f"[DB] Initialized: {db_type}")

def db_get_user(user_id: int) -> Dict:
    """Получить пользователя из БД"""
    row = run_sql("""
        SELECT balance, total_deposit, total_profit, trading,
               auto_trade, auto_trade_max_daily, auto_trade_min_winrate,
               auto_trade_today, auto_trade_last_reset, referrer_id
        FROM users WHERE user_id = ?
    """, (user_id,), fetch="one")

    if not row:
        # Явно указываем balance=0.0 и total_deposit=0.0 при создании
        run_sql("INSERT INTO users (user_id, balance, total_deposit) VALUES (?, 0.0, 0.0)", (user_id,))
        logger.info(f"[DB] New user {user_id} created with balance=0.0")
        return {
            'balance': 0.0, 'total_deposit': 0.0, 'total_profit': 0.0, 'trading': False,
            'auto_trade': False, 'auto_trade_max_daily': 10, 'auto_trade_min_winrate': 70,
            'auto_trade_today': 0, 'auto_trade_last_reset': None, 'referrer_id': None
        }

    return {
        'balance': row['balance'],
        'total_deposit': row['total_deposit'],
        'total_profit': row['total_profit'],
        'trading': bool(row['trading']),
        'auto_trade': bool(row['auto_trade'] or 0),
        'auto_trade_max_daily': int(row['auto_trade_max_daily'] or 10),
        'auto_trade_min_winrate': int(row['auto_trade_min_winrate'] or 70),
        'auto_trade_today': int(row['auto_trade_today'] or 0),
        'auto_trade_last_reset': row['auto_trade_last_reset'],
        'referrer_id': row.get('referrer_id')
    }

# Whitelist of allowed columns for user updates (SQL injection prevention)
ALLOWED_USER_COLUMNS = {
    'balance', 'total_deposit', 'total_profit', 'trading', 'referrer_id',
    'auto_trade', 'auto_trade_max_daily', 'auto_trade_min_winrate',
    'auto_trade_today', 'auto_trade_last_reset'
}

def db_update_user(user_id: int, **kwargs):
    """Обновить данные пользователя (с защитой от SQL injection)"""
    for key, value in kwargs.items():
        # Security: Only allow whitelisted column names
        if key not in ALLOWED_USER_COLUMNS:
            logger.warning(f"[SECURITY] Blocked attempt to update invalid column: {key}")
            continue
        if key in ['trading', 'auto_trade']:
            value = 1 if value else 0
        run_sql(f"UPDATE users SET {key} = ? WHERE user_id = ?", (value, user_id))

def db_get_positions(user_id: int) -> List[Dict]:
    """Получить открытые позиции"""
    positions = run_sql("SELECT * FROM positions WHERE user_id = ?", (user_id,), fetch="all")
    logger.debug(f"[DB] User {user_id}: {len(positions)} positions from DB")
    return positions

def db_add_position(user_id: int, pos: Dict) -> int:
    """Добавить позицию"""
    if USE_POSTGRES:
        query = """INSERT INTO positions
            (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl, bybit_qty, realized_pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id"""
    else:
        query = """INSERT INTO positions
            (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl, bybit_qty, realized_pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    pos_id = run_sql(query,
        (user_id, pos['symbol'], pos['direction'], pos['entry'], pos['current'],
         pos['sl'], pos['tp'], pos['amount'], pos['commission'], pos.get('pnl', 0), pos.get('bybit_qty', 0), pos.get('realized_pnl', 0)), fetch="id")
    logger.info(f"[DB] Position {pos_id} added for user {user_id}")
    return pos_id

# Whitelist of allowed position columns for updates (security)
ALLOWED_POSITION_COLUMNS = {
    'current', 'sl', 'tp', 'pnl', 'bybit_qty', 'realized_pnl', 'amount'
}

def db_update_position(pos_id: int, **kwargs):
    """Обновить позицию (с защитой от SQL injection)"""
    for key, value in kwargs.items():
        if key not in ALLOWED_POSITION_COLUMNS:
            logger.warning(f"[SECURITY] Blocked attempt to update invalid position column: {key}")
            continue
        run_sql(f"UPDATE positions SET {key} = ? WHERE id = ?", (value, pos_id))

def db_close_position(pos_id: int, exit_price: float, pnl: float, reason: str):
    """Закрыть позицию и перенести в историю"""
    # Получаем позицию
    pos = run_sql("SELECT * FROM positions WHERE id = ?", (pos_id,), fetch="one")
    if not pos:
        return
    
    # Переносим в историю
    closed_at = datetime.now().isoformat()
    run_sql("""INSERT INTO history 
        (user_id, symbol, direction, entry, exit_price, sl, tp, amount, commission, pnl, reason, opened_at, closed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (pos['user_id'], pos['symbol'], pos['direction'], pos['entry'], exit_price, 
         pos['sl'], pos['tp'], pos['amount'], pos['commission'], pnl, reason, pos['opened_at'], closed_at))
    
    # Удаляем из активных
    run_sql("DELETE FROM positions WHERE id = ?", (pos_id,))
    
    # Invalidate stats cache for this user (stats changed with new trade in history)
    invalidate_stats_cache(pos['user_id'])
    
    # Записываем результат для статистики smart analyzer
    record_trade_result(pnl)
    
    logger.info(f"[DB] Position {pos_id} closed: {reason}, PnL: ${pnl:.2f}")
    
    # Comprehensive logging
    try:
        # Calculate holding time if possible
        holding_minutes = None
        if pos.get('opened_at'):
            try:
                opened = datetime.fromisoformat(str(pos['opened_at']).replace('Z', '+00:00'))
                closed = datetime.fromisoformat(closed_at)
                holding_minutes = (closed - opened).total_seconds() / 60
            except:
                pass
        
        trade_logger.log_trade_close(
            user_id=pos['user_id'], symbol=pos['symbol'], direction=pos['direction'],
            amount=pos['amount'], entry=pos['entry'], exit_price=exit_price,
            pnl=pnl, reason=reason, position_id=pos_id, holding_minutes=holding_minutes
        )
    except Exception as e:
        logger.warning(f"[TRADE_LOGGER] Failed to log trade close: {e}")

def db_get_history(user_id: int, limit: int = 20) -> List[Dict]:
    """Получить историю сделок"""
    return run_sql("SELECT * FROM history WHERE user_id = ? ORDER BY closed_at DESC LIMIT ?", (user_id, limit), fetch="all")

def db_get_user_stats(user_id: int, use_cache: bool = True) -> Dict:
    """Полная статистика пользователя по ВСЕМ сделкам (с кэшированием)"""
    # Check cache first (30 second TTL)
    if use_cache:
        cached = stats_cache.get(user_id)
        if cached is not None:
            return cached
    
    row = run_sql("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
            SUM(pnl) as total_pnl
        FROM history WHERE user_id = ?
    """, (user_id,), fetch="one")
    
    if not row:
        result = {'total': 0, 'wins': 0, 'losses': 0, 'winrate': 0, 'total_pnl': 0}
    else:
        total = int(row['total'] or 0)
        wins = int(row['wins'] or 0)
        losses = int(row['losses'] or 0)
        total_pnl = float(row['total_pnl'] or 0)
        winrate = int(wins / total * 100) if total > 0 else 0
        result = {'total': total, 'wins': wins, 'losses': losses, 'winrate': winrate, 'total_pnl': total_pnl}
    
    # Cache the result
    stats_cache.set(user_id, result)
    return result

def db_sync_user_profit(user_id: int) -> float:
    """
    Синхронизирует total_profit пользователя с суммой PnL из истории.
    Возвращает новое значение total_profit.
    """
    stats = db_get_user_stats(user_id)
    total_pnl = stats['total_pnl']
    
    run_sql("""
        UPDATE users SET total_profit = ? WHERE id = ?
    """, (total_pnl, user_id))
    
    # Очищаем кэш пользователя
    users_cache.pop(user_id, None)
    
    logger.info(f"[SYNC] User {user_id} total_profit synced to ${total_pnl:.2f}")
    return total_pnl

def db_sync_all_profits() -> int:
    """
    Синхронизирует total_profit для ВСЕХ пользователей с историей сделок.
    Возвращает количество обновленных пользователей.
    """
    # Получаем всех пользователей с историей
    rows = run_sql("""
        SELECT DISTINCT user_id FROM history
    """, fetch="all")
    
    if not rows:
        return 0
    
    updated = 0
    for row in rows:
        user_id = row['user_id']
        db_sync_user_profit(user_id)
        updated += 1
    
    logger.info(f"[SYNC] Synced total_profit for {updated} users")
    return updated

def db_get_loss_streak(user_id: int) -> int:
    """
    Получить текущую серию убытков пользователя.
    Считает последовательные убыточные сделки с конца истории.
    
    Returns:
        Количество убытков подряд (0 если последняя сделка прибыльная)
    """
    rows = run_sql("""
        SELECT pnl FROM history 
        WHERE user_id = ? 
        ORDER BY closed_at DESC 
        LIMIT 10
    """, (user_id,), fetch="all")
    
    if not rows:
        return 0
    
    streak = 0
    for row in rows:
        if row['pnl'] < 0:
            streak += 1
        else:
            break  # Прерываем при первой прибыльной сделке
    
    return streak

def db_get_real_winrate(min_trades: int = 20) -> Dict:
    """
    Получить РЕАЛЬНЫЙ win rate из истории всех сделок.
    Используется для отображения честного процента в сигналах.
    
    Returns:
        {'winrate': float, 'trades': int, 'reliable': bool}
        reliable=True если данных достаточно (>min_trades)
    """
    # PostgreSQL и SQLite используют разный синтаксис для дат
    if USE_POSTGRES:
        date_filter = "closed_at > NOW() - INTERVAL '30 days'"
    else:
        date_filter = "closed_at > datetime('now', '-30 days')"
    
    row = run_sql(f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
            AVG(CASE WHEN pnl > 0 THEN pnl ELSE NULL END) as avg_win,
            AVG(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE NULL END) as avg_loss
        FROM history 
        WHERE {date_filter}
    """, fetch="one")
    
    if not row or not row['total']:
        return {'winrate': 75, 'trades': 0, 'reliable': False, 'avg_win': 0, 'avg_loss': 0}
    
    total = int(row['total'] or 0)
    wins = int(row['wins'] or 0)
    avg_win = float(row['avg_win'] or 0)
    avg_loss = float(row['avg_loss'] or 0)
    
    if total >= min_trades:
        winrate = (wins / total) * 100
        return {
            'winrate': round(winrate, 1), 
            'trades': total, 
            'reliable': True,
            'avg_win': avg_win,
            'avg_loss': avg_loss
        }
    
    # Недостаточно данных - возвращаем оценку
    return {
        'winrate': 75,  # Дефолтная оценка
        'trades': total, 
        'reliable': False,
        'avg_win': avg_win,
        'avg_loss': avg_loss
    }

# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================
def db_set_referrer(user_id: int, referrer_id: int) -> bool:
    """Установить реферера для пользователя"""
    if user_id == referrer_id:
        return False
    
    # Проверяем что у юзера ещё нет реферера
    row = run_sql("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,), fetch="one")
    if row and row.get('referrer_id'):
        return False
    
    # Проверяем что реферер существует
    ref = run_sql("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,), fetch="one")
    if not ref:
        return False
    
    run_sql("UPDATE users SET referrer_id = ? WHERE user_id = ?", (referrer_id, user_id))
    logger.info(f"[REF] User {user_id} referred by {referrer_id}")
    return True

def db_get_referrer(user_id: int) -> Optional[int]:
    """Получить реферера пользователя"""
    row = run_sql("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,), fetch="one")
    return row['referrer_id'] if row and row.get('referrer_id') else None

def db_get_referrals_count(user_id: int) -> int:
    """Количество рефералов пользователя"""
    row = run_sql("SELECT COUNT(*) as cnt FROM users WHERE referrer_id = ?", (user_id,), fetch="one")
    return row['cnt'] if row else 0

# Многоуровневая реферальная система - проценты от комиссии для каждого уровня
# Определяем здесь, до использования в функциях ниже
REFERRAL_COMMISSION_LEVELS = [
    5.0,   # Уровень 1 (прямой реферал): 5% от комиссии
    3.0,   # Уровень 2 (реферал реферала): 3% от комиссии
    2.0,   # Уровень 3 (реферал реферала реферала): 2% от комиссии
]
REFERRAL_BONUS_LEVELS = [5.0, 2.0, 1.0]  # Бонусы за депозит по уровням: $5, $2, $1
MAX_REFERRAL_LEVELS = len(REFERRAL_COMMISSION_LEVELS)  # Максимум уровней

# ==================== РАСШИРЕННЫЕ ФУНКЦИИ РЕФЕРАЛЬНОЙ СИСТЕМЫ ====================

def ensure_referral_earnings_table():
    """Создать таблицу referral_earnings если её нет"""
    try:
        if USE_POSTGRES:
            run_sql('''CREATE TABLE IF NOT EXISTS referral_earnings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                from_user_id BIGINT NOT NULL,
                amount REAL NOT NULL,
                level INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            run_sql("CREATE INDEX IF NOT EXISTS idx_ref_earn_user ON referral_earnings(user_id)")
            run_sql("CREATE INDEX IF NOT EXISTS idx_ref_earn_from ON referral_earnings(from_user_id)")
        else:
            run_sql('''CREATE TABLE IF NOT EXISTS referral_earnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                from_user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                level INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )''')
            run_sql("CREATE INDEX IF NOT EXISTS idx_ref_earn_user ON referral_earnings(user_id)")
            run_sql("CREATE INDEX IF NOT EXISTS idx_ref_earn_from ON referral_earnings(from_user_id)")
        logger.info("[DB] referral_earnings table ensured")
        return True
    except Exception as e:
        logger.error(f"[DB] Failed to create referral_earnings table: {e}")
        return False

# Флаг для однократной проверки таблицы
_referral_table_checked = False

def _ensure_referral_table():
    """Проверить таблицу один раз при первом использовании"""
    global _referral_table_checked
    if not _referral_table_checked:
        ensure_referral_earnings_table()
        _referral_table_checked = True

def db_save_referral_earning(user_id: int, from_user_id: int, amount: float, level: int, source: str):
    """Сохранить запись о реферальном заработке"""
    _ensure_referral_table()
    try:
        if USE_POSTGRES:
            run_sql("""
                INSERT INTO referral_earnings (user_id, from_user_id, amount, level, source, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, from_user_id, amount, level, source))
        else:
            run_sql("""
                INSERT INTO referral_earnings (user_id, from_user_id, amount, level, source, created_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, (user_id, from_user_id, amount, level, source))
        logger.info(f"[REF_EARN] Saved: user {user_id} earned ${amount:.2f} from {from_user_id} (level {level}, source: {source})")
    except Exception as e:
        logger.error(f"[REF_EARN] Failed to save: {e}")

def db_get_referral_commission_earned(user_id: int) -> float:
    """
    Получить общую сумму заработка с рефералов из таблицы referral_earnings
    """
    _ensure_referral_table()
    try:
        row = run_sql("SELECT COALESCE(SUM(amount), 0) as total FROM referral_earnings WHERE user_id = ?", (user_id,), fetch="one")
        return round(row['total'] if row else 0.0, 2)
    except Exception as e:
        logger.warning(f"[REF_EARN] Query error: {e}")
        return 0.0

def db_get_referral_earnings_by_level(user_id: int) -> Dict[int, float]:
    """
    Получить заработок по каждому уровню реферальной системы
    """
    _ensure_referral_table()
    result = {1: 0.0, 2: 0.0, 3: 0.0}
    try:
        rows = run_sql("""
            SELECT level, COALESCE(SUM(amount), 0) as total 
            FROM referral_earnings 
            WHERE user_id = ?
            GROUP BY level
            ORDER BY level
        """, (user_id,), fetch="all")
        
        for row in rows:
            level = row.get('level', 1)
            if 1 <= level <= 3:
                result[level] = round(row['total'], 2)
    except Exception as e:
        logger.warning(f"[REF_EARN] Query earnings_by_level error: {e}")
    return result

def db_get_referrals_list(user_id: int, level: int = 1) -> List[Dict]:
    """
    Получить список рефералов пользователя с их статистикой
    level=1: прямые рефералы
    """
    _ensure_referral_table()
    try:
        if level == 1:
            # Прямые рефералы
            referrals = run_sql("""
                SELECT u.user_id, u.balance, u.total_deposit,
                       COALESCE((SELECT SUM(amount) FROM referral_earnings WHERE user_id = ? AND from_user_id = u.user_id), 0) as earned
                FROM users u
                WHERE u.referrer_id = ?
                ORDER BY u.total_deposit DESC
            """, (user_id, user_id), fetch="all")
        else:
            # Для уровней 2 и 3 - сложнее, пока возвращаем пустой список
            referrals = []
        return referrals
    except Exception as e:
        logger.warning(f"[REF_EARN] Query referrals_list error: {e}")
        return []

def db_get_referrals_stats(user_id: int) -> Dict:
    """
    Получить полную статистику реферальной программы пользователя
    """
    # Количество рефералов по уровням
    level1_count = db_get_referrals_count(user_id)
    
    # Рефералы 2-го уровня (рефералы моих рефералов)
    level1_refs = run_sql("SELECT user_id FROM users WHERE referrer_id = ?", (user_id,), fetch="all")
    level2_count = 0
    level3_count = 0
    
    for ref in level1_refs:
        ref_id = ref['user_id']
        # Рефералы моего реферала = уровень 2
        level2_refs = run_sql("SELECT user_id FROM users WHERE referrer_id = ?", (ref_id,), fetch="all")
        level2_count += len(level2_refs)
        
        # Рефералы уровня 3
        for ref2 in level2_refs:
            ref2_id = ref2['user_id']
            level3_refs_count = run_sql("SELECT COUNT(*) as cnt FROM users WHERE referrer_id = ?", (ref2_id,), fetch="one")
            level3_count += level3_refs_count['cnt'] if level3_refs_count else 0
    
    # Заработок по уровням
    earnings_by_level = db_get_referral_earnings_by_level(user_id)
    total_earned = sum(earnings_by_level.values())
    
    # Общий депозит рефералов (только уровень 1)
    total_referral_deposits = run_sql("""
        SELECT COALESCE(SUM(total_deposit), 0) as total 
        FROM users 
        WHERE referrer_id = ?
    """, (user_id,), fetch="one")
    
    return {
        'level1_count': level1_count,
        'level2_count': level2_count,
        'level3_count': level3_count,
        'total_count': level1_count + level2_count + level3_count,
        'earnings_level1': earnings_by_level.get(1, 0.0),
        'earnings_level2': earnings_by_level.get(2, 0.0),
        'earnings_level3': earnings_by_level.get(3, 0.0),
        'total_earned': round(total_earned, 2),
        'referral_deposits': round(total_referral_deposits['total'] if total_referral_deposits else 0, 2)
    }

def db_get_referrer_chain(user_id: int, max_levels: int = MAX_REFERRAL_LEVELS) -> List[int]:
    """
    Получить цепочку рефереров от пользователя до корня (админа)
    Возвращает список [уровень1, уровень2, уровень3, ...] или до админа
    """
    chain = []
    current_id = user_id
    visited = set()  # Защита от циклов
    
    for level in range(max_levels):
        referrer_id = db_get_referrer(current_id)
        
        # Если нет реферера или это админ - останавливаемся
        if not referrer_id:
            break
        
        # Защита от циклов
        if referrer_id in visited or referrer_id == current_id:
            break
        
        # Если реферер - админ, добавляем и останавливаемся
        if referrer_id in ADMIN_IDS:
            chain.append(referrer_id)
            break
        
        chain.append(referrer_id)
        visited.add(referrer_id)
        current_id = referrer_id
    
    return chain

# Referral bonus tracking to prevent abuse
_referral_bonuses_given: Dict[int, set] = {}  # {referrer_id: {user_ids who gave bonus}}
MAX_REFERRAL_BONUSES_PER_DAY = 10  # Limit bonuses per referrer per day

def db_check_referral_bonus_given(referrer_id: int, user_id: int) -> bool:
    """Check if referral bonus was already given for this user"""
    # Check in-memory cache first
    if referrer_id in _referral_bonuses_given:
        if user_id in _referral_bonuses_given[referrer_id]:
            return True
    
    # Check in DB (via setting key)
    key = f"ref_bonus_{referrer_id}_{user_id}"
    return db_get_setting(key) == "1"

def db_mark_referral_bonus_given(referrer_id: int, user_id: int):
    """Mark that referral bonus was given for this user"""
    # Save to memory
    if referrer_id not in _referral_bonuses_given:
        _referral_bonuses_given[referrer_id] = set()
    _referral_bonuses_given[referrer_id].add(user_id)
    
    # Save to DB
    key = f"ref_bonus_{referrer_id}_{user_id}"
    db_set_setting(key, "1")

def db_get_daily_referral_bonus_count(referrer_id: int) -> int:
    """Get count of referral bonuses given today"""
    from datetime import date
    today = date.today().isoformat()
    key = f"ref_daily_{referrer_id}_{today}"
    count = db_get_setting(key, "0")
    try:
        return int(count)
    except ValueError:
        return 0

def db_increment_daily_referral_bonus(referrer_id: int):
    """Increment daily referral bonus counter"""
    from datetime import date
    today = date.today().isoformat()
    key = f"ref_daily_{referrer_id}_{today}"
    current = db_get_daily_referral_bonus_count(referrer_id)
    db_set_setting(key, str(current + 1))

def db_add_referral_bonus(referrer_id: int, amount: float, from_user_id: int = 0, level: int = 1) -> bool:
    """
    Добавить реферальный бонус с защитой от злоупотреблений
    Returns: True if bonus was added, False if blocked
    
    Args:
        referrer_id: ID реферера
        amount: Сумма бонуса
        from_user_id: ID пользователя, который принёс бонус
        level: Уровень реферала (1, 2 или 3)
    """
    # Check if bonus already given for this user
    if from_user_id and db_check_referral_bonus_given(referrer_id, from_user_id):
        logger.warning(f"[REF] Blocked duplicate bonus: {referrer_id} <- {from_user_id}")
        return False
    
    # Check daily limit
    daily_count = db_get_daily_referral_bonus_count(referrer_id)
    if daily_count >= MAX_REFERRAL_BONUSES_PER_DAY:
        logger.warning(f"[REF] Daily limit reached for {referrer_id}: {daily_count}/{MAX_REFERRAL_BONUSES_PER_DAY}")
        return False
    
    # Add bonus (atomic SQL update)
    run_sql("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, referrer_id))
    
    # Синхронизируем кэш с БД (избегаем race conditions)
    if referrer_id in users_cache:
        updated_user = run_sql("SELECT balance FROM users WHERE user_id = ?", (referrer_id,), fetch="one")
        if updated_user:
            users_cache[referrer_id]['balance'] = sanitize_balance(updated_user['balance'])
        else:
            # Fallback: если не удалось прочитать, удаляем из кэша - он перечитается при следующем доступе
            users_cache.delete(referrer_id)
    
    # СОХРАНЯЕМ запись о заработке в таблицу referral_earnings
    if from_user_id:
        db_save_referral_earning(
            user_id=referrer_id,
            from_user_id=from_user_id,
            amount=amount,
            level=level,
            source='first_deposit_bonus'
        )
    
    # Mark as given
    if from_user_id:
        db_mark_referral_bonus_given(referrer_id, from_user_id)
    db_increment_daily_referral_bonus(referrer_id)
    
    logger.info(f"[REF] Bonus ${amount} added to {referrer_id} (from user {from_user_id}, level {level})")
    return True

async def process_multilevel_deposit_bonus(user_id: int, bot=None) -> List[Dict]:
    """
    Обработать многоуровневые бонусы при первом депозите пользователя.
    Начисляет бонусы всем рефералам в цепочке (3 уровня).
    
    Args:
        user_id: ID пользователя, который сделал депозит
        bot: Telegram bot instance для отправки уведомлений
    
    Returns:
        Список начисленных бонусов [{referrer_id, amount, level}]
    """
    bonuses_given = []
    
    # Получаем цепочку рефералов
    referrer_chain = db_get_referrer_chain(user_id, MAX_REFERRAL_LEVELS)
    
    if not referrer_chain:
        logger.info(f"[REF_DEPOSIT] No referrer chain for user {user_id}")
        return bonuses_given
    
    logger.info(f"[REF_DEPOSIT] Processing deposit bonus for user {user_id}, chain: {referrer_chain}")
    
    # Начисляем бонусы по уровням
    for level, referrer_id in enumerate(referrer_chain):
        if level >= len(REFERRAL_BONUS_LEVELS):
            break
        
        bonus_amount = REFERRAL_BONUS_LEVELS[level]
        
        if bonus_amount > 0:
            # Для уровня 1 используем стандартную проверку на дубликаты
            # Для уровней 2 и 3 - проверяем отдельно
            bonus_key = f"deposit_bonus_l{level+1}_{referrer_id}_{user_id}"
            if db_get_setting(bonus_key) == "1":
                logger.info(f"[REF_DEPOSIT] Skipping duplicate bonus for level {level+1}")
                continue
            
            # Начисляем бонус (atomic SQL update)
            run_sql("UPDATE users SET balance = balance + ? WHERE user_id = ?", (bonus_amount, referrer_id))
            
            # Синхронизируем кэш с БД (избегаем race conditions)
            if referrer_id in users_cache:
                updated_user = run_sql("SELECT balance FROM users WHERE user_id = ?", (referrer_id,), fetch="one")
                if updated_user:
                    users_cache[referrer_id]['balance'] = sanitize_balance(updated_user['balance'])
                else:
                    users_cache.delete(referrer_id)
            
            # Сохраняем в referral_earnings
            db_save_referral_earning(
                user_id=referrer_id,
                from_user_id=user_id,
                amount=bonus_amount,
                level=level + 1,
                source='first_deposit_bonus'
            )
            
            # Помечаем как выплаченный
            db_set_setting(bonus_key, "1")
            
            bonuses_given.append({
                'referrer_id': referrer_id,
                'amount': bonus_amount,
                'level': level + 1
            })
            
            logger.info(f"[REF_DEPOSIT] Level {level+1}: ${bonus_amount} to user {referrer_id}")
            
            # Отправляем уведомление
            if bot:
                try:
                    level_text = f"(уровень {level+1})" if level > 0 else ""
                    await bot.send_message(
                        referrer_id,
                        f"<b>📥 Реферальный бонус {level_text}</b>\n\n"
                        f"Ваш реферал сделал первый депозит!\n"
                        f"Бонус: <b>+${bonus_amount:.2f}</b>",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"[REF_DEPOSIT] Failed to notify {referrer_id}: {e}")
    
    return bonuses_given

# ==================== АЛЕРТЫ ====================
def db_add_alert(user_id: int, symbol: str, target_price: float, direction: str) -> int:
    """Добавить алерт"""
    if USE_POSTGRES:
        query = "INSERT INTO alerts (user_id, symbol, target_price, direction) VALUES (?, ?, ?, ?) RETURNING id"
    else:
        query = "INSERT INTO alerts (user_id, symbol, target_price, direction) VALUES (?, ?, ?, ?)"
    alert_id = run_sql(query, (user_id, symbol, target_price, direction), fetch="id")
    logger.info(f"[ALERT] Created #{alert_id} for {user_id}: {symbol} {direction} ${target_price}")
    return alert_id

def db_get_active_alerts() -> List[Dict]:
    """Получить все активные алерты"""
    return run_sql("SELECT * FROM alerts WHERE triggered = 0", fetch="all")

def db_get_user_alerts(user_id: int) -> List[Dict]:
    """Получить алерты пользователя"""
    return run_sql("SELECT * FROM alerts WHERE user_id = ? AND triggered = 0", (user_id,), fetch="all")

def db_trigger_alert(alert_id: int):
    """Пометить алерт как сработавший"""
    run_sql("UPDATE alerts SET triggered = 1 WHERE id = ?", (alert_id,))

def db_delete_alert(alert_id: int, user_id: int) -> bool:
    """Удалить алерт"""
    # Для проверки удаления нужен отдельный запрос
    before = run_sql("SELECT COUNT(*) as cnt FROM alerts WHERE id = ? AND user_id = ?", (alert_id, user_id), fetch="one")
    if before and before['cnt'] > 0:
        run_sql("DELETE FROM alerts WHERE id = ? AND user_id = ?", (alert_id, user_id))
        return True
    return False

# Инициализация БД при старте
init_db()

# ==================== КОНФИГ ====================
COMMISSION_PERCENT = 2.0  # Комиссия 2% за сделку
MIN_DEPOSIT = 2  # Минимальный депозит $2
STARS_RATE = 50  # 50 звёзд = $1
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]  # ID админов

# Configure rate limiter after ADMIN_IDS is defined
configure_rate_limiter(run_sql, USE_POSTGRES, ADMIN_IDS)
REFERRAL_BONUS = 5.0  # $5 бонус рефереру уровня 1 при депозите (для совместимости)
COMMISSION_WITHDRAW_THRESHOLD = 10.0  # Авто-вывод комиссий при накоплении $10

# Многоуровневая реферальная система - проценты от комиссии для каждого уровня
ADMIN_CRYPTO_ID_RAW = os.getenv("ADMIN_CRYPTO_ID", "")  # CryptoBot ID админа для вывода комиссий
# Убираем префикс "U" если он есть (формат CryptoBot: U1077249 -> 1077249)
ADMIN_CRYPTO_ID = ADMIN_CRYPTO_ID_RAW.lstrip("Uu") if ADMIN_CRYPTO_ID_RAW else ""

# Счётчик накопленных комиссий (теперь персистентный в БД)
pending_commission = 0.0

def db_get_setting(key: str, default: str = "") -> str:
    """Get a system setting from DB"""
    row = run_sql("SELECT value FROM system_settings WHERE key = ?", (key,), fetch="one")
    return row['value'] if row else default

def db_set_setting(key: str, value: str):
    """Set a system setting in DB"""
    if USE_POSTGRES:
        run_sql("""
            INSERT INTO system_settings (key, value, updated_at) 
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET value = ?, updated_at = CURRENT_TIMESTAMP
        """, (key, value, value))
    else:
        run_sql("INSERT OR REPLACE INTO system_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))", (key, value))

def load_pending_commission() -> float:
    """Load pending commission from DB"""
    global pending_commission
    value = db_get_setting('pending_commission', '0.0')
    try:
        pending_commission = float(value)
    except ValueError:
        pending_commission = 0.0
    logger.info(f"[COMMISSION] Loaded from DB: ${pending_commission:.2f}")
    
    # Логируем статус комиссий
    if ADMIN_CRYPTO_ID:
        logger.info(f"[COMMISSION] ✅ ADMIN_CRYPTO_ID настроен: {ADMIN_CRYPTO_ID} (исходный: {ADMIN_CRYPTO_ID_RAW})")
        logger.info(f"[COMMISSION] Порог авто-вывода: ${COMMISSION_WITHDRAW_THRESHOLD:.2f}")
        if pending_commission >= COMMISSION_WITHDRAW_THRESHOLD:
            logger.info(f"[COMMISSION] ⚠️ Накоплено ${pending_commission:.2f} >= ${COMMISSION_WITHDRAW_THRESHOLD:.2f} - будет выведено при следующей сделке")
    else:
        logger.warning(f"[COMMISSION] ❌ ADMIN_CRYPTO_ID не настроен! Комиссии не будут выводиться.")
        logger.warning(f"[COMMISSION] Установите ADMIN_CRYPTO_ID в .env (например: U1077249 или 1077249)")
    
    return pending_commission

def save_pending_commission():
    """Save pending commission to DB"""
    global pending_commission
    db_set_setting('pending_commission', str(pending_commission))
    logger.debug(f"[COMMISSION] Saved to DB: ${pending_commission:.2f}")

# ==================== PERSISTENT INVOICES ====================
_pending_invoices_db: Dict[int, Dict] = {}  # {invoice_id: {'user_id': int, 'amount': float}}

def db_add_pending_invoice(invoice_id: int, user_id: int, amount: float):
    """Save pending invoice to DB"""
    if USE_POSTGRES:
        run_sql("""
            INSERT INTO pending_invoices (invoice_id, user_id, amount, expires_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP + INTERVAL '1 hour')
            ON CONFLICT (invoice_id) DO UPDATE SET user_id = ?, amount = ?
        """, (invoice_id, user_id, amount, user_id, amount))
    else:
        run_sql("""
            INSERT OR REPLACE INTO pending_invoices (invoice_id, user_id, amount, expires_at)
            VALUES (?, ?, ?, datetime('now', '+1 hour'))
        """, (invoice_id, user_id, amount))
    _pending_invoices_db[invoice_id] = {'user_id': user_id, 'amount': amount}
    logger.info(f"[INVOICE] Saved: #{invoice_id} user={user_id} amount=${amount}")

def db_get_pending_invoice(invoice_id: int) -> Optional[Dict]:
    """Get pending invoice from DB"""
    if invoice_id in _pending_invoices_db:
        return _pending_invoices_db[invoice_id]
    row = run_sql("SELECT user_id, amount FROM pending_invoices WHERE invoice_id = ?", (invoice_id,), fetch="one")
    if row:
        result = {'user_id': row['user_id'], 'amount': row['amount']}
        _pending_invoices_db[invoice_id] = result
        return result
    return None

def db_remove_pending_invoice(invoice_id: int):
    """Remove pending invoice from DB"""
    run_sql("DELETE FROM pending_invoices WHERE invoice_id = ?", (invoice_id,))
    _pending_invoices_db.pop(invoice_id, None)
    logger.info(f"[INVOICE] Removed: #{invoice_id}")

def db_get_all_pending_invoices() -> List[Dict]:
    """Получить все pending invoices для автоматической проверки"""
    if USE_POSTGRES:
        rows = run_sql("""
            SELECT invoice_id, user_id, amount 
            FROM pending_invoices 
            WHERE expires_at > NOW()
            ORDER BY created_at DESC
        """, fetch="all")
    else:
        rows = run_sql("""
            SELECT invoice_id, user_id, amount 
            FROM pending_invoices 
            WHERE expires_at > datetime('now')
            ORDER BY created_at DESC
        """, fetch="all")
    
    # Обновляем кэш
    for row in rows:
        _pending_invoices_db[row['invoice_id']] = {
            'user_id': row['user_id'],
            'amount': row['amount']
        }
    
    return rows if rows else []

def db_cleanup_expired_invoices():
    """Remove expired invoices (older than 1 hour)"""
    if USE_POSTGRES:
        run_sql("DELETE FROM pending_invoices WHERE expires_at < CURRENT_TIMESTAMP")
    else:
        run_sql("DELETE FROM pending_invoices WHERE expires_at < datetime('now')")
    logger.info("[INVOICE] Cleaned up expired invoices")

def load_pending_invoices():
    """Load all pending invoices from DB on startup"""
    global _pending_invoices_db
    rows = run_sql("SELECT invoice_id, user_id, amount FROM pending_invoices", fetch="all")
    if rows:
        for row in rows:
            _pending_invoices_db[row['invoice_id']] = {'user_id': row['user_id'], 'amount': row['amount']}
        logger.info(f"[INVOICE] Loaded {len(rows)} pending invoices from DB")
    else:
        logger.info("[INVOICE] No pending invoices in DB")

# ==================== ADMIN AUDIT LOGGING ====================
def audit_log(admin_id: int, action: str, details: str = "", target_user: int = None):
    """Enhanced audit logging with rate limiting protection"""
    # Rate limit audit logs to prevent spam
    if admin_id not in ADMIN_IDS:
        logger.warning(f"[AUDIT] Non-admin {admin_id} attempted audit log")
        return
    """
    Log admin action for audit trail
    
    Args:
        admin_id: Admin user ID who performed the action
        action: Action type (e.g., 'ADD_BALANCE', 'CLEAR_DB', 'TOGGLE_HEDGE')
        details: Additional details about the action
        target_user: Target user ID if applicable
    """
    from datetime import datetime
    timestamp = datetime.now().isoformat()
    
    log_entry = {
        'timestamp': timestamp,
        'admin_id': admin_id,
        'action': action,
        'details': details,
        'target_user': target_user
    }
    
    # Log to file/console
    target_str = f" -> user {target_user}" if target_user else ""
    logger.warning(f"[AUDIT] Admin {admin_id}: {action}{target_str} | {details}")
    
    # Store in DB for persistence
    key = f"audit_{timestamp}_{admin_id}"
    import json
    db_set_setting(key, json.dumps(log_entry))

async def get_recent_audit_logs(limit: int = 20) -> list:
    """Get recent audit log entries"""
    import json
    rows = run_sql(
        "SELECT key, value FROM system_settings WHERE key LIKE 'audit_%' ORDER BY key DESC LIMIT ?",
        (limit,), fetch="all"
    )
    logs = []
    if rows:
        for row in rows:
            try:
                logs.append(json.loads(row['value']))
            except:
                pass
    return logs

# ==================== SECURITY LIMITS ====================
# Базовое значение - увеличено с 5 до 8
MAX_POSITIONS_PER_USER = 8  # Maximum open positions per user (увеличено для большего количества возможностей)
MIN_BALANCE_RESERVE = 5.0    # Minimum balance to keep after trade
MAX_SINGLE_TRADE = 10000.0   # Maximum single trade amount
MAX_BALANCE = 1000000.0      # Maximum user balance (sanity check)

def get_max_positions_for_user(balance: float) -> int:
    """
    Динамическое определение максимального количества позиций на основе баланса
    
    Логика:
    - Баланс < $100: максимум 3 позиции (меньше риска при малом капитале)
    - Баланс $100-500: максимум 5 позиций
    - Баланс $500-1000: максимум 8 позиций
    - Баланс $1000-5000: максимум 10 позиций
    - Баланс > $5000: максимум 12 позиций
    
    Это позволяет:
    - Защитить мелкие аккаунты от чрезмерной диверсификации
    - Дать крупным аккаунтам больше возможностей
    """
    if balance < 100:
        return 3
    elif balance < 500:
        return 5
    elif balance < 1000:
        return 8
    elif balance < 5000:
        return 10
    else:
        return 12  # Максимум для крупных аккаунтов


# === УМНОЕ РАСПРЕДЕЛЕНИЕ КАПИТАЛА v2.0 ===
# Категории монет для определения максимального размера позиции
COIN_CATEGORY_MAP = {
    # Топ-монеты (высокая ликвидность, низкий риск) - до 20% баланса
    'major': ['BTC', 'ETH'],
    
    # Layer 1 (хорошая ликвидность) - до 15% баланса
    'layer1': ['SOL', 'BNB', 'XRP', 'AVAX', 'NEAR', 'APT', 'SUI', 'SEI', 'TON', 'INJ', 
               'ATOM', 'DOT', 'ADA', 'TRX', 'LTC'],
    
    # Layer 2 (средняя ликвидность) - до 12% баланса
    'layer2': ['ARB', 'OP', 'STRK', 'ZK', 'MATIC', 'POL', 'MANTA', 'METIS', 'IMX'],
    
    # DeFi - до 10% баланса
    'defi': ['UNI', 'AAVE', 'MKR', 'CRV', 'LDO', 'PENDLE', 'GMX', 'DYDX', 'SNX', 
             'COMP', 'SUSHI', '1INCH', 'LINK'],
    
    # AI/Data - до 10% баланса
    'ai': ['FET', 'RNDR', 'TAO', 'WLD', 'ARKM', 'AGIX', 'OCEAN', 'GRT', 'FIL', 'AR'],
    
    # Gaming/NFT - до 8% баланса
    'gaming': ['GALA', 'AXS', 'SAND', 'MANA', 'PIXEL', 'SUPER', 'MAGIC', 'BLUR', 'IMX'],
    
    # Мемы (высокая волатильность) - до 6% баланса
    'memes': ['DOGE', 'PEPE', 'SHIB', 'FLOKI', 'BONK', 'WIF', 'MEME', 'TURBO', 
              'NEIRO', 'POPCAT', 'MOG', 'BRETT', 'BOME', 'MYRO', 'SLERF'],
    
    # Новые листинги (высокий риск) - до 5% баланса
    'new': ['JUP', 'ENA', 'W', 'ETHFI', 'AEVO', 'PORTAL', 'DYM', 'ALT', 'PYTH']
}

# Максимальные проценты от баланса для каждой категории
CATEGORY_MAX_PERCENT = {
    'major': 0.20,    # 20% для BTC/ETH
    'layer1': 0.15,   # 15% для Layer 1
    'layer2': 0.12,   # 12% для Layer 2
    'defi': 0.10,     # 10% для DeFi
    'ai': 0.10,       # 10% для AI
    'gaming': 0.08,   # 8% для Gaming
    'memes': 0.06,    # 6% для мемов
    'new': 0.05,      # 5% для новых листингов
    'unknown': 0.08   # 8% для неизвестных
}

def get_coin_category(symbol: str) -> str:
    """
    Определить категорию монеты
    
    Args:
        symbol: Символ (например, "BTC/USDT" или "BTC")
    
    Returns:
        Категория ('major', 'layer1', 'memes', etc.)
    """
    # Извлекаем базовый символ
    base = symbol.split('/')[0].upper() if '/' in symbol else symbol.replace('USDT', '').upper()
    
    for category, coins in COIN_CATEGORY_MAP.items():
        if base in coins:
            return category
    
    return 'unknown'


def get_max_position_percent(symbol: str) -> float:
    """
    Получить максимальный процент от баланса для позиции
    
    Args:
        symbol: Символ монеты
    
    Returns:
        Максимальный процент (0.05 - 0.20)
    """
    category = get_coin_category(symbol)
    return CATEGORY_MAX_PERCENT.get(category, 0.08)


def calculate_smart_bet_size(balance: float, symbol: str, quality: 'SetupQuality', 
                            loss_streak: int = 0) -> float:
    """
    Рассчитать умный размер ставки на основе:
    - Категории монеты
    - Качества сетапа
    - Серии убытков
    - Баланса пользователя
    
    Args:
        balance: Баланс пользователя
        symbol: Символ монеты
        quality: Качество сетапа
        loss_streak: Количество убытков подряд
    
    Returns:
        Размер ставки в USD
    """
    # Базовый процент по категории монеты
    max_percent = get_max_position_percent(symbol)
    
    # Множитель по качеству сетапа
    quality_mult = {
        SetupQuality.A_PLUS: 1.0,   # 100% от макс. процента
        SetupQuality.A: 0.85,       # 85% от макс. процента
        SetupQuality.B: 0.70,       # 70% от макс. процента
        SetupQuality.C: 0.50,       # 50% от макс. процента
        SetupQuality.D: 0.30        # 30% от макс. процента
    }.get(quality, 0.60)
    
    # Множитель по серии убытков
    loss_mult = 1.0
    if loss_streak >= 4:
        loss_mult = 0.40  # -60% после 4+ убытков
    elif loss_streak >= 3:
        loss_mult = 0.50  # -50% после 3 убытков
    elif loss_streak >= 2:
        loss_mult = 0.75  # -25% после 2 убытков
    
    # Рассчитываем размер ставки
    bet_size = balance * max_percent * quality_mult * loss_mult
    
    # Применяем ограничения
    bet_size = max(AUTO_TRADE_MIN_BET, bet_size)  # Минимум $10
    bet_size = min(AUTO_TRADE_MAX_BET, bet_size)  # Максимум $500
    bet_size = min(bet_size, balance * 0.20)      # Не более 20% баланса в любом случае
    bet_size = min(bet_size, balance - MIN_BALANCE_RESERVE)  # Оставляем резерв
    
    category = get_coin_category(symbol)
    logger.info(f"[SMART_BET] {symbol} ({category}): bet=${bet_size:.2f} (max={max_percent:.0%}, quality={quality_mult:.0%}, loss={loss_mult:.0%})")
    
    return bet_size


# === ДИНАМИЧЕСКИЙ WHITELIST v2.0 ===
# Статический список как fallback + динамическая проверка
ALLOWED_SYMBOLS = {
    # === ОСНОВНЫЕ (всегда разрешены) ===
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT',
    
    # === LAYER 1 ===
    'AVAX/USDT', 'NEAR/USDT', 'APT/USDT', 'SUI/USDT', 'SEI/USDT',
    'TON/USDT', 'INJ/USDT', 'TIA/USDT', 'ATOM/USDT', 'DOT/USDT',
    'ADA/USDT', 'FTM/USDT', 'ALGO/USDT', 'HBAR/USDT', 'ICP/USDT',
    
    # === LAYER 2 ===
    'ARB/USDT', 'OP/USDT', 'STRK/USDT', 'ZK/USDT', 'MATIC/USDT',
    'POL/USDT', 'MANTA/USDT', 'METIS/USDT', 'IMX/USDT',
    
    # === МЕМЫ (высокая волатильность) ===
    'DOGE/USDT', 'PEPE/USDT', 'SHIB/USDT', 'FLOKI/USDT', 'BONK/USDT',
    'WIF/USDT', 'MEME/USDT', 'TURBO/USDT', 'NEIRO/USDT', 'POPCAT/USDT',
    'MOG/USDT', 'BRETT/USDT', 'BOME/USDT', 'MYRO/USDT', 'SLERF/USDT',
    'PEOPLE/USDT', 'LUNC/USDT', 'BABYDOGE/USDT',
    
    # === DeFi ===
    'UNI/USDT', 'AAVE/USDT', 'MKR/USDT', 'CRV/USDT', 'LDO/USDT',
    'PENDLE/USDT', 'GMX/USDT', 'DYDX/USDT', 'SNX/USDT', 'COMP/USDT',
    'SUSHI/USDT', '1INCH/USDT', 'YFI/USDT', 'BAL/USDT',
    
    # === AI & Data ===
    'FET/USDT', 'RNDR/USDT', 'TAO/USDT', 'WLD/USDT', 'ARKM/USDT',
    'AGIX/USDT', 'OCEAN/USDT', 'GRT/USDT', 'FIL/USDT', 'AR/USDT',
    
    # === Gaming & NFT ===
    'GALA/USDT', 'AXS/USDT', 'SAND/USDT', 'MANA/USDT', 'PIXEL/USDT',
    'SUPER/USDT', 'MAGIC/USDT', 'BLUR/USDT',
    
    # === Новые листинги (высокий потенциал) ===
    'JUP/USDT', 'ENA/USDT', 'W/USDT', 'ETHFI/USDT', 'AEVO/USDT',
    'PORTAL/USDT', 'DYM/USDT', 'ALT/USDT', 'PYTH/USDT',
    'SOMI/USDT', 'ROSE/USDT', 'HYPE/USDT', 'FARTCOIN/USDT', 'PUMPFUN/USDT',
    'IP/USDT', 'MYX/USDT', 'ACU/USDT', 'AXL/USDT', 'ENSO/USDT',
    
    # === Прочие ликвидные ===
    'LINK/USDT', 'LTC/USDT', 'TRX/USDT', 'ORDI/USDT', 'BCH/USDT',
    'ETC/USDT', 'XLM/USDT', 'VET/USDT', 'THETA/USDT', 'EGLD/USDT'
}

# Кэш для динамически проверенных символов
# {symbol: {'valid': bool, 'turnover': float, 'checked_at': datetime}}
_dynamic_symbol_cache: Dict[str, Dict] = {}
_DYNAMIC_CACHE_TTL = 3600  # 1 час
_MIN_TURNOVER_FOR_DYNAMIC = 10_000_000  # $10M минимальный оборот

async def check_symbol_dynamically(symbol: str) -> tuple:
    """
    Динамическая проверка символа через Bybit API
    
    Проверяет:
    - Оборот >$10M за 24ч
    - Символ торгуется на Bybit Linear
    - Не стейблкоин
    
    Returns: (is_valid: bool, error_message: str or None, turnover: float)
    """
    global _dynamic_symbol_cache
    
    # Проверяем кэш
    cached = _dynamic_symbol_cache.get(symbol)
    if cached:
        cache_age = (datetime.now() - cached['checked_at']).total_seconds()
        if cache_age < _DYNAMIC_CACHE_TTL:
            return cached['valid'], cached.get('error'), cached.get('turnover', 0)
    
    try:
        import aiohttp
        
        # Конвертируем символ в формат Bybit
        bybit_symbol = symbol.replace('/', '')
        
        async with aiohttp.ClientSession() as session:
            url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={bybit_symbol}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    # API недоступен - используем статический список
                    return symbol in ALLOWED_SYMBOLS, None, 0
                
                data = await resp.json()
        
        if data.get('retCode') != 0:
            return symbol in ALLOWED_SYMBOLS, None, 0
        
        tickers = data.get('result', {}).get('list', [])
        if not tickers:
            # Символ не найден на Bybit
            _dynamic_symbol_cache[symbol] = {
                'valid': False,
                'error': f"Symbol {symbol} not found on Bybit",
                'turnover': 0,
                'checked_at': datetime.now()
            }
            return False, f"Symbol {symbol} not found on Bybit", 0
        
        ticker = tickers[0]
        turnover = float(ticker.get('turnover24h', '0'))
        
        # Проверяем оборот
        if turnover < _MIN_TURNOVER_FOR_DYNAMIC:
            _dynamic_symbol_cache[symbol] = {
                'valid': False,
                'error': f"Low turnover ${turnover/1e6:.1f}M < $10M",
                'turnover': turnover,
                'checked_at': datetime.now()
            }
            return False, f"Low turnover ${turnover/1e6:.1f}M < $10M", turnover
        
        # Символ валиден
        _dynamic_symbol_cache[symbol] = {
            'valid': True,
            'error': None,
            'turnover': turnover,
            'checked_at': datetime.now()
        }
        logger.info(f"[DYNAMIC] Symbol {symbol} validated: turnover ${turnover/1e6:.1f}M")
        return True, None, turnover
        
    except Exception as e:
        logger.warning(f"[DYNAMIC] Error checking {symbol}: {e}")
        # При ошибке - fallback на статический список
        return symbol in ALLOWED_SYMBOLS, None, 0

def validate_amount(amount: float, balance: float, min_amount: float = 1.0) -> tuple:
    """
    Validate trade amount
    Returns: (is_valid: bool, error_message: str or None)
    """
    if not isinstance(amount, (int, float)):
        return False, "Invalid amount type"
    if amount <= 0:
        return False, "Amount must be positive"
    if amount < min_amount:
        return False, f"Minimum amount is ${min_amount}"
    if amount > MAX_SINGLE_TRADE:
        return False, f"Maximum trade is ${MAX_SINGLE_TRADE}"
    if amount > balance:
        return False, f"Insufficient balance (${balance:.2f})"
    if balance - amount < MIN_BALANCE_RESERVE:
        return False, f"Must keep at least ${MIN_BALANCE_RESERVE} reserve"
    return True, None

def validate_symbol(symbol: str) -> tuple:
    """
    Validate trading symbol v2.0 - ДИНАМИЧЕСКАЯ + СТАТИЧЕСКАЯ проверка
    
    Логика:
    1. Сначала проверяем статический whitelist (быстро)
    2. Если не в списке - можно проверить динамически (через API)
    
    Returns: (is_valid: bool, error_message: str or None)
    """
    if not symbol or not isinstance(symbol, str):
        return False, "Invalid symbol"
    symbol = symbol.upper().strip()
    
    # Быстрая проверка статического списка
    if symbol in ALLOWED_SYMBOLS:
        return True, None
    
    # Проверяем кэш динамических символов
    cached = _dynamic_symbol_cache.get(symbol)
    if cached:
        cache_age = (datetime.now() - cached['checked_at']).total_seconds()
        if cache_age < _DYNAMIC_CACHE_TTL:
            if cached['valid']:
                return True, None
            else:
                return False, cached.get('error', f"Symbol {symbol} not supported")
    
    # Символ не в статическом списке и не в кэше
    # Для синхронной функции - просто отклоняем
    # Динамическая проверка будет выполнена асинхронно при необходимости
    return False, f"Symbol {symbol} not in whitelist"


async def validate_symbol_async(symbol: str) -> tuple:
    """
    Асинхронная валидация символа с динамической проверкой
    
    Returns: (is_valid: bool, error_message: str or None)
    """
    if not symbol or not isinstance(symbol, str):
        return False, "Invalid symbol"
    symbol = symbol.upper().strip()
    
    # Быстрая проверка статического списка
    if symbol in ALLOWED_SYMBOLS:
        return True, None
    
    # Динамическая проверка через API
    is_valid, error, turnover = await check_symbol_dynamically(symbol)
    
    if is_valid:
        return True, None
    else:
        return False, error or f"Symbol {symbol} not supported"

def validate_direction(direction: str) -> tuple:
    """
    Validate trade direction
    Returns: (is_valid: bool, error_message: str or None)
    """
    if direction not in ['LONG', 'SHORT']:
        return False, "Direction must be LONG or SHORT"
    return True, None

def sanitize_balance(balance: float) -> float:
    """Ensure balance stays within safe bounds and is valid"""
    import math
    # Handle None, NaN, Inf
    if balance is None or (isinstance(balance, float) and (math.isnan(balance) or math.isinf(balance))):
        logger.warning(f"[SANITIZE] Invalid balance value: {balance}, defaulting to 0.0")
        return 0.0
    return max(0.0, min(MAX_BALANCE, float(balance)))


def sanitize_pnl(pnl: float, max_pnl: float = None) -> float:
    """Ensure PnL is valid and within reasonable bounds"""
    import math
    # Handle None, NaN, Inf
    if pnl is None or (isinstance(pnl, float) and (math.isnan(pnl) or math.isinf(pnl))):
        logger.warning(f"[SANITIZE] Invalid PnL value: {pnl}, defaulting to 0.0")
        return 0.0
    
    pnl_float = float(pnl)
    
    # Handle extremely small values (PostgreSQL real overflow)
    if abs(pnl_float) < 1e-10:
        return 0.0
    
    # Handle extremely large values
    if abs(pnl_float) > 1e10:
        logger.warning(f"[SANITIZE] PnL too large: {pnl_float}, capping")
        return 1e10 if pnl_float > 0 else -1e10
    
    # Bound to reasonable range if max_pnl specified
    if max_pnl:
        return round(max(-max_pnl, min(max_pnl, pnl_float)), 4)
    return round(pnl_float, 4)


def sanitize_amount(amount: float) -> float:
    """Ensure amount is valid and positive"""
    import math
    # Handle None, NaN, Inf
    if amount is None or (isinstance(amount, float) and (math.isnan(amount) or math.isinf(amount))):
        logger.warning(f"[SANITIZE] Invalid amount value: {amount}, defaulting to 0.0")
        return 0.0
    return max(0.0, float(amount))

# ==================== BINANCE API ====================
BINANCE_API = "https://api.binance.com/api/v3"

async def get_real_price(symbol: str) -> Optional[float]:
    """Получить реальную цену с Binance"""
    try:
        binance_symbol = symbol.replace("/", "")  # BTC/USDT -> BTCUSDT
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BINANCE_API}/ticker/price?symbol={binance_symbol}") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data['price'])
    except Exception as e:
        logger.error(f"[BINANCE] Price fetch error for {symbol}: {e}")
    return None

# Cache is now managed by cache_manager.py
# price_cache, users_cache, positions_cache are imported from cache_manager

async def get_cached_price(symbol: str) -> Optional[float]:
    """Получить цену с кэшированием"""
    # Check cache first
    cached = price_cache.get(symbol)
    if cached:
        return cached
    
    # Fetch new price
    price = await get_real_price(symbol)
    if price:
        price_cache.set(symbol, price, ttl=3)  # 3 seconds TTL
    return price

# ==================== ДАННЫЕ (кэш в памяти) ====================
# Caches are now thread-safe via cache_manager
rate_limits: Dict[int, Dict] = {}  # Deprecated - kept for compatibility

# ==================== TRANSACTION LOCKS ====================
# Per-user locks to prevent race conditions on balance operations
_user_locks: Dict[int, asyncio.Lock] = {}

def isolate_errors(func):
    """Decorator to isolate errors in async functions"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"[ISOLATE] Error in {func.__name__}: {e}")
            return None
    return wrapper

def get_user_lock(user_id: int) -> asyncio.Lock:
    """Get or create a lock for a specific user's balance operations"""
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]

async def safe_balance_update(user_id: int, delta: float, reason: str = "") -> bool:
    """
    Safely update user balance with locking to prevent race conditions.
    
    Args:
        user_id: User ID
        delta: Amount to add (positive) or subtract (negative)
        reason: Reason for the update (for logging)
    
    Returns:
        True if successful, False if insufficient balance
    """
    lock = get_user_lock(user_id)
    async with lock:
        user = get_user(user_id)
        old_balance = user['balance']
        new_balance = old_balance + delta
        
        if new_balance < 0:
            logger.warning(f"[BALANCE] Blocked: user {user_id} delta={delta:.2f} would result in negative balance")
            return False
        
        user['balance'] = sanitize_balance(new_balance)
        save_user(user_id)
        logger.info(f"[BALANCE] User {user_id}: {delta:+.2f} -> ${user['balance']:.2f} ({reason})")
        
        # Comprehensive balance logging
        try:
            trade_logger.log_balance_change(user_id, old_balance, user['balance'], reason)
        except:
            pass
        
        return True

# ==================== RATE LIMITING ====================
# Rate limiting is now handled by rate_limiter.py decorator
# Old in-memory rate_limits dict is kept for backward compatibility but not used
rate_limits: Dict[int, Dict] = {}  # Deprecated - kept for compatibility

# ==================== КОМИССИИ (АВТО-ВЫВОД) ====================
# Очередь уведомлений для рефереров (чтобы не спамить при каждой сделке)
_referral_notifications_queue: Dict[int, float] = {}  # {referrer_id: accumulated_amount}

async def add_commission(amount: float, user_id: Optional[int] = None, bot=None):
    """
    Добавить комиссию и вывести при достижении порога (с персистентностью)
    Распределяет комиссию между рефералами и админом
    
    Args:
        amount: Общая сумма комиссии
        user_id: ID пользователя, который открыл сделку (для реферальной системы)
        bot: Telegram bot instance для отправки уведомлений (опционально)
    """
    global pending_commission
    
    # Распределяем комиссию между рефералами и админом
    if user_id:
        # Логируем реферера пользователя для отладки
        direct_referrer = db_get_referrer(user_id)
        logger.info(f"[REF_COMMISSION] User {user_id} direct referrer: {direct_referrer}")
        
        referrer_chain = db_get_referrer_chain(user_id, MAX_REFERRAL_LEVELS)
        
        # Добавить логирование и проверку
        if not referrer_chain:
            logger.info(f"[REF_COMMISSION] No referrer chain for user {user_id}, all commission (${amount:.4f}) to admin")
            pending_commission += amount
            save_pending_commission()
            return
        
        logger.info(f"[REF_COMMISSION] Chain for user {user_id}: {referrer_chain}")
        total_referral_share = 0.0
        
        # Начисляем комиссию рефералам по уровням
        for level, referrer_id in enumerate(referrer_chain):
            if level < len(REFERRAL_COMMISSION_LEVELS):
                # Проверяем, что реферер существует
                referrer = run_sql("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,), fetch="one")
                if not referrer:
                    logger.warning(f"[REF_COMMISSION] Referrer {referrer_id} not found, skipping")
                    continue
                
                # Процент от комиссии для этого уровня
                level_percent = REFERRAL_COMMISSION_LEVELS[level]
                referral_commission = amount * (level_percent / 100)
                
                if referral_commission > 0:  # Начисляем только если комиссия > 0
                    # Начисляем рефереру (atomic SQL update)
                    run_sql("UPDATE users SET balance = balance + ? WHERE user_id = ?", (referral_commission, referrer_id))
                    
                    # Синхронизируем кэш с БД (избегаем race conditions)
                    if referrer_id in users_cache:
                        updated_user = run_sql("SELECT balance FROM users WHERE user_id = ?", (referrer_id,), fetch="one")
                        if updated_user:
                            users_cache[referrer_id]['balance'] = sanitize_balance(updated_user['balance'])
                        else:
                            users_cache.delete(referrer_id)
                    
                    # СОХРАНЯЕМ запись о заработке в таблицу referral_earnings
                    db_save_referral_earning(
                        user_id=referrer_id,
                        from_user_id=user_id,
                        amount=referral_commission,
                        level=level + 1,  # 1-indexed уровень
                        source='trade_commission'
                    )
                    
                    # Добавляем в очередь уведомлений (группируем по реферерам)
                    if referrer_id not in _referral_notifications_queue:
                        _referral_notifications_queue[referrer_id] = 0.0
                    _referral_notifications_queue[referrer_id] += referral_commission
                    
                    total_referral_share += referral_commission
                    logger.info(f"[REF_COMMISSION] Level {level+1}: ${referral_commission:.2f} to user {referrer_id} ({level_percent}% of ${amount:.2f})")
        
        # Остаток идет админу
        admin_commission = amount - total_referral_share
        pending_commission += admin_commission
        logger.info(f"[COMMISSION] Total: ${amount:.2f}, Referrals: ${total_referral_share:.2f}, Admin: ${admin_commission:.2f}")
    else:
        # Если user_id не указан - вся комиссия идет админу (старый режим)
        pending_commission += amount
        logger.info(f"[COMMISSION] +${amount:.2f} (no user_id, all to admin)")
    
    save_pending_commission()  # Persist to DB
    
    logger.info(f"[COMMISSION] Накоплено: ${pending_commission:.2f}")
    
    # Авто-вывод при достижении порога
    if pending_commission >= COMMISSION_WITHDRAW_THRESHOLD and ADMIN_CRYPTO_ID:
        await withdraw_commission()

async def send_referral_notifications(bot) -> int:
    """
    Отправить накопленные уведомления рефералам (вызывается периодически)
    Возвращает количество отправленных уведомлений
    """
    global _referral_notifications_queue
    
    if not _referral_notifications_queue or not bot:
        return 0
    
    sent_count = 0
    notifications_to_send = _referral_notifications_queue.copy()
    _referral_notifications_queue.clear()
    
    for referrer_id, total_amount in notifications_to_send.items():
        if total_amount >= 0.01:  # Минимум $0.01 для уведомления
            try:
                await bot.send_message(
                    referrer_id,
                    f"<b>💰 Реферальный доход</b>\n\n"
                    f"Вы заработали <b>${total_amount:.2f}</b> с комиссий ваших рефералов!",
                    parse_mode="HTML"
                )
                sent_count += 1
                logger.info(f"[REF_NOTIFY] Sent notification to {referrer_id}: ${total_amount:.2f}")
            except Exception as e:
                logger.warning(f"[REF_NOTIFY] Failed to notify {referrer_id}: {e}")
    
    return sent_count

async def withdraw_commission():
    """Вывести накопленные комиссии на кошелёк админа"""
    global pending_commission
    
    if pending_commission < 1:
        logger.info(f"[COMMISSION] Пропуск вывода: сумма ${pending_commission:.2f} < $1")
        return False
    
    amount = pending_commission
    
    # CryptoBot Transfer API
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN", "")
    if not crypto_token:
        logger.warning("[COMMISSION] ❌ CRYPTO_BOT_TOKEN не настроен")
        return False
    
    if not ADMIN_CRYPTO_ID:
        logger.warning("[COMMISSION] ❌ ADMIN_CRYPTO_ID не настроен в .env")
        return False
    
    testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
    base_url = "https://testnet-pay.crypt.bot" if testnet else "https://pay.crypt.bot"
    
    logger.info(f"[COMMISSION] Попытка вывода ${amount:.2f} на ID {ADMIN_CRYPTO_ID} (testnet={testnet})")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Сначала проверим баланс бота
            async with session.get(
                f"{base_url}/api/getBalance",
                headers={"Crypto-Pay-API-Token": crypto_token}
            ) as resp:
                balance_data = await resp.json()
                logger.info(f"[COMMISSION] Bot balance: {balance_data}")
                
                if balance_data.get("ok"):
                    balances = balance_data.get("result", [])
                    usdt_balance = next((b for b in balances if b.get("currency_code") == "USDT"), None)
                    if usdt_balance:
                        available = float(usdt_balance.get("available", 0))
                        logger.info(f"[COMMISSION] USDT доступно: ${available:.2f}")
                        
                        # Если доступно меньше, чем нужно - выводим то, что есть (минимум $1)
                        if available < amount:
                            if available >= 1.0:
                                logger.info(f"[COMMISSION] ⚠️ Доступно меньше запрошенного: ${available:.2f} < ${amount:.2f}, выводим доступное")
                                amount = available  # Выводим то, что есть
                            else:
                                logger.warning(f"[COMMISSION] ❌ Недостаточно USDT на балансе бота: ${available:.2f} < $1.00 (минимум для вывода)")
                                return False
            
            # Трансфер на CryptoBot ID админа
            try:
                user_id_int = int(ADMIN_CRYPTO_ID)
            except (ValueError, TypeError):
                logger.error(f"[COMMISSION] ❌ Неверный формат ADMIN_CRYPTO_ID: '{ADMIN_CRYPTO_ID}' (должен быть числом, например: 1077249 или U1077249)")
                return False
            
            transfer_payload = {
                "user_id": user_id_int,
                "asset": "USDT",
                "amount": str(round(amount, 2)),
                "spend_id": f"commission_{int(datetime.now().timestamp())}"
            }
            logger.info(f"[COMMISSION] Transfer payload: {transfer_payload}")
            
            async with session.post(
                f"{base_url}/api/transfer",
                headers={"Crypto-Pay-API-Token": crypto_token},
                json=transfer_payload
            ) as resp:
                data = await resp.json()
                logger.info(f"[COMMISSION] Transfer response: {data}")
                
                if data.get("ok"):
                    # Вычитаем выведенную сумму из pending_commission (может быть частичный вывод)
                    pending_commission -= amount
                    if pending_commission < 0:
                        pending_commission = 0  # Защита от отрицательных значений
                    save_pending_commission()  # Persist to DB
                    logger.info(f"[COMMISSION] ✅ Выведено ${amount:.2f} на CryptoBot ID {ADMIN_CRYPTO_ID}, осталось: ${pending_commission:.2f}")
                    return True
                else:
                    error = data.get("error", {})
                    error_code = error.get("code", "unknown")
                    error_name = error.get("name", "Unknown")
                    logger.error(f"[COMMISSION] ❌ Ошибка вывода: {error_code} - {error_name}")
                    logger.error(f"[COMMISSION] Full response: {data}")
                    return False
    except Exception as e:
        logger.error(f"[COMMISSION] ❌ Exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# ==================== BATCH ОТПРАВКА (для 500+ юзеров) ====================
async def send_message_batch(bot, user_ids: List[int], text: str, keyboard=None, parse_mode="HTML"):
    """Отправить сообщение многим юзерам параллельно (батчами по 30)"""
    BATCH_SIZE = 30  # Telegram rate limit: ~30 msg/sec
    
    async def send_one(user_id):
        try:
            await bot.send_message(
                user_id, text, 
                reply_markup=keyboard,
                parse_mode=parse_mode
            )
            return True
        except Exception as e:
            logger.error(f"[BATCH] Error sending to {user_id}: {e}")
            return False
    
    sent = 0
    for i in range(0, len(user_ids), BATCH_SIZE):
        batch = user_ids[i:i+BATCH_SIZE]
        results = await asyncio.gather(*[send_one(uid) for uid in batch])
        sent += sum(results)
        
        # Пауза между батчами чтобы не превысить лимиты
        if i + BATCH_SIZE < len(user_ids):
            await asyncio.sleep(1)
    
    return sent

# ==================== УТИЛИТЫ ====================
def format_price(price: float) -> str:
    """Умное форматирование цены в зависимости от величины"""
    if price >= 1000:
        return f"${price:,.0f}"      # $91,000
    elif price >= 10:
        return f"${price:.1f}"       # $45.2
    elif price >= 1:
        return f"${price:.2f}"       # $1.80
    elif price >= 0.01:
        return f"${price:.4f}"       # $0.0032
    else:
        return f"${price:.6f}"       # $0.000001

def get_user(user_id: int) -> Dict:
    """Получить пользователя (с кэшированием, thread-safe)"""
    user = users_cache.get(user_id)
    if user is None:
        user = db_get_user(user_id)
        users_cache.set(user_id, user)
    return user

def save_user(user_id: int):
    """Сохранить пользователя в БД (thread-safe)"""
    user = users_cache.get(user_id)
    if user:
        balance = user['balance']
        total_deposit = user['total_deposit']
        
        db_update_user(user_id,
            balance=balance,
            total_deposit=total_deposit,
            total_profit=user['total_profit'],
            trading=user['trading'],
            auto_trade=user.get('auto_trade', False),
            auto_trade_max_daily=user.get('auto_trade_max_daily', 10),
            auto_trade_min_winrate=user.get('auto_trade_min_winrate', 70),
            auto_trade_today=user.get('auto_trade_today', 0),
            auto_trade_last_reset=user.get('auto_trade_last_reset')
        )
        
        logger.debug(f"[SAVE_USER] User {user_id} saved: balance=${balance:.2f}, deposit=${total_deposit:.2f}")

def get_positions(user_id: int) -> List[Dict]:
    """Получить позиции (с кэшированием, thread-safe)
    
    Returns a copy of cached positions to prevent external modification.
    """
    try:
        positions = positions_cache.get(user_id)
        if positions is None:
            positions = db_get_positions(user_id)
            if positions:
                positions_cache.set(user_id, positions)
        # Return a shallow copy to prevent cache corruption
        return list(positions) if positions else []
    except Exception as e:
        logger.error(f"[CACHE] Error getting positions for user {user_id}: {e}")
        # Fallback to DB on cache error
        try:
            return db_get_positions(user_id)
        except Exception:
            return []

def update_positions_cache(user_id: int, positions: List[Dict]):
    """Обновить кэш позиций (thread-safe)"""
    try:
        positions_cache.set(user_id, positions)
    except Exception as e:
        logger.error(f"[CACHE] Error updating positions cache for user {user_id}: {e}")

# ==================== БАННЕРЫ ДЛЯ МЕНЮ ====================
# Кэш file_id для баннеров (загружается из БД)
BANNER_CACHE = {}

def get_banner(banner_type: str) -> str:
    """Получить file_id баннера из кэша или БД"""
    if banner_type not in BANNER_CACHE:
        BANNER_CACHE[banner_type] = db_get_setting(f"banner_{banner_type}")
    return BANNER_CACHE.get(banner_type, "")

def set_banner(banner_type: str, file_id: str):
    """Установить file_id баннера"""
    BANNER_CACHE[banner_type] = file_id
    db_set_setting(f"banner_{banner_type}", file_id)
    logger.info(f"[BANNER] Set {banner_type}: {file_id[:30]}...")

async def send_menu_photo(bot, chat_id: int, banner_type: str, text: str, reply_markup, message_to_edit=None):
    """Отправить или отредактировать сообщение с фото"""
    from telegram import InputMediaPhoto
    
    try:
        banner_id = get_banner(banner_type)
    except Exception as e:
        logger.warning(f"[BANNER] Error getting banner {banner_type}: {e}")
        banner_id = None
    
    if message_to_edit:
        try:
            current_has_photo = message_to_edit.photo is not None and len(message_to_edit.photo) > 0
            
            if current_has_photo and banner_id:
                # Оба с фото - редактируем media
                try:
                    await message_to_edit.edit_media(
                        media=InputMediaPhoto(media=banner_id, caption=text, parse_mode="HTML"),
                        reply_markup=reply_markup
                    )
                    return
                except Exception as e:
                    logger.warning(f"[BANNER] edit_media failed: {e}")
            
            elif current_has_photo and not banner_id:
                # Было фото, нужен текст - редактируем caption
                try:
                    await message_to_edit.edit_caption(
                        caption=text,
                        reply_markup=reply_markup,
                        parse_mode="HTML"
                    )
                    return
                except Exception as e:
                    logger.warning(f"[BANNER] edit_caption failed: {e}")
            
            elif not current_has_photo and not banner_id:
                # Оба текст - редактируем текст
                try:
                    await message_to_edit.edit_text(
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode="HTML"
                    )
                    return
                except Exception as e:
                    logger.warning(f"[BANNER] edit_text failed: {e}")
            
            # Если текст->фото или что-то пошло не так - удаляем и отправляем новое
            try:
                await message_to_edit.delete()
            except Exception as e:
                logger.debug(f"[BANNER] Could not delete message: {e}")
        except Exception as e:
            logger.warning(f"[BANNER] Error processing message_to_edit: {e}")
    
    # Отправляем новое сообщение
    if banner_id:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=banner_id,
                caption=text,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
            return
        except Exception as e:
            logger.warning(f"[BANNER] Failed to send photo: {e}, falling back to text")
    
    # Fallback - просто текст
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[BANNER] Failed to send message to {chat_id}: {e}", exc_info=True)
        raise  # Пробрасываем ошибку выше для обработки


async def edit_or_send(query, text: str, reply_markup, parse_mode: str = "HTML"):
    """Умное редактирование - работает и с фото и с текстом"""
    message = query.message
    
    if message.photo:
        # Сообщение с фото - редактируем caption
        try:
            await message.edit_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
            return
        except BadRequest as e:
            # Сообщение не изменилось или другая BadRequest ошибка
            logger.debug(f"[EDIT_OR_SEND] BadRequest on edit_caption: {e}")
            raise  # Пробрасываем BadRequest дальше
        except Exception as e:
            logger.warning(f"[EDIT_OR_SEND] Error editing caption: {e}")
            # Пробуем удалить и отправить новое
            try:
                await message.delete()
                await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception as e2:
                logger.error(f"[EDIT_OR_SEND] Fallback failed: {e2}")
                raise
    
    # Обычное текстовое сообщение
    try:
        await message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        # Сообщение не изменилось или другая BadRequest ошибка
        logger.debug(f"[EDIT_OR_SEND] BadRequest on edit_text: {e}")
        raise  # Пробрасываем BadRequest дальше
    except Exception as e:
        logger.warning(f"[EDIT_OR_SEND] Error editing text: {e}")
        # Пробуем удалить и отправить новое
        try:
            await message.delete()
            await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception as e2:
            logger.error(f"[EDIT_OR_SEND] Fallback failed: {e2}")
            raise

# ==================== ГЛАВНЫЙ ЭКРАН ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    try:
        logger.info(f"[START] User {user_id}")
        
        # Принудительно читаем из БД (не из кэша) для актуального баланса
        users_cache.pop(user_id, None)
        
        # Обработка реферальной ссылки ДО создания пользователя
        referrer_id_to_set = None
        if context.args and len(context.args) > 0:
            ref_arg = context.args[0]
            if ref_arg.startswith("ref_"):
                try:
                    referrer_id_to_set = int(ref_arg.replace("ref_", ""))
                    logger.info(f"[REF] Referral link detected: user {user_id} from {referrer_id_to_set}")
                except ValueError:
                    pass
        
        # Получаем пользователя (создастся если не существует)
        try:
            user = get_user(user_id)
        except Exception as e:
            logger.error(f"[START] Error getting user {user_id}: {e}", exc_info=True)
            trade_logger.log_error(f"Error getting user in /start: {e}", error=e, user_id=user_id)
            # Пытаемся отправить сообщение об ошибке
            try:
                await update.message.reply_text(
                    "<b>❌ Ошибка</b>\n\nПроизошла ошибка при загрузке данных. Попробуйте позже.",
                    parse_mode="HTML"
                )
            except:
                pass
            return
        
        # Устанавливаем реферера если ссылка была и пользователь новый (без реферера)
        if referrer_id_to_set and not user.get('referrer_id'):
            try:
                if db_set_referrer(user_id, referrer_id_to_set):
                    logger.info(f"[REF] User {user_id} registered via referral from {referrer_id_to_set}")
                    # Обновляем кэш
                    users_cache.pop(user_id, None)
                    user = get_user(user_id)
            except Exception as e:
                logger.warning(f"[START] Error setting referrer: {e}")
        
        balance = user.get('balance', 0.0)
        trading_status = "ВКЛ" if user.get('trading', False) else "ВЫКЛ"
        auto_trade_status = "ВКЛ" if user.get('auto_trade', False) else "ВЫКЛ"
        
        # Получаем статистику пользователя
        try:
            stats = db_get_user_stats(user_id)
            wins = stats.get('wins', 0)
            total_trades = stats.get('total', 0)
            winrate = stats.get('winrate', '0%')
            total_profit = stats.get('total_pnl', 0.0)
            profit_str = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
        except Exception as e:
            logger.warning(f"[START] Error getting stats for user {user_id}: {e}")
            wins = 0
            total_trades = 0
            winrate = '0%'
            profit_str = "$0.00"
        
        text = f"""Торговля: {trading_status}
Авто-трейд: {auto_trade_status}

📊 Статистика: {wins}/{total_trades} ({winrate}%) | Профит: {profit_str}

💰 Баланс: <b>${balance:.2f}</b>"""
        
        keyboard = [
            [InlineKeyboardButton(f"{'❌ Выкл' if user.get('trading', False) else '✅ Вкл'}", callback_data="toggle"),
             InlineKeyboardButton(f"{'✅' if user.get('auto_trade', False) else '❌'} Авто-трейд", callback_data="auto_trade_menu")],
            [InlineKeyboardButton("💳 Пополнить", callback_data="deposit"), InlineKeyboardButton("📊 Сделки", callback_data="trades")],
            [InlineKeyboardButton("Дополнительно", callback_data="more_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            if update.callback_query:
                # Возврат с другого меню - редактируем
                await send_menu_photo(
                    context.bot, user_id, "menu",
                    text, reply_markup,
                    message_to_edit=update.callback_query.message
                )
            else:
                # Новый /start
                await send_menu_photo(context.bot, user_id, "menu", text, reply_markup)
        except Exception as e:
            logger.error(f"[START] Error sending menu to user {user_id}: {e}", exc_info=True)
            trade_logger.log_error(f"Error sending menu in /start: {e}", error=e, user_id=user_id)
            # Fallback - отправляем простое текстовое сообщение
            try:
                if update.message:
                    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
                elif update.callback_query:
                    await update.callback_query.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
            except Exception as fallback_error:
                logger.error(f"[START] Fallback also failed for user {user_id}: {fallback_error}")
    except Exception as e:
        logger.error(f"[START] Critical error for user {user_id}: {e}", exc_info=True)
        trade_logger.log_error(f"Critical error in /start: {e}", error=e, user_id=user_id)
        # Пытаемся отправить сообщение об ошибке
        try:
            if update.message:
                await update.message.reply_text(
                    "<b>❌ Ошибка</b>\n\nПроизошла критическая ошибка. Попробуйте позже или обратитесь к администратору.",
                    parse_mode="HTML"
                )
        except:
            pass

# ==================== ПОПОЛНЕНИЕ ====================
async def deposit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        query = update.callback_query
        if not query:
            logger.warning("[DEPOSIT] No callback_query in update")
            return
        
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            logger.warning("[DEPOSIT] No user_id in update")
            return
        
        logger.info(f"[DEPOSIT] User {user_id}")
        
        try:
            await query.answer()
        except Exception as e:
            logger.warning(f"[DEPOSIT] Error answering callback: {e}")
        
        user = get_user(user_id)
        balance = user['balance']
        
        text = f"""Минимум: ${MIN_DEPOSIT}

💰 Баланс: <b>${balance:.2f}</b>"""
        
        keyboard = [
            [InlineKeyboardButton("⭐ Telegram Stars", callback_data="pay_stars")],
            [InlineKeyboardButton("💎 Crypto (USDT/TON)", callback_data="pay_crypto")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back")]
        ]
        
        await send_menu_photo(
            context.bot, user_id, "deposit",
            text, InlineKeyboardMarkup(keyboard),
            message_to_edit=query.message
        )
    except Exception as e:
        logger.error(f"[DEPOSIT] Error in deposit_menu: {e}")
        trade_logger.log_error(f"Error in deposit_menu: {e}", error=e, user_id=user_id if 'user_id' in locals() else None)

async def pay_stars_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = """<b>⭐ Пополнение через Telegram Stars</b>

Курс: <b>50 ⭐ = $1</b>

Выберите сумму пополнения:"""
    
    # 50 stars = $1
    keyboard = [
        [
            InlineKeyboardButton("🧪 $0.02 (1⭐)", callback_data="stars_1"),
            InlineKeyboardButton("$1 (50⭐)", callback_data="stars_50")
        ],
        [
            InlineKeyboardButton("$5 (250⭐)", callback_data="stars_250"),
            InlineKeyboardButton("$10 (500⭐)", callback_data="stars_500")
        ],
        [
            InlineKeyboardButton("$25 (1250⭐)", callback_data="stars_1250"),
            InlineKeyboardButton("$50 (2500⭐)", callback_data="stars_2500")
        ],
        [InlineKeyboardButton("$100 (5000⭐)", callback_data="stars_5000")],
        [InlineKeyboardButton("✏️ Своё значение", callback_data="stars_custom")],
        [InlineKeyboardButton("🔙 Назад", callback_data="deposit")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def stars_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрос своей суммы для Stars"""
    query = update.callback_query
    await query.answer()
    
    text = """<b>✏️ Своё значение</b>

Введите сумму в долларах (от $1 до $500):

<i>Например: 15</i>"""
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="pay_stars")]]
    
    context.user_data['awaiting_stars_amount'] = True
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def send_stars_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    stars_map = {"stars_1": 1, "stars_50": 50, "stars_250": 250, "stars_500": 500, "stars_1250": 1250, "stars_2500": 2500, "stars_5000": 5000}
    stars = stars_map.get(query.data, 50)
    usd = stars / STARS_RATE  # Дробное деление для маленьких сумм
    
    logger.info(f"[STARS] User {user_id} requested invoice: {stars} stars = ${usd}")
    
    try:
        await query.message.delete()
    except Exception:
        pass
    
    try:
        usd_label = f"${usd:.2f}" if usd < 1 else f"${usd:.0f}"
        await context.bot.send_invoice(
            chat_id=user_id,
            title=f"Пополнение {usd_label}",
            description=f"Пополнение баланса на {usd_label}",
            payload=f"deposit_{usd}",
            provider_token="",  # Пустая строка для Telegram Stars
            currency="XTR",
            prices=[LabeledPrice(label=usd_label, amount=stars)]
        )
        logger.info(f"[STARS] Invoice sent successfully to user {user_id}: {stars} stars")
    except Exception as e:
        logger.error(f"[STARS] Failed to send invoice to user {user_id}: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=user_id,
            text="<b>❌ Ошибка</b>\n\nНе удалось создать счёт для оплаты Stars.\nПопробуйте позже или используйте другой способ пополнения.",
            parse_mode="HTML"
        )

async def precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    user_id = query.from_user.id
    logger.info(f"[STARS] PreCheckout received from user {user_id}: payload={query.invoice_payload}, amount={query.total_amount}")
    await query.answer(ok=True)
    logger.info(f"[STARS] PreCheckout approved for user {user_id}")

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    logger.info(f"[STARS] Successful payment received from user {user_id}")
    
    try:
        user = get_user(user_id)
        
        payment = update.message.successful_payment
        stars = payment.total_amount
        usd = stars / STARS_RATE  # Дробное деление для маленьких сумм
        
        logger.info(f"[STARS] Payment details - user {user_id}: {stars} stars = ${usd:.4f}, payload={payment.invoice_payload}")
        
        if usd <= 0:
            logger.error(f"[PAYMENT] User {user_id}: Invalid payment amount {stars} stars")
            await update.message.reply_text(
                "<b>❌ Ошибка</b>\n\nНекорректная сумма платежа.",
                parse_mode="HTML"
            )
            return
        
        # Проверяем первый депозит для реферального бонуса
        # Первый депозит = когда total_deposit был 0 до этого депозита
        old_total_deposit = user.get('total_deposit', 0) or 0
        is_first_deposit = old_total_deposit == 0.0
        
        # Atomic balance update with lock
        async with get_user_lock(user_id):
            user = get_user(user_id)  # Re-read with lock
            user['balance'] += usd
            user['total_deposit'] = (user.get('total_deposit', 0) or 0) + usd
            save_user(user_id)
        
        logger.info(f"[PAYMENT] User {user_id} deposited ${usd} via Stars (balance: ${user['balance']:.2f})")
        
        # Многоуровневые реферальные бонусы при первом депозите
        if is_first_deposit:
            try:
                bonuses = await process_multilevel_deposit_bonus(user_id, bot=context.bot)
                if bonuses:
                    logger.info(f"[PAYMENT] Referral bonuses given: {bonuses}")
            except Exception as e:
                logger.error(f"[PAYMENT] Referral bonus error for user {user_id}: {e}")
            
        text = f"""<b>✅ Оплата успешна</b>

Зачислено: <b>${usd:.2f}</b>

💰 Баланс: ${user['balance']:.2f}"""
        
        keyboard = [[InlineKeyboardButton("🏠 Домой", callback_data="back")]]
        
        # Отправляем с баннером если есть
        banner_id = get_banner("payment")
        if banner_id:
            await update.message.reply_photo(
                photo=banner_id,
                caption=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"[PAYMENT] Critical error for user {user_id}: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "<b>❌ Ошибка обработки платежа</b>\n\nОбратитесь в поддержку.",
                parse_mode="HTML"
            )
        except:
            pass

# ==================== CRYPTO ПОПОЛНЕНИЕ ====================
async def pay_crypto_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = """<b>💎 Через Crypto</b>

Выбери сумму:"""
    
    keyboard = [
        [
            InlineKeyboardButton("$1", callback_data="crypto_1"),
            InlineKeyboardButton("$5", callback_data="crypto_5")
        ],
        [
            InlineKeyboardButton("$10", callback_data="crypto_10"),
            InlineKeyboardButton("$25", callback_data="crypto_25")
        ],
        [
            InlineKeyboardButton("$50", callback_data="crypto_50"),
            InlineKeyboardButton("$100", callback_data="crypto_100")
        ],
        [InlineKeyboardButton("💵 Своя сумма", callback_data="crypto_custom")],
        [InlineKeyboardButton("🔙 Назад", callback_data="deposit")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def crypto_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрос своей суммы для crypto депозита"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['awaiting_crypto_amount'] = True
    
    text = """<b>💎 Своя сумма</b>

Введи сумму в USDT (от $1 до $1000):"""
    
    keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data="pay_crypto")]]
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def handle_crypto_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Обработка ввода своей суммы для crypto"""
    if not context.user_data.get('awaiting_crypto_amount'):
        return False
    
    try:
        amount = float(update.message.text.replace('$', '').replace(',', '.').strip())
        if amount < 1 or amount > 1000:
            await update.message.reply_text(
                "<b>❌ Ошибка</b>\n\nСумма должна быть от $1.00 до $1000.00",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="pay_crypto")]]),
                parse_mode="HTML"
            )
            return True
        
        context.user_data['awaiting_crypto_amount'] = False
        
        # Создаём инвойс напрямую
        user_id = update.effective_user.id
        amount = int(amount) if amount == int(amount) else round(amount, 2)
        
        crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
        if not crypto_token:
            await update.message.reply_text(
                "<b>❌ Временно недоступно</b>\n\nCrypto-платежи временно недоступны. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]]),
                parse_mode="HTML"
            )
            return True
        
        try:
            is_testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
            base_url = "https://testnet-pay.crypt.bot/api" if is_testnet else "https://pay.crypt.bot/api"
            
            async with aiohttp.ClientSession() as session:
                headers = {"Crypto-Pay-API-Token": crypto_token}
                payload = {
                    "asset": "USDT",
                    "amount": str(amount),
                    "description": f"Пополнение баланса ${amount}",
                    "payload": f"{user_id}_{amount}",
                    "expires_in": 3600
                }
                
                async with session.post(f"{base_url}/createInvoice", headers=headers, json=payload) as resp:
                    data = await resp.json()
                    
                    if not data.get("ok"):
                        raise Exception(data.get("error", {}).get("name", "Unknown error"))
                    
                    invoice = data["result"]
            
            db_add_pending_invoice(invoice['invoice_id'], user_id, amount)
            
            text = f"""<b>💎 Оплата</b>

К оплате: <b>${amount:.2f} USDT</b>

После оплаты средства будут зачислены автоматически."""
            
            keyboard = [
                [InlineKeyboardButton("💳 Оплатить", url=invoice['bot_invoice_url'])],
                [InlineKeyboardButton("🔙 Отмена", callback_data="deposit")]
            ]
            
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"[CRYPTO] Custom amount error: {e}")
            await update.message.reply_text(
                "<b>❌ Ошибка</b>\n\nНе удалось создать платёж. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]]),
                parse_mode="HTML"
            )
        
        return True
        
    except ValueError:
        await update.message.reply_text(
            "<b>❌ Ошибка</b>\n\nВведи число, например: 15 или 25.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="pay_crypto")]]),
            parse_mode="HTML"
        )
        return True

async def create_crypto_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    amount_map = {"crypto_1": 1, "crypto_5": 5, "crypto_10": 10, "crypto_25": 25, "crypto_50": 50, "crypto_100": 100}
    amount = amount_map.get(query.data, 1)
    user_id = update.effective_user.id
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    
    if not crypto_token:
        await edit_or_send(query, "<b>❌ Временно недоступно</b>\n\nCrypto-платежи временно недоступны. Попробуйте позже.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]]))
        return
    
    try:
        is_testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
        base_url = "https://testnet-pay.crypt.bot/api" if is_testnet else "https://pay.crypt.bot/api"
        logger.info(f"[CRYPTO] Using API: {base_url}")
        
        # Прямой запрос к CryptoBot API
        async with aiohttp.ClientSession() as session:
            headers = {"Crypto-Pay-API-Token": crypto_token}
            payload = {
                "asset": "USDT",
                "amount": str(amount),
                "description": f"Пополнение баланса ${amount}",
                "payload": f"{user_id}_{amount}",
                "expires_in": 3600
            }
            
            async with session.post(f"{base_url}/createInvoice", headers=headers, json=payload) as resp:
                data = await resp.json()
                logger.info(f"[CRYPTO] Response: {data}")
                
                if not data.get("ok"):
                    raise Exception(data.get("error", {}).get("name", "Unknown error"))
                
                invoice = data["result"]
        
        # Сохраняем invoice_id для проверки (в БД для персистентности)
        db_add_pending_invoice(invoice['invoice_id'], user_id, amount)
        
        text = f"""<b>💎 Оплата</b>

К оплате: <b>${amount} USDT</b>

После оплаты средства будут зачислены автоматически."""
        
        keyboard = [
            [InlineKeyboardButton("💳 Оплатить", url=invoice['bot_invoice_url'])],
            [InlineKeyboardButton("🔙 Отмена", callback_data="deposit")]
        ]
        
        await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
        
    except Exception as e:
        logger.error(f"[CRYPTO] Error: {e}")
        await query.edit_message_text(
            "<b>❌ Ошибка</b>\n\nНе удалось создать платёж. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]]),
            parse_mode="HTML"
        )

async def check_crypto_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    try:
        invoice_id = int(query.data.split("_")[1])
    except (ValueError, IndexError):
        await query.answer("Ошибка данных", show_alert=True)
        return
    
    # Use persistent DB instead of context.bot_data
    pending_info = db_get_pending_invoice(invoice_id)
    if not pending_info:
        await query.edit_message_text(
            "<b>❌ Ошибка</b>\n\nПлатёж не найден или истёк. Создайте новый.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]]),
            parse_mode="HTML"
        )
        return
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    if not crypto_token:
        await query.answer("Ошибка", show_alert=True)
        return
    
    # Показываем статус проверки
    await query.edit_message_text(
        "<b>⏳ Проверяем ваш платёж...</b>",
        parse_mode="HTML"
    )
    
    try:
        is_testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
        base_url = "https://testnet-pay.crypt.bot/api" if is_testnet else "https://pay.crypt.bot/api"
        
        # Прямой запрос к CryptoBot API
        async with aiohttp.ClientSession() as session:
            headers = {"Crypto-Pay-API-Token": crypto_token}
            params = {"invoice_ids": invoice_id}
            
            async with session.get(f"{base_url}/getInvoices", headers=headers, params=params) as resp:
                data = await resp.json()
                
                if not data.get("ok") or not data.get("result", {}).get("items"):
                    # Платёж не найден - возвращаем к оплате
                    amount = pending_info['amount']
                    text = f"""<b>💎 Оплата</b>

К оплате: <b>${amount:.2f} USDT</b>

Платёж ещё не получен. Оплатите и попробуйте снова."""
                    
                    # Получаем URL инвойса заново
                    keyboard = [
                        [InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_{invoice_id}")],
                        [InlineKeyboardButton("🔙 Отмена", callback_data="deposit")]
                    ]
                    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
                    return
                
                invoice = data["result"]["items"][0]
        
        if invoice.get("status") == "paid":
            # Remove from DB
            db_remove_pending_invoice(invoice_id)
            user_id = pending_info['user_id']
            amount = pending_info['amount']
            
            # Защита от race conditions при изменении баланса
            async with get_user_lock(user_id):
                user = get_user(user_id)
                old_balance = user['balance']
                old_total_deposit = user['total_deposit']
                
                # Первый депозит = когда total_deposit был 0 ДО этого депозита
                is_first_deposit = old_total_deposit == 0.0
                
                user['balance'] = sanitize_balance(user['balance'] + amount)
                user['total_deposit'] += amount
                save_user(user_id)
                
                logger.info(f"[DEPOSIT] User {user_id}: ${old_balance:.2f} + ${amount:.2f} = ${user['balance']:.2f} (total_deposit: ${user['total_deposit']:.2f})")
            
            logger.info(f"[CRYPTO] User {user_id} deposited ${amount}")
            
            # Многоуровневые реферальные бонусы при первом депозите
            if is_first_deposit:
                try:
                    bonuses = await process_multilevel_deposit_bonus(user_id, bot=context.bot)
                    if bonuses:
                        logger.info(f"[CRYPTO] Referral bonuses given: {bonuses}")
                except Exception as e:
                    logger.error(f"[CRYPTO] Referral bonus error for user {user_id}: {e}")
            
            text = f"""<b>✅ Оплата успешна</b>

Зачислено: <b>${amount:.2f}</b>

💰 Баланс: ${user['balance']:.2f}"""
            
            keyboard = [[InlineKeyboardButton("🏠 Домой", callback_data="back")]]
            
            # Удаляем старое сообщение и отправляем с баннером
            banner_id = get_banner("payment")
            try:
                await query.message.delete()
            except:
                pass
            if banner_id:
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=banner_id,
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            else:
                await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        else:
            # Платёж не оплачен
            amount = pending_info['amount']
            text = f"""<b>💎 Оплата</b>

К оплате: <b>${amount:.2f} USDT</b>

Платёж ещё не получен. Оплатите и попробуйте снова."""
            
            keyboard = [
                [InlineKeyboardButton("💳 Оплатить", url=invoice.get('bot_invoice_url', ''))],
                [InlineKeyboardButton("🔄 Проверить снова", callback_data=f"check_{invoice_id}")],
                [InlineKeyboardButton("🔙 Отмена", callback_data="deposit")]
            ]
            await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
            
    except Exception as e:
        logger.error(f"[CRYPTO] Check error: {e}")
        await query.edit_message_text(
            "<b>❌ Ошибка проверки</b>\n\nНе удалось проверить платёж. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]]),
            parse_mode="HTML"
        )

# ==================== АВТОМАТИЧЕСКАЯ ПРОВЕРКА КРИПТО-ПЛАТЕЖЕЙ ====================
async def check_pending_crypto_payments(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Автоматическая проверка всех pending крипто-платежей"""
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    if not crypto_token:
        return
    
    try:
        # Получаем все pending invoices из БД
        pending_invoices = db_get_all_pending_invoices()
        if not pending_invoices:
            return
        
        is_testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
        base_url = "https://testnet-pay.crypt.bot/api" if is_testnet else "https://pay.crypt.bot/api"
        
        async with aiohttp.ClientSession() as session:
            headers = {"Crypto-Pay-API-Token": crypto_token}
            
            for invoice_info in pending_invoices:
                invoice_id = invoice_info['invoice_id']
                user_id = invoice_info['user_id']
                amount = invoice_info['amount']
                
                try:
                    params = {"invoice_ids": invoice_id}
                    async with session.get(f"{base_url}/getInvoices", headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        data = await resp.json()
                        
                        if not data.get("ok") or not data.get("result", {}).get("items"):
                            continue
                        
                        invoice = data["result"]["items"][0]
                        
                        if invoice.get("status") == "paid":
                            # Платёж оплачен - зачисляем средства
                            db_remove_pending_invoice(invoice_id)
                            
                            # Защита от race conditions при изменении баланса
                            async with get_user_lock(user_id):
                                user = get_user(user_id)
                                old_balance = user['balance']
                                old_total_deposit = user['total_deposit']
                                
                                # Первый депозит = когда total_deposit был 0 ДО этого депозита
                                is_first_deposit = old_total_deposit == 0.0
                                
                                user['balance'] = sanitize_balance(user['balance'] + amount)
                                user['total_deposit'] += amount
                                save_user(user_id)
                            
                            logger.info(f"[CRYPTO_AUTO] User {user_id}: ${old_balance:.2f} + ${amount:.2f} = ${user['balance']:.2f}")
                            
                            # Многоуровневые реферальные бонусы при первом депозите
                            if is_first_deposit:
                                try:
                                    bonuses = await process_multilevel_deposit_bonus(user_id, bot=context.bot)
                                    if bonuses:
                                        logger.info(f"[CRYPTO_AUTO] Referral bonuses given: {bonuses}")
                                except Exception as e:
                                    logger.error(f"[CRYPTO_AUTO] Referral bonus error for user {user_id}: {e}")
                            
                            # Уведомляем пользователя
                            try:
                                payment_text = f"""<b>✅ Оплата успешна</b>

Зачислено: <b>${amount:.2f}</b>

💰 Баланс: ${user['balance']:.2f}"""
                                payment_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Домой", callback_data="back")]])
                                banner_id = get_banner("payment")
                                if banner_id:
                                    await context.bot.send_photo(
                                        chat_id=user_id,
                                        photo=banner_id,
                                        caption=payment_text,
                                        parse_mode="HTML",
                                        reply_markup=payment_keyboard
                                    )
                                else:
                                    await context.bot.send_message(
                                        user_id,
                                        payment_text,
                                        parse_mode="HTML",
                                        reply_markup=payment_keyboard
                                    )
                            except Exception as e:
                                logger.warning(f"[CRYPTO_AUTO] Failed to notify user {user_id}: {e}")
                            
                            # Небольшая задержка между проверками
                            await asyncio.sleep(0.5)
                
                except Exception as e:
                    logger.warning(f"[CRYPTO_AUTO] Error checking invoice {invoice_id}: {e}")
                    continue
    
    except Exception as e:
        logger.error(f"[CRYPTO_AUTO] Error in auto-check: {e}")

# ==================== ВЫВОД СРЕДСТВ ====================
async def withdraw_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Меню вывода средств"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    MIN_WITHDRAW = 5.0  # Минимум для вывода
    
    text = f"""<b>💸 Вывод средств</b>

Минимум для вывода: <b>${MIN_WITHDRAW:.2f} USDT</b>

Выбери сумму для вывода:

💰 Баланс: ${user['balance']:.2f}"""
    
    keyboard = [
        [InlineKeyboardButton("$10", callback_data="withdraw_10"),
         InlineKeyboardButton("$25", callback_data="withdraw_25"),
         InlineKeyboardButton("$50", callback_data="withdraw_50")],
        [InlineKeyboardButton("$100", callback_data="withdraw_100"),
         InlineKeyboardButton("$250", callback_data="withdraw_250"),
         InlineKeyboardButton("Всё", callback_data="withdraw_all")],
        [InlineKeyboardButton("💵 Своя сумма", callback_data="withdraw_custom")],
        [InlineKeyboardButton("🔙 Назад", callback_data="more_menu")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка вывода средств"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    try:
        user = get_user(user_id)
        
        MIN_WITHDRAW = 5.0
        WITHDRAW_FEE = 0.0  # Комиссия на вывод (можно настроить)
        
        # Определяем сумму
        if query.data == "withdraw_all":
            amount = max(0, (user.get('balance', 0) or 0) - WITHDRAW_FEE)
        elif query.data.startswith("withdraw_"):
            try:
                amount = float(query.data.split("_")[1])
            except (ValueError, IndexError):
                amount = 0
        else:
            amount = 0
        
        # Валидация
        if amount < MIN_WITHDRAW:
            await edit_or_send(
                query,
                f"<b>❌ Ошибка</b>\n\nМинимальная сумма для вывода: ${MIN_WITHDRAW:.2f}",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="withdraw_menu")]])
            )
            return
        
        balance = user.get('balance', 0) or 0
        if amount > balance:
            await edit_or_send(
                query,
                f"<b>❌ Ошибка</b>\n\nНедостаточно средств.\n\n💰 Баланс: ${balance:.2f}",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="withdraw_menu")]])
            )
            return
        
        # Проверяем наличие открытых позиций
        user_positions = get_positions(user_id)
        if user_positions:
            total_in_positions = sum(p.get('amount', 0) or 0 for p in user_positions)
            available = balance - total_in_positions
            
            if amount > available:
                await edit_or_send(
                    query,
                    f"<b>❌ Ошибка</b>\n\nНедостаточно свободных средств.\n\n"
                    f"📊 В позициях: ${total_in_positions:.2f}\n"
                    f"💵 Доступно: ${available:.2f}\n\n"
                    f"💰 Баланс: ${balance:.2f}",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="withdraw_menu")]])
                )
                return
    except Exception as e:
        logger.error(f"[WITHDRAW] Error handling withdraw for user {user_id}: {e}", exc_info=True)
        try:
            await edit_or_send(
                query,
                "<b>❌ Ошибка</b>\n\nНе удалось обработать запрос на вывод.",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="more_menu")]])
            )
        except:
            pass
        return
    
    # Запрашиваем адрес для вывода
    context.user_data['pending_withdraw'] = {
        'amount': amount,
        'user_id': user_id
    }
    
    text = f"""<b>💸 Вывод средств</b>

Сумма: <b>${amount:.2f} USDT</b>

Отправь адрес кошелька USDT (TRC20) для получения средств.

Или отправь свой Telegram ID для вывода через CryptoBot."""
    
    keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data="withdraw_menu")]]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def withdraw_custom_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик для кнопки 'Своя сумма' в выводе"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    MIN_WITHDRAW = 5.0
    
    context.user_data['awaiting_withdraw_amount'] = True
    
    text = f"""<b>💸 Своя сумма</b>

Минимум: <b>${MIN_WITHDRAW:.2f} USDT</b>

Введи сумму для вывода:

💰 Баланс: ${user['balance']:.2f}"""
    
    await edit_or_send(
        query,
        text,
        InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="withdraw_menu")]])
    )

async def process_withdraw_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка адреса для вывода"""
    user_id = update.effective_user.id
    
    if 'pending_withdraw' not in context.user_data:
        return False
    
    pending = context.user_data['pending_withdraw']
    if pending['user_id'] != user_id:
        return False
    
    amount = pending['amount']
    address_or_id = update.message.text.strip()
    
    # Проверяем формат (может быть адрес или Telegram ID)
    is_telegram_id = address_or_id.isdigit() and len(address_or_id) >= 8
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    if not crypto_token:
        await update.message.reply_text(
            "<b>❌ Ошибка</b>\n\nВывод временно недоступен.",
            parse_mode="HTML"
        )
        del context.user_data['pending_withdraw']
        return True
    
    user = get_user(user_id)
    
    # Проверяем баланс ещё раз
    if amount > user['balance']:
        await update.message.reply_text(
            "<b>❌ Ошибка</b>\n\nНедостаточно средств.",
            parse_mode="HTML"
        )
        del context.user_data['pending_withdraw']
        return True
    
    # Показываем статус
    status_msg = await update.message.reply_text(
        "<b>⏳ Обрабатываем вывод...</b>",
        parse_mode="HTML"
    )
    
    try:
        is_testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
        base_url = "https://testnet-pay.crypt.bot/api" if is_testnet else "https://pay.crypt.bot/api"
        
        async with aiohttp.ClientSession() as session:
            headers = {"Crypto-Pay-API-Token": crypto_token}
            
            if is_telegram_id:
                # Вывод через CryptoBot на Telegram ID
                payload = {
                    "user_id": int(address_or_id),
                    "asset": "USDT",
                    "amount": str(amount),
                    "spend_id": f"{user_id}_{int(datetime.now().timestamp())}"
                }
                
                async with session.post(f"{base_url}/transfer", headers=headers, json=payload) as resp:
                    data = await resp.json()
                    
                    if not data.get("ok"):
                        error_msg = data.get("error", {}).get("name", "Unknown error")
                        raise Exception(error_msg)
                    
                    transfer = data["result"]
                    
                    # Списываем с баланса (с блокировкой)
                    async with get_user_lock(user_id):
                        user = get_user(user_id)  # Re-read with lock
                        # Double-check balance
                        if user.get('balance', 0) < amount:
                            await status_msg.edit_text(
                                "<b>❌ Ошибка</b>\n\nНедостаточно средств на балансе.",
                                parse_mode="HTML"
                            )
                            if 'pending_withdraw' in context.user_data:
                                del context.user_data['pending_withdraw']
                            return True
                        
                        user['balance'] = sanitize_balance(user['balance'] - amount)
                        save_user(user_id)
                    
                    logger.info(f"[WITHDRAW] User {user_id} withdrew ${amount:.2f} to Telegram ID {address_or_id}")
                    
                    await status_msg.edit_text(
                        f"""<b>✅ Вывод выполнен</b>

Сумма: <b>${amount:.2f} USDT</b>
Получатель: Telegram ID {address_or_id}

💰 Баланс: ${user['balance']:.2f}""",
                        parse_mode="HTML"
                    )
                    # Удаляем pending_withdraw после успешного вывода
                    if 'pending_withdraw' in context.user_data:
                        del context.user_data['pending_withdraw']
                    return True  # Явный возврат при успехе
            else:
                # Вывод на внешний адрес (через CryptoBot)
                # CryptoBot не поддерживает прямой вывод на адрес, только через transfer
                # Поэтому используем transfer на Telegram ID или создаём invoice для получения адреса
                await status_msg.edit_text(
                    "<b>❌ Ошибка</b>\n\nВывод на внешний адрес временно недоступен.\n"
                    "Используй свой Telegram ID для вывода через CryptoBot.",
                    parse_mode="HTML"
                )
                del context.user_data['pending_withdraw']
                return True
    
    except Exception as e:
        error_str = str(e).upper()
        logger.error(f"[WITHDRAW] Error for user {user_id}: {e}")
        
        # Определяем понятное сообщение в зависимости от ошибки
        if "METHOD_DISABLED" in error_str:
            error_message = "Вывод временно недоступен. Метод transfer отключен в настройках CryptoBot.\n\nОбратитесь к администратору."
            logger.error(f"[WITHDRAW] METHOD_DISABLED: Transfer method is disabled in CryptoBot settings")
        elif "INSUFFICIENT" in error_str or "NOT_ENOUGH" in error_str:
            error_message = "Недостаточно средств на балансе бота для вывода."
        elif "USER_NOT_FOUND" in error_str:
            error_message = "Пользователь с таким Telegram ID не найден в CryptoBot."
        elif "INVALID" in error_str:
            error_message = "Неверные данные для вывода. Проверьте Telegram ID."
        else:
            error_message = f"Произошла ошибка: {e}"
        
        # Проверяем, что status_msg существует
        if 'status_msg' in locals():
            await status_msg.edit_text(
                f"<b>❌ Ошибка вывода</b>\n\n{error_message}",
                parse_mode="HTML"
            )
        else:
            # Если status_msg не был создан, отправляем новое сообщение
            await update.message.reply_text(
                f"<b>❌ Ошибка вывода</b>\n\n{error_message}",
                parse_mode="HTML"
            )
    
    # Удаляем pending_withdraw только если он еще существует
    if 'pending_withdraw' in context.user_data:
        del context.user_data['pending_withdraw']
    return True

# ==================== ТОРГОВЛЯ ====================
@rate_limit(max_requests=10, window_seconds=60, action_type="toggle")
async def toggle_trading(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        query = update.callback_query
        if not query:
            logger.warning("[TOGGLE] No callback_query in update")
            return
        
        try:
            await query.answer()
        except Exception as e:
            logger.warning(f"[TOGGLE] Error answering callback: {e}")
        
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            logger.warning("[TOGGLE] No user_id in update")
            return
        
        logger.info(f"[TOGGLE] User {user_id}")
        
        # Принудительно читаем из БД чтобы избежать рассинхрона
        users_cache.pop(user_id, None)
        user = get_user(user_id)
        
        new_state = not user['trading']
        
        # Разрешаем включать/выключать торговлю без проверки баланса
        # (для тестирования на реальных деньгах с балансом $0)
        # Обновляем состояние
        user['trading'] = new_state
        
        # Сохраняем напрямую в БД
        db_update_user(user_id, trading=new_state)
        logger.info(f"[TOGGLE] User {user_id} trading = {new_state} (balance: ${user['balance']:.2f})")
        
        # Очищаем кэш чтобы start() получил свежие данные из БД
        users_cache.pop(user_id, None)
        
        await start(update, context)
    except Exception as e:
        logger.error(f"[TOGGLE] Error in toggle_trading: {e}")
        trade_logger.log_error(f"Error in toggle_trading: {e}", error=e, user_id=user_id if 'user_id' in locals() else None)

# ==================== АВТО-ТРЕЙД НАСТРОЙКИ ====================
@rate_limit(max_requests=20, window_seconds=60, action_type="auto_trade")
async def auto_trade_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Меню настроек авто-трейда"""
    try:
        query = update.callback_query
        if not query:
            logger.warning("[AUTO_TRADE_MENU] No callback_query in update")
            return
        
        try:
            await query.answer()
        except Exception as e:
            logger.warning(f"[AUTO_TRADE_MENU] Error answering callback: {e}")
        
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            logger.warning("[AUTO_TRADE_MENU] No user_id in update")
            return
        
        users_cache.pop(user_id, None)  # Обновляем из БД
        user = get_user(user_id)
        balance = user.get('balance', 0)
        positions = get_positions(user_id)
        
        auto_enabled = user.get('auto_trade', False)
        max_daily = user.get('auto_trade_max_daily', 10)
        min_wr = user.get('auto_trade_min_winrate', 70)
        today_count = user.get('auto_trade_today', 0)
        
        status = "✅ ВКЛ" if auto_enabled else "❌ ВЫКЛ"
        
        # Диагностика - почему сигнал может быть пропущен
        max_positions = get_max_positions_for_user(balance)
        current_positions = len(positions)
        available_balance = balance - sum(p.get('amount', 0) for p in positions)
        
        blockers = []
        if not auto_enabled:
            blockers.append("❌ Авто-трейд выключен")
        if current_positions >= max_positions:
            blockers.append(f"❌ Лимит позиций ({current_positions}/{max_positions})")
        if today_count >= max_daily:
            blockers.append(f"❌ Лимит сделок за день ({today_count}/{max_daily})")
        if available_balance < AUTO_TRADE_MIN_BET:
            blockers.append(f"❌ Мало средств (${available_balance:.0f} из ${AUTO_TRADE_MIN_BET} мин.)")
        
        if blockers:
            status_detail = "\n".join(blockers)
        else:
            status_detail = "✅ Готов к торговле"
        
        text = f"""Статус: {status}
Сделок сегодня: {today_count}/{max_daily}
Успешность от: {min_wr}%

<b>Диагностика:</b>
{status_detail}

<i>Позиций: {current_positions}/{max_positions}
Свободно: ${available_balance:.2f}</i>

<blockquote>Бот автоматически входит в сделки по сигналам. Все, что вам нужно — настроить % успешности сделок и ждать, пока YULA войдет в позицию.</blockquote>"""
        
        keyboard = [
            [InlineKeyboardButton(f"{'❌ Выключить' if auto_enabled else '✅ Включить'}", callback_data="auto_trade_toggle")],
            [InlineKeyboardButton(f"📊 Сделок/день: {max_daily}", callback_data="auto_trade_daily_menu")],
            [InlineKeyboardButton(f"📊 Успешность: {min_wr}%", callback_data="auto_trade_winrate_menu")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back")]
        ]
        
        await send_menu_photo(
            context.bot, user_id, "autotrade",
            text, InlineKeyboardMarkup(keyboard),
            message_to_edit=query.message
        )
    except Exception as e:
        logger.error(f"[AUTO_TRADE_MENU] Error in auto_trade_menu: {e}")
        trade_logger.log_error(f"Error in auto_trade_menu: {e}", error=e, user_id=user_id if 'user_id' in locals() else None)

async def auto_trade_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Вкл/выкл авто-трейда"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    new_state = not user.get('auto_trade', False)
    user['auto_trade'] = new_state
    db_update_user(user_id, auto_trade=new_state)
    
    logger.info(f"[AUTO_TRADE] User {user_id} auto_trade = {new_state}")
    
    await auto_trade_menu(update, context)

async def auto_trade_daily_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Меню выбора сделок в день"""
    query = update.callback_query
    await query.answer()
    
    user = get_user(update.effective_user.id)
    current = user.get('auto_trade_max_daily', 10)
    
    text = f"""<b>📊 Сделок в день</b>

Текущее: {current}

Выбери лимит:"""
    
    keyboard = [
        [InlineKeyboardButton("3", callback_data="auto_daily_3"),
         InlineKeyboardButton("5", callback_data="auto_daily_5"),
         InlineKeyboardButton("10", callback_data="auto_daily_10")],
        [InlineKeyboardButton("15", callback_data="auto_daily_15"),
         InlineKeyboardButton("20", callback_data="auto_daily_20"),
         InlineKeyboardButton("∞", callback_data="auto_daily_999")],
        [InlineKeyboardButton("🔙 Назад", callback_data="auto_trade_menu")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def auto_trade_set_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установить лимит сделок в день"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    value = int(query.data.replace("auto_daily_", ""))
    
    user = get_user(user_id)
    user['auto_trade_max_daily'] = value
    db_update_user(user_id, auto_trade_max_daily=value)
    
    logger.info(f"[AUTO_TRADE] User {user_id} max_daily = {value}")
    
    await auto_trade_menu(update, context)

async def auto_trade_winrate_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Меню выбора минимальной успешности"""
    query = update.callback_query
    await query.answer()
    
    user = get_user(update.effective_user.id)
    current = user.get('auto_trade_min_winrate', 70)
    
    text = f"""<b>📊 Успешность</b>

Текущее: {current}%

Торговать сигналы от:"""
    
    keyboard = [
        [InlineKeyboardButton("60%", callback_data="auto_wr_60"),
         InlineKeyboardButton("65%", callback_data="auto_wr_65"),
         InlineKeyboardButton("70%", callback_data="auto_wr_70")],
        [InlineKeyboardButton("75%", callback_data="auto_wr_75"),
         InlineKeyboardButton("80%", callback_data="auto_wr_80"),
         InlineKeyboardButton("85%", callback_data="auto_wr_85")],
        [InlineKeyboardButton("90%", callback_data="auto_wr_90"),
         InlineKeyboardButton("95%", callback_data="auto_wr_95")],
        [InlineKeyboardButton("🔙 Назад", callback_data="auto_trade_menu")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def auto_trade_set_winrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установить успешность для авто-трейда"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    value = int(query.data.replace("auto_wr_", ""))
    
    user = get_user(user_id)
    user['auto_trade_min_winrate'] = value
    db_update_user(user_id, auto_trade_min_winrate=value)
    
    logger.info(f"[AUTO_TRADE] User {user_id} min_winrate = {value}%")
    
    await auto_trade_menu(update, context)

async def sync_bybit_positions(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Синхронизация позиций с Bybit - закрывает позиции которые закрылись на бирже.
    Оптимизированная версия с таймаутами и батчингом.

    Returns:
        Количество синхронизированных (закрытых) позиций
    """
    try:
        # Quick check if hedging is enabled with timeout
        hedging_enabled = await asyncio.wait_for(is_hedging_enabled(), timeout=2.0)
        if not hedging_enabled:
            return 0
    except (asyncio.TimeoutError, Exception):
        return 0

    user_positions = get_positions(user_id)
    if not user_positions:
        return 0

    user = get_user(user_id)
    synced = 0
    closed_pos_ids = []  # Track closed positions for batch cache update

    # Get all open positions on Bybit with timeout
    try:
        bybit_positions = await asyncio.wait_for(hedger.get_all_positions(), timeout=5.0)
    except (asyncio.TimeoutError, Exception) as e:
        logger.warning(f"[SYNC] Timeout/error getting Bybit positions: {e}")
        return 0
    
    # Build lookup dict: symbol -> {size, side}
    bybit_data = {}
    for bp in bybit_positions:
        bybit_side = "LONG" if bp.get('side') == "Buy" else "SHORT"
        bybit_data[bp['symbol']] = {
            'size': float(bp.get('size', 0)),
            'side': bybit_side
        }
    
    # Get closed positions with timeout
    try:
        closed_pnl = await asyncio.wait_for(hedger.get_closed_pnl(limit=100), timeout=5.0)
    except (asyncio.TimeoutError, Exception):
        closed_pnl = []

    import time as time_module
    current_time_ms = int(time_module.time() * 1000)

    for pos in user_positions:
        try:
            bybit_symbol = pos['symbol'].replace("/", "")
            bybit_info = bybit_data.get(bybit_symbol, {'size': 0, 'side': None})
            bybit_size = bybit_info['size']
            bybit_side = bybit_info['side']
            expected_qty = pos.get('bybit_qty', 0)
            
            # Direction mismatch check
            direction_mismatch = bybit_side is not None and bybit_side != pos['direction']

            # Handle orphan positions (no bybit_qty)
            # ВАЖНО: Не закрываем orphan позиции автоматически!
            # Они нормально работают без Bybit (симуляция на основе цен Binance)
            # Закрытие orphan было причиной потери баланса для обычных пользователей
            if expected_qty == 0:
                # Пропускаем - позиция будет обновляться и закрываться по TP/SL в update_positions
                logger.debug(f"[SYNC] Skipping orphan position {pos['id']} - managed locally")
                continue
            
            # Position is closed if: size=0, size << expected, or direction mismatch
            is_closed = bybit_size == 0 or (expected_qty > 0 and bybit_size < expected_qty * 0.1) or direction_mismatch

            if is_closed:
                real_pnl = pos.get('pnl', 0)
                
                # Find real PnL from Bybit closed trades
                for closed in closed_pnl:
                    if closed['symbol'] == bybit_symbol:
                        bybit_pnl = closed['closed_pnl']
                        bybit_qty = closed.get('qty', 0)
                        bybit_time = closed.get('updated_time', 0)
                        closed_side = closed.get('side', '')
                        
                        time_diff = (current_time_ms - bybit_time) / 1000 if bybit_time else 999999
                        qty_ratio = bybit_qty / expected_qty if expected_qty > 0 else 0
                        expected_side = "Sell" if pos['direction'] == "LONG" else "Buy"
                        side_match = closed_side == expected_side or not closed_side
                        
                        # Use Bybit PnL if validation passes
                        if time_diff < 600 and qty_ratio > 0.5 and side_match:
                            real_pnl = bybit_pnl
                            break

                # Sanitize and calculate return
                real_pnl = sanitize_pnl(real_pnl, max_pnl=pos['amount'] * LEVERAGE * 2)
                returned = sanitize_amount(pos['amount']) + real_pnl
                
                # Update balance with lock
                async with get_user_lock(user_id):
                    user = get_user(user_id)
                    user['balance'] = sanitize_balance(user['balance'] + returned)
                    user['total_profit'] += real_pnl
                    save_user(user_id)

                # Close in DB
                db_close_position(pos['id'], pos.get('current', pos['entry']), real_pnl, 'BYBIT_SYNC')
                closed_pos_ids.append(pos['id'])
                synced += 1

                # Non-blocking notification
                ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
                asyncio.create_task(_send_sync_notification(
                    context, user_id, ticker, real_pnl, user['balance']
                ))
                
        except Exception as e:
            logger.error(f"[SYNC] Error processing position {pos.get('id')}: {e}")
            continue

    # Batch update cache once at the end
    if closed_pos_ids:
        try:
            positions_cache.set(user_id, db_get_positions(user_id))
        except Exception:
            pass

    if synced > 0:
        logger.info(f"[SYNC] User {user_id}: synced {synced} positions")
    
    return synced


async def _send_sync_notification(context, user_id: int, ticker: str, pnl: float, balance: float, 
                                   is_orphan: bool = False, returned: float = 0):
    """Helper to send sync notification without blocking"""
    try:
        if is_orphan:
            text = f"<b>📡 Синхронизация</b>\n\n{ticker} закрыт (не был на Bybit)\nВозврат: <b>${returned:.2f}</b>\n\n💰 Баланс: ${balance:.2f}"
        else:
            pnl_abs = abs(pnl)
            pnl_sign = "+" if pnl >= 0 else "-"
            text = f"<b>📡 Bybit</b>\n\n{ticker} закрыт\nИтого: <b>{pnl_sign}${pnl_abs:.2f}</b>\n\n💰 Баланс: ${balance:.2f}"
        
        await asyncio.wait_for(
            context.bot.send_message(user_id, text, parse_mode="HTML"),
            timeout=3.0
        )
    except (asyncio.TimeoutError, Exception):
        pass  # Non-critical, silently ignore


def stack_positions(positions: List[Dict]) -> List[Dict]:
    """
    Группирует одинаковые позиции (тот же символ + направление) в одну
    
    Для отображения - в БД остаются раздельными
    """
    if not positions:
        return []
    
    # Группируем по (symbol, direction)
    groups = {}
    for pos in positions:
        key = (pos['symbol'], pos['direction'])
        if key not in groups:
            groups[key] = []
        groups[key].append(pos)
    
    stacked = []
    for (symbol, direction), group in groups.items():
        if len(group) == 1:
            # Одна позиция - возвращаем как есть
            stacked.append(group[0])
        else:
            # Несколько позиций - объединяем
            total_amount = sum(p['amount'] for p in group)
            total_pnl = sum(p.get('pnl', 0) for p in group)
            
            # Weighted average entry price
            weighted_entry = sum(p['entry'] * p['amount'] for p in group) / total_amount if total_amount > 0 else group[0]['entry']
            
            # Используем последнюю текущую цену
            current = group[-1].get('current', group[-1]['entry'])
            
            # TP/SL берём от первой позиции (они обычно одинаковые)
            tp = group[0].get('tp', 0)
            sl = group[0].get('sl', 0)
            
            # Собираем ID всех позиций для закрытия
            position_ids = [p['id'] for p in group]
            
            # Агрегируем realized_pnl и другие поля
            total_realized_pnl = sum(p.get('realized_pnl', 0) or 0 for p in group)
            tp1 = group[0].get('tp1', tp)
            tp2 = group[0].get('tp2', tp)
            tp3 = group[0].get('tp3', tp)
            tp1_hit = any(p.get('tp1_hit', False) for p in group)
            tp2_hit = any(p.get('tp2_hit', False) for p in group)
            
            stacked.append({
                'id': position_ids[0],  # Главный ID для отображения
                'position_ids': position_ids,  # Все ID для закрытия
                'symbol': symbol,
                'direction': direction,
                'entry': weighted_entry,
                'current': current,
                'amount': total_amount,
                'tp': tp,
                'tp1': tp1,
                'tp2': tp2,
                'tp3': tp3,
                'tp1_hit': tp1_hit,
                'tp2_hit': tp2_hit,
                'sl': sl,
                'pnl': total_pnl,
                'realized_pnl': total_realized_pnl,
                'commission': sum(p.get('commission', 0) for p in group),
                'stacked_count': len(group)  # Сколько позиций объединено
            })
    
    return stacked


@rate_limit(max_requests=10, window_seconds=60, action_type="close_symbol")
async def close_symbol_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть все позиции по конкретному символу"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    # Получаем символ из callback_data: close_symbol|BTC/USDT
    parts = query.data.split("|")
    if len(parts) < 2:
        await query.answer("Ошибка", show_alert=True)
        return
    
    symbol = parts[1]
    ticker = symbol.split("/")[0] if "/" in symbol else symbol.replace("USDT", "")
    
    # Находим позиции по этому символу (только с amount > 0)
    positions_to_close = [p for p in user_positions if p['symbol'] == symbol and p.get('amount', 0) > 0]
    
    # Удаляем позиции с amount=0 (полностью закрыты частичными тейками)
    zero_amount_positions = [p for p in user_positions if p['symbol'] == symbol and p.get('amount', 0) <= 0]
    for zero_pos in zero_amount_positions:
        realized_pnl = zero_pos.get('realized_pnl', 0) or 0
        db_close_position(zero_pos['id'], zero_pos.get('current', zero_pos['entry']), realized_pnl, 'FULLY_CLOSED')
        logger.info(f"[CLOSE_SYMBOL] Removed zero-amount position {zero_pos['id']}")
    
    if not positions_to_close:
        await edit_or_send(
            query,
            f"<b>📭 Нет открытых позиций</b>\n\nПо {ticker}",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
        )
        return
    
    await edit_or_send(query, f"<b>⏳ Закрываем {ticker}...</b>", None)
    
    # СНАЧАЛА закрываем на Bybit с timeout protection
    try:
        hedging_enabled = await asyncio.wait_for(is_hedging_enabled(), timeout=3.0)
    except (asyncio.TimeoutError, Exception):
        hedging_enabled = False
    
    failed_positions = []
    
    if hedging_enabled:
        for pos in positions_to_close:
            bybit_qty = pos.get('bybit_qty', 0)
            try:
                if bybit_qty > 0:
                    hedge_result = await asyncio.wait_for(
                        hedge_close(pos['id'], symbol, pos['direction'], bybit_qty),
                        timeout=5.0
                    )
                else:
                    hedge_result = await asyncio.wait_for(
                        hedge_close(pos['id'], symbol, pos['direction'], None),
                        timeout=5.0
                    )
                
                if not hedge_result:
                    logger.warning(f"[CLOSE_SYMBOL] Failed to close {symbol} pos {pos['id']} on Bybit")
                    failed_positions.append(pos)
            except asyncio.TimeoutError:
                logger.error(f"[CLOSE_SYMBOL] Timeout closing {symbol} pos {pos['id']}")
                failed_positions.append(pos)
            except Exception as e:
                logger.error(f"[CLOSE_SYMBOL] Error closing {symbol} pos {pos['id']}: {e}")
                failed_positions.append(pos)
    
    # Убираем позиции которые не удалось закрыть на Bybit
    if failed_positions and hedging_enabled:
        positions_to_close = [p for p in positions_to_close if p not in failed_positions]
        if not positions_to_close:
            await edit_or_send(
                query,
                f"<b>❌ Ошибка закрытия</b>\n\n"
                f"Не удалось закрыть позиции на Bybit.\n"
                f"Попробуйте ещё раз.",
                InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
            )
            return
    
    # Закрываем в БД и считаем PnL
    total_pnl = 0
    total_returned = 0
    
    # Получаем реальную цену один раз для всех позиций
    real_price = await get_cached_price(symbol)
    
    for pos in positions_to_close:
        close_price = real_price if real_price else pos['current']
        
        # Пересчитываем PnL с реальной ценой
        if pos['direction'] == "LONG":
            pnl = (close_price - pos['entry']) / pos['entry'] * pos['amount'] * LEVERAGE
        else:
            pnl = (pos['entry'] - close_price) / pos['entry'] * pos['amount'] * LEVERAGE
        
        pnl -= pos.get('commission', 0)
        
        returned = pos['amount'] + pnl
        total_pnl += pnl
        total_returned += returned
        
        db_close_position(pos['id'], close_price, pnl, 'MANUAL_CLOSE')
        # Явно удаляем из кэша по ID
        pos_id_to_remove = pos['id']
        current_positions = positions_cache.get(user_id, [])
        if current_positions:
            positions_cache.set(user_id, [p for p in current_positions if p.get('id') != pos_id_to_remove])
    
    # Обновляем баланс ОДИН РАЗ после всех закрытий (с локом для защиты от race conditions)
    async with get_user_lock(user_id):
        user = get_user(user_id)  # Re-read with lock
        user['balance'] = sanitize_balance(user['balance'] + total_returned)
        user['total_profit'] += total_pnl
        save_user(user_id)
    
    pnl_sign = "+" if total_pnl >= 0 else ""
    pnl_emoji = "✅" if total_pnl >= 0 else "❌"
    
    text = f"""<b>{pnl_emoji} {ticker} закрыт</b>

PnL: {pnl_sign}${total_pnl:.2f}

💰 Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades"),
                 InlineKeyboardButton("🔙 Меню", callback_data="back")]]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
    logger.info(f"[CLOSE_SYMBOL] User {user_id}: closed {ticker}, PnL=${total_pnl:.2f}")


@rate_limit(max_requests=5, window_seconds=60, action_type="close_all")
async def close_all_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть все открытые позиции пользователя с улучшенной обработкой ошибок"""
    query = update.callback_query
    
    try:
        await query.answer()
    except Exception:
        pass
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    # Filter positions with amount > 0, remove "empty" ones in background
    zero_amount_positions = [p for p in user_positions if p.get('amount', 0) <= 0]
    if zero_amount_positions:
        async def cleanup_zero():
            for zero_pos in zero_amount_positions:
                try:
                    realized_pnl = zero_pos.get('realized_pnl', 0) or 0
                    db_close_position(zero_pos['id'], zero_pos.get('current', zero_pos['entry']), realized_pnl, 'FULLY_CLOSED')
                except Exception:
                    pass
        asyncio.create_task(cleanup_zero())
    
    user_positions = [p for p in user_positions if p.get('amount', 0) > 0]
    
    if not user_positions:
        try:
            await edit_or_send(
                query,
                "<b>💼 Нет позиций</b>\n\nНет открытых сделок",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
        except Exception:
            pass
        return
    
    try:
        await edit_or_send(query, "<b>⏳ Закрываем позиции...</b>", None)
    except Exception:
        pass
    
    # === GROUP POSITIONS BY SYMBOL FOR BYBIT CLOSING ===
    close_prices = {}  # (symbol, direction) -> close_price
    failed_symbols = []  # Symbols that failed to close
    bybit_errors = []  # Error messages for user
    
    try:
        hedging_enabled = await asyncio.wait_for(is_hedging_enabled(), timeout=3.0)
    except (asyncio.TimeoutError, Exception):
        hedging_enabled = False
    
    if hedging_enabled:
        by_symbol = {}
        for pos in user_positions:
            key = (pos['symbol'], pos['direction'])
            if key not in by_symbol:
                by_symbol[key] = []
            by_symbol[key].append(pos)
        
        # Close on Bybit by symbol with timeout protection
        for (symbol, direction), positions in by_symbol.items():
            total_qty = sum(p.get('bybit_qty', 0) for p in positions)
            ticker = symbol.split("/")[0] if "/" in symbol else symbol
            
            try:
                # Timeout protection for hedge_close (5 seconds)
                if total_qty > 0:
                    hedge_result = await asyncio.wait_for(
                        hedge_close(positions[0]['id'], symbol, direction, total_qty),
                        timeout=5.0
                    )
                else:
                    hedge_result = await asyncio.wait_for(
                        hedge_close(positions[0]['id'], symbol, direction, None),
                        timeout=5.0
                    )
                
                if not hedge_result:
                    logger.warning(f"[CLOSE_ALL] Failed to close {symbol} {direction} on Bybit")
                    failed_symbols.append((symbol, direction))
                    bybit_errors.append(f"{ticker}")
                    continue
                
                # Get real close price with timeout (no delay needed - just check)
                try:
                    close_side = "Sell" if direction == "LONG" else "Buy"
                    order_info = await asyncio.wait_for(
                        hedger.get_last_order_price(symbol, close_side),
                        timeout=3.0
                    )
                    if order_info and order_info.get('price'):
                        close_prices[(symbol, direction)] = order_info['price']
                except (asyncio.TimeoutError, Exception):
                    pass  # Use current price as fallback
                    
            except asyncio.TimeoutError:
                logger.error(f"[CLOSE_ALL] Timeout closing {symbol} {direction}")
                failed_symbols.append((symbol, direction))
                bybit_errors.append(f"{ticker} (таймаут)")
            except Exception as e:
                logger.error(f"[CLOSE_ALL] Error closing {symbol} {direction}: {e}")
                failed_symbols.append((symbol, direction))
                bybit_errors.append(f"{ticker}")
    
    # Filter positions that failed to close on Bybit
    positions_to_close = user_positions[:]
    if failed_symbols and hedging_enabled:
        positions_to_close = [p for p in user_positions if (p['symbol'], p['direction']) not in failed_symbols]
        
        if not positions_to_close:
            error_list = ", ".join(bybit_errors[:5])  # Show max 5 failed symbols
            if len(bybit_errors) > 5:
                error_list += f" и ещё {len(bybit_errors) - 5}"
            
            try:
                await edit_or_send(
                    query,
                    f"<b>❌ Ошибка закрытия</b>\n\n"
                    f"Не удалось закрыть: {error_list}\n\n"
                    f"Попробуйте ещё раз или закройте вручную.",
                    InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Повторить", callback_data="close_all")],
                        [InlineKeyboardButton("📊 Сделки", callback_data="trades")]
                    ])
                )
            except Exception:
                pass
            return
    
    # === CLOSE ALL POSITIONS IN DB ===
    total_pnl = 0
    total_returned = 0
    closed_count = 0
    winners = 0
    losers = 0
    close_errors = 0
    
    for pos in positions_to_close:
        try:
            # Get real close price if available
            close_price = close_prices.get((pos['symbol'], pos['direction']), pos.get('current', pos['entry']))
            
            # Calculate PnL with real price
            if pos['direction'] == "LONG":
                pnl_percent = (close_price - pos['entry']) / pos['entry']
            else:
                pnl_percent = (pos['entry'] - close_price) / pos['entry']
            pnl = pos['amount'] * LEVERAGE * pnl_percent - pos.get('commission', 0)
            
            returned = pos['amount'] + pnl
            
            # Update stats
            total_pnl += pnl
            total_returned += returned
            closed_count += 1
            
            if pnl > 0:
                winners += 1
            elif pnl < 0:
                losers += 1
            
            # Close in DB with real price
            db_close_position(pos['id'], close_price, pnl, 'CLOSE_ALL')
            
        except Exception as e:
            logger.error(f"[CLOSE_ALL] Error closing position {pos.get('id')}: {e}")
            close_errors += 1
    
    # Update cache once after all closures
    try:
        positions_cache.set(user_id, db_get_positions(user_id))
    except Exception:
        pass
    
    # Update balance with lock for race condition protection
    try:
        async with get_user_lock(user_id):
            user = get_user(user_id)  # Re-read with lock
            user['balance'] = sanitize_balance(user['balance'] + total_returned)
            user['total_profit'] += total_pnl
            save_user(user_id)
    except Exception as e:
        logger.error(f"[CLOSE_ALL] Error updating balance: {e}")
    
    # Build result message
    pnl_abs = abs(total_pnl)
    
    # Add warning about failed positions if any
    warning_text = ""
    if failed_symbols and hedging_enabled:
        failed_count = len(failed_symbols)
        warning_text = f"\n⚠️ {failed_count} позиций не закрыто на Bybit\n"
    if close_errors > 0:
        warning_text += f"\n⚠️ {close_errors} ошибок при закрытии\n"
    
    if total_pnl > 0:
        text = f"""<b>📊 Сделки закрыты</b>

Закрыто: {closed_count}
✅ {winners} прибыльных
❌ {losers} убыточных{warning_text}

Итого: <b>+${pnl_abs:.2f}</b>
Хороший сет.

💰 Баланс: ${user['balance']:.2f}"""
    elif total_pnl < 0:
        text = f"""<b>📊 Сделки закрыты</b>

Закрыто: {closed_count}
✅ {winners} прибыльных
❌ {losers} убыточных{warning_text}

Итого: <b>-${pnl_abs:.2f}</b>
Следующий будет лучше.

💰 Баланс: ${user['balance']:.2f}"""
    else:
        text = f"""<b>📊 Сделки закрыты</b>

Закрыто: {closed_count}{warning_text}

Итого: $0.00
Капитал сохранён.

💰 Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Новые сигналы", callback_data="back")]]
    
    try:
        await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
    except Exception:
        pass
    
    logger.info(f"[CLOSE_ALL] User {user_id}: closed {closed_count} positions, total PnL: ${total_pnl:.2f}")


@rate_limit(max_requests=30, window_seconds=60, action_type="show_trades")
async def show_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        query = update.callback_query
        if not query:
            return
        
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
        
        # Answer callback immediately to prevent Telegram timeout
        try:
            await query.answer()
        except Exception:
            pass
        
        user = get_user(user_id)
        
        # Load positions from cache first (fast response)
        try:
            user_positions = get_positions(user_id)
        except Exception as e:
            logger.error(f"[TRADES] Error getting positions: {e}")
            user_positions = []
        
        # Background sync - truly non-blocking with fire-and-forget
        async def background_sync():
            """Background sync - doesn't block user response"""
            try:
                synced = await asyncio.wait_for(sync_bybit_positions(user_id, context), timeout=3.0)
                if synced > 0:
                    positions_cache.set(user_id, db_get_positions(user_id))
            except (asyncio.TimeoutError, Exception):
                pass  # Non-critical, silently ignore
        
        # Fire and forget - don't await
        asyncio.create_task(background_sync())
        
        # Clean up zero-amount positions in background (don't block UI)
        zero_amount = [p for p in user_positions if p.get('amount', 0) <= 0]
        if zero_amount:
            async def cleanup_zero_positions():
                for zero_pos in zero_amount:
                    try:
                        realized_pnl = zero_pos.get('realized_pnl', 0) or 0
                        db_close_position(zero_pos['id'], zero_pos.get('current', zero_pos['entry']), realized_pnl, 'FULLY_CLOSED')
                    except Exception:
                        pass
            asyncio.create_task(cleanup_zero_positions())
        
        # Filter positions for display
        user_positions = [p for p in user_positions if p.get('amount', 0) > 0]
        
        # Update cache if we filtered any
        if zero_amount:
            positions_cache.set(user_id, user_positions)
        
        # Get stats (now cached with 30s TTL - fast)
        stats = db_get_user_stats(user_id)
        wins = stats['wins']
        total_trades = stats['total']
        winrate = stats['winrate']
        total_profit = stats['total_pnl']
        profit_str = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
        
        if not user_positions:
            # Показываем статистику даже когда нет позиций
            logger.info(f"[TRADES] No positions found, showing empty state")
            text = f"""<b>💼 Нет открытых позиций</b>

📊 Статистика:
Сделок: <b>{total_trades}</b>
Побед: <b>{wins}</b>
Winrate: <b>{winrate}%</b>
💵 Профит: {profit_str}

💰 Баланс: ${user['balance']:.2f}"""
            
            keyboard = [
                [InlineKeyboardButton("🔙 Назад", callback_data="back"), InlineKeyboardButton("🔄 Обновить", callback_data="trades")]
            ]
            try:
                await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
            except BadRequest:
                pass  # Message unchanged - normal
            except Exception as e:
                logger.error(f"[TRADES] Error sending message: {e}")
                try:
                    await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
                except Exception:
                    pass
            return
        
        # Stack identical positions for display
        stacked = stack_positions(user_positions)
        
        text = "<b>💼 Позиции</b>\n\n"
        
        keyboard = []
        for pos in stacked:
            pnl = pos.get('pnl', 0)
            emoji = "🟢" if pnl >= 0 else "🔴"
            pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
            ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
            dir_text = "LONG" if pos['direction'] == "LONG" else "SHORT"
            current = pos.get('current', pos['entry'])
            
            # Расчёт PnL в процентах
            if pos['direction'] == "LONG":
                pnl_percent = (current - pos['entry']) / pos['entry'] * 100 * LEVERAGE
            else:
                pnl_percent = (pos['entry'] - current) / pos['entry'] * 100 * LEVERAGE
            pnl_pct_str = f"+{pnl_percent:.0f}%" if pnl_percent >= 0 else f"{pnl_percent:.0f}%"
            
            # Показываем количество стакнутых позиций
            stack_info = f" (x{pos['stacked_count']})" if pos.get('stacked_count', 1) > 1 else ""
            
            # Определяем какой TP активен
            tp1_hit = pos.get('tp1_hit', False)
            tp2_hit = pos.get('tp2_hit', False)
            if tp2_hit:
                tp_status = "TP3"
                current_tp = pos.get('tp3', pos['tp'])
            elif tp1_hit:
                tp_status = "TP2"
                current_tp = pos.get('tp2', pos['tp'])
            else:
                tp_status = "TP1"
                current_tp = pos.get('tp1', pos['tp'])
            
            # Реализованный P&L для этой позиции
            realized_pnl = pos.get('realized_pnl', 0) or 0
            realized_pnl_str = f"+${realized_pnl:.2f}" if realized_pnl >= 0 else f"-${abs(realized_pnl):.2f}"
            
            text += f"{ticker} | {dir_text} | ${pos['amount']:.2f} | x{LEVERAGE}{stack_info}\n"
            text += f"${current:,.2f} → {tp_status}: ${current_tp:,.2f} | SL: ${pos['sl']:,.2f}\n"
            text += f"\nPnL: <b>{pnl_str}</b> ({pnl_pct_str})"
            if realized_pnl != 0:
                text += f" | Реализованный: {realized_pnl_str}"
            text += "\n\n"
            
            # Для стакнутых позиций передаём все ID через запятую
            if pos.get('position_ids'):
                close_data = f"closestack_{','.join(str(pid) for pid in pos['position_ids'])}"
            else:
                close_data = f"close_{pos['id']}"
            
            keyboard.append([InlineKeyboardButton(f"❌ Закрыть {ticker}", callback_data=close_data)])
        
        # Общий профит - используем сумму PnL из истории
        total_profit = stats['total_pnl']
        profit_str = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
        
        # Баланс и статистика всегда показываются внизу
        # Убираем лишние переносы строк в конце
        text = text.rstrip("\n")
        text += f"\n\n💰 Баланс: ${user['balance']:.2f}\n"
        text += f"📊 Статистика: {wins}/{total_trades} ({winrate}%) | Профит: {profit_str}"
        
        # Кнопка закрыть все (если больше 1 позиции)
        if len(user_positions) > 0:
            keyboard.append([InlineKeyboardButton("❌ Закрыть все", callback_data="close_all")])
        
        keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="trades")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
        
        try:
            await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
        except BadRequest:
            pass  # Message unchanged - normal
        except Exception as e:
            logger.error(f"[TRADES] Error sending trades: {e}")
            try:
                await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            except Exception:
                pass
    except Exception as e:
        logger.error(f"[TRADES] Critical error: {e}", exc_info=True)
        try:
            if update.callback_query:
                await update.callback_query.answer("❌ Ошибка загрузки", show_alert=True)
        except:
            pass

# ==================== СИГНАЛЫ ====================
# Кэш последних сигналов для предотвращения дубликатов
last_signals: Dict[str, Dict] = {}  # {symbol: {'direction': str, 'price': float, 'time': datetime}}
SIGNAL_COOLDOWN = 60  # 1 минута между одинаковыми сигналами
PRICE_CHANGE_THRESHOLD = 0.002  # 0.2% изменение цены для нового сигнала
LEVERAGE = 20  # Плечо x20

# ==================== АВТО-ТОРГОВЛЯ ====================
AUTO_TRADE_ENABLED = True  # Включить автоматическое принятие сделок
AUTO_TRADE_USER_ID = int(os.getenv("ADMIN_IDS", "0").split(",")[0])  # ID юзера для авто-трейда (первый админ)
AUTO_TRADE_MIN_BET = 10  # Минимальная ставка $
AUTO_TRADE_MAX_BET = 500  # Максимальная ставка $
AUTO_TRADE_START_BALANCE = 1500  # Стартовый баланс для авто-трейда

def calculate_auto_bet(confidence: float, balance: float, atr_percent: float = 0, user_id: int = None) -> tuple:
    """
    Рассчитать размер ставки и плечо на основе уверенности и волатильности (ATR)
    
    Стратегия: 
    - Базовый размер от уверенности
    - Корректировка на волатильность (высокий ATR = меньше позиция)
    - Уменьшение после серии убытков
    - Профессиональный риск-менеджмент
    
    Returns:
        (bet_amount, leverage)
    """
    # Базовое плечо (фиксированное для предсказуемости)
    leverage = LEVERAGE  # Используем глобальное плечо
    
    # === ДИНАМИЧЕСКИЙ РАЗМЕР ПОСЛЕ УБЫТКОВ ===
    loss_streak_multiplier = 1.0
    if user_id:
        loss_streak = db_get_loss_streak(user_id)
        if loss_streak >= 3:
            # После 3+ убытков подряд: -50% от ставки
            loss_streak_multiplier = 0.5
            logger.info(f"[BET] User {user_id}: {loss_streak} losses in a row - reducing bet by 50%")
        elif loss_streak >= 2:
            # После 2 убытков подряд: -25% от ставки
            loss_streak_multiplier = 0.75
            logger.info(f"[BET] User {user_id}: {loss_streak} losses in a row - reducing bet by 25%")
    
    # Уверенность от 28% до 95% (после фильтров)
    # Чем выше уверенность - тем больше ставка
    
    if confidence >= 85:
        # Очень высокая уверенность - максимальная ставка
        bet_percent = 0.15  # 15% от баланса
    elif confidence >= 75:
        # Высокая уверенность
        bet_percent = 0.12  # 12% от баланса
    elif confidence >= 65:
        # Хорошая уверенность
        bet_percent = 0.10  # 10% от баланса
    elif confidence >= 55:
        # Средняя уверенность
        bet_percent = 0.07  # 7% от баланса
    elif confidence >= 45:
        # Умеренная уверенность
        bet_percent = 0.05  # 5% от баланса
    else:
        # Низкая уверенность - минимальная ставка
        bet_percent = 0.03  # 3% от баланса
    
    # === ATR-BASED ADJUSTMENT ===
    # Корректируем размер на основе волатильности
    # Высокая волатильность = уменьшаем позицию (больше риск)
    # Низкая волатильность = можно брать больше
    volatility_multiplier = 1.0
    
    if atr_percent > 0:
        if atr_percent > 3.0:
            # Очень высокая волатильность - уменьшаем на 40%
            volatility_multiplier = 0.6
            logger.info(f"[ATR] High volatility ({atr_percent:.2f}%) - reducing position by 40%")
        elif atr_percent > 2.0:
            # Высокая волатильность - уменьшаем на 25%
            volatility_multiplier = 0.75
            logger.info(f"[ATR] Elevated volatility ({atr_percent:.2f}%) - reducing position by 25%")
        elif atr_percent > 1.5:
            # Умеренная волатильность - уменьшаем на 15%
            volatility_multiplier = 0.85
        elif atr_percent < 0.5:
            # Низкая волатильность - можно увеличить на 20%
            volatility_multiplier = 1.2
            logger.info(f"[ATR] Low volatility ({atr_percent:.2f}%) - increasing position by 20%")
        elif atr_percent < 0.8:
            # Низкая-нормальная волатильность - увеличить на 10%
            volatility_multiplier = 1.1
    
    # Применяем корректировки: волатильность и серия убытков
    bet_percent = bet_percent * volatility_multiplier * loss_streak_multiplier
    
    bet = balance * bet_percent
    
    # Ограничения
    bet = max(AUTO_TRADE_MIN_BET, min(AUTO_TRADE_MAX_BET, bet))
    
    # Не ставить больше 20% баланса за раз (защита от слива)
    bet = min(bet, balance * 0.20)
    
    logger.info(f"[BET] Confidence={confidence}%, ATR={atr_percent:.2f}%, vol_mult={volatility_multiplier}, loss_mult={loss_streak_multiplier}, bet=${bet:.0f}")
    
    return round(bet, 0), leverage


# ==================== SMART SIGNAL (единственный режим) ====================
async def send_smart_signal(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Умная отправка сигналов v2.0
    
    ПРИНЦИПЫ:
    1. Только КАЧЕСТВЕННЫЕ сетапы (A+ и A) - без мусора
    2. Торговля ТОЛЬКО по тренду  
    3. Динамические TP/SL на основе структуры рынка
    4. Защита капитала: cooldown, max drawdown, pause после убытков
    5. Минимум R/R 1:2.5
    6. Интервал 5 минут между анализами
    
    ЦЕЛЬ: 1-3 качественные сделки в день, а не спам
    """
    global smart
    
    logger.info("[SMART] ========== Smart Signal v2.0 ==========")
    
    # Получаем активных юзеров (с балансом >= MIN_DEPOSIT)
    # Включаем всех пользователей с достаточным балансом, независимо от статуса trading
    # Статус trading будет проверяться при отправке сигналов
    rows = run_sql("SELECT user_id, balance FROM users WHERE balance >= ?", (MIN_DEPOSIT,), fetch="all")
    active_users = [row['user_id'] for row in rows] if rows else []
    
    # Считаем пользователей с включенным авто-трейдом
    auto_trade_count_row = run_sql(
        "SELECT COUNT(*) as cnt FROM users WHERE auto_trade = 1 AND balance >= ?",
        (AUTO_TRADE_MIN_BET,), fetch="one"
    )
    auto_trade_users_count = auto_trade_count_row['cnt'] if auto_trade_count_row else 0
    has_auto_trade = auto_trade_users_count > 0
    
    # Баланс для расчёта сетапа (берём максимальный среди авто-трейд юзеров)
    max_balance_row = run_sql(
        "SELECT MAX(balance) as max_bal FROM users WHERE auto_trade = 1 AND balance >= ?",
        (AUTO_TRADE_MIN_BET,), fetch="one"
    )
    auto_balance = max_balance_row['max_bal'] if max_balance_row and max_balance_row['max_bal'] else 0
    
    logger.info(f"[SMART] Активных юзеров: {len(active_users)}, Авто-трейд юзеров: {auto_trade_users_count}, Auto balance: ${auto_balance:.2f}")
    
    if not active_users and not has_auto_trade:
        logger.info("[SMART] Нет активных юзеров и авто-трейд юзеров")
        return
    
    # Проверяем состояние торговли
    trading_state = get_trading_state()
    if trading_state['is_paused']:
        logger.info(f"[SMART] Торговля на паузе до {trading_state['pause_until']}")
        return
    
    logger.info(f"[SMART] Активных юзеров: {len(active_users)}, Авто-трейд юзеров: {auto_trade_users_count}")
    if active_users:
        logger.info(f"[SMART] Активные юзеры: {active_users}")
    logger.info(f"[SMART] Сделок сегодня: {trading_state['daily_trades']}, Убытков подряд: {trading_state['consecutive_losses']}")
    
    try:
        # === ИЩЕМ ЛУЧШИЙ СЕТАП ===
        setup = await find_best_setup(balance=auto_balance)
        
        if setup is None:
            logger.info("[SMART] Нет качественных сетапов")
            return
        
        logger.info(f"[SMART] ✓ Найден сетап: {setup.symbol} {setup.direction}")
        logger.info(f"[SMART] Качество: {setup.quality.name}, R/R: {setup.risk_reward:.2f}, Уверенность: {setup.confidence:.0%}")
        logger.info(f"[SMART] Режим рынка: {setup.market_regime.name}")
        
        # Данные для сигнала
        symbol = setup.symbol
        direction = setup.direction
        entry = setup.entry
        sl = setup.stop_loss
        tp1 = setup.take_profit_1
        tp2 = setup.take_profit_2
        tp3 = setup.take_profit_3
        
        # Процентные уровни
        tp1_percent = abs(tp1 - entry) / entry * 100
        tp2_percent = abs(tp2 - entry) / entry * 100
        tp3_percent = abs(tp3 - entry) / entry * 100
        sl_percent = abs(sl - entry) / entry * 100
        
        # Confidence = качество сетапа
        confidence_percent = int(setup.confidence * 100)
        
        # Качество как текст (только A+ и A принимаются)
        quality_emoji = {
            SetupQuality.A_PLUS: "🌟 A+",
            SetupQuality.A: "⭐ A",
        }.get(setup.quality, "⭐")
        
        # Режим рынка как текст
        regime_text = {
            MarketRegime.STRONG_UPTREND: "📈 Сильный тренд ↑",
            MarketRegime.UPTREND: "📈 Тренд ↑",
            MarketRegime.STRONG_DOWNTREND: "📉 Сильный тренд ↓",
            MarketRegime.DOWNTREND: "📉 Тренд ↓",
            MarketRegime.RANGING: "⚖️ Боковик"
        }.get(setup.market_regime, "")
        
        # === ПРОВЕРКА ДУБЛИКАТА ===
        now = datetime.now()
        if symbol in last_signals:
            last = last_signals[symbol]
            time_diff = (now - last['time']).total_seconds()
            
            if time_diff < SIGNAL_COOLDOWN * 2:  # Удвоенный cooldown для smart режима
                logger.info(f"[SMART] Пропуск: недавний сигнал по {symbol}")
                return
        
        last_signals[symbol] = {'direction': direction, 'price': entry, 'time': now}
        
        # === АВТО-ТОРГОВЛЯ ДЛЯ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ ===
        # Список пользователей, для которых авто-трейд выполнен
        auto_trade_executed_users = set()
        
        # Получаем ВСЕХ пользователей с включенным auto_trade
        all_auto_trade_users = run_sql(
            "SELECT user_id FROM users WHERE auto_trade = 1 AND balance >= ?",
            (AUTO_TRADE_MIN_BET,), fetch="all"
        )
        
        logger.info(f"[AUTO_TRADE] Найдено {len(all_auto_trade_users)} пользователей с авто-трейдом")
        
        if len(all_auto_trade_users) == 0:
            logger.info("[AUTO_TRADE] Нет пользователей с auto_trade=1 и балансом >= AUTO_TRADE_MIN_BET")
        
        # Проверяем каждого пользователя с auto_trade
        for auto_user_row in all_auto_trade_users:
            auto_user_id = auto_user_row['user_id']
            
            try:
                auto_user = get_user(auto_user_id)
                auto_balance = auto_user.get('balance', 0)
                
                # Проверяем настройки ЭТОГО пользователя
                user_auto_enabled = auto_user.get('auto_trade', False)
                user_min_winrate = auto_user.get('auto_trade_min_winrate', 70)
                user_max_daily = auto_user.get('auto_trade_max_daily', 20)
                user_today_count = auto_user.get('auto_trade_today', 0)
                
                # Сброс счётчика сделок за день
                from datetime import date as dt_date
                today = dt_date.today().isoformat()
                last_reset = auto_user.get('auto_trade_last_reset')
                if last_reset != today:
                    user_today_count = 0
                    auto_user['auto_trade_today'] = 0
                    auto_user['auto_trade_last_reset'] = today
                    db_update_user(auto_user_id, auto_trade_today=0, auto_trade_last_reset=today)
                
                # === ПРОВЕРКИ ДЛЯ ВХОДА ===
                skip_reason = None
                
                if not user_auto_enabled:
                    skip_reason = "выключен"
                elif confidence_percent < user_min_winrate:
                    skip_reason = f"confidence {confidence_percent}% < {user_min_winrate}%"
                elif user_today_count >= user_max_daily:
                    skip_reason = f"лимит сделок {user_today_count}/{user_max_daily}"
                elif auto_balance < AUTO_TRADE_MIN_BET:
                    skip_reason = f"баланс ${auto_balance:.0f} < ${AUTO_TRADE_MIN_BET}"
                
                if skip_reason:
                    logger.info(f"[AUTO_TRADE] User {auto_user_id}: пропуск - {skip_reason}")
                    continue
                
                logger.info(f"[AUTO_TRADE] User {auto_user_id}: проверка пройдена, открываем сделку")
                
                # === ВАЛИДАЦИЯ ===
                auto_positions = get_positions(auto_user_id)
                
                # Лимит позиций
                dynamic_max_positions = get_max_positions_for_user(auto_balance)
                if len(auto_positions) >= dynamic_max_positions:
                    logger.info(f"[AUTO_TRADE] User {auto_user_id}: лимит позиций ({len(auto_positions)}/{dynamic_max_positions})")
                    continue
                
                # Проверка корреляции
                if ADVANCED_POSITION_MANAGEMENT:
                    try:
                        is_safe, corr_reason = check_correlation_risk(
                            auto_positions, symbol, direction, auto_balance,
                            correlation_threshold=0.7, max_exposure_percent=30.0
                        )
                        if not is_safe:
                            logger.info(f"[AUTO_TRADE] User {auto_user_id}: корреляция - {corr_reason}")
                            continue
                    except Exception as e:
                        logger.warning(f"[AUTO_TRADE] User {auto_user_id}: ошибка корреляции: {e}")
                
                # Проверка символа (асинхронная - позволяет динамические символы с Bybit)
                valid, error = await validate_symbol_async(symbol)
                if not valid:
                    logger.warning(f"[AUTO_TRADE] User {auto_user_id}: невалидный символ: {error}")
                    continue
                
                # === РАСЧЁТ СТАВКИ ===
                loss_streak = db_get_loss_streak(auto_user_id)
                auto_bet = calculate_smart_bet_size(
                    balance=auto_balance,
                    symbol=symbol,
                    quality=setup.quality,
                    loss_streak=loss_streak
                )
                
                # Проверяем резерв баланса
                if auto_balance - auto_bet < MIN_BALANCE_RESERVE:
                    auto_bet = max(0, auto_balance - MIN_BALANCE_RESERVE)
                    if auto_bet < AUTO_TRADE_MIN_BET:
                        logger.info(f"[AUTO_TRADE] User {auto_user_id}: недостаточно с резервом")
                        continue
                
                ticker = symbol.split("/")[0]
                
                # === BYBIT ХЕДЖИРОВАНИЕ (только для первого пользователя) ===
                bybit_qty = 0
                hedging_enabled = await is_hedging_enabled()
                bybit_success = True
                
                # Bybit хедж открываем только если это первый авто-трейд по этому сигналу
                # и только для AUTO_TRADE_USER_ID (админ)
                if hedging_enabled and auto_user_id == AUTO_TRADE_USER_ID and len(auto_trade_executed_users) == 0:
                    hedge_amount = float(auto_bet * LEVERAGE)
                    hedge_result = await hedge_open(0, symbol, direction, hedge_amount, 
                                                   sl=float(sl), tp1=float(tp1), tp2=float(tp2), tp3=float(tp3))
                    
                    if hedge_result:
                        bybit_qty = hedge_result.get('qty', 0)
                        logger.info(f"[AUTO_TRADE] ✓ Bybit открыт: qty={bybit_qty}")
                        
                        await asyncio.sleep(0.5)
                        bybit_pos = await hedger.get_position_data(symbol)
                        if not bybit_pos or bybit_pos.get('size', 0) == 0:
                            logger.error("[AUTO_TRADE] ❌ Bybit не подтвердил позицию")
                            bybit_success = False
                        else:
                            # Сохраняем РЕАЛЬНЫЙ размер с Bybit
                            real_bybit_qty = float(bybit_pos.get('size', 0))
                            if real_bybit_qty > 0 and bybit_qty > 0 and abs(real_bybit_qty - bybit_qty) / bybit_qty > 0.01:
                                logger.info(f"[AUTO_TRADE] Correcting qty: calculated={bybit_qty}, real={real_bybit_qty}")
                                bybit_qty = real_bybit_qty
                            increment_bybit_opened()
                    else:
                        logger.error("[AUTO_TRADE] ❌ Bybit ошибка")
                        bybit_success = False
                
                if not bybit_success and auto_user_id == AUTO_TRADE_USER_ID:
                    logger.warning(f"[AUTO_TRADE] User {auto_user_id}: Bybit failed, skipping")
                    continue
                
                # === ОТКРЫТИЕ ПОЗИЦИИ ===
                commission = auto_bet * (COMMISSION_PERCENT / 100)
                
                # Обновляем баланс
                async with get_user_lock(auto_user_id):
                    auto_user = get_user(auto_user_id)
                    auto_user['balance'] -= auto_bet
                    auto_user['balance'] = sanitize_balance(auto_user['balance'])
                    new_balance = auto_user['balance']
                    save_user(auto_user_id)
                
                await add_commission(commission, user_id=auto_user_id)
                
                # Создаём позицию С ЗАЩИТОЙ ОТ ПОТЕРИ ДЕНЕГ
                try:
                    position = {
                        'symbol': symbol,
                        'direction': direction,
                        'entry': float(entry),
                        'current': float(entry),
                        'amount': float(auto_bet),
                        'tp': float(tp1),
                        'tp1': float(tp1),
                        'tp2': float(tp2),
                        'tp3': float(tp3),
                        'tp1_hit': False,
                        'tp2_hit': False,
                        'sl': float(sl),
                        'commission': float(commission),
                        'pnl': float(-commission),
                        'bybit_qty': bybit_qty if auto_user_id == AUTO_TRADE_USER_ID else 0,
                        'original_amount': float(auto_bet)
                    }
                    
                    pos_id = db_add_position(auto_user_id, position)
                    position['id'] = pos_id
                    
                    # Обновляем кэш
                    positions_cache.set(auto_user_id, db_get_positions(auto_user_id))
                except Exception as pos_error:
                    # КРИТИЧЕСКАЯ ОШИБКА: позиция не создалась, возвращаем деньги
                    logger.critical(f"[AUTO_TRADE] ❌ CRITICAL: Position creation failed for user {auto_user_id}, restoring ${auto_bet}! Error: {pos_error}")
                    async with get_user_lock(auto_user_id):
                        auto_user = get_user(auto_user_id)
                        auto_user['balance'] += auto_bet  # Возвращаем деньги
                        auto_user['balance'] = sanitize_balance(auto_user['balance'])
                        save_user(auto_user_id)
                    trade_logger.log_error(f"Auto-trade position creation failed, restored ${auto_bet} to user {auto_user_id}", error=pos_error, user_id=auto_user_id)
                    continue  # Пропускаем этого юзера
                
                # Отправляем уведомление
                auto_msg = f"""<b>🤖 АВТО-ТРЕЙД</b>

<b>📡 {confidence_percent}%</b> | {ticker} | {direction} | x{LEVERAGE}

<b>${auto_bet:.2f}</b> открыто автоматически

Вход: <b>${entry:,.2f}</b>

TP1: ${tp1:,.2f} (<b>+{tp1_percent:.1f}%</b>) — 50%
TP2: ${tp2:,.2f} (+{tp2_percent:.1f}%) — 30%
TP3: ${tp3:,.2f} (+{tp3_percent:.1f}%) — 20%
SL: ${sl:,.2f} (-{sl_percent:.1f}%)

R/R: 1:{setup.risk_reward:.1f}

💰 Баланс: ${new_balance:.2f}"""
                
                auto_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"❌ Закрыть {ticker}", callback_data=f"close_symbol|{symbol}"),
                     InlineKeyboardButton("📊 Сделки", callback_data="trades")]
                ])
                
                try:
                    await context.bot.send_message(auto_user_id, auto_msg, parse_mode="HTML", reply_markup=auto_keyboard)
                    logger.info(f"[AUTO_TRADE] ✅ User {auto_user_id}: {direction} {ticker} ${auto_bet:.2f}")
                    auto_trade_executed_users.add(auto_user_id)
                except Exception as e:
                    logger.error(f"[AUTO_TRADE] ❌ User {auto_user_id}: ошибка отправки: {e}")
                    auto_trade_executed_users.add(auto_user_id)  # Всё равно помечаем
                
                # Обновляем счётчик сделок
                auto_user['auto_trade_today'] = user_today_count + 1
                db_update_user(auto_user_id, auto_trade_today=user_today_count + 1)
                
            except Exception as e:
                logger.error(f"[AUTO_TRADE] User {auto_user_id}: критическая ошибка: {e}", exc_info=True)
                trade_logger.log_error(f"Critical error in auto_trade for user {auto_user_id}: {e}", error=e, user_id=auto_user_id, symbol=symbol)
                continue
        
        logger.info(f"[AUTO_TRADE] Выполнено для {len(auto_trade_executed_users)} пользователей")
        
        # === ОТПРАВКА СИГНАЛОВ ОСТАЛЬНЫМ ЮЗЕРАМ ===
        signal_sent_to_users = False
        
        logger.info(f"[SMART] Отправка сигналов {len(active_users)} активным пользователям (trading=1)")
        
        for user_id in active_users:
            # Пропускаем тех, для кого авто-трейд уже выполнен
            if user_id in auto_trade_executed_users:
                continue
            
            user = get_user(user_id)
            balance = user['balance']
            # trading может быть 0/1 (int) или True/False (bool), приводим к bool
            trading_enabled = bool(user.get('trading', False))
            
            # Отправляем сигналы только пользователям с достаточным балансом
            if balance < MIN_DEPOSIT:
                logger.info(f"[SMART] Пропуск {user_id}: баланс ${balance:.2f} < ${MIN_DEPOSIT}")
                continue
            
            ticker = symbol.split("/")[0]
            d = 'L' if direction == "LONG" else 'S'
            
            # Если ручной трейд выключен - пропускаем (авто-трейд уже обработан выше)
            if not trading_enabled:
                # Если для этого пользователя авто-трейд сработал - уже получил уведомление
                if user_id in auto_trade_executed_users:
                    logger.debug(f"[SMART] User {user_id}: trading=0, но auto_trade выполнен")
                    continue
                # Если авто-трейд не сработал, но trading выключен - просто пропускаем
                logger.debug(f"[SMART] User {user_id}: trading=0, пропуск сигнала")
                continue
            
            # Если ручной трейд включен - отправляем обычный сигнал для входа
            text = f"""<b>📡 {confidence_percent}%</b> | {ticker} | {direction} | x{LEVERAGE}

Вход: <b>${entry:,.2f}</b>

TP1: ${tp1:,.2f} (<b>+{tp1_percent:.1f}%</b>) — 50%
TP2: ${tp2:,.2f} (+{tp2_percent:.1f}%) — 30%
TP3: ${tp3:,.2f} (+{tp3_percent:.1f}%) — 20%
SL: ${sl:,.2f} (-{sl_percent:.1f}%)

R/R: 1:{setup.risk_reward:.1f}

💰 Баланс: ${balance:.2f}"""
            
            # Кнопки
            if balance >= 100:
                amounts = [10, 25, 50, 100]
            elif balance >= 25:
                amounts = [5, 10, 25]
            elif balance >= 10:
                amounts = [3, 5, 10]
            else:
                amounts = [1, 2, 3]
            
            amounts = [a for a in amounts if a <= balance]
            
            entry_str = f"{entry:.4f}" if entry < 100 else f"{entry:.0f}"
            sl_str = f"{sl:.4f}" if sl < 100 else f"{sl:.0f}"
            tp1_str = f"{tp1:.4f}" if tp1 < 100 else f"{tp1:.0f}"
            tp2_str = f"{tp2:.4f}" if tp2 < 100 else f"{tp2:.0f}"
            tp3_str = f"{tp3:.4f}" if tp3 < 100 else f"{tp3:.0f}"
            
            keyboard = []
            if amounts:
                row = [InlineKeyboardButton(f"${amt}", callback_data=f"e|{symbol}|{d}|{entry_str}|{sl_str}|{tp1_str}|{tp2_str}|{tp3_str}|{amt}|{confidence_percent}") for amt in amounts[:4]]
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton("💵 Своя сумма", callback_data=f"custom|{symbol}|{d}|{entry_str}|{sl_str}|{tp1_str}|{tp2_str}|{tp3_str}|{confidence_percent}")])
            keyboard.append([InlineKeyboardButton("❌ Пропустить", callback_data="skip")])
            
            try:
                await context.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
                logger.info(f"[SMART] ✅ Сигнал отправлен пользователю {user_id} (баланс: ${balance:.2f})")
                signal_sent_to_users = True  # Отметим что сигнал был отправлен
            except Exception as e:
                logger.error(f"[SMART] ❌ Ошибка отправки пользователю {user_id}: {e}")
        
        # Увеличиваем accepted один раз на сигнал, если он был отправлен пользователям или открыт через автотрейд
        if signal_sent_to_users or len(auto_trade_executed_users) > 0:
            increment_accepted()
    
    except Exception as e:
        logger.error(f"[SMART] ❌ Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await smart.close()


async def enter_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    # e|SYM|D|ENTRY|SL|TP1|TP2|TP3|AMT|WINRATE
    data = query.data.split("|")
    if len(data) < 7:
        await query.edit_message_text("<b>❌ Ошибка</b>\n\nНеверные данные сигнала.", parse_mode="HTML")
        return

    try:
        symbol = data[1]
        # Нормализуем символ: BTC -> BTC/USDT
        if "/" not in symbol and not symbol.endswith("USDT"):
            symbol = f"{symbol}/USDT"
        direction = "LONG" if data[2] in ['L', 'LONG'] else "SHORT"
        entry = float(data[3])
        sl = float(data[4])
        tp1 = float(data[5])
        
        # Поддержка старого и нового формата callback
        if len(data) >= 10:
            # Новый формат с тремя TP
            tp2 = float(data[6])
            tp3 = float(data[7])
            amount = float(data[8])
            winrate = int(data[9]) if len(data) > 9 else 75
        else:
            # Старый формат - рассчитываем TP2, TP3
            tp2 = entry + (tp1 - entry) * 2 if direction == "LONG" else entry - (entry - tp1) * 2
            tp3 = entry + (tp1 - entry) * 3.5 if direction == "LONG" else entry - (entry - tp1) * 3.5
            amount = float(data[6])
            winrate = int(data[7]) if len(data) > 7 else 75
        
        tp = tp1  # Для совместимости
    except (ValueError, IndexError):
        await query.edit_message_text("<b>❌ Ошибка данных</b>\n\nПроверьте параметры сигнала.", parse_mode="HTML")
        return

    # === INPUT VALIDATION ===
    # Validate symbol
    valid, error = validate_symbol(symbol)
    if not valid:
        await query.edit_message_text(f"<b>❌ Ошибка</b>\n\n{error}", parse_mode="HTML")
        logger.warning(f"[SECURITY] User {user_id}: Invalid symbol {symbol}")
        return
    
    # Validate direction
    valid, error = validate_direction(direction)
    if not valid:
        await query.edit_message_text(f"<b>❌ Ошибка</b>\n\n{error}", parse_mode="HTML")
        logger.warning(f"[SECURITY] User {user_id}: Invalid direction {direction}")
        return
    
    # Validate amount
    valid, error = validate_amount(amount, user['balance'])
    if not valid:
        await query.edit_message_text(f"<b>❌ Ошибка</b>\n\n{error}", parse_mode="HTML")
        return
    
    # Check max positions limit - ДИНАМИЧЕСКИЙ лимит на основе баланса
    dynamic_max_positions = get_max_positions_for_user(user['balance'])
    if len(user_positions) >= dynamic_max_positions:
        await query.edit_message_text(
            f"<b>❌ Лимит позиций</b>\n\n"
            f"Максимум для вашего баланса: {dynamic_max_positions}\n"
            f"Текущих позиций: {len(user_positions)}\n\n"
            f"Закройте существующие сделки перед открытием новых.",
            parse_mode="HTML"
        )
        logger.info(f"[LIMIT] User {user_id}: Max positions reached ({len(user_positions)}/{dynamic_max_positions})")
        return

    # === ПРОВЕРКА КОРРЕЛЯЦИИ ===
    try:
        if ADVANCED_POSITION_MANAGEMENT:
            is_safe, corr_reason = check_correlation_risk(
                user_positions, symbol, direction, user['balance'],
                correlation_threshold=0.7,  # 70% корреляция
                max_exposure_percent=30.0   # Макс. 30% в одном направлении
            )
            if not is_safe:
                await query.edit_message_text(
                    f"<b>⚠️ Риск корреляции</b>\n\n"
                    f"{corr_reason}\n\n"
                    f"Закройте часть позиций перед открытием новых.",
                    parse_mode="HTML"
                )
                logger.info(f"[CORR] User {user_id}: Blocked due to correlation risk: {corr_reason}")
                return
    except Exception as e:
        logger.warning(f"[CORR] Error checking correlation: {e}")

    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_emoji = "🟢" if direction == "LONG" else "🔴"

    # === ЗАЩИТА: Не добавлять к убыточной позиции ===
    for p in user_positions:
        if p['symbol'] == symbol and p['direction'] == direction:
            # Рассчитываем текущий PnL%
            if p.get('current') and p.get('entry'):
                if direction == "LONG":
                    pnl_pct = (p['current'] - p['entry']) / p['entry'] * 100
                else:
                    pnl_pct = (p['entry'] - p['current']) / p['entry'] * 100
                
                # Если позиция в минусе более 1.5% - не добавляем
                if pnl_pct < -1.5:
                    await query.edit_message_text(
                        f"<b>⛔ Добавление заблокировано</b>\n\n"
                        f"{ticker} {direction} уже в минусе {pnl_pct:.1f}%\n"
                        f"Нельзя усреднять убыточную позицию",
                        parse_mode="HTML"
                    )
                    logger.info(f"[PROTECTION] User {user_id}: blocked adding to losing {ticker} {direction} (PnL={pnl_pct:.1f}%)")
                    return
            break

    # === ПОКАЗЫВАЕМ "ОТКРЫВАЕМ..." ===
    await query.edit_message_text(f"<b>⏳ Открываем</b>\n\n{ticker} | {direction} | <b>${amount:.2f}</b>", parse_mode="HTML")

    # === ХЕДЖИРОВАНИЕ: СНАЧАЛА открываем на Bybit ===
    bybit_qty = 0
    hedging_enabled = await is_hedging_enabled()
    
    if hedging_enabled:
        hedge_result = await hedge_open(0, symbol, direction, amount * LEVERAGE, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
        if hedge_result:
            bybit_qty = hedge_result.get('qty', 0)
            logger.info(f"[HEDGE] ✓ Hedged on Bybit: qty={bybit_qty}, partial TPs created")
            
            # === ВЕРИФИКАЦИЯ: проверяем что позиция реально открылась на Bybit ===
            await asyncio.sleep(0.5)  # Даём Bybit время на обработку
            bybit_pos = await hedger.get_position_data(symbol)
            if not bybit_pos or bybit_pos.get('size', 0) == 0:
                logger.error(f"[HEDGE] ❌ VERIFICATION FAILED: Bybit position not found after open!")
                # Позиция не появилась - отменяем
                await query.edit_message_text(
                    f"<b>❌ Ошибка открытия</b>\n\n"
                    f"Bybit не подтвердил позицию.\n"
                    f"Попробуйте ещё раз.",
                    parse_mode="HTML"
                )
                return
            
            # Сохраняем РЕАЛЬНЫЙ размер с Bybit (может отличаться из-за округления)
            real_bybit_qty = float(bybit_pos.get('size', 0))
            if real_bybit_qty > 0 and abs(real_bybit_qty - bybit_qty) / bybit_qty > 0.01:  # Разница > 1%
                logger.info(f"[HEDGE] Correcting qty: calculated={bybit_qty}, real={real_bybit_qty}")
                bybit_qty = real_bybit_qty
            
            # Успешно открыто на Bybit - инкрементируем статистику
            increment_bybit_opened()
        else:
            # Bybit не открыл позицию - НЕ создаём в боте
            logger.error(f"[HEDGE] ❌ Failed to open on Bybit - aborting trade")
            await query.edit_message_text(
                f"<b>❌ Ошибка открытия</b>\n\n"
                f"Не удалось открыть позицию на Bybit.\n"
                f"Проверьте баланс и настройки API.",
                parse_mode="HTML"
            )
            return

    # Комиссия за открытие (только после успешного открытия на Bybit)
    commission = amount * (COMMISSION_PERCENT / 100)
    
    # Защита от race conditions при изменении баланса
    async with get_user_lock(user_id):
        user = get_user(user_id)  # Re-read with lock
        user['balance'] -= amount
        user['balance'] = sanitize_balance(user['balance'])  # Security: ensure non-negative
        save_user(user_id)  # Сохраняем в БД

    # Добавляем комиссию в накопитель (авто-вывод) с учетом рефералов
    await add_commission(commission, user_id=user_id)

    # === СОЗДАЁМ ПОЗИЦИЮ С ЗАЩИТОЙ ОТ ПОТЕРИ ДЕНЕГ ===
    try:
        # === ПРОВЕРЯЕМ ЕСТЬ ЛИ УЖЕ ПОЗИЦИЯ С ТАКИМ СИМВОЛОМ И НАПРАВЛЕНИЕМ ===
        existing = None
        for p in user_positions:
            if p['symbol'] == symbol and p['direction'] == direction:
                existing = p
                break

        if existing:
            # === ПРОВЕРЯЕМ СИНХРОНИЗАЦИЮ С BYBIT ===
            if hedging_enabled and existing.get('bybit_qty', 0) > 0:
                bybit_pos = await hedger.get_position_data(symbol)
                if not bybit_pos or bybit_pos.get('size', 0) == 0:
                    logger.warning(f"[TRADE] Existing position {symbol} not found on Bybit, creating as new")
                    existing = None  # Создаём новую позицию вместо усреднения
            
        if existing:
            # === ДОБАВЛЯЕМ К СУЩЕСТВУЮЩЕЙ ПОЗИЦИИ ===
            old_amount = existing['amount']
            new_amount = old_amount + amount
            
            # Weighted average entry price
            new_entry = (existing['entry'] * old_amount + entry * amount) / new_amount
            
            # Добавляем qty к существующему
            new_bybit_qty = existing.get('bybit_qty', 0) + bybit_qty
            
            # Обновляем позицию
            existing['amount'] = new_amount
            existing['entry'] = new_entry
            existing['commission'] = existing.get('commission', 0) + commission
            existing['bybit_qty'] = new_bybit_qty
            # Пересчитываем PnL
            existing['pnl'] = -existing['commission']
            
            # Обновляем в БД
            db_update_position(existing['id'], 
                amount=new_amount, 
                entry=new_entry, 
                commission=existing['commission'],
                bybit_qty=new_bybit_qty,
                pnl=existing['pnl']
            )
            
            pos_id = existing['id']
            logger.info(f"[TRADE] User {user_id} added ${amount} to existing {direction} {symbol}, total=${new_amount}")
            
            # Обновляем кэш после изменения позиции
            positions_cache.set(user_id, db_get_positions(user_id))
        else:
            # === СОЗДАЁМ НОВУЮ ПОЗИЦИЮ С ТРЕМЯ TP ===
            position = {
                'symbol': symbol,
                'direction': direction,
                'amount': amount,
                'entry': entry,
                'current': entry,
                'sl': sl,
                'tp': tp1,  # Основной TP = TP1
                'tp1': tp1,
                'tp2': tp2,
                'tp3': tp3,
                'tp1_hit': False,  # Флаги частичных тейков
                'tp2_hit': False,
                'pnl': -commission,
                'commission': commission,
                'bybit_qty': bybit_qty,
                'realized_pnl': 0,  # Реализованный P&L для этой позиции
                'original_amount': amount  # Для расчёта частичных закрытий
            }

            pos_id = db_add_position(user_id, position)
            position['id'] = pos_id

            # Обновляем кэш - загружаем все позиции из БД
            positions_cache.set(user_id, db_get_positions(user_id))
            
            logger.info(f"[TRADE] ✅ Позиция открыта: User {user_id} {direction} {symbol} ${amount:.2f}, TP1={tp1:.4f}, TP2={tp2:.4f}, TP3={tp3:.4f}")
            
            # Comprehensive logging
            trade_logger.log_trade_open(
                user_id=user_id, symbol=symbol, direction=direction,
                amount=amount, entry=entry, sl=sl, tp=tp1,
                bybit_qty=bybit_qty, position_id=pos_id
            )
    except Exception as e:
        # КРИТИЧЕСКАЯ ОШИБКА: позиция не создалась, возвращаем деньги
        logger.critical(f"[TRADE] ❌ CRITICAL: Position creation failed for user {user_id}, restoring ${amount}! Error: {e}")
        async with get_user_lock(user_id):
            user = get_user(user_id)
            user['balance'] += amount  # Возвращаем деньги
            user['balance'] = sanitize_balance(user['balance'])
            save_user(user_id)
        trade_logger.log_error(f"Position creation failed, restored ${amount} to user {user_id}", error=e, user_id=user_id)
        await query.edit_message_text(
            f"<b>❌ Ошибка создания позиции</b>\n\n"
            f"Деньги возвращены на баланс.\n"
            f"Попробуйте ещё раз.",
            parse_mode="HTML"
        )
        return
    
    dir_text = "LONG" if direction == "LONG" else "SHORT"
    tp1_percent = abs(tp1 - entry) / entry * 100
    tp2_percent = abs(tp2 - entry) / entry * 100
    tp3_percent = abs(tp3 - entry) / entry * 100
    sl_percent = abs(sl - entry) / entry * 100
    
    text = f"""<b>✅ {winrate}%</b> | {ticker} | {dir_text} | x{LEVERAGE}

<b>${amount:.2f}</b> открыто

Вход: <b>${entry:,.2f}</b>

TP1: ${tp1:,.2f} (<b>+{tp1_percent:.1f}%</b>) — 50%
TP2: ${tp2:,.2f} (+{tp2_percent:.1f}%) — 30%
TP3: ${tp3:,.2f} (+{tp3_percent:.1f}%) — 20%
SL: ${sl:,.2f} (-{sl_percent:.1f}%)

💰 Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Пытаемся отредактировать сообщение, если не получается - отправляем новое
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
        logger.info(f"[TRADE] ✅ Уведомление об открытии отправлено (edit) пользователю {user_id}")
    except Exception as e:
        logger.warning(f"[TRADE] ⚠️ Не удалось отредактировать сообщение пользователю {user_id}: {e}, отправляю новое")
        # Пытаемся отправить новое сообщение если edit не удался
        try:
            await context.bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode="HTML")
            logger.info(f"[TRADE] ✅ Уведомление об открытии отправлено (новое сообщение) пользователю {user_id}")
        except Exception as e2:
            logger.error(f"[TRADE] ❌ Критическая ошибка отправки пользователю {user_id}: {e2}")

async def close_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    try:
        pos_id = int(query.data.split("_")[1])
    except (ValueError, IndexError):
        await query.answer("Ошибка данных", show_alert=True)
        return
    
    try:
        user = get_user(user_id)
        user_positions = get_positions(user_id)
        
        pos = next((p for p in user_positions if p['id'] == pos_id), None)
        
        if not pos:
            await query.answer("❌ Позиция не найдена", show_alert=True)
            return
        
        # Проверка на нулевой amount (позиция уже полностью закрыта частичными тейками)
        amount = pos.get('amount', 0)
        if amount <= 0:
            # Позиция уже закрыта - удаляем из БД
            realized_pnl = pos.get('realized_pnl', 0) or 0
            db_close_position(pos_id, pos.get('current', pos['entry']), realized_pnl, 'FULLY_CLOSED')
            
            # Удаляем из кэша
            try:
                cached_positions = positions_cache.get(user_id)
                if cached_positions:
                    positions_cache.set(user_id, [p for p in cached_positions if p.get('id') != pos_id])
            except:
                pass
            
            ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
            await query.edit_message_text(
                f"<b>✅ {ticker} закрыт</b>\n\n"
                f"Позиция была полностью закрыта частичными тейками.\n"
                f"Реализованный P&L: <b>+${realized_pnl:.2f}</b>\n\n"
                f"💰 Баланс: ${user['balance']:.2f}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
            )
            logger.info(f"[CLOSE] User {user_id}: position {pos_id} was already fully closed, removed from DB")
            return
        
        ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
        
        # Показываем статус
        await edit_or_send(query, f"<b>⏳ Закрываем {ticker}...</b>", None)
        
        # === ХЕДЖИРОВАНИЕ: СНАЧАЛА закрываем на Bybit ===
        close_price = pos.get('current') or pos.get('entry', 0)
        if close_price <= 0:
            logger.error(f"[CLOSE] Invalid close price for position {pos_id}")
            close_price = pos.get('entry', 0)
        
        try:
            hedging_enabled = await asyncio.wait_for(is_hedging_enabled(), timeout=3.0)
        except (asyncio.TimeoutError, Exception):
            hedging_enabled = False
        
        if hedging_enabled:
            bybit_qty = pos.get('bybit_qty', 0)
            if bybit_qty > 0:
                try:
                    hedge_result = await asyncio.wait_for(
                        hedge_close(pos_id, pos['symbol'], pos['direction'], bybit_qty),
                        timeout=5.0
                    )
                    if hedge_result:
                        # Получаем реальную цену закрытия с Bybit (no delay needed)
                        try:
                            close_side = "Sell" if pos['direction'] == "LONG" else "Buy"
                            order_info = await asyncio.wait_for(
                                hedger.get_last_order_price(pos['symbol'], close_side),
                                timeout=3.0
                            )
                            if order_info and order_info.get('price'):
                                close_price = order_info['price']
                        except (asyncio.TimeoutError, Exception):
                            pass  # Use current price as fallback
                    else:
                        # Bybit не закрыл - НЕ закрываем в боте
                        logger.warning(f"[HEDGE] Failed to close on Bybit - position kept open")
                        await edit_or_send(
                            query,
                            f"<b>❌ Ошибка закрытия</b>\n\n"
                            f"Не удалось закрыть позицию на Bybit.\n"
                            f"Позиция сохранена. Попробуйте ещё раз.",
                            InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔄 Повторить", callback_data=f"close_{pos_id}")],
                                [InlineKeyboardButton("📊 Сделки", callback_data="trades")]
                            ])
                        )
                        return
                except asyncio.TimeoutError:
                    logger.error(f"[HEDGE] Timeout closing position {pos_id} on Bybit")
                    await edit_or_send(
                        query,
                        f"<b>⏱️ Таймаут</b>\n\n"
                        f"Закрытие заняло слишком много времени.\n"
                        f"Попробуйте ещё раз.",
                        InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔄 Повторить", callback_data=f"close_{pos_id}")],
                            [InlineKeyboardButton("📊 Сделки", callback_data="trades")]
                        ])
                    )
                    return
                except Exception as e:
                    logger.error(f"[HEDGE] Error closing position {pos_id} on Bybit: {e}")
                    await edit_or_send(
                        query,
                        f"<b>❌ Ошибка Bybit</b>\n\n"
                        f"Ошибка при закрытии на Bybit: {str(e)[:50]}\n"
                        f"Позиция сохранена.",
                        InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
                    )
                    return
        
        # Пересчитываем PnL с реальной ценой закрытия
        entry_price = pos.get('entry', 0)
        if entry_price <= 0:
            logger.error(f"[CLOSE] Invalid entry price for position {pos_id}")
            entry_price = close_price  # Fallback
        
        if pos['direction'] == "LONG":
            pnl_percent = (close_price - entry_price) / entry_price if entry_price > 0 else 0
        else:
            pnl_percent = (entry_price - close_price) / entry_price if entry_price > 0 else 0
        
        amount = pos.get('amount', 0)
        commission = pos.get('commission', 0)
        pnl = amount * LEVERAGE * pnl_percent - commission
        
        returned = amount + pnl
        
        # Use lock for balance update
        async with get_user_lock(user_id):
            user = get_user(user_id)  # Re-read with lock
            user['balance'] = sanitize_balance(user['balance'] + returned)
            user['total_profit'] = (user.get('total_profit', 0) or 0) + pnl
            save_user(user_id)
        
        # Закрываем в БД и удаляем из кэша
        db_close_position(pos_id, close_price, pnl, 'MANUAL')
        
        # Явно удаляем из кэша по ID
        try:
            cached_positions = positions_cache.get(user_id)
            if cached_positions:
                positions_cache.set(user_id, [p for p in cached_positions if p.get('id') != pos_id])
        except Exception as e:
            logger.warning(f"[CLOSE] Cache update error: {e}")
        
        pnl_abs = abs(pnl)
        
        if pnl > 0:
            text = f"""<b>✅ Сделка закрыта</b>

{ticker} | <b>+${pnl_abs:.2f}</b>
Чистая работа.

💰 Баланс: ${user['balance']:.2f}"""
        elif pnl == 0:
            text = f"""<b>📊 Безубыток</b>

{ticker} | $0.00
Вышли без потерь.

💰 Баланс: ${user['balance']:.2f}"""
        else:
            text = f"""<b>❌ Сделка закрыта</b>

{ticker} | <b>-${pnl_abs:.2f}</b>
Часть стратегии.

💰 Баланс: ${user['balance']:.2f}"""
        
        keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
        await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))
        
        logger.info(f"[CLOSE] User {user_id} closed {ticker} position {pos_id}, PnL: ${pnl:.2f}")
        
    except Exception as e:
        logger.error(f"[CLOSE] Critical error closing position {pos_id} for user {user_id}: {e}", exc_info=True)
        try:
            await edit_or_send(
                query,
                "<b>❌ Ошибка</b>\n\nНе удалось закрыть позицию. Попробуйте позже.",
                InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
            )
        except:
            pass


async def close_stacked_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть несколько стакнутых позиций одним нажатием"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    try:
        # closestack_1,2,3 -> [1, 2, 3]
        ids_str = query.data.replace("closestack_", "")
        position_ids = [int(pid) for pid in ids_str.split(",")]
    except (ValueError, IndexError):
        await query.answer("Ошибка данных", show_alert=True)
        return
    
    if not position_ids:
        await query.answer("Позиции не найдены", show_alert=True)
        return
    
    # Находим все позиции для закрытия
    to_close = [p for p in user_positions if p['id'] in position_ids]
    
    if not to_close:
        await query.answer("Позиции не найдены", show_alert=True)
        return
    
    # Фильтруем позиции с amount > 0
    zero_amount = [p for p in to_close if p.get('amount', 0) <= 0]
    for zero_pos in zero_amount:
        realized_pnl = zero_pos.get('realized_pnl', 0) or 0
        db_close_position(zero_pos['id'], zero_pos.get('current', zero_pos['entry']), realized_pnl, 'FULLY_CLOSED')
        logger.info(f"[CLOSE_STACK] Removed zero-amount position {zero_pos['id']}")
    
    to_close = [p for p in to_close if p.get('amount', 0) > 0]
    
    if not to_close:
        # Все позиции были уже закрыты
        await edit_or_send(query, "<b>✅ Позиции закрыты</b>\n\nВсе позиции были уже закрыты частичными тейками.", 
                          InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]))
        return
    
    await edit_or_send(query, "<b>⏳ Закрываем позиции...</b>", None)
    
    ticker = to_close[0]['symbol'].split("/")[0] if "/" in to_close[0]['symbol'] else to_close[0]['symbol']
    
    # === ГРУППИРУЕМ ПО СИМВОЛУ ДЛЯ BYBIT ===
    close_prices = {}  # symbol -> close_price
    failed_closes = []  # Позиции которые не удалось закрыть на Bybit
    
    try:
        hedging_enabled = await asyncio.wait_for(is_hedging_enabled(), timeout=3.0)
    except (asyncio.TimeoutError, Exception):
        hedging_enabled = False
    
    if hedging_enabled:
        by_symbol = {}
        for pos in to_close:
            key = (pos['symbol'], pos['direction'])
            if key not in by_symbol:
                by_symbol[key] = []
            by_symbol[key].append(pos)
        
        # Закрываем на Bybit по символам и получаем реальные цены
        for (symbol, direction), positions in by_symbol.items():
            total_qty = sum(p.get('bybit_qty', 0) for p in positions)
            if total_qty > 0:
                try:
                    hedge_result = await asyncio.wait_for(
                        hedge_close(positions[0]['id'], symbol, direction, total_qty),
                        timeout=5.0
                    )
                    if hedge_result:
                        # Получаем реальную цену закрытия (no delay needed)
                        try:
                            close_side = "Sell" if direction == "LONG" else "Buy"
                            order_info = await asyncio.wait_for(
                                hedger.get_last_order_price(symbol, close_side),
                                timeout=3.0
                            )
                            if order_info and order_info.get('price'):
                                close_prices[(symbol, direction)] = order_info['price']
                        except (asyncio.TimeoutError, Exception):
                            pass  # Use current price as fallback
                    else:
                        logger.warning(f"[CLOSE_STACKED] Failed to close {symbol} {direction} on Bybit")
                        failed_closes.extend(positions)
                except asyncio.TimeoutError:
                    logger.error(f"[CLOSE_STACKED] Timeout closing {symbol} {direction}")
                    failed_closes.extend(positions)
                except Exception as e:
                    logger.error(f"[CLOSE_STACKED] Error closing {symbol} {direction}: {e}")
                    failed_closes.extend(positions)
    
    # Убираем позиции которые не удалось закрыть на Bybit
    if failed_closes and hedging_enabled:
        to_close = [p for p in to_close if p not in failed_closes]
        if not to_close:
            await edit_or_send(query,
                f"<b>❌ Ошибка закрытия</b>\n\n"
                f"Не удалось закрыть позиции на Bybit.\n"
                f"Позиции сохранены. Попробуйте ещё раз.",
                InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
            )
            return
    
    # === ЗАКРЫВАЕМ В БД ===
    total_pnl = 0
    total_returned = 0
    
    for pos in to_close:
        # Получаем реальную цену закрытия если есть
        close_price = close_prices.get((pos['symbol'], pos['direction']), pos.get('current', pos['entry']))
        
        # Пересчитываем PnL с реальной ценой
        if pos['direction'] == "LONG":
            pnl_percent = (close_price - pos['entry']) / pos['entry']
        else:
            pnl_percent = (pos['entry'] - close_price) / pos['entry']
        pnl = pos['amount'] * LEVERAGE * pnl_percent - pos.get('commission', 0)
        
        returned = pos['amount'] + pnl
        
        total_pnl += pnl
        total_returned += returned
        
        # Закрываем в БД
        db_close_position(pos['id'], close_price, pnl, 'MANUAL')
        # Явно удаляем из кэша по ID
        pos_id_to_remove = pos['id']
        current_positions = positions_cache.get(user_id, [])
        if current_positions:
            positions_cache.set(user_id, [p for p in current_positions if p.get('id') != pos_id_to_remove])
    
    # Обновляем баланс (с локом для защиты от race conditions)
    async with get_user_lock(user_id):
        user = get_user(user_id)  # Re-read with lock
        user['balance'] = sanitize_balance(user['balance'] + total_returned)
        user['total_profit'] += total_pnl
        save_user(user_id)
    
    pnl_abs = abs(total_pnl)
    
    if total_pnl > 0:
        text = f"""<b>✅ Сделки закрыты</b>

{ticker} | <b>+${pnl_abs:.2f}</b>
Закрыто: {len(to_close)}
Чистая работа.

💰 Баланс: ${user['balance']:.2f}"""
    elif total_pnl == 0:
        text = f"""<b>📊 Безубыток</b>

{ticker} | $0.00
Закрыто: {len(to_close)}
Вышли без потерь.

💰 Баланс: ${user['balance']:.2f}"""
    else:
        text = f"""<b>❌ Сделки закрыты</b>

{ticker} | <b>-${pnl_abs:.2f}</b>
Закрыто: {len(to_close)}
Часть стратегии.

💰 Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def custom_amount_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрос своей суммы"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    try:
        # custom|SYM|D|ENTRY|SL|TP1|TP2|TP3|WINRATE (новый формат)
        # custom|SYM|D|ENTRY|SL|TP|WINRATE (старый формат)
        data = query.data.split("|")
        if len(data) < 6:
            await query.edit_message_text("<b>❌ Ошибка</b>\n\nНекорректные данные сигнала.", parse_mode="HTML")
            return
        
        # Validate symbol
        symbol = data[1] if len(data) > 1 else ''
        if not symbol:
            await query.edit_message_text("<b>❌ Ошибка</b>\n\nПустой символ.", parse_mode="HTML")
            return
        
        # Validate direction
        direction = data[2] if len(data) > 2 else ''
        if direction not in ['L', 'S', 'LONG', 'SHORT']:
            await query.edit_message_text("<b>❌ Ошибка</b>\n\nНекорректное направление.", parse_mode="HTML")
            return
        
        # Validate numeric values
        try:
            entry = float(data[3]) if len(data) > 3 else 0
            sl = float(data[4]) if len(data) > 4 else 0
            if entry <= 0 or sl <= 0:
                await query.edit_message_text("<b>❌ Ошибка</b>\n\nНекорректные цены.", parse_mode="HTML")
                return
        except (ValueError, IndexError):
            await query.edit_message_text("<b>❌ Ошибка</b>\n\nНекорректные числовые данные.", parse_mode="HTML")
            return
        
        # Сохраняем данные сигнала
        if len(data) >= 9:
            # Новый формат с тремя TP
            context.user_data['pending_trade'] = {
                'symbol': data[1],
                'direction': data[2],
                'entry': data[3],
                'sl': data[4],
                'tp1': data[5],
                'tp2': data[6],
                'tp3': data[7],
                'tp': data[5],  # для совместимости
                'winrate': data[8] if len(data) > 8 else '75'
            }
        else:
            # Старый формат
            context.user_data['pending_trade'] = {
                'symbol': data[1],
                'direction': data[2],
                'entry': data[3],
                'sl': data[4],
                'tp': data[5],
                'tp1': data[5],
                'winrate': data[6] if len(data) > 6 else '75'
            }
        
        user = get_user(user_id)
        
    except Exception as e:
        logger.error(f"[CUSTOM_PROMPT] Error parsing data for user {user_id}: {e}")
        await query.edit_message_text("<b>❌ Ошибка</b>\n\nОшибка обработки данных.", parse_mode="HTML")
        return
    
    text = f"""<b>💵 Своя сумма</b>

Минимум: $1

Введи сумму:

💰 Баланс: ${user['balance']:.2f}"""

    keyboard = [
        [InlineKeyboardButton("❌ Отмена", callback_data="skip")],
        [InlineKeyboardButton("🏠 Домой", callback_data="back"), InlineKeyboardButton("📊 Сделки", callback_data="trades")]
    ]
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def handle_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка введённой суммы"""
    # Проверяем crypto custom amount сначала
    if context.user_data.get('awaiting_crypto_amount'):
        handled = await handle_crypto_custom_amount(update, context)
        if handled:
            return
    
    # Проверяем Stars custom amount
    if context.user_data.get('awaiting_stars_amount'):
        try:
            amount = int(float(update.message.text.replace(",", ".").replace("$", "").strip()))
            user_id = update.effective_user.id
            
            if amount < 1:
                await update.message.reply_text(
                    "<b>❌ Ошибка</b>\n\nМинимальная сумма: $1",
                    parse_mode="HTML"
                )
                return True
            
            if amount > 500:
                await update.message.reply_text(
                    "<b>❌ Ошибка</b>\n\nМаксимальная сумма: $500",
                    parse_mode="HTML"
                )
                return True
            
            context.user_data['awaiting_stars_amount'] = False
            stars = amount * STARS_RATE
            
            logger.info(f"[STARS] User {user_id} requested custom invoice: {stars} stars = ${amount}")
            
            try:
                await context.bot.send_invoice(
                    chat_id=user_id,
                    title=f"Пополнение ${amount}",
                    description=f"Пополнение баланса на ${amount}",
                    payload=f"deposit_{amount}",
                    provider_token="",
                    currency="XTR",
                    prices=[LabeledPrice(label=f"${amount}", amount=stars)]
                )
                logger.info(f"[STARS] Custom invoice sent to user {user_id}: {stars} stars")
            except Exception as e:
                logger.error(f"[STARS] Failed to send custom invoice: {e}")
                await update.message.reply_text(
                    "<b>❌ Ошибка</b>\n\nНе удалось создать счёт.\nПопробуйте позже.",
                    parse_mode="HTML"
                )
            return True
            
        except ValueError:
            await update.message.reply_text(
                "<b>❌ Ошибка</b>\n\nВведите число, например: 15",
                parse_mode="HTML"
            )
            return True
    
    # Проверяем сумму для вывода
    if context.user_data.get('awaiting_withdraw_amount'):
        try:
            amount = float(update.message.text.replace(",", ".").replace("$", "").strip())
            user_id = update.effective_user.id
            user = get_user(user_id)
            MIN_WITHDRAW = 5.0
            
            if amount < MIN_WITHDRAW:
                await update.message.reply_text(
                    f"<b>❌ Ошибка</b>\n\nМинимальная сумма: ${MIN_WITHDRAW:.2f}",
                    parse_mode="HTML"
                )
                return True
            
            if amount > user['balance']:
                await update.message.reply_text(
                    f"<b>❌ Ошибка</b>\n\nНедостаточно средств.\n\n💰 Баланс: ${user['balance']:.2f}",
                    parse_mode="HTML"
                )
                return True
            
            # Устанавливаем сумму и запрашиваем адрес
            context.user_data['awaiting_withdraw_amount'] = False
            context.user_data['pending_withdraw'] = {
                'amount': amount,
                'user_id': user_id
            }
            
            await update.message.reply_text(
                f"""<b>💸 Вывод средств</b>

Сумма: <b>${amount:.2f} USDT</b>

Отправь адрес кошелька USDT (TRC20) для получения средств.

Или отправь свой Telegram ID для вывода через CryptoBot.""",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Отмена", callback_data="withdraw_menu")]]),
                parse_mode="HTML"
            )
            return True
        except ValueError:
            await update.message.reply_text(
                "<b>❌ Ошибка</b>\n\nВведи число, например: 15 или 25.5",
                parse_mode="HTML"
            )
            return True
    
    # Проверяем адрес для вывода
    if 'pending_withdraw' in context.user_data:
        handled = await process_withdraw_address(update, context)
        if handled:
            return
    
    if 'pending_trade' not in context.user_data:
        return

    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)

    try:
        amount = float(update.message.text.replace(",", ".").replace("$", "").strip())
    except ValueError:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nВведи число, например: 15 или 25.5", parse_mode="HTML")
        return

    if amount < 1:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nМинимум: $1.00", parse_mode="HTML")
        return

    if amount > user['balance']:
        await update.message.reply_text(
            f"<b>❌ Недостаточно средств</b>\n\n"
            f"Введи другую сумму:\n\n"
            f"💰 Баланс: ${user['balance']:.2f}",
            parse_mode="HTML"
        )
        return  # pending_trade сохраняется, можно ввести снова

    trade = context.user_data.pop('pending_trade')

    # Выполняем сделку с валидацией
    try:
        symbol = trade.get('symbol', '')
        if not symbol:
            await update.message.reply_text("<b>❌ Ошибка</b>\n\nНекорректный символ.", parse_mode="HTML")
            return
        
        # Validate symbol
        valid, error = validate_symbol(symbol)
        if not valid:
            await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{error}", parse_mode="HTML")
            return
        
        direction = "LONG" if trade.get('direction', '') == 'L' else "SHORT"
        
        # Validate direction
        valid, error = validate_direction(direction)
        if not valid:
            await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{error}", parse_mode="HTML")
            return
        
        entry_str = trade.get('entry', '0')
        sl_str = trade.get('sl', '0')
        tp_str = trade.get('tp', trade.get('tp1', '0'))
        
        entry = float(entry_str) if entry_str else 0
        sl = float(sl_str) if sl_str else 0
        
        if entry <= 0 or sl <= 0:
            await update.message.reply_text("<b>❌ Ошибка</b>\n\nНекорректные цены входа/стопа.", parse_mode="HTML")
            return
        
        tp1 = float(trade.get('tp1', tp_str)) if trade.get('tp1', tp_str) else entry
        tp2_default = entry + (tp1 - entry) * 2 if direction == "LONG" else entry - (entry - tp1) * 2
        tp3_default = entry + (tp1 - entry) * 3.5 if direction == "LONG" else entry - (entry - tp1) * 3.5
        tp2 = float(trade.get('tp2', tp2_default)) if trade.get('tp2') else tp2_default
        tp3 = float(trade.get('tp3', tp3_default)) if trade.get('tp3') else tp3_default
        tp = tp1  # для совместимости
        
        winrate_str = trade.get('winrate', '75')
        winrate = int(winrate_str) if winrate_str and winrate_str.isdigit() else 75
        winrate = max(0, min(100, winrate))  # Clamp between 0-100
        
    except (ValueError, TypeError) as e:
        logger.warning(f"[CUSTOM_AMOUNT] Invalid trade data for user {user_id}: {e}")
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nНекорректные данные сделки.", parse_mode="HTML")
        return
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol

    # === ЗАЩИТА: Не добавлять к убыточной позиции ===
    for p in user_positions:
        if p['symbol'] == symbol and p['direction'] == direction:
            if p.get('current') and p.get('entry'):
                if direction == "LONG":
                    pnl_pct = (p['current'] - p['entry']) / p['entry'] * 100
                else:
                    pnl_pct = (p['entry'] - p['current']) / p['entry'] * 100
                
                if pnl_pct < -1.5:
                    await update.message.reply_text(
                        f"<b>⛔ Добавление заблокировано</b>\n\n"
                        f"{ticker} {direction} уже в минусе {pnl_pct:.1f}%\n"
                        f"Нельзя усреднять убыточную позицию",
                        parse_mode="HTML"
                    )
                    logger.info(f"[PROTECTION] User {user_id}: blocked adding to losing {ticker} {direction} (PnL={pnl_pct:.1f}%)")
                    return
            break

    # === ХЕДЖИРОВАНИЕ: СНАЧАЛА открываем на Bybit ===
    bybit_qty = 0
    hedging_enabled = await is_hedging_enabled()
    
    if hedging_enabled:
        hedge_result = await hedge_open(0, symbol, direction, amount * LEVERAGE, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
        if hedge_result:
            bybit_qty = hedge_result.get('qty', 0)
            logger.info(f"[HEDGE] ✓ Hedged on Bybit: qty={bybit_qty}, partial TPs created")
            
            # Верификация что позиция реально открылась
            await asyncio.sleep(0.5)
            bybit_pos = await hedger.get_position_data(symbol)
            if not bybit_pos or bybit_pos.get('size', 0) == 0:
                logger.error(f"[HEDGE] ❌ VERIFICATION FAILED: position not found on Bybit")
                await update.message.reply_text(
                    f"<b>❌ Ошибка открытия</b>\n\n"
                    f"Bybit не подтвердил позицию.\n"
                    f"Попробуйте ещё раз.",
                    parse_mode="HTML"
                )
                return
            
            # Сохраняем РЕАЛЬНЫЙ размер с Bybit (может отличаться из-за округления)
            real_bybit_qty = float(bybit_pos.get('size', 0))
            if real_bybit_qty > 0 and abs(real_bybit_qty - bybit_qty) / bybit_qty > 0.01:  # Разница > 1%
                logger.info(f"[HEDGE] Correcting qty: calculated={bybit_qty}, real={real_bybit_qty}")
                bybit_qty = real_bybit_qty
            
            # Успешно открыто на Bybit - инкрементируем статистику
            increment_bybit_opened()
        else:
            logger.error(f"[HEDGE] ❌ Failed to open on Bybit - aborting trade")
            await update.message.reply_text(
                f"<b>❌ Ошибка открытия</b>\n\n"
                f"Не удалось открыть позицию на Bybit.\n"
                f"Проверьте баланс и настройки API.",
                parse_mode="HTML"
            )
            return

    # Комиссия за открытие (только после успешного открытия на Bybit)
    commission = amount * (COMMISSION_PERCENT / 100)
    
    # Защита от race conditions при изменении баланса
    async with get_user_lock(user_id):
        user = get_user(user_id)  # Re-read with lock
        user['balance'] -= amount
        user['balance'] = sanitize_balance(user['balance'])  # Security: ensure non-negative
        save_user(user_id)

    # Добавляем комиссию в накопитель (авто-вывод) с учетом рефералов
    await add_commission(commission, user_id=user_id)

    # === СОЗДАЁМ ПОЗИЦИЮ С ЗАЩИТОЙ ОТ ПОТЕРИ ДЕНЕГ ===
    try:
        # === ПРОВЕРЯЕМ ЕСТЬ ЛИ УЖЕ ПОЗИЦИЯ С ТАКИМ СИМВОЛОМ И НАПРАВЛЕНИЕМ ===
        existing = None
        for p in user_positions:
            if p['symbol'] == symbol and p['direction'] == direction:
                existing = p
                break

        if existing:
            # === ПРОВЕРЯЕМ СИНХРОНИЗАЦИЮ С BYBIT ===
            if hedging_enabled and existing.get('bybit_qty', 0) > 0:
                bybit_pos = await hedger.get_position_data(symbol)
                if not bybit_pos or bybit_pos.get('size', 0) == 0:
                    logger.warning(f"[TRADE] Existing position {symbol} not found on Bybit, creating as new")
                    existing = None  # Создаём новую позицию вместо усреднения
            
        if existing:
            # === ДОБАВЛЯЕМ К СУЩЕСТВУЮЩЕЙ ПОЗИЦИИ ===
            old_amount = existing['amount']
            new_amount = old_amount + amount
            
            # Weighted average entry price
            new_entry = (existing['entry'] * old_amount + entry * amount) / new_amount
            
            # Добавляем qty к существующему
            new_bybit_qty = existing.get('bybit_qty', 0) + bybit_qty
            
            # Обновляем позицию
            existing['amount'] = new_amount
            existing['entry'] = new_entry
            existing['commission'] = existing.get('commission', 0) + commission
            existing['bybit_qty'] = new_bybit_qty
            existing['pnl'] = -existing['commission']
            
            # Обновляем в БД
            db_update_position(existing['id'], 
                amount=new_amount, 
                entry=new_entry, 
                commission=existing['commission'],
                bybit_qty=new_bybit_qty,
                pnl=existing['pnl']
            )
            
            pos_id = existing['id']
            logger.info(f"[TRADE] User {user_id} added ${amount} to existing {direction} {symbol} (custom), total=${new_amount}")
        else:
            # === СОЗДАЁМ НОВУЮ ПОЗИЦИЮ С ТРЕМЯ TP ===
            position = {
                'symbol': symbol,
                'direction': direction,
                'amount': amount,
                'entry': entry,
                'current': entry,
                'sl': sl,
                'tp': tp1,
                'tp1': tp1,
                'tp2': tp2,
                'tp3': tp3,
                'tp1_hit': False,
                'tp2_hit': False,
                'pnl': -commission,
                'commission': commission,
                'bybit_qty': bybit_qty,
                'realized_pnl': 0,  # Реализованный P&L для этой позиции
                'original_amount': amount
            }

            pos_id = db_add_position(user_id, position)
            position['id'] = pos_id

            # Обновляем кэш - загружаем все позиции из БД
            positions_cache.set(user_id, db_get_positions(user_id))
            
            logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount} x{LEVERAGE} (custom), TP1/2/3={tp1:.4f}/{tp2:.4f}/{tp3:.4f}")
    except Exception as e:
        # КРИТИЧЕСКАЯ ОШИБКА: позиция не создалась, возвращаем деньги
        logger.critical(f"[TRADE] ❌ CRITICAL: Position creation failed for user {user_id}, restoring ${amount}! Error: {e}")
        async with get_user_lock(user_id):
            user = get_user(user_id)
            user['balance'] += amount  # Возвращаем деньги
            user['balance'] = sanitize_balance(user['balance'])
            save_user(user_id)
        trade_logger.log_error(f"Position creation failed (custom), restored ${amount} to user {user_id}", error=e, user_id=user_id)
        await update.message.reply_text(
            f"<b>❌ Ошибка создания позиции</b>\n\n"
            f"Деньги возвращены на баланс.\n"
            f"Попробуйте ещё раз.",
            parse_mode="HTML"
        )
        return
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_text = "LONG" if direction == "LONG" else "SHORT"
    tp1_percent = abs(tp1 - entry) / entry * 100
    tp2_percent = abs(tp2 - entry) / entry * 100
    tp3_percent = abs(tp3 - entry) / entry * 100
    sl_percent = abs(sl - entry) / entry * 100
    
    text = f"""<b>✅ {winrate}%</b> | {ticker} | {dir_text} | x{LEVERAGE}

<b>${amount:.2f}</b> открыто

Вход: <b>${entry:,.2f}</b>

TP1: ${tp1:,.2f} (<b>+{tp1_percent:.1f}%</b>) — 50%
TP2: ${tp2:,.2f} (+{tp2_percent:.1f}%) — 30%
TP3: ${tp3:,.2f} (+{tp3_percent:.1f}%) — 20%
SL: ${sl:,.2f} (-{sl_percent:.1f}%)

💰 Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

@rate_limit(max_requests=30, window_seconds=60, action_type="skip_signal")
async def skip_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    logger.info(f"[SKIP] User {update.effective_user.id}")
    
    # Очищаем pending trade если был
    if 'pending_trade' in context.user_data:
        del context.user_data['pending_trade']
    
        await query.answer("✅ Пропущено")
    try:
        await query.message.delete()
    except:
        pass

async def unknown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ловим необработанные callbacks"""
    try:
        query = update.callback_query
        if not query:
            return
        logger.warning(f"[UNKNOWN] User {update.effective_user.id}, data: {query.data}")
        await query.answer("❌ Неизвестная команда")
    except Exception as e:
        logger.error(f"[UNKNOWN] Error handling unknown callback: {e}", exc_info=True)

# ==================== ОБНОВЛЕНИЕ ПОЗИЦИЙ ====================
@isolate_errors
async def update_positions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновление цен и PnL с реальными данными Bybit (если хеджирование) или Binance
    Errors are isolated to prevent one user's failure from affecting others
    """
    try:
        # === СИНХРОНИЗАЦИЯ С BYBIT: проверяем закрытые позиции ===
        bybit_open_symbols = set()
        bybit_sync_available = False  # Флаг что данные с Bybit получены успешно
        
        if await is_hedging_enabled():
            try:
                bybit_positions = await hedger.get_all_positions()
                bybit_open_symbols = {p['symbol'] for p in bybit_positions}
                bybit_sync_available = True  # Успешно получили данные (даже если список пустой)
                logger.debug(f"[BYBIT_SYNC] Открытых позиций на Bybit: {len(bybit_positions)}")
            except Exception as e:
                logger.warning(f"[BYBIT_SYNC] Ошибка получения позиций: {e}", exc_info=True)
                trade_logger.log_error(f"Error getting Bybit positions: {e}", error=e)
        
        # Process users in batches with locking
        user_ids = list(positions_cache.keys())
        total_positions = sum(len(positions_cache.get(uid, [])) for uid in user_ids)
        logger.debug(f"[UPDATE_POSITIONS] Обработка {len(user_ids)} пользователей, {total_positions} позиций")
        BATCH_SIZE = 10  # Process 10 users at a time
        
        for batch_start in range(0, len(user_ids), BATCH_SIZE):
            batch_user_ids = user_ids[batch_start:batch_start + BATCH_SIZE]
            
            # Process batch concurrently
            tasks = []
            for user_id in batch_user_ids:
                tasks.append(process_user_positions(user_id, bybit_sync_available, bybit_open_symbols, context))
            
            # Wait for batch to complete - errors are isolated per user
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Log any exceptions that occurred
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    user_id = batch_user_ids[i]
                    logger.error(f"[UPDATE_POSITIONS] Error processing user {user_id}: {result}", exc_info=True)
                    trade_logger.log_error(f"Error in update_positions for user {user_id}: {result}", error=result, user_id=user_id)
        
        # Cleanup expired cache entries
        try:
            cleanup_caches()
        except Exception as e:
            logger.warning(f"[UPDATE_POSITIONS] Error cleaning caches: {e}")
    except Exception as e:
        logger.error(f"[UPDATE_POSITIONS] Critical error: {e}", exc_info=True)
        trade_logger.log_error(f"Critical error in update_positions: {e}", error=e)


async def process_user_positions(user_id: int, bybit_sync_available: bool, 
                                 bybit_open_symbols: set, context: ContextTypes.DEFAULT_TYPE):
    """Process positions for a single user with locking"""
    user_lock = get_user_lock(user_id)
    
    try:
        async with user_lock:
            user_positions = get_positions(user_id)
            if not user_positions:
                return
            
            logger.debug(f"[PROCESS_USER] User {user_id}: {len(user_positions)} позиций")
            user = get_user(user_id)
            
            # === ПРОВЕРКА: позиция закрылась на Bybit? ===
            # Проверяем если данные с Bybit получены успешно (даже если там 0 позиций)
            if bybit_sync_available:
                for pos in user_positions[:]:
                    # Пропускаем позиции без bybit_qty
                    if pos.get('bybit_qty', 0) <= 0:
                        continue
                    
                    bybit_symbol = pos['symbol'].replace('/', '')
                    
                    # Если позиция была на Bybit но её больше нет - закрылась по TP/SL
                    if bybit_symbol not in bybit_open_symbols:
                        logger.info(f"[BYBIT_SYNC] Position {bybit_symbol} not found on Bybit! Bot has bybit_qty={pos.get('bybit_qty', 0)}")
                        ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
                        
                        # Получаем реальный PnL с Bybit
                        real_pnl = pos['pnl']
                        exit_price = pos['current']
                        reason = "CLOSED"
                        
                        try:
                            closed_trades = await hedger.get_closed_pnl(pos['symbol'], limit=5)
                            if closed_trades:
                                bybit_pnl = closed_trades[0]['closed_pnl']
                                bybit_exit = closed_trades[0]['exit_price']
                                bybit_time = closed_trades[0].get('updated_time', 0)
                                
                                # Валидация: PnL должен быть реалистичным
                                # Увеличен лимит - P&L может быть большим при высоком плече
                                max_reasonable_pnl = pos['amount'] * LEVERAGE * 2.0  # 200% от позиции
                                import time as time_module
                                current_time_ms = int(time_module.time() * 1000)
                                time_diff = (current_time_ms - bybit_time) / 1000 if bybit_time else 999999
                                
                                logger.info(f"[BYBIT_SYNC] Bybit data: pnl=${bybit_pnl:.2f}, time_diff={time_diff:.0f}s, max=${max_reasonable_pnl:.2f}")
                                
                                # ВСЕГДА используем Bybit PnL если он есть и время < 10 мин
                                if time_diff < 600:  # 10 мин для sync
                                    real_pnl = bybit_pnl
                                    exit_price = bybit_exit
                                    reason = "TP" if real_pnl > 0 else "SL"
                                    logger.info(f"[BYBIT_SYNC] Using Bybit PnL: ${real_pnl:.2f}")
                                else:
                                    logger.warning(f"[BYBIT_SYNC] Bybit trade too old ({time_diff:.0f}s), using local ${pos['pnl']:.2f}")
                        except Exception as e:
                            logger.warning(f"[BYBIT_SYNC] Ошибка получения closed PnL: {e}")
                        
                        # Возвращаем деньги пользователю (с локом для защиты от race conditions)
                        returned = pos['amount'] + real_pnl
                        async with get_user_lock(user_id):
                            user = get_user(user_id)  # Re-read with lock
                            user['balance'] = sanitize_balance(user['balance'] + returned)
                            user['total_profit'] += real_pnl
                            save_user(user_id)
                        
                        # Закрываем в БД
                        db_close_position(pos['id'], exit_price, real_pnl, f'BYBIT_{reason}')
                        
                        # Явно удаляем из кэша по ID (надёжнее чем remove)
                        pos_id_to_remove = pos['id']
                        updated_positions = [p for p in user_positions if p.get('id') != pos_id_to_remove]
                        update_positions_cache(user_id, updated_positions)
                        user_positions = updated_positions  # Update local reference
                        logger.info(f"[BYBIT_SYNC] Position {pos_id_to_remove} removed from cache, remaining: {len(updated_positions)}")
                        
                        # Уведомление
                        pnl_sign = "+" if real_pnl >= 0 else ""
                        pnl_emoji = "✅" if real_pnl >= 0 else "📉"
                        try:
                            await context.bot.send_message(
                                user_id,
                                f"<b>📡 Bybit</b>\n\n"
                                f"{ticker} закрыт\n"
                                f"{pos['direction']} | {reason}\n"
                                f"{pnl_emoji} {pnl_sign}${real_pnl:.2f}\n\n"
                                f"💰 Баланс: ${user['balance']:.2f}",
                                parse_mode="HTML"
                            )
                            logger.info(f"[BYBIT_SYNC] User {user_id}: {ticker} closed on Bybit, PnL=${real_pnl:.2f}")
                        except Exception as e:
                            logger.error(f"[BYBIT_SYNC] Notify error: {e}")
                        continue
        
        for pos in user_positions[:]:  # копия для безопасного удаления
            real_price = None
            
            # Если хеджирование включено - берём markPrice с Bybit (точнее для PnL)
            if await is_hedging_enabled():
                bybit_data = await hedger.get_position_data(pos['symbol'])
                if bybit_data and bybit_data.get('current'):
                    real_price = bybit_data['current']  # markPrice с Bybit
                    logger.debug(f"[UPDATE] {pos['symbol']}: using Bybit price ${real_price:.4f}")
            
            # Fallback на Binance если Bybit недоступен
            if not real_price:
                real_price = await get_cached_price(pos['symbol'])
            
            if real_price:
                pos['current'] = real_price
            else:
                # Фоллбэк на симуляцию если API недоступен
                change = random.uniform(-0.003, 0.004)
                pos['current'] = pos['current'] * (1 + change)
            
            # Проверка на микро-amount (< $0.01) - автозакрытие
            if pos['amount'] < 0.01:
                realized_pnl = pos.get('realized_pnl', 0) or 0
                db_close_position(pos['id'], pos.get('current', pos['entry']), realized_pnl, 'MICRO_CLOSE')
                updated_positions = [p for p in user_positions if p.get('id') != pos['id']]
                update_positions_cache(user_id, updated_positions)
                user_positions = updated_positions
                logger.info(f"[PROCESS] Auto-closed micro position {pos['id']} (amount=${pos['amount']:.6f})")
                continue
            
            # PnL - ВСЕГДА рассчитываем локально (Bybit PnL общий для всей позиции, не для отдельной записи бота)
            # ВАЖНО: комиссия НЕ вычитается здесь - она уже учтена в начальном pnl = -commission при открытии
            if pos['direction'] == "LONG":
                pnl_percent = (pos['current'] - pos['entry']) / pos['entry'] if pos['entry'] > 0 else 0
            else:
                pnl_percent = (pos['entry'] - pos['current']) / pos['entry'] if pos['entry'] > 0 else 0
            
            # Валидация PnL - ограничиваем экстремальные значения
            raw_pnl = pos['amount'] * LEVERAGE * pnl_percent
            pos['pnl'] = sanitize_pnl(raw_pnl, max_pnl=pos['amount'] * LEVERAGE * 2)  # Max 200% от позиции
            
            # Дополнительная проверка на NaN/Inf перед записью в БД
            if pos['pnl'] is None or (isinstance(pos['pnl'], float) and (pos['pnl'] != pos['pnl'] or abs(pos['pnl']) > 1e10)):
                pos['pnl'] = 0.0
                logger.warning(f"[PROCESS] Fixed invalid PnL for position {pos['id']}")
            
            pnl_percent_display = pnl_percent * 100  # Для удобства
            
            # Обновляем в БД
            db_update_position(pos['id'], current=pos['current'], pnl=round(pos['pnl'], 4))
            
            # === ПРОДВИНУТОЕ УПРАВЛЕНИЕ ПОЗИЦИЯМИ ===
            if ADVANCED_POSITION_MANAGEMENT:
                try:
                    # 1. ТРЕЙЛИНГ-СТОПЫ
                    # Получаем ATR для трейлинг-стопа
                    try:
                        klines_1h = await smart.get_klines(pos['symbol'], '1h', 50)
                        if klines_1h and len(klines_1h) >= 20:
                            highs = [float(k[2]) for k in klines_1h]
                            lows = [float(k[3]) for k in klines_1h]
                            closes = [float(k[4]) for k in klines_1h]
                            atr = smart.calculate_atr(highs, lows, closes)
                            
                            # Добавляем позицию в трейлинг если её там нет
                            if pos['id'] not in trailing_manager.active_trailing:
                                trailing_manager.add_position(
                                    pos['id'],
                                    pos['entry'],
                                    pos['direction'],
                                    atr,
                                    pos['sl']
                                )
                            
                            # Обновляем трейлинг-стоп
                            new_sl = trailing_manager.update_position(pos['id'], pos['current'])
                            if new_sl and new_sl != pos['sl']:
                                old_sl = pos['sl']
                                pos['sl'] = new_sl
                                db_update_position(pos['id'], sl=new_sl)
                                
                                # Обновляем SL на Bybit
                                if await is_hedging_enabled():
                                    await hedger.set_trading_stop(
                                        pos['symbol'].replace("/", ""),
                                        pos['direction'],
                                        sl=new_sl
                                    )
                                logger.info(f"[TRAIL] Position {pos['id']}: SL moved {old_sl:.4f} -> {new_sl:.4f}")
                    except Exception as e:
                        logger.warning(f"[TRAIL] Error updating trailing stop: {e}")
                    
                    # 2. ЧАСТИЧНОЕ ЗАКРЫТИЕ НА ПОЛПУТИ К TP
                    if not pos.get('tp1_hit', False) and pnl_percent_display > 0:
                        partial_close_amount = calculate_partial_close_amount(
                            pos['entry'],
                            pos['current'],
                            pos.get('tp1', pos['tp']),
                            pos['direction'],
                            pos['amount']
                        )
                        
                        if partial_close_amount > 0:
                            # Закрываем 25% на полпути к TP
                            close_percent = partial_close_amount / pos['amount']
                            close_qty = pos.get('bybit_qty', 0) * close_percent if pos.get('bybit_qty', 0) > 0 else 0
                            
                            if await is_hedging_enabled() and close_qty > 0:
                                await hedge_close(pos['id'], pos['symbol'], pos['direction'], close_qty)
                            
                            # PnL с учетом пропорциональной комиссии
                            original_amount = pos.get('original_amount', pos['amount'])
                            partial_commission = pos.get('commission', 0) * (partial_close_amount / original_amount)
                            
                            if pos['direction'] == "LONG":
                                partial_pnl = (pos['current'] - pos['entry']) / pos['entry'] * partial_close_amount * LEVERAGE - partial_commission
                            else:
                                partial_pnl = (pos['entry'] - pos['current']) / pos['entry'] * partial_close_amount * LEVERAGE - partial_commission
                            
                            returned = partial_close_amount + partial_pnl
                            
                            # Защита от race conditions при изменении баланса
                            async with get_user_lock(user_id):
                                user = get_user(user_id)  # Re-read with lock
                                user['balance'] = sanitize_balance(user['balance'] + returned)
                                user['total_profit'] += partial_pnl
                                save_user(user_id)
                            
                            pos['amount'] -= partial_close_amount
                            if pos.get('bybit_qty', 0) > 0:
                                pos['bybit_qty'] -= close_qty
                            
                            current_realized = pos.get('realized_pnl', 0) or 0
                            pos['realized_pnl'] = current_realized + partial_pnl
                            
                            db_update_position(pos['id'], amount=pos['amount'], bybit_qty=pos.get('bybit_qty', 0), realized_pnl=pos['realized_pnl'])
                            
                            logger.info(f"[PARTIAL] Position {pos['id']}: Closed 25% at halfway to TP, PnL=${partial_pnl:.2f}")
                    
                    # 3. УМНОЕ ДОБАВЛЕНИЕ К ПОЗИЦИЯМ (SCALING IN) - только для позиций в небольшом минусе
                    if -2.0 < pnl_percent_display < 0.5 and user['balance'] >= 10:
                        try:
                            # Получаем данные для анализа
                            klines_1h = await smart.get_klines(pos['symbol'], '1h', 100)
                            if klines_1h and len(klines_1h) >= 50:
                                # Парсим данные
                                opens = [float(k[1]) for k in klines_1h]
                                highs = [float(k[2]) for k in klines_1h]
                                lows = [float(k[3]) for k in klines_1h]
                                closes = [float(k[4]) for k in klines_1h]
                                volumes = [float(k[5]) for k in klines_1h]
                                
                                # Анализ
                                swings = smart.find_swing_points(highs, lows, lookback=5)
                                key_levels = smart.find_key_levels(highs, lows, closes, touches_required=2)
                                atr = smart.calculate_atr(highs, lows, closes)
                                rsi = smart.calculate_rsi(closes)
                                volume_data = smart.calculate_volume_profile(volumes)
                                
                                # MTF и SMC (упрощённо)
                                mtf_aligned = True  # Можно расширить
                                order_blocks = smart.find_order_blocks(opens, highs, lows, closes)
                                fvgs = smart.find_fair_value_gaps(highs, lows)
                                divergence = smart.detect_divergence(closes, highs, lows)
                                
                                # Определяем режим рынка
                                atr_percent = (atr / pos['current']) * 100
                                price_change_24h = (closes[-1] - closes[-24]) / closes[-24] * 100 if len(closes) >= 24 else 0
                                market_regime = smart.determine_market_regime(swings, atr_percent, price_change_24h)
                                
                                analysis_data = {
                                    'klines': klines_1h,
                                    'key_levels': key_levels,
                                    'swings': swings,
                                    'market_regime': market_regime.value if hasattr(market_regime, 'value') else str(market_regime),
                                    'rsi': rsi,
                                    'volume_data': volume_data,
                                    'mtf_aligned': mtf_aligned,
                                    'order_blocks': order_blocks,
                                    'fvgs': fvgs,
                                    'divergence': divergence
                                }
                                
                                should_scale, opportunity = should_scale_in_smart(pos, pos['current'], analysis_data)
                                
                                if should_scale and opportunity['confidence'] >= 0.6:
                                    scale_size = calculate_scale_in_size(pos, opportunity, user['balance'])
                                    
                                    if scale_size >= 10 and user['balance'] >= scale_size:
                                        # Добавляем к позиции (упрощённо - можно расширить с полной логикой)
                                        logger.info(f"[SCALE] Opportunity to scale in: ${scale_size:.2f}, confidence: {opportunity['confidence']:.0%}")
                                        # Здесь можно добавить автоматическое добавление или уведомление пользователю
                        except Exception as e:
                            logger.warning(f"[SCALE] Error analyzing scaling opportunity: {e}")
                    
                    # 4. PYRAMID TRADING - для прибыльных позиций
                    if pnl_percent_display > 2.0 and user['balance'] >= 10:
                        try:
                            klines_1h = await smart.get_klines(pos['symbol'], '1h', 50)
                            if klines_1h and len(klines_1h) >= 20:
                                highs = [float(k[2]) for k in klines_1h]
                                lows = [float(k[3]) for k in klines_1h]
                                closes = [float(k[4]) for k in klines_1h]
                                volumes = [float(k[5]) for k in klines_1h]
                                
                                atr = smart.calculate_atr(highs, lows, closes)
                                volume_data = smart.calculate_volume_profile(volumes)
                                
                                # Определяем режим рынка
                                swings = smart.find_swing_points(highs, lows, lookback=5)
                                atr_percent = (atr / pos['current']) * 100
                                price_change_24h = (closes[-1] - closes[-24]) / closes[-24] * 100 if len(closes) >= 24 else 0
                                market_regime = smart.determine_market_regime(swings, atr_percent, price_change_24h)
                                
                                # Рассчитываем скорость движения
                                if len(closes) >= 5:
                                    if pos['direction'] == "LONG":
                                        movement_speed = (closes[-1] - closes[-5]) / closes[-5] / (atr / closes[-1]) if atr > 0 else 0
                                    else:
                                        movement_speed = (closes[-5] - closes[-1]) / closes[-5] / (atr / closes[-1]) if atr > 0 else 0
                                else:
                                    movement_speed = 0
                                
                                analysis_data = {
                                    'market_regime': market_regime.value if hasattr(market_regime, 'value') else str(market_regime),
                                    'mtf_aligned': True,  # Упрощённо
                                    'volume_data': volume_data,
                                    'atr': atr,
                                    'order_blocks': [],
                                    'exhaustion_signals': []
                                }
                                
                                should_pyr, pyramid_opp = should_pyramid(pos, pos['current'], analysis_data)
                                
                                if should_pyr and pyramid_opp['confidence'] >= 0.65:
                                    pyramid_size = calculate_pyramid_size(pos, pyramid_opp, user['balance'])
                                    
                                    if pyramid_size >= 10 and user['balance'] >= pyramid_size:
                                        logger.info(f"[PYRAMID] Opportunity to pyramid: ${pyramid_size:.2f}, confidence: {pyramid_opp['confidence']:.0%}")
                                        # Здесь можно добавить автоматическое добавление pyramid позиции
                        except Exception as e:
                            logger.warning(f"[PYRAMID] Error analyzing pyramid opportunity: {e}")
                    
                    # 5. РАННИЙ ВЫХОД ПРИ РАЗВОРОТЕ
                    if pnl_percent_display > 0.5:  # Только для позиций в плюсе
                        try:
                            klines_1h = await smart.get_klines(pos['symbol'], '1h', 50)
                            if klines_1h and len(klines_1h) >= 20:
                                highs = [float(k[2]) for k in klines_1h]
                                lows = [float(k[3]) for k in klines_1h]
                                closes = [float(k[4]) for k in klines_1h]
                                volumes = [float(k[5]) for k in klines_1h]
                                
                                key_levels = smart.find_key_levels(highs, lows, closes, touches_required=2)
                                divergence = smart.detect_divergence(closes, highs, lows)
                                volume_data = smart.calculate_volume_profile(volumes)
                                
                                # Детекция exhaustion patterns (упрощённо)
                                exhaustion_patterns = []
                                vsa = smart.analyze_vsa([float(k[1]) for k in klines_1h], highs, lows, closes, volumes)
                                if vsa.get('pattern') in ['buying_climax', 'selling_climax']:
                                    exhaustion_patterns.append(vsa['pattern'])
                                
                                reversal_signals = detect_reversal_signals(
                                    pos,
                                    pos['current'],
                                    divergence,
                                    exhaustion_patterns,
                                    volume_data,
                                    key_levels,
                                    pos['direction']
                                )
                                
                                should_exit, action, close_percent = should_exit_early(
                                    pos,
                                    pos['current'],
                                    reversal_signals,
                                    pnl_percent_display
                                )
                                
                                if should_exit and close_percent > 0:
                                    close_amount = pos['amount'] * close_percent
                                    close_qty = pos.get('bybit_qty', 0) * close_percent if pos.get('bybit_qty', 0) > 0 else 0
                                    
                                    if await is_hedging_enabled() and close_qty > 0:
                                        await hedge_close(pos['id'], pos['symbol'], pos['direction'], close_qty)
                                    
                                    # PnL с учетом пропорциональной комиссии
                                    original_amount = pos.get('original_amount', pos['amount'])
                                    partial_commission = pos.get('commission', 0) * (close_amount / original_amount)
                                    
                                    if pos['direction'] == "LONG":
                                        exit_pnl = (pos['current'] - pos['entry']) / pos['entry'] * close_amount * LEVERAGE - partial_commission
                                    else:
                                        exit_pnl = (pos['entry'] - pos['current']) / pos['entry'] * close_amount * LEVERAGE - partial_commission
                                    
                                    returned = close_amount + exit_pnl
                                    
                                    # Защита от race conditions при изменении баланса
                                    async with get_user_lock(user_id):
                                        user = get_user(user_id)  # Re-read with lock
                                        user['balance'] = sanitize_balance(user['balance'] + returned)
                                        user['total_profit'] += exit_pnl
                                        save_user(user_id)
                                    
                                    if close_percent >= 1.0:
                                        # Полное закрытие
                                        db_close_position(pos['id'], pos['current'], exit_pnl, f"EARLY_EXIT_{action}")
                                        update_positions_cache(user_id, [p for p in user_positions if p.get('id') != pos['id']])
                                        trailing_manager.remove_position(pos['id'])
                                        
                                        try:
                                            await context.bot.send_message(
                                                user_id,
                                                f"<b>⚠️ Ранний выход</b>\n\n"
                                                f"{ticker} | {pos['direction']}\n"
                                                f"Признаки разворота: {', '.join(reversal_signals['signals'][:2])}\n"
                                                f"<b>+${exit_pnl:.2f}</b>\n\n"
                                                f"💰 Баланс: ${user['balance']:.2f}",
                                                parse_mode="HTML"
                                            )
                                        except:
                                            pass
                                        continue
                                    else:
                                        # Частичное закрытие
                                        pos['amount'] -= close_amount
                                        if pos.get('bybit_qty', 0) > 0:
                                            pos['bybit_qty'] -= close_qty
                                        
                                        current_realized = pos.get('realized_pnl', 0) or 0
                                        pos['realized_pnl'] = current_realized + exit_pnl
                                        
                                        db_update_position(pos['id'], amount=pos['amount'], bybit_qty=pos.get('bybit_qty', 0), realized_pnl=pos['realized_pnl'])
                                        
                                        logger.info(f"[EARLY_EXIT] Position {pos['id']}: Closed {close_percent:.0%} early, PnL=${exit_pnl:.2f}")
                        except Exception as e:
                            logger.warning(f"[EARLY_EXIT] Error detecting reversal: {e}")
                
                except Exception as e:
                    logger.error(f"[ADVANCED] Error in advanced position management: {e}")
            
            # === ЧАСТИЧНЫЕ ТЕЙКИ TP1, TP2, TP3 ===
            ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
            tp1 = pos.get('tp1', pos['tp'])
            tp2 = pos.get('tp2', tp1 * 1.5 if pos['direction'] == "LONG" else tp1 * 0.5)
            tp3 = pos.get('tp3', tp1 * 2 if pos['direction'] == "LONG" else tp1 * 0.3)
            
            if pos['direction'] == "LONG":
                hit_tp1 = pos['current'] >= tp1 and not pos.get('tp1_hit', False)
                hit_tp2 = pos['current'] >= tp2 and not pos.get('tp2_hit', False)
                hit_tp3 = pos['current'] >= tp3
                hit_sl = pos['current'] <= pos['sl']
            else:
                hit_tp1 = pos['current'] <= tp1 and not pos.get('tp1_hit', False)
                hit_tp2 = pos['current'] <= tp2 and not pos.get('tp2_hit', False)
                hit_tp3 = pos['current'] <= tp3
                hit_sl = pos['current'] >= pos['sl']
            
            # === TP1: Закрываем 50%, двигаем SL в безубыток ===
            if hit_tp1 and not hit_sl:
                close_percent = 0.50
                close_amount = pos['amount'] * close_percent
                remaining_amount = pos['amount'] - close_amount
                
                # СНАЧАЛА закрываем на Bybit
                bybit_closed = False
                if await is_hedging_enabled() and pos.get('bybit_qty', 0) > 0:
                    close_qty = pos['bybit_qty'] * close_percent
                    hedge_result = await hedge_close(pos['id'], pos['symbol'], pos['direction'], close_qty)
                    if hedge_result:
                        pos['bybit_qty'] -= close_qty
                        bybit_closed = True
                        logger.info(f"[TP1] Bybit closed {close_qty} qty for {ticker}")
                    else:
                        logger.error(f"[TP1] ❌ Failed to close on Bybit - skipping TP1 update")
                        continue  # Пропускаем - попробуем в следующем цикле
                else:
                    bybit_closed = True  # Хеджирование отключено или нет qty
                
                # PnL от частичного закрытия с учетом пропорциональной комиссии
                original_amount = pos.get('original_amount', pos['amount'] + close_amount)
                partial_commission = pos.get('commission', 0) * (close_amount / original_amount)
                
                if pos['direction'] == "LONG":
                    partial_pnl = (pos['current'] - pos['entry']) / pos['entry'] * close_amount * LEVERAGE - partial_commission
                else:
                    partial_pnl = (pos['entry'] - pos['current']) / pos['entry'] * close_amount * LEVERAGE - partial_commission
                
                # Возвращаем часть и профит (с локом для защиты от race conditions)
                returned = close_amount + partial_pnl
                async with get_user_lock(user_id):
                    user = get_user(user_id)  # Re-read with lock
                    user['balance'] = sanitize_balance(user['balance'] + returned)
                    user['total_profit'] += partial_pnl
                    save_user(user_id)
                
                # Обновляем позицию
                pos['amount'] = remaining_amount
                pos['tp1_hit'] = True
                pos['sl'] = pos['entry'] * 1.001 if pos['direction'] == "LONG" else pos['entry'] * 0.999  # SL в безубыток
                pos['tp'] = tp2  # Следующий TP
                # Накапливаем realized_pnl
                current_realized = pos.get('realized_pnl', 0) or 0
                new_realized = current_realized + partial_pnl
                pos['realized_pnl'] = new_realized
                
                db_update_position(pos['id'], amount=remaining_amount, sl=pos['sl'], tp=pos['tp'], bybit_qty=pos['bybit_qty'], realized_pnl=new_realized)
                
                # Обновляем SL на Bybit
                if bybit_closed and await is_hedging_enabled():
                    await hedger.set_trading_stop(pos['symbol'].replace("/", ""), pos['direction'], tp=tp2, sl=pos['sl'])
                
                try:
                    await context.bot.send_message(user_id, f"""<b>✅ TP1 достигнут</b>

{ticker} | <b>+${partial_pnl:.2f}</b>
Закрыто 50%, SL → безубыток
Следующая цель: TP2

💰 Баланс: ${user['balance']:.2f}""", parse_mode="HTML")
                    logger.info(f"[TP1] User {user_id} {ticker}: +${partial_pnl:.2f}, remaining {remaining_amount:.0f}")
                except Exception as e:
                    logger.error(f"[TP1] Notify error: {e}")
                continue
            
            # === TP2: Закрываем ещё 30%, запускаем агрессивный трейлинг ===
            if hit_tp2 and pos.get('tp1_hit', False) and not hit_sl:
                close_percent = 0.30 / 0.50  # 30% от оставшихся 50% = 60% текущей позиции
                close_amount = pos['amount'] * close_percent
                remaining_amount = pos['amount'] - close_amount
                
                # СНАЧАЛА закрываем на Bybit
                bybit_closed = False
                if await is_hedging_enabled() and pos.get('bybit_qty', 0) > 0:
                    close_qty = pos['bybit_qty'] * close_percent
                    hedge_result = await hedge_close(pos['id'], pos['symbol'], pos['direction'], close_qty)
                    if hedge_result:
                        pos['bybit_qty'] -= close_qty
                        bybit_closed = True
                        logger.info(f"[TP2] Bybit closed {close_qty} qty for {ticker}")
                    else:
                        logger.error(f"[TP2] ❌ Failed to close on Bybit - skipping TP2 update")
                        continue  # Пропускаем - попробуем в следующем цикле
                else:
                    bybit_closed = True
                
                # PnL с учетом пропорциональной комиссии
                original_amount = pos.get('original_amount', pos['amount'] + close_amount)
                partial_commission = pos.get('commission', 0) * (close_amount / original_amount)
                
                if pos['direction'] == "LONG":
                    partial_pnl = (pos['current'] - pos['entry']) / pos['entry'] * close_amount * LEVERAGE - partial_commission
                else:
                    partial_pnl = (pos['entry'] - pos['current']) / pos['entry'] * close_amount * LEVERAGE - partial_commission
                
                returned = close_amount + partial_pnl
                
                # Защита от race conditions при изменении баланса
                async with get_user_lock(user_id):
                    user = get_user(user_id)  # Re-read with lock
                    user['balance'] = sanitize_balance(user['balance'] + returned)
                    user['total_profit'] += partial_pnl
                    save_user(user_id)
                
                pos['amount'] = remaining_amount
                pos['tp2_hit'] = True
                pos['tp'] = tp3
                
                # SL подтягиваем к TP1 уровню
                if pos['direction'] == "LONG":
                    pos['sl'] = tp1 * 0.998
                else:
                    pos['sl'] = tp1 * 1.002
                
                # Накапливаем realized_pnl
                current_realized = pos.get('realized_pnl', 0) or 0
                new_realized = current_realized + partial_pnl
                pos['realized_pnl'] = new_realized
                
                db_update_position(pos['id'], amount=remaining_amount, sl=pos['sl'], tp=pos['tp'], bybit_qty=pos['bybit_qty'], realized_pnl=new_realized)
                
                # Обновляем SL на Bybit
                if bybit_closed and await is_hedging_enabled():
                    await hedger.set_trading_stop(pos['symbol'].replace("/", ""), pos['direction'], tp=tp3, sl=pos['sl'])
                
                try:
                    await context.bot.send_message(user_id, f"""<b>✅ TP2 достигнут</b>

{ticker} | <b>+${partial_pnl:.2f}</b>
Закрыто 80%, moonbag 20%
Цель: TP3

💰 Баланс: ${user['balance']:.2f}""", parse_mode="HTML")
                    logger.info(f"[TP2] User {user_id} {ticker}: +${partial_pnl:.2f}, runner {remaining_amount:.0f}")
                except Exception as e:
                    logger.error(f"[TP2] Notify error: {e}")
                continue
            
            # === TP3 или SL: Полное закрытие ===
            if hit_tp3 or hit_sl:
                real_pnl = pos['pnl']  # Default - локальный PnL
                exit_price = pos['current']
                
                # СНАЧАЛА закрываем на Bybit
                if await is_hedging_enabled():
                    bybit_qty = pos.get('bybit_qty', 0)
                    if bybit_qty > 0:
                        hedge_result = await hedge_close(pos['id'], pos['symbol'], pos['direction'], bybit_qty)
                        if hedge_result:
                            logger.info(f"[HEDGE] Auto-closed position {pos['id']} on Bybit (qty={bybit_qty})")
                            
                            # Получаем РЕАЛЬНЫЙ PnL с Bybit
                            await asyncio.sleep(0.5)
                            try:
                                closed_trades = await hedger.get_closed_pnl(pos['symbol'], limit=5)
                                if closed_trades:
                                    bybit_pnl = closed_trades[0]['closed_pnl']
                                    bybit_exit = closed_trades[0]['exit_price']
                                    bybit_qty = closed_trades[0].get('qty', 0)
                                    bybit_time = closed_trades[0].get('updated_time', 0)
                                    
                                    # Валидация: PnL должен быть реалистичным
                                    max_reasonable_pnl = pos['amount'] * LEVERAGE * 2.0  # 200% от позиции
                                    import time as time_module
                                    current_time_ms = int(time_module.time() * 1000)
                                    time_diff = (current_time_ms - bybit_time) / 1000 if bybit_time else 999999
                                    
                                    logger.info(f"[TP3/SL] Bybit data: pnl=${bybit_pnl:.2f}, qty={bybit_qty}, time_diff={time_diff:.0f}s")
                                    
                                    # ВСЕГДА используем Bybit PnL если он есть и время < 5 мин
                                    if time_diff < 300:  # 5 мин для TP/SL
                                        real_pnl = bybit_pnl
                                        exit_price = bybit_exit
                                        logger.info(f"[TP3/SL] Using Bybit PnL: ${real_pnl:.2f}")
                                    else:
                                        logger.warning(f"[TP3/SL] Bybit trade too old ({time_diff:.0f}s), using local ${pos['pnl']:.2f}")
                            except Exception as e:
                                logger.warning(f"[TP3/SL] Failed to get real PnL: {e}")
                        else:
                            logger.error(f"[HEDGE] ❌ Failed to auto-close on Bybit - skipping local close")
                            continue  # Пропускаем - попробуем в следующем цикле
                
                returned = pos['amount'] + real_pnl
                
                # Защита от race conditions при изменении баланса
                async with get_user_lock(user_id):
                    user = get_user(user_id)  # Re-read with lock
                    user['balance'] = sanitize_balance(user['balance'] + returned)
                    user['total_profit'] += real_pnl
                    save_user(user_id)
                
                reason = 'TP3' if hit_tp3 else 'SL'
                db_close_position(pos['id'], exit_price, real_pnl, reason)
                # Явно удаляем из кэша по ID
                pos_id_to_remove = pos['id']
                current_positions = positions_cache.get(user_id, [])
                if current_positions:
                    positions_cache.set(user_id, [p for p in current_positions if p.get('id') != pos_id_to_remove])
                
                pnl_abs = abs(real_pnl)
                
                if hit_tp3:
                    text = f"""<b>🎯 TP3 Runner</b>

{ticker} | <b>+${pnl_abs:.2f}</b>
{format_price(pos['entry'])} → {format_price(exit_price)}
Все цели достигнуты!

💰 Баланс: ${user['balance']:.2f}"""
                elif real_pnl >= 0:
                    text = f"""<b>📊 Безубыток</b>

{ticker} | ${real_pnl:.2f}
Защитный стоп сработал.

💰 Баланс: ${user['balance']:.2f}"""
                else:
                    text = f"""<b>📉 Stop Loss</b>

{ticker} | <b>-${pnl_abs:.2f}</b>
Стоп отработал.

💰 Баланс: ${user['balance']:.2f}"""
                
                try:
                    await context.bot.send_message(
                        user_id, text,
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
                    )
                    logger.info(f"[AUTO-CLOSE] User {user_id} {reason} {ticker}: Real PnL=${real_pnl:.2f}, Balance: ${user['balance']:.2f}")
                except Exception as e:
                    logger.error(f"[AUTO-CLOSE] Failed to notify user {user_id}: {e}")
    except Exception as e:
        logger.error(f"[PROCESS_USER] Error processing user {user_id}: {e}", exc_info=True)
        trade_logger.log_error(f"Error processing positions for user {user_id}: {e}", error=e, user_id=user_id)

# ==================== АДМИН-ПАНЕЛЬ ====================
def db_get_stats() -> Dict:
    """Статистика для админов"""
    row = run_sql("SELECT COUNT(*) as cnt, SUM(balance) as bal, SUM(total_deposit) as dep, SUM(total_profit) as prof FROM users", fetch="one")
    users_count = row['cnt'] or 0 if row else 0
    total_balance = row['bal'] or 0 if row else 0
    total_deposits = row['dep'] or 0 if row else 0
    total_profit = row['prof'] or 0 if row else 0
    
    row = run_sql("SELECT COUNT(*) as cnt FROM users WHERE trading = 1", fetch="one")
    active_traders = row['cnt'] or 0 if row else 0
    
    row = run_sql("SELECT COUNT(*) as cnt FROM positions", fetch="one")
    open_positions = row['cnt'] or 0 if row else 0
    
    row = run_sql("SELECT COUNT(*) as cnt, SUM(pnl) as pnl FROM history", fetch="one")
    total_trades = row['cnt'] or 0 if row else 0
    realized_pnl = row['pnl'] or 0 if row else 0
    
    row = run_sql("SELECT SUM(commission) as com FROM history", fetch="one")
    commissions = row['com'] or 0 if row else 0
    row = run_sql("SELECT SUM(commission) as com FROM positions", fetch="one")
    commissions += row['com'] or 0 if row else 0
    
    return {
        'users': users_count,
        'active_traders': active_traders,
        'total_balance': total_balance,
        'total_deposits': total_deposits,
        'total_profit': total_profit,
        'open_positions': open_positions,
        'total_trades': total_trades,
        'realized_pnl': realized_pnl,
        'commissions': commissions
    }

@rate_limit(max_requests=20, window_seconds=60, action_type="admin")
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Админ-панель"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    stats = db_get_stats()
    
    text = f"""<b>📊 Админ-панель</b>

👥 Пользователи: {stats['users']}
🟢 Активных: {stats['active_traders']}

💰 Баланс: ${stats['total_balance']:.2f}
📥 Депозиты: ${stats['total_deposits']:.2f}
📈 Профит: ${stats['total_profit']:.2f}

📋 Позиций: {stats['open_positions']}
✅ Сделок: {stats['total_trades']}
💵 P&L: ${stats['realized_pnl']:.2f}

🏦 Комиссии: ${stats['commissions']:.2f}"""
    
    await update.message.reply_text(text, parse_mode="HTML")

async def logs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Просмотр логов ошибок: /logs [hours] [limit]"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    # Параметры
    hours = 24
    limit = 20
    if context.args:
        try:
            if len(context.args) >= 1:
                hours = int(context.args[0])
            if len(context.args) >= 2:
                limit = int(context.args[1])
        except ValueError:
            await update.message.reply_text(
                "<b>❌ Ошибка</b>\n\nИспользование: /logs [hours] [limit]\n"
                "Пример: /logs 24 20",
                parse_mode="HTML"
            )
            return
    
    try:
        # Получаем ошибки
        error_summary = trade_logger.get_error_summary(hours=hours)
        recent_errors = trade_logger.get_recent_logs(
            category=LogCategory.ERROR,
            hours=hours,
            limit=limit
        )
        
        # Ошибки /start
        start_errors = []
        for err in recent_errors:
            msg = err.get('message', '').upper()
            if 'START' in msg or '/START' in msg or 'start' in err.get('message', ''):
                start_errors.append(err)
        
        # Ошибки сделок
        trade_errors = []
        for err in recent_errors:
            cat = err.get('category', '')
            if cat in ['TRADE_OPEN', 'TRADE_CLOSE']:
                trade_errors.append(err)
        
        # Формируем отчет
        text = f"""<b>📋 ЛОГИ ОШИБОК</b>

⏰ Период: {hours} часов
📊 Лимит: {limit} записей

<b>📈 Статистика:</b>
Всего ошибок: {error_summary.get('total_errors', 0)}
Уникальных: {error_summary.get('unique_errors', 0)}

<b>🔴 Ошибки /start:</b> {len(start_errors)}
<b>💼 Ошибки сделок:</b> {len(trade_errors)}

<b>🔝 Топ ошибок:</b>
"""
        
        top_errors = error_summary.get('top_errors', [])[:5]
        if top_errors:
            for i, err in enumerate(top_errors, 1):
                msg = err.get('message', 'N/A')[:60]
                count = err.get('count', 0)
                text += f"{i}. ({count}x) {msg}\n"
        else:
            text += "Нет ошибок\n"
        
        # Детали последних ошибок
        if recent_errors:
            text += f"\n<b>📝 Последние ошибки:</b>\n"
            for i, err in enumerate(recent_errors[:5], 1):
                timestamp = err.get('timestamp', 'N/A')
                if isinstance(timestamp, str) and len(timestamp) > 19:
                    timestamp = timestamp[:19]
                msg = err.get('message', 'N/A')[:50]
                user_id_err = err.get('user_id', 'N/A')
                text += f"\n{i}. [{timestamp}]\n"
                text += f"   User: {user_id_err}\n"
                text += f"   {msg}\n"
        else:
            text += "\n✅ Ошибок не найдено!"
        
        # Разбиваем на части если слишком длинное
        if len(text) > 4000:
            parts = text.split('\n\n')
            current_part = ""
            for part in parts:
                if len(current_part) + len(part) > 4000:
                    await update.message.reply_text(current_part, parse_mode="HTML")
                    current_part = part + "\n\n"
                else:
                    current_part += part + "\n\n"
            if current_part:
                await update.message.reply_text(current_part, parse_mode="HTML")
        else:
            await update.message.reply_text(text, parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"[LOGS] Error getting logs: {e}", exc_info=True)
        await update.message.reply_text(
            f"<b>❌ Ошибка получения логов</b>\n\n{str(e)}",
            parse_mode="HTML"
        )

async def sync_profits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Синхронизация total_profit всех пользователей с историей сделок (админ)"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    await update.message.reply_text("<b>⏳ Синхронизация профитов...</b>", parse_mode="HTML")
    
    try:
        updated = db_sync_all_profits()
        await update.message.reply_text(
            f"<b>✅ Синхронизация завершена</b>\n\nОбновлено пользователей: {updated}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"[SYNC] Error syncing profits: {e}", exc_info=True)
        await update.message.reply_text(
            f"<b>❌ Ошибка синхронизации</b>\n\n{str(e)}",
            parse_mode="HTML"
        )

async def commission_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Статус и вывод комиссий (админ)"""
    admin_id = update.effective_user.id
    
    if admin_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    stats = db_get_stats()
    
    # Проверяем баланс CryptoBot
    crypto_balance = "❓ Не удалось проверить"
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN", "")
    testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
    
    if crypto_token:
        try:
            base_url = "https://testnet-pay.crypt.bot" if testnet else "https://pay.crypt.bot"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{base_url}/api/getBalance",
                    headers={"Crypto-Pay-API-Token": crypto_token}
                ) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        balances = data.get("result", [])
                        usdt = next((b for b in balances if b.get("currency_code") == "USDT"), None)
                        if usdt:
                            crypto_balance = f"${float(usdt.get('available', 0)):.2f} USDT"
                        else:
                            crypto_balance = "$0.00 USDT"
                    else:
                        crypto_balance = f"❌ {data.get('error', {}).get('name', 'Error')}"
        except Exception as e:
            crypto_balance = f"❌ {str(e)[:30]}"
    else:
        crypto_balance = "❌ CRYPTO_BOT_TOKEN не настроен"
    
    text = f"""💰 <b>КОМИССИИ</b>

📊 Всего заработано: <b>${stats['commissions']:.2f}</b>
⏳ В ожидании вывода: <b>${pending_commission:.2f}</b>
🎯 Порог авто-вывода: ${COMMISSION_WITHDRAW_THRESHOLD}

<b>CryptoBot:</b>
├ Баланс: {crypto_balance}
├ Admin ID: <code>{ADMIN_CRYPTO_ID or '❌ Не настроен'}</code>
└ Testnet: {'Да' if testnet else 'Нет'}

💡 Комиссия {COMMISSION_PERCENT}% взимается при открытии сделки и накапливается до порога, затем автовыводится."""
    
    keyboard = []
    if pending_commission >= 1:
        keyboard.append([InlineKeyboardButton(f"💸 Вывести ${pending_commission:.2f}", callback_data="withdraw_commission")])
    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="refresh_commission")])
    
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)


async def optimizer_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View optimizer stats and recommendations (admin)"""
    admin_id = update.effective_user.id
    
    if admin_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    await update.message.reply_text("<b>⏳ Анализируем данные...</b>", parse_mode="HTML")
    
    try:
        # Run analysis
        result = await auto_optimizer.run_analysis()
        
        if result.get('status') == 'insufficient_data':
            await update.message.reply_text(
                f"<b>📊 Оптимизатор</b>\n\n"
                f"Недостаточно данных для анализа.\n"
                f"Сделок: {result.get('trades_analyzed', 0)}\n"
                f"Минимум: 20",
                parse_mode="HTML"
            )
            return
        
        overall = result.get('overall_stats', {})
        recommendations = result.get('recommendations', [])
        
        # Format recommendations
        rec_text = ""
        if recommendations:
            rec_text = "\n\n<b>💡 Рекомендации:</b>\n"
            for i, rec in enumerate(recommendations[:5], 1):
                conf = rec.get('confidence', 0) * 100
                rec_text += f"\n{i}. <b>{rec.get('parameter', 'N/A')}</b>\n"
                rec_text += f"   └ {rec.get('reason', 'N/A')}\n"
                rec_text += f"   └ Уверенность: {conf:.0f}%\n"
        
        # Best/worst symbols
        symbol_analysis = result.get('symbol_analysis', {})
        best_symbols = sorted(
            [(s, d) for s, d in symbol_analysis.items() if d['total_trades'] >= 5],
            key=lambda x: x[1]['win_rate'], reverse=True
        )[:3]
        worst_symbols = sorted(
            [(s, d) for s, d in symbol_analysis.items() if d['total_trades'] >= 5],
            key=lambda x: x[1]['win_rate']
        )[:3]
        
        symbol_text = ""
        if best_symbols:
            symbol_text += "\n\n<b>✅ Лучшие символы:</b>\n"
            for s, d in best_symbols:
                symbol_text += f"   {s}: {d['win_rate']:.0%} ({d['total_trades']} сделок)\n"
        
        if worst_symbols:
            symbol_text += "\n<b>⚠️ Худшие символы:</b>\n"
            for s, d in worst_symbols:
                symbol_text += f"   {s}: {d['win_rate']:.0%} ({d['total_trades']} сделок)\n"
        
        text = f"""<b>📊 Авто-Оптимизатор</b>

<b>Общая статистика (30 дней):</b>
├ Сделок: {overall.get('total_trades', 0)}
├ Win Rate: {overall.get('win_rate', 0):.1%}
└ P&L: ${overall.get('total_pnl', 0):.2f}
{symbol_text}{rec_text}

<i>Анализ обновляется каждые 4 часа</i>"""
        
        await update.message.reply_text(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"[OPTIMIZER] Command error: {e}", exc_info=True)
        await update.message.reply_text(
            f"<b>❌ Ошибка анализа</b>\n\n{str(e)[:100]}",
            parse_mode="HTML"
        )


async def refresh_commission_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновить статус комиссий"""
    query = update.callback_query
    await query.answer()
    
    admin_id = update.effective_user.id
    if admin_id not in ADMIN_IDS:
        return
    
    # Перезагружаем из БД
    load_pending_commission()
    
    # Вызываем commission_cmd логику
    stats = db_get_stats()
    
    crypto_balance = "❓"
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN", "")
    testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
    
    if crypto_token:
        try:
            base_url = "https://testnet-pay.crypt.bot" if testnet else "https://pay.crypt.bot"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{base_url}/api/getBalance",
                    headers={"Crypto-Pay-API-Token": crypto_token}
                ) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        balances = data.get("result", [])
                        usdt = next((b for b in balances if b.get("currency_code") == "USDT"), None)
                        crypto_balance = f"${float(usdt.get('available', 0)):.2f}" if usdt else "$0.00"
        except:
            crypto_balance = "❌"
    
    text = f"""💰 <b>КОМИССИИ</b>

📊 Всего: <b>${stats['commissions']:.2f}</b>
⏳ Накоплено: <b>${pending_commission:.2f}</b>
🎯 Порог: ${COMMISSION_WITHDRAW_THRESHOLD}

CryptoBot: {crypto_balance} | ID: <code>{ADMIN_CRYPTO_ID or '—'}</code>"""
    
    keyboard = []
    if pending_commission >= 1:
        keyboard.append([InlineKeyboardButton(f"💸 Вывести ${pending_commission:.2f}", callback_data="withdraw_commission")])
    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="refresh_commission")])
    
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def withdraw_commission_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback для вывода комиссий"""
    query = update.callback_query
    await query.answer()
    
    admin_id = update.effective_user.id
    if admin_id not in ADMIN_IDS:
        return
    
    amount_to_withdraw = pending_commission
    await query.edit_message_text("<b>⏳ Выводим комиссию...</b>\n\nПроверяем баланс CryptoBot и отправляем перевод...", parse_mode="HTML")
    
    success = await withdraw_commission()
    
    if success:
        # Audit log
        audit_log(admin_id, "WITHDRAW_COMMISSION", f"amount=${amount_to_withdraw:.2f}")
        text = f"""✅ <b>Комиссия выведена!</b>

Сумма: <b>${amount_to_withdraw:.2f}</b>
Получатель: CryptoBot ID {ADMIN_CRYPTO_ID}

Проверьте @CryptoBot"""
        keyboard = [[InlineKeyboardButton("🔙 К статусу", callback_data="refresh_commission")]]
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        text = f"""❌ <b>Ошибка вывода</b>

Возможные причины:
• Недостаточно средств на балансе CryptoBot
• Неверный ADMIN_CRYPTO_ID
• Ошибка API CryptoBot

Проверьте логи сервера для деталей.

<code>/commission</code> - посмотреть статус"""
        keyboard = [[InlineKeyboardButton("🔄 Попробовать снова", callback_data="withdraw_commission")]]
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def test_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест генерации SMART сигнала"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    await update.message.reply_text("<b>⏳ Ищу качественный SMART сетап...</b>", parse_mode="HTML")
    
    try:
        # Получаем баланс для расчёта
        auto_user = get_user(AUTO_TRADE_USER_ID) if AUTO_TRADE_USER_ID else {}
        balance = auto_user.get('balance', 0)
        
        # Ищем лучший сетап через SmartAnalyzer
        setup = await find_best_setup(balance=balance)
        
        if setup:
            quality_name = setup.quality.name
            regime_name = setup.market_regime.name
            reasoning = "\n".join([f"• {r}" for r in setup.reasoning[:3]])
            warnings = "\n".join([f"• {w}" for w in setup.warnings[:2]]) if setup.warnings else "Нет"
            
            text = f"""🧪 <b>SMART TEST: Сетап найден!</b>

<b>{setup.symbol}</b> | {setup.direction}
Качество: {quality_name}
Confidence: {setup.confidence:.0%}
R/R: 1:{setup.risk_reward:.1f}
Режим: {regime_name}

<b>Вход:</b> {format_price(setup.entry)}
<b>TP1:</b> {format_price(setup.take_profit_1)}
<b>TP2:</b> {format_price(setup.take_profit_2)}
<b>TP3:</b> {format_price(setup.take_profit_3)}
<b>SL:</b> {format_price(setup.stop_loss)}

<b>Анализ:</b>
{reasoning}

<b>⚠️ Риски:</b>
{warnings}"""
        else:
            # Получаем статистику отклонений
            stats = get_signal_stats()
            state = get_trading_state()
            
            text = f"""🧪 <b>SMART TEST: Нет качественных сетапов</b>

<b>Причина:</b> Не найдено A+ или A сетапов

<b>Статистика:</b>
Проанализировано: {stats['analyzed']}
Отклонено: {stats['rejected']}

<b>Состояние:</b>
Сделок сегодня: {state['daily_trades']}
Убытков подряд: {state['consecutive_losses']}
На паузе: {'Да' if state['is_paused'] else 'Нет'}

Интервал: 300 сек (5 мин)"""
        
        await update.message.reply_text(text, parse_mode="HTML")
    
    except Exception as e:
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")
    finally:
        await smart.close()


async def whale_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Анализ китов на Hyperliquid: /whale [COIN]"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    if not ADVANCED_FEATURES:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nWhale tracker не загружен", parse_mode="HTML")
        return
    
    coin = context.args[0].upper() if context.args else "BTC"
    
    await update.message.reply_text(f"<b>⏳ Анализирую китов для {coin}...</b>", parse_mode="HTML")
    
    try:
        analysis = await get_combined_whale_analysis(coin)
        
        whale_data = analysis.get('whale_data')
        funding_data = analysis.get('funding_data', {})
        reasoning = analysis.get('reasoning', [])
        
        signal_emoji = "🟢" if analysis.get('direction') == 'LONG' else "🔴" if analysis.get('direction') == 'SHORT' else "⚪"
        
        text = f"""<b>🐋 Whale Analysis: {coin}</b>

{signal_emoji} <b>Сигнал:</b> {analysis.get('direction') or 'Нет'}
<b>Уверенность:</b> {analysis.get('confidence', 0):.0%}

<b>📊 Киты:</b>
• Всего: {whale_data.whale_count if whale_data else 0}
• Объём: ${whale_data.size_usd/1000:.0f}K

<b>💰 Фандинг:</b>
{funding_data.get('reasoning', 'Нет данных')}

<b>📝 Анализ:</b>
"""
        for r in reasoning[:4]:
            text += f"• {r}\n"
        
        await update.message.reply_text(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"[WHALE] Error: {e}")
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")


async def memes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сканер мемкоинов: /memes"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    if not ADVANCED_FEATURES:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nMeme scanner не загружен", parse_mode="HTML")
        return
    
    await update.message.reply_text("<b>⏳ Сканирую мемкоины...</b>", parse_mode="HTML")
    
    try:
        opportunities = await get_meme_opportunities()
        
        if not opportunities:
            await update.message.reply_text("<b>📊 Мем-сканер</b>\n\nНет активных сигналов по мемам", parse_mode="HTML")
            return
        
        text = "<b>🎰 Мем-сканер</b>\n\n"
        
        for opp in opportunities[:5]:
            signal_emoji = "🟢" if opp['signal'] == 'LONG' else "🔴"
            
            text += f"{signal_emoji} <b>{opp['coin']}</b> | {opp['signal']}\n"
            text += f"   💪 Сила: {opp['strength']}/5\n"
            text += f"   📊 RSI: {opp['rsi']:.0f}\n"
            text += f"   📈 1h: {opp['change_1h']:+.1f}%\n"
            text += f"   🔥 Объём: x{opp['volume_spike']:.1f}\n"
            
            for r in opp.get('reasoning', [])[:2]:
                text += f"   • {r}\n"
            text += "\n"
        
        await update.message.reply_text(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"[MEMES] Error: {e}")
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")


async def market_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Анализ рынка: /market"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    if not ADVANCED_FEATURES:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nMarket analyzer не загружен", parse_mode="HTML")
        return
    
    await update.message.reply_text("<b>⏳ Анализирую рынок...</b>", parse_mode="HTML")
    
    try:
        context_data = await get_market_context()
        
        # Fear & Greed emoji
        fg = context_data['fear_greed']
        if fg < 25:
            fg_emoji = "😱"
            fg_text = "Экстремальный страх"
        elif fg < 45:
            fg_emoji = "😰"
            fg_text = "Страх"
        elif fg < 55:
            fg_emoji = "😐"
            fg_text = "Нейтрально"
        elif fg < 75:
            fg_emoji = "😊"
            fg_text = "Жадность"
        else:
            fg_emoji = "🤑"
            fg_text = "Экстремальная жадность"
        
        # Altseason
        alt = context_data['altseason']
        if alt > 70:
            alt_text = "🚀 АЛЬТсезон!"
        elif alt > 50:
            alt_text = "📈 Альты растут"
        elif alt > 30:
            alt_text = "⚖️ Нейтрально"
        else:
            alt_text = "₿ BTC сезон"
        
        text = f"""<b>📊 Рыночный контекст</b>

{fg_emoji} <b>Fear & Greed:</b> {fg} ({fg_text})
₿ <b>BTC Доминация:</b> {context_data['btc_dominance']:.1f}%
📊 <b>ETH/BTC:</b> {context_data['eth_btc']:.4f}

{alt_text}
<b>Altseason Index:</b> {alt}/100

<b>💡 Рекомендации:</b>
"""
        for rec in context_data.get('recommendation', []):
            text += f"• {rec}\n"
        
        text += f"\n<b>🎯 Лучшая категория:</b> {context_data.get('best_category', 'layer1').upper()}"
        
        await update.message.reply_text(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"[MARKET] Error: {e}")
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")


async def news_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Анализ новостей и Twitter: /news [COIN]
    Показывает текущий сентимент, новости, сигналы от трейдеров
    """
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    if not NEWS_FEATURES:
        await update.message.reply_text(
            "<b>❌ News Analyzer не загружен</b>\n\n"
            "Проверьте файл news_analyzer.py",
            parse_mode="HTML"
        )
        return
    
    await update.message.reply_text("<b>⏳ Анализирую новости и Twitter...</b>", parse_mode="HTML")
    
    try:
        # Получаем данные параллельно
        sentiment_task = get_market_sentiment()
        signals_task = get_news_signals()
        manipulations_task = detect_manipulations()
        events_task = get_upcoming_events()
        
        results = await asyncio.gather(
            sentiment_task, signals_task, manipulations_task, events_task,
            return_exceptions=True
        )
        
        sentiment = results[0] if not isinstance(results[0], Exception) else {}
        signals = results[1] if not isinstance(results[1], Exception) else []
        manipulations = results[2] if not isinstance(results[2], Exception) else []
        events = results[3] if not isinstance(results[3], Exception) else []
        
        # === ФОРМИРУЕМ СООБЩЕНИЕ ===
        text = "<b>📰 News & Twitter Analyzer</b>\n\n"
        
        # Сентимент
        score = sentiment.get('score', 0)
        trend = sentiment.get('trend', 'NEUTRAL')
        
        if score > 30:
            sent_emoji = "🟢"
        elif score < -30:
            sent_emoji = "🔴"
        else:
            sent_emoji = "⚪"
        
        text += f"{sent_emoji} <b>Сентимент рынка:</b> {score:.0f}/100 ({trend})\n\n"
        
        # Предупреждения о манипуляциях
        if manipulations:
            text += "<b>⚠️ ПРЕДУПРЕЖДЕНИЯ:</b>\n"
            for m in manipulations[:3]:
                text += f"• {m['type']}: {m['description']}\n"
            text += "\n"
        
        # Предстоящие макро-события
        if events:
            text += "<b>📅 Макро-события:</b>\n"
            for event in events[:3]:
                text += f"• {event.name}: {event.description}\n"
                text += f"  ⏰ {event.scheduled_time.strftime('%H:%M UTC')}\n"
            text += "\n"
        
        # Торговые сигналы от новостей
        if signals:
            text += "<b>📈 Сигналы от новостей:</b>\n"
            for signal in signals[:5]:
                dir_emoji = "🟢" if signal.direction == 'LONG' else "🔴"
                impact = "⚡" * min(3, signal.impact.value - 2)
                
                text += f"\n{dir_emoji} <b>{signal.direction}</b> {', '.join(signal.affected_coins[:3])} {impact}\n"
                text += f"   📊 Уверенность: {signal.confidence:.0%}\n"
                text += f"   📰 {signal.source}\n"
                
                for reason in signal.reasoning[:2]:
                    text += f"   • {reason[:50]}\n"
        else:
            text += "<b>📊 Сигналов от новостей нет</b>\n"
            text += "Twitter и новостные ленты спокойны\n"
        
        # Мониторимые аккаунты
        text += "\n<b>👀 Мониторинг Twitter:</b>\n"
        text += "Trump, SEC, Fed, Top Traders, Binance...\n"
        
        await update.message.reply_text(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"[NEWS] Error in news_cmd: {e}")
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")


async def signal_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Статистика генерации сигналов: /signalstats [reset]"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    args = context.args
    
    if args and args[0].lower() == "reset":
        reset_signal_stats()
        await update.message.reply_text("<b>✅ Статистика сброшена</b>", parse_mode="HTML")
        return
    
    stats = get_signal_stats()
    
    total = stats['analyzed']
    accepted = stats['accepted']
    rejected = stats['rejected']
    rate = (accepted / total * 100) if total > 0 else 0
    
    reasons = stats['reasons']
    
    # Сортируем причины по количеству
    sorted_reasons = sorted(reasons.items(), key=lambda x: x[1], reverse=True)
    
    reasons_text = ""
    for reason, count in sorted_reasons:
        if count > 0:
            reason_name = {
                'low_confidence': '🎯 Низкая уверенность',
                'low_quality': '⭐ Низкое качество',
                'bad_rr': '📊 Плохой R/R',
                'bad_regime': '🌊 Плохой режим рынка',
                'no_setup': '❌ Нет сетапа',
                'state_blocked': '⏸️ Пауза/лимит',
                'outside_hours': '🕐 Вне торговых часов',
                'liquidity_zone': '💧 Зона ликвидности',
            }.get(reason, reason)
            reasons_text += f"• {reason_name}: {count}\n"
    
    if not reasons_text:
        reasons_text = "Нет отклонений\n"
    
    bybit_opened = stats.get('bybit_opened', 0)
    bybit_rate = (bybit_opened / accepted * 100) if accepted > 0 else 0
    
    # Новые метрики дисбаланса
    extreme_moves = stats.get('extreme_moves_detected', 0)
    imbalance_trades = stats.get('imbalance_trades', 0)
    
    # Smart Analyzer State
    smart_state = get_trading_state()
    
    pause_info = ""
    if smart_state['is_paused']:
        pause_info = f"⏸️ Пауза до: {smart_state['pause_until']}\n"
    
    text = f"""<b>📊 Статистика SMART сигналов</b>

Проанализировано: {total}
✅ Принято: {accepted}
❌ Отклонено: {rejected}
📈 Конверсия: {rate:.1f}%

🔗 Было на Bybit: {bybit_opened} ({bybit_rate:.0f}%)

<b>🔥 Дисбаланс/Экстремумы:</b>
Экстрем. движения: {extreme_moves}
Дисбаланс-сделки: {imbalance_trades}

<b>Причины отклонения:</b>
{reasons_text}

<b>🤖 Smart Trading State:</b>
Сделок сегодня: {smart_state['daily_trades']}
PnL сегодня: ${smart_state['daily_pnl']:.2f}
Убытков подряд: {smart_state['consecutive_losses']}
{pause_info}
Сброс: /signalstats reset"""
    
    await update.message.reply_text(text, parse_mode="HTML")


async def setbanner_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Установить баннер: /setbanner menu|deposit|autotrade|payment (ответом на фото)"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    args = context.args
    valid_types = ["menu", "deposit", "autotrade", "payment"]
    
    # Проверяем есть ли тип баннера
    if not args or args[0] not in valid_types:
        # Показываем текущие баннеры
        status_lines = []
        for bt in valid_types:
            banner_id = get_banner(bt)
            status = f"✅ <code>{banner_id[:25]}...</code>" if banner_id else "❌ Не установлен"
            status_lines.append(f"• <b>{bt}</b>: {status}")
        
        await update.message.reply_text(
            f"""<b>🖼 Баннеры меню</b>

{chr(10).join(status_lines)}

<b>Как установить:</b>
1. Ответь на фото командой:
   <code>/setbanner menu</code> — главное меню
   <code>/setbanner deposit</code> — пополнение
   <code>/setbanner autotrade</code> — авто-трейд
   <code>/setbanner payment</code> — успешная оплата""",
            parse_mode="HTML"
        )
        return
    
    banner_type = args[0]
    
    # Проверяем есть ли фото в ответе
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        file_id = update.message.reply_to_message.photo[-1].file_id
        set_banner(banner_type, file_id)
        
        await update.message.reply_text(
            f"<b>✅ Баннер '{banner_type}' установлен!</b>",
            parse_mode="HTML"
        )
    elif update.message.photo:
        # Фото в том же сообщении (с caption)
        file_id = update.message.photo[-1].file_id
        set_banner(banner_type, file_id)
        
        await update.message.reply_text(
            f"<b>✅ Баннер '{banner_type}' установлен!</b>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"<b>❌ Ответь на фото</b>\n\nОтветь на фото командой /setbanner {banner_type}",
            parse_mode="HTML"
        )


async def autotrade_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Управление авто-торговлей: /autotrade [on|off|status|balance AMOUNT]"""
    global AUTO_TRADE_ENABLED
    
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    args = context.args
    
    if not args:
        # Показать статус
        auto_user = get_user(AUTO_TRADE_USER_ID) if AUTO_TRADE_USER_ID else None
        balance = auto_user['balance'] if auto_user else 0
        positions = get_positions(AUTO_TRADE_USER_ID) if AUTO_TRADE_USER_ID else []
        
        status = "✅ ВКЛ" if AUTO_TRADE_ENABLED else "❌ ВЫКЛ"
        
        text = f"""🤖 <b>АВТО-ТОРГОВЛЯ</b>

Статус: {status}
User ID: {AUTO_TRADE_USER_ID}
💰 Баланс: ${balance:.2f}
Открытых позиций: {len(positions)}

Настройки:
• Мин. ставка: ${AUTO_TRADE_MIN_BET}
• Макс. ставка: ${AUTO_TRADE_MAX_BET}
• Плечо: x10-x25 (по уверенности)

Команды:
/autotrade on — включить
/autotrade off — выключить
/autotrade balance 1500 — установить баланс
/autotrade clear — очистить позиции и историю"""
        
        await update.message.reply_text(text, parse_mode="HTML")
        return
    
    cmd = args[0].lower()
    
    if cmd == "on":
        AUTO_TRADE_ENABLED = True
        audit_log(user_id, "AUTO_TRADE_TOGGLE", "enabled=True")
        await update.message.reply_text("<b>✅ Авто-торговля включена</b>", parse_mode="HTML")
    elif cmd == "off":
        AUTO_TRADE_ENABLED = False
        audit_log(user_id, "AUTO_TRADE_TOGGLE", "enabled=False")
        await update.message.reply_text("<b>❌ Авто-торговля выключена</b>", parse_mode="HTML")
    elif cmd == "balance" and len(args) > 1:
        try:
            new_balance = float(args[1])
            run_sql("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, AUTO_TRADE_USER_ID))
            # Обновляем кэш
            if AUTO_TRADE_USER_ID in users_cache:
                users_cache[AUTO_TRADE_USER_ID]['balance'] = new_balance
            audit_log(user_id, "SET_AUTO_TRADE_BALANCE", f"balance=${new_balance:.0f}", target_user=AUTO_TRADE_USER_ID)
            await update.message.reply_text(f"<b>✅ Баланс установлен</b>\n\n<b>${new_balance:.2f}</b>", parse_mode="HTML")
        except ValueError:
            await update.message.reply_text("<b>❌ Ошибка</b>\n\nНеверная сумма", parse_mode="HTML")
    elif cmd == "clear":
        # Очистить ВСЕ данные во всей БД
        run_sql("DELETE FROM positions")
        run_sql("DELETE FROM history")
        run_sql("DELETE FROM alerts")
        
        # Сбрасываем статистику всех пользователей
        run_sql("UPDATE users SET total_profit = 0, auto_trade_today = 0")
        
        # Очищаем весь кэш
        positions_cache.clear()
        users_cache.clear()
        
        audit_log(user_id, "CLEAR_DATABASE", "Cleared all positions, history, alerts, stats")
        await update.message.reply_text("<b>✅ База данных очищена</b>\n\n• Позиции\n• История\n• Алерты\n• Статистика", parse_mode="HTML")
        logger.info(f"[ADMIN] User {user_id} cleared ALL database")
    else:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nНеизвестная команда. Используй: on, off, balance AMOUNT, clear", parse_mode="HTML")

@rate_limit(max_requests=10, window_seconds=60, action_type="health_check")
async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Health check endpoint for monitoring"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    from hedger import hedger
    import time
    
    start_time = time.time()
    status_lines = ["<b>🏥 Health Check</b>\n"]
    all_ok = True
    
    # 1. Database check
    try:
        db_start = time.time()
        row = run_sql("SELECT COUNT(*) as cnt FROM users", fetch="one")
        db_time = (time.time() - db_start) * 1000
        user_count = row['cnt'] if row else 0
        status_lines.append(f"✅ DB: {user_count} users ({db_time:.0f}ms)")
    except Exception as e:
        status_lines.append(f"❌ DB: {str(e)[:50]}")
        all_ok = False
    
    # 2. Binance API check
    try:
        api_start = time.time()
        price = await get_real_price("BTC/USDT")
        api_time = (time.time() - api_start) * 1000
        if price:
            status_lines.append(f"✅ Binance: BTC ${price:,.0f} ({api_time:.0f}ms)")
        else:
            status_lines.append("⚠️ Binance: No price")
            all_ok = False
    except Exception as e:
        status_lines.append(f"❌ Binance: {str(e)[:50]}")
        all_ok = False
    
    # 3. Bybit check
    try:
        bybit_balance = await hedger.get_balance()
        hedging_on = await is_hedging_enabled()
        if bybit_balance is not None:
            status_lines.append(f"✅ Bybit: ${bybit_balance:.2f} (hedge: {'ON' if hedging_on else 'OFF'})")
        else:
            status_lines.append("⚠️ Bybit: No balance")
    except Exception as e:
        status_lines.append(f"❌ Bybit: {str(e)[:50]}")
        all_ok = False
    
    # 4. Cache stats
    users_in_cache = len(users_cache)
    positions_in_cache = sum(len(p) for p in positions_cache.values())
    status_lines.append(f"📊 Cache: {users_in_cache} users, {positions_in_cache} positions")
    
    # 5. Commission status
    status_lines.append(f"💰 Pending commission: ${pending_commission:.2f}")
    
    # 6. Memory usage (basic)
    try:
        import sys
        cache_size = sys.getsizeof(users_cache) + sys.getsizeof(positions_cache)
        status_lines.append(f"🧠 Cache size: ~{cache_size / 1024:.1f}KB")
    except:
        pass
    
    # Total time
    total_time = (time.time() - start_time) * 1000
    status_lines.append(f"\n⏱ Total: {total_time:.0f}ms")
    status_lines.append(f"{'✅ ALL OK' if all_ok else '⚠️ ISSUES DETECTED'}")
    
    await update.message.reply_text("\n".join(status_lines), parse_mode="HTML")

async def test_bybit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест подключения к Bybit"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    from hedger import hedger
    
    await update.message.reply_text("<b>⏳ Проверяю Bybit...</b>", parse_mode="HTML")
    
    # Проверка настроек
    api_key = os.getenv("BYBIT_API_KEY", "")
    demo_mode = os.getenv("BYBIT_DEMO", "").lower() in ("true", "1", "yes")
    testnet = os.getenv("BYBIT_TESTNET", "").lower() in ("true", "1", "yes")
    
    status = []
    status.append(f"API Key: {'✅ Есть' if api_key else '❌ Нет'}")
    status.append(f"Demo Mode: {'✅ Вкл' if demo_mode else '❌ Выкл'}")
    status.append(f"Testnet: {'✅ Вкл' if testnet else '❌ Выкл'}")
    status.append(f"URL: {hedger.base_url}")
    status.append(f"Enabled: {'✅' if hedger.enabled else '❌'}")
    
    # Тест баланса (raw request для диагностики)
    try:
        import aiohttp
        timestamp = str(int(datetime.now().timestamp() * 1000))
        recv_window = "5000"
        params = {"accountType": "UNIFIED"}
        params_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        
        sign_str = f"{timestamp}{api_key}{recv_window}{params_str}"
        api_secret = os.getenv("BYBIT_API_SECRET", "")
        
        import hmac, hashlib
        signature = hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
        
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": recv_window
        }
        if demo_mode:
            headers["X-BAPI-DEMO-TRADING"] = "true"
        
        url = f"{hedger.base_url}/v5/account/wallet-balance?accountType=UNIFIED"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
                status.append(f"\n📦 Raw: {str(data)[:200]}")
                
                ret_code = data.get("retCode") if data else None
                ret_msg = data.get("retMsg") if data else "No response"
                
                if ret_code == 0:
                    result = data.get("result", {})
                    coin_list = result.get("list", [])
                    if coin_list and len(coin_list) > 0:
                        coins = coin_list[0].get("coin", [])
                        for coin in coins:
                            if coin.get("coin") == "USDT":
                                status.append(f"💰 Баланс USDT: ${float(coin.get('walletBalance', 0)):,.2f}")
                                break
                        else:
                            status.append(f"⚠️ USDT не найден")
                    else:
                        status.append(f"⚠️ Список пуст: {result}")
                else:
                    status.append(f"❌ Bybit: {ret_msg} (code: {ret_code})")
    except Exception as e:
        status.append(f"\n❌ Ошибка: {e}")
    
    # Тест цены
    try:
        price = await hedger.get_price("BTC/USDT")
        if price:
            status.append(f"📊 BTC цена: ${price:,.2f}")
        else:
            status.append(f"❌ Не удалось получить цену")
    except Exception as e:
        status.append(f"❌ Ошибка цены: {e}")
    
    await update.message.reply_text("<b>🔧 BYBIT TEST</b>\n\n" + "\n".join(status), parse_mode="HTML")

async def test_hedge(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест открытия/закрытия хеджа"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    await update.message.reply_text("<b>⏳ Тестирую хеджирование на BTC...</b>", parse_mode="HTML")
    
    # Пробуем открыть минимальную позицию
    result = await hedge_open(999999, "BTC/USDT", "LONG", 10.0)
    
    if result:
        qty = result.get('qty', 0)
        await update.message.reply_text(f"<b>✅ Хедж открыт</b>\n\nOrder ID: {result.get('order_id')}\nQty: {qty}\n\n⏳ Закрываю через 5 сек...", parse_mode="HTML")
        await asyncio.sleep(5)
        # Тест: закрываем используя qty из открытия
        close_result = await hedge_close(999999, "BTC/USDT", "LONG", qty if qty > 0 else None)
        if close_result:
            await update.message.reply_text("<b>✅ Хедж закрыт</b>", parse_mode="HTML")
        else:
            await update.message.reply_text("<b>❌ Ошибка закрытия</b>", parse_mode="HTML")
    else:
        await update.message.reply_text("<b>❌ Ошибка</b>\n\nНе удалось открыть хедж. Проверь логи Railway.", parse_mode="HTML")

@rate_limit(max_requests=5, window_seconds=300, action_type="admin_broadcast")
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Рассылка всем пользователям"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    if not context.args:
        await update.message.reply_text("<b>📋 Использование</b>\n\n/broadcast <сообщение>", parse_mode="HTML")
        return
    
    message = " ".join(context.args)
    
    rows = run_sql("SELECT user_id FROM users", fetch="all")
    all_users = [row['user_id'] for row in rows] if rows else []
    
    sent = 0
    failed = 0
    
    for uid in all_users:
        try:
            await context.bot.send_message(uid, f"<b>📢 Рассылка</b>\n\n{message}", parse_mode="HTML")
            sent += 1
        except:
            failed += 1
    
            await update.message.reply_text(f"<b>📢 Рассылка</b>\n\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}", parse_mode="HTML")

async def reset_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сброс: закрыть все позиции и установить баланс"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return
    
    # /reset [user_id] [balance] или /reset [balance]
    if not context.args:
        await update.message.reply_text("<b>📋 Использование</b>\n\n/reset 1500 — себе\n/reset 123456 1500 — юзеру\n/reset all 0 — всем закрыть позиции", parse_mode="HTML")
        return
    
    try:
        if context.args[0].lower() == "all":
            # Закрыть все позиции у всех
            run_sql("DELETE FROM positions")
            positions_cache.clear()
            await update.message.reply_text("<b>✅ Все позиции закрыты</b>\n\nУ всех пользователей", parse_mode="HTML")
            return
        
        if len(context.args) == 1:
            target_id = user_id
            balance = float(context.args[0])
        else:
            target_id = int(context.args[0])
            balance = float(context.args[1])
        
        # Закрыть позиции пользователя
        run_sql("DELETE FROM positions WHERE user_id = ?", (target_id,))
        positions_cache.set(target_id, [])
        
        # Установить баланс
        db_update_user(target_id, balance=balance)
        if target_id in users_cache:
            users_cache[target_id]['balance'] = balance
        
        await update.message.reply_text(f"<b>✅ Готово</b>\n\n👤 User: {target_id}\n💰 Баланс: <b>${balance:.2f}</b>\n📊 Позиции: закрыты", parse_mode="HTML")
        
    except (ValueError, IndexError) as e:
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")

@rate_limit(max_requests=1, window_seconds=600, action_type="admin_reset_all")
async def reset_everything(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Полный сброс ВСЕХ данных: пользователи, позиции, история, кэши"""
    user_id = update.effective_user.id

    if user_id not in ADMIN_IDS:
        await update.message.reply_text("<b>⛔ Доступ закрыт</b>", parse_mode="HTML")
        return

    # Требуем подтверждение
    if not context.args or context.args[0].lower() != "confirm":
        await update.message.reply_text(
            "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
            "Эта команда удалит ВСЁ:\n"
            "• Всех пользователей\n"
            "• Все позиции\n"
            "• Всю историю сделок\n"
            "• Все алерты\n"
            "• Все кэши\n\n"
            "Для подтверждения напишите:\n"
            "<code>/resetall confirm</code>",
            parse_mode="HTML"
        )
        return

    try:
        # Сначала закрываем все открытые позиции на Bybit
        hedging_enabled = await is_hedging_enabled()
        closed_count = 0
        failed_count = 0
        
        if hedging_enabled:
            await update.message.reply_text("<b>⏳ Закрываю позиции на Bybit...</b>", parse_mode="HTML")
            
            # Получаем все позиции из БД
            all_positions = run_sql("SELECT * FROM positions", fetch="all")
            
            for pos in all_positions:
                bybit_qty = pos.get('bybit_qty', 0) or 0
                if bybit_qty > 0:
                    try:
                        symbol = pos['symbol']
                        direction = pos['direction']
                        hedge_result = await hedge_close(pos['id'], symbol, direction, bybit_qty)
                        if hedge_result:
                            closed_count += 1
                            logger.info(f"[RESET] Closed position {pos['id']} on Bybit: {symbol} {direction} qty={bybit_qty}")
                        else:
                            failed_count += 1
                            logger.warning(f"[RESET] Failed to close position {pos['id']} on Bybit")
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"[RESET] Error closing position {pos.get('id')}: {e}")
        
        # Очищаем все таблицы (важен порядок из-за foreign key constraints)
        # Сначала удаляем зависимые таблицы, потом основные
        run_sql("DELETE FROM alerts")  # Зависит от users
        run_sql("DELETE FROM positions")  # Зависит от users
        run_sql("DELETE FROM history")  # Зависит от users
        run_sql("DELETE FROM users")  # Основная таблица
        
        # Очищаем кэши
        positions_cache.clear()
        users_cache.clear()
        
        # Сбрасываем статистику сигналов (уже импортировано из smart_analyzer)
        reset_signal_stats()
        
        # Формируем сообщение
        message_parts = ["✅ <b>Полный сброс выполнен!</b>\n\n"]
        
        if hedging_enabled and (closed_count > 0 or failed_count > 0):
            message_parts.append("📡 <b>Bybit:</b>\n")
            if closed_count > 0:
                message_parts.append(f"✅ Закрыто позиций: {closed_count}\n")
            if failed_count > 0:
                message_parts.append(f"❌ Ошибок закрытия: {failed_count}\n")
            message_parts.append("\n")
        
        message_parts.append("🗑 <b>Удалено:</b>\n")
        message_parts.append("• Все пользователи\n")
        message_parts.append("• Все позиции\n")
        message_parts.append("• Вся история сделок\n")
        message_parts.append("• Все алерты\n")
        message_parts.append("• Все кэши\n")
        message_parts.append("• Статистика сигналов\n\n")
        message_parts.append("Бот готов к работе с нуля.")
        
        await update.message.reply_text("".join(message_parts), parse_mode="HTML")
        logger.info(f"[ADMIN] Full reset executed by user {user_id} (Bybit: {closed_count} closed, {failed_count} failed)")

    except Exception as e:
        await update.message.reply_text(f"<b>❌ Ошибка</b>\n\n{e}", parse_mode="HTML")
        logger.error(f"[ADMIN] Reset error: {e}")

# ==================== ДИАГНОСТИКА БАЛАНСА ====================
async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Диагностика баланса - показывает данные из кэша и БД"""
    user_id = update.effective_user.id
    
    # Данные из кэша
    cached_user = users_cache.get(user_id)
    cached_balance = cached_user.get('balance', 'N/A') if cached_user else 'Нет в кэше'
    cached_deposit = cached_user.get('total_deposit', 'N/A') if cached_user else 'N/A'
    
    # Данные напрямую из БД
    db_row = run_sql("SELECT balance, total_deposit, total_profit FROM users WHERE user_id = ?", (user_id,), fetch="one")
    db_balance = db_row['balance'] if db_row else 'Не найден'
    db_deposit = db_row['total_deposit'] if db_row else 'N/A'
    db_profit = db_row['total_profit'] if db_row else 'N/A'
    
    # Позиции
    cached_positions = positions_cache.get(user_id, [])
    db_positions = db_get_positions(user_id)
    
    positions_value = sum(p.get('amount', 0) for p in db_positions)
    
    text = f"""<b>🔍 Диагностика баланса</b>

<b>Кэш:</b>
├ Баланс: ${cached_balance if isinstance(cached_balance, (int, float)) else cached_balance}
├ Депозит: ${cached_deposit if isinstance(cached_deposit, (int, float)) else cached_deposit}

<b>База данных:</b>
├ Баланс: ${db_balance if isinstance(db_balance, (int, float)) else db_balance}
├ Депозит: ${db_deposit if isinstance(db_deposit, (int, float)) else db_deposit}
├ Профит: ${db_profit if isinstance(db_profit, (int, float)) else db_profit}

<b>Позиции:</b>
├ В кэше: {len(cached_positions)}
├ В БД: {len(db_positions)}
├ Заморожено в позициях: ${positions_value:.2f}

<i>Если баланс в кэше и БД отличается - нажмите /start для синхронизации</i>"""
    
    await update.message.reply_text(text, parse_mode="HTML")
    
    # Логируем для отладки
    logger.info(f"[DIAG] User {user_id}: cache_balance={cached_balance}, db_balance={db_balance}, positions={len(db_positions)}, frozen=${positions_value:.2f}")

# ==================== РЕФЕРАЛЬНАЯ КОМАНДА ====================
async def referral_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Реферальная ссылка"""
    user_id = update.effective_user.id
    bot_username = (await context.bot.get_me()).username
    
    # Получаем полную статистику
    stats = db_get_referrals_stats(user_id)
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    
    # Формируем детальную информацию
    levels_info = ""
    if REFERRAL_COMMISSION_LEVELS:
        levels_info = "\n<b>📊 Уровни и заработок:</b>\n"
        for level, percent in enumerate(REFERRAL_COMMISSION_LEVELS, 1):
            count_key = f'level{level}_count'
            earnings_key = f'earnings_level{level}'
            count = stats.get(count_key, 0)
            earned = stats.get(earnings_key, 0.0)
            levels_info += f"├ Уровень {level}: {percent}% • {count} чел • <b>${earned:.2f}</b>\n"
    
    # Формируем информацию о бонусах за депозит
    deposit_bonus_info = ""
    if REFERRAL_BONUS_LEVELS:
        deposit_bonus_info = "├ За депозит реферала: "
        bonus_parts = [f"${b:.0f}" for b in REFERRAL_BONUS_LEVELS]
        deposit_bonus_info += " / ".join(bonus_parts) + " (по уровням)\n"
    
    text = f"""<b>🤝 Реферальная программа</b>

<b>💎 Вознаграждения:</b>
{deposit_bonus_info}└ С комиссий сделок: {REFERRAL_COMMISSION_LEVELS[0]}% / {REFERRAL_COMMISSION_LEVELS[1]}% / {REFERRAL_COMMISSION_LEVELS[2]}%
{levels_info}
<b>📈 Итого:</b>
├ Рефералов всего: <b>{stats['total_count']}</b>
├ Депозиты рефералов: <b>${stats['referral_deposits']:.2f}</b>
└ 💰 Заработано: <b>${stats['total_earned']:.2f}</b>

🔗 <b>Твоя ссылка:</b>
<code>{ref_link}</code>"""
    
    keyboard = [[InlineKeyboardButton("👥 Мои рефералы", callback_data="my_referrals")]]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# ==================== МЕНЮ "ДОПОЛНИТЕЛЬНО" ====================
async def more_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Меню дополнительных функций"""
    query = update.callback_query
    await query.answer()
    
    text = "<b>Дополнительно</b>\n\nВыбери раздел:"
    
    keyboard = [
        [InlineKeyboardButton("🤝 Рефералка", callback_data="referral_menu")],
        [InlineKeyboardButton("📜 История", callback_data="history_menu")],
        [InlineKeyboardButton("💸 Вывод", callback_data="withdraw_menu")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def referral_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать реферальную программу через меню"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    bot_username = (await context.bot.get_me()).username
    
    # Получаем полную статистику
    stats = db_get_referrals_stats(user_id)
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    
    # Формируем детальную информацию
    levels_info = ""
    if REFERRAL_COMMISSION_LEVELS:
        levels_info = "\n<b>📊 Уровни и заработок:</b>\n"
        for level, percent in enumerate(REFERRAL_COMMISSION_LEVELS, 1):
            count_key = f'level{level}_count'
            earnings_key = f'earnings_level{level}'
            count = stats.get(count_key, 0)
            earned = stats.get(earnings_key, 0.0)
            levels_info += f"├ Уровень {level}: {percent}% • {count} чел • <b>${earned:.2f}</b>\n"
    
    # Формируем информацию о бонусах за депозит
    deposit_bonus_info = ""
    if REFERRAL_BONUS_LEVELS:
        deposit_bonus_info = "├ За депозит реферала: "
        bonus_parts = [f"${b:.0f}" for b in REFERRAL_BONUS_LEVELS]
        deposit_bonus_info += " / ".join(bonus_parts) + " (по уровням)\n"
    
    text = f"""<b>🤝 Реферальная программа</b>

<b>💎 Вознаграждения:</b>
{deposit_bonus_info}└ С комиссий сделок: {REFERRAL_COMMISSION_LEVELS[0]}% / {REFERRAL_COMMISSION_LEVELS[1]}% / {REFERRAL_COMMISSION_LEVELS[2]}%
{levels_info}
<b>📈 Итого:</b>
├ Рефералов всего: <b>{stats['total_count']}</b>
├ Депозиты рефералов: <b>${stats['referral_deposits']:.2f}</b>
└ 💰 Заработано: <b>${stats['total_earned']:.2f}</b>

🔗 <b>Твоя ссылка:</b>
<code>{ref_link}</code>"""
    
    keyboard = [
        [InlineKeyboardButton("👥 Мои рефералы", callback_data="my_referrals")],
        [InlineKeyboardButton("🔙 Назад", callback_data="more_menu")]
    ]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def my_referrals_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать список рефералов пользователя"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    referrals = db_get_referrals_list(user_id, level=1)
    
    if not referrals:
        text = "<b>👥 Мои рефералы</b>\n\nУ вас пока нет рефералов.\n\nПоделитесь своей ссылкой с друзьями!"
    else:
        text = f"<b>👥 Мои рефералы ({len(referrals)})</b>\n\n"
        
        for i, ref in enumerate(referrals[:15], 1):  # Показываем максимум 15
            ref_id = ref.get('user_id', 0)
            deposit = ref.get('total_deposit', 0) or 0
            earned = ref.get('earned', 0) or 0
            
            # Маскируем ID для приватности
            masked_id = f"{str(ref_id)[:3]}***{str(ref_id)[-2:]}" if len(str(ref_id)) > 5 else f"***{str(ref_id)[-3:]}"
            
            status = "✅" if deposit > 0 else "⏳"
            text += f"{status} <code>{masked_id}</code> • Депозит: ${deposit:.0f} • Доход: ${earned:.2f}\n"
        
        if len(referrals) > 15:
            text += f"\n<i>...и ещё {len(referrals) - 15} рефералов</i>"
        
        # Итого
        total_deposit = sum(r.get('total_deposit', 0) or 0 for r in referrals)
        total_earned = sum(r.get('earned', 0) or 0 for r in referrals)
        text += f"\n\n<b>Итого депозитов:</b> ${total_deposit:.2f}\n<b>Ваш доход:</b> ${total_earned:.2f}"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="referral_menu")]]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

async def history_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать историю сделок через меню"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    trades = db_get_history(user_id, limit=10)
    
    if not trades:
        text = "<b>📜 История пуста</b>"
    else:
        text = "<b>📜 История сделок</b>\n\n"
        for t in trades:
            pnl_str = f"+${t['pnl']:.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):.2f}"
            ticker = t['symbol'].split("/")[0] if "/" in t['symbol'] else t['symbol']
            text += f"{ticker} {t['direction']} | <b>{pnl_str}</b> | {t['reason']}\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="more_menu")]]
    
    await edit_or_send(query, text, InlineKeyboardMarkup(keyboard))

# ==================== ИСТОРИЯ СДЕЛОК ====================
async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История сделок пользователя"""
    user_id = update.effective_user.id
    trades = db_get_history(user_id, limit=10)
    
    if not trades:
        await update.message.reply_text("<b>📜 История пуста</b>", parse_mode="HTML")
        return
    
    text = "<b>📜 История сделок</b>\n\n"
    for t in trades:
        pnl_str = f"+${t['pnl']:.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):.2f}"
        ticker = t['symbol'].split("/")[0] if "/" in t['symbol'] else t['symbol']
        pnl_indicator = "+" if t['pnl'] >= 0 else "-"
        text += f"{ticker} {t['direction']} | <b>{pnl_str}</b> | {t['reason']}\n"
    
    await update.message.reply_text(text, parse_mode="HTML")

# ==================== MAIN ====================
def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN not set")
        return
    
    # Load persistent data from DB
    load_pending_commission()
    load_pending_invoices()
    
    app = Application.builder().token(token).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("logs", logs_cmd))
    app.add_handler(CommandHandler("health", health_check))
    app.add_handler(CommandHandler("commission", commission_cmd))
    app.add_handler(CommandHandler("optimizer", optimizer_cmd))
    app.add_handler(CommandHandler("syncprofits", sync_profits_cmd))
    app.add_handler(CommandHandler("testbybit", test_bybit))
    app.add_handler(CommandHandler("testhedge", test_hedge))
    app.add_handler(CommandHandler("testsignal", test_signal))
    app.add_handler(CommandHandler("signalstats", signal_stats_cmd))
    app.add_handler(CommandHandler("whale", whale_cmd))
    app.add_handler(CommandHandler("memes", memes_cmd))
    app.add_handler(CommandHandler("market", market_cmd))
    app.add_handler(CommandHandler("news", news_cmd))  # News analyzer
    app.add_handler(CommandHandler("autotrade", autotrade_cmd))
    app.add_handler(CommandHandler("setbanner", setbanner_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("reset", reset_all))
    app.add_handler(CommandHandler("resetall", reset_everything))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("ref", referral_cmd))
    app.add_handler(CommandHandler("balance", balance_cmd))  # Диагностика баланса
    
    # Оплата Stars
    app.add_handler(PreCheckoutQueryHandler(precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    
    # Callbacks
    app.add_handler(CallbackQueryHandler(toggle_trading, pattern="^toggle$"))
    app.add_handler(CallbackQueryHandler(auto_trade_menu, pattern="^auto_trade_menu$"))
    app.add_handler(CallbackQueryHandler(auto_trade_toggle, pattern="^auto_trade_toggle$"))
    app.add_handler(CallbackQueryHandler(auto_trade_daily_menu, pattern="^auto_trade_daily_menu$"))
    app.add_handler(CallbackQueryHandler(auto_trade_set_daily, pattern="^auto_daily_"))
    app.add_handler(CallbackQueryHandler(auto_trade_winrate_menu, pattern="^auto_trade_winrate_menu$"))
    app.add_handler(CallbackQueryHandler(auto_trade_set_winrate, pattern="^auto_wr_"))
    app.add_handler(CallbackQueryHandler(close_symbol_trades, pattern="^close_symbol\\|"))
    app.add_handler(CallbackQueryHandler(deposit_menu, pattern="^deposit$"))
    app.add_handler(CallbackQueryHandler(pay_stars_menu, pattern="^pay_stars$"))
    app.add_handler(CallbackQueryHandler(stars_custom_amount, pattern="^stars_custom$"))
    app.add_handler(CallbackQueryHandler(send_stars_invoice, pattern="^stars_\\d+$"))
    app.add_handler(CallbackQueryHandler(pay_crypto_menu, pattern="^pay_crypto$"))
    app.add_handler(CallbackQueryHandler(crypto_custom_amount, pattern="^crypto_custom$"))
    app.add_handler(CallbackQueryHandler(create_crypto_invoice, pattern="^crypto_\\d+$"))
    app.add_handler(CallbackQueryHandler(check_crypto_payment, pattern="^check_"))
    app.add_handler(CallbackQueryHandler(show_trades, pattern="^(trades|my_positions|refresh_positions)$"))
    app.add_handler(CallbackQueryHandler(enter_trade, pattern="^e\\|"))
    app.add_handler(CallbackQueryHandler(custom_amount_prompt, pattern="^custom\\|"))
    app.add_handler(CallbackQueryHandler(close_all_trades, pattern="^close_all$"))
    app.add_handler(CallbackQueryHandler(close_stacked_trades, pattern="^closestack_"))
    app.add_handler(CallbackQueryHandler(close_trade, pattern="^close_\\d+$"))
    app.add_handler(CallbackQueryHandler(skip_signal, pattern="^skip$"))
    app.add_handler(CallbackQueryHandler(withdraw_commission_callback, pattern="^withdraw_commission$"))
    app.add_handler(CallbackQueryHandler(refresh_commission_callback, pattern="^refresh_commission$"))
    app.add_handler(CallbackQueryHandler(more_menu, pattern="^more_menu$"))
    app.add_handler(CallbackQueryHandler(referral_menu, pattern="^referral_menu$"))
    app.add_handler(CallbackQueryHandler(my_referrals_menu, pattern="^my_referrals$"))
    app.add_handler(CallbackQueryHandler(history_menu, pattern="^history_menu$"))
    app.add_handler(CallbackQueryHandler(withdraw_menu, pattern="^withdraw_menu$"))
    app.add_handler(CallbackQueryHandler(handle_withdraw, pattern="^withdraw_(all|\\d+)$"))
    app.add_handler(CallbackQueryHandler(withdraw_custom_handler, pattern="^withdraw_custom$"))
    app.add_handler(CallbackQueryHandler(start, pattern="^back$"))
    
    # Обработка текста для своей суммы и адреса вывода
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_amount))
    
    # Catch-all для неизвестных callbacks (должен быть последним)
    app.add_handler(CallbackQueryHandler(unknown_callback))
    
    # Добавляем общий error handler для callback'ов
    async def callback_error_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Wrapper для обработки ошибок в callback'ах"""
        try:
            query = update.callback_query
            if query:
                try:
                    await query.answer("❌ Произошла ошибка. Попробуйте позже.")
                except:
                    pass
        except:
            pass
    
    # Jobs
    if app.job_queue:
        app.job_queue.run_repeating(update_positions, interval=5, first=5)
        
        if AUTO_TRADE_USER_ID and AUTO_TRADE_USER_ID != 0:
            app.job_queue.run_repeating(send_smart_signal, interval=120, first=10)  # 2 минуты
        
        # Cleanup caches - оборачиваем в async функцию
        async def cleanup_caches_job(context):
            cleanup_caches()
        app.job_queue.run_repeating(cleanup_caches_job, interval=300, first=300)
        
        # Автоматическая проверка крипто-платежей каждые 15 секунд
        app.job_queue.run_repeating(check_pending_crypto_payments, interval=15, first=15)
        
        # Отправка уведомлений рефералам каждые 60 секунд (группируем уведомления)
        async def send_ref_notifications_job(context):
            await send_referral_notifications(context.bot)
        app.job_queue.run_repeating(send_ref_notifications_job, interval=60, first=60)
        
        # === NEWS ANALYZER JOB ===
        # Новости используются только для внутреннего анализа и влияния на сделки
        # Алерты отключены - данные применяются в smart_analyzer через enhance_setup_with_news
        if NEWS_FEATURES:
            logger.info("[INIT] News analyzer enabled (internal use only, no alerts)")
        
        # === TRADE LOGGER MAINTENANCE JOB ===
        async def logger_maintenance_job(context):
            """Flush logs and clean up old entries"""
            try:
                trade_logger.flush()
                # Clean up logs older than 30 days once per day (check if 24 hours passed)
                import random
                if random.random() < 0.0007:  # ~once per day with 5 min interval
                    trade_logger.cleanup_old_logs(days=30)
            except Exception as e:
                logger.warning(f"[LOGGER] Maintenance error: {e}")
        
        app.job_queue.run_repeating(logger_maintenance_job, interval=300, first=60)  # Every 5 minutes
        
        # === AUTO OPTIMIZER JOB ===
        async def auto_optimizer_job(context):
            """Run automated optimization analysis"""
            try:
                if auto_optimizer.should_run_analysis():
                    result = await auto_optimizer.run_analysis()
                    
                    if result.get('status') == 'completed':
                        recommendations = result.get('recommendations', [])
                        overall_stats = result.get('overall_stats', {})
                        
                        logger.info(
                            f"[OPTIMIZER] Analysis complete: "
                            f"{overall_stats.get('total_trades', 0)} trades, "
                            f"{overall_stats.get('win_rate', 0):.1%} win rate, "
                            f"{len(recommendations)} recommendations"
                        )
                        
                        # Log significant recommendations
                        for rec in recommendations:
                            if rec.get('confidence', 0) >= 0.6:
                                trade_logger.log_optimization(
                                    parameter=rec.get('parameter'),
                                    old_value=rec.get('current_value'),
                                    new_value=rec.get('recommended_value'),
                                    reason=rec.get('reason'),
                                    expected_improvement=rec.get('expected_improvement')
                                )
            except Exception as e:
                logger.error(f"[OPTIMIZER] Job error: {e}")
        
        # Run optimizer every 4 hours
        app.job_queue.run_repeating(auto_optimizer_job, interval=14400, first=300)  # 4 hours, start after 5 min
        
        logger.info("[JOBS] All periodic tasks registered")
    else:
        logger.warning("[JOBS] JobQueue NOT available!")
    
    # Error handler with detailed logging
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        # Special handling for Conflict errors (multiple bot instances)
        if isinstance(context.error, Conflict):
            error_msg = str(context.error)
            if "getUpdates" in error_msg or "terminated by other" in error_msg:
                logger.critical(
                    "[ERROR_HANDLER] CONFLICT: Multiple bot instances detected! "
                    "This usually means:\n"
                    "1. Another instance is still running (check deployments)\n"
                    "2. Multiple replicas are configured (should be 1)\n"
                    "3. Old instance didn't shut down properly\n"
                    "The bot will retry automatically, but you should check your deployment configuration."
                )
                # Don't notify users about this - it's an infrastructure issue
                return
        
        # Log detailed error information
        error_details = {
            'error': str(context.error) if context.error else 'Unknown error',
            'error_type': type(context.error).__name__ if context.error else 'Unknown',
        }
        
        if update:
            try:
                if hasattr(update, 'effective_user') and update.effective_user:
                    error_details['user_id'] = update.effective_user.id
                    error_details['username'] = update.effective_user.username
                if hasattr(update, 'callback_query') and update.callback_query:
                    error_details['callback_data'] = update.callback_query.data
                if hasattr(update, 'message') and update.message:
                    error_details['message_text'] = update.message.text[:100] if update.message.text else None
            except Exception as e:
                logger.warning(f"[ERROR_HANDLER] Error extracting details: {e}")
        
        logger.error(f"[ERROR_HANDLER] Details: {error_details}", exc_info=context.error)
        
        # Comprehensive error logging
        try:
            trade_logger.log_error(
                message=f"Error: {error_details.get('error_type', 'Unknown')} - {error_details.get('error', 'No details')}",
                error=context.error,
                user_id=error_details.get('user_id'),
                context=error_details
            )
        except Exception as e:
            logger.warning(f"[ERROR_HANDLER] Failed to log to trade_logger: {e}")
        
        # Notify user (only for user-initiated updates, not for jobs)
        if update and hasattr(update, 'effective_user') and update.effective_user:
            try:
                await context.bot.send_message(
                    update.effective_user.id, 
                    "<b>❌ Ошибка</b>\n\nПроизошла ошибка. Попробуйте позже.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"[ERROR_HANDLER] Could not notify user: {e}")
    
    app.add_error_handler(error_handler)
    
    # Устанавливаем меню команд
    async def post_init(application):
        from telegram import BotCommand
        commands = [
            BotCommand("start", "🏠 Главное меню"),
        ]
        await application.bot.set_my_commands(commands)
        logger.info("[BOT] Commands menu set")
    
    app.post_init = post_init
    
    # Log startup configuration summary
    logger.info("=" * 50)
    logger.info("BOT STARTING")
    logger.info("=" * 50)
    logger.info(f"[CONFIG] Database: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")
    logger.info(f"[CONFIG] Admin IDs: {ADMIN_IDS if ADMIN_IDS else 'Not configured'}")
    logger.info(f"[CONFIG] Bybit hedge user: {AUTO_TRADE_USER_ID if AUTO_TRADE_USER_ID else 'Not configured'}")
    logger.info(f"[CONFIG] Auto-trade: Enabled for ALL users with auto_trade=1")
    logger.info(f"[CONFIG] Commission: {COMMISSION_PERCENT}%")
    logger.info(f"[CONFIG] Leverage: x{LEVERAGE}")
    logger.info(f"[CONFIG] Min deposit: ${MIN_DEPOSIT}")
    logger.info(f"[CONFIG] Advanced features: {'Enabled' if ADVANCED_FEATURES else 'Disabled'}")
    logger.info(f"[CONFIG] Advanced position management: {'Enabled' if ADVANCED_POSITION_MANAGEMENT else 'Disabled'}")
    logger.info(f"[CONFIG] Crypto token: {'Configured' if os.getenv('CRYPTO_BOT_TOKEN') else 'Not configured'}")
    logger.info(f"[CONFIG] Bybit API: {'Configured' if os.getenv('BYBIT_API_KEY') else 'Not configured'}")
    logger.info("=" * 50)
    logger.info("BOT STARTED SUCCESSFULLY")
    logger.info("=" * 50)
    
    # Start dashboard in background thread
    try:
        init_dashboard(run_sql, USE_POSTGRES)
        dashboard_thread = start_dashboard_thread()
        logger.info(f"[CONFIG] Dashboard: Running on port {os.getenv('DASHBOARD_PORT', 5000)}")
    except Exception as e:
        logger.warning(f"[DASHBOARD] Failed to start: {e}")
    
    # Log startup to trade logger
    trade_logger.log_system("Bot started", data={
        'database': 'PostgreSQL' if USE_POSTGRES else 'SQLite',
        'admin_ids': ADMIN_IDS,
        'auto_trade_user': AUTO_TRADE_USER_ID,
        'commission': COMMISSION_PERCENT,
        'leverage': LEVERAGE,
        'advanced_features': ADVANCED_FEATURES,
        'advanced_position_management': ADVANCED_POSITION_MANAGEMENT,
        'news_features': NEWS_FEATURES
    })
    
    # Graceful shutdown
    import signal as sig
    
    def shutdown(signum, frame):
        logger.info("Shutting down gracefully...")
        trade_logger.log_system("Bot shutting down")
        trade_logger.flush()  # Flush any buffered logs
        for user_id in users_cache:
            save_user(user_id)
        logger.info("Data saved. Goodbye!")
    
    sig.signal(sig.SIGTERM, shutdown)
    sig.signal(sig.SIGINT, shutdown)
    
    # Выбор режима: webhook или polling
    WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    PORT = int(os.getenv("PORT", 8443))
    
    if WEBHOOK_URL:
        logger.info(f"[MODE] Webhook: {WEBHOOK_URL}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=token,
            webhook_url=f"{WEBHOOK_URL}/{token}",
            drop_pending_updates=True
        )
    else:
        logger.info("[MODE] Polling")
        logger.info("[POLLING] Starting with drop_pending_updates=True")
        logger.info("[POLLING] If you see Conflict errors, check:")
        logger.info("[POLLING] 1. Only ONE replica/instance should be running")
        logger.info("[POLLING] 2. Old instances must be fully stopped before starting new ones")
        logger.info("[POLLING] 3. Check deployment configuration for multiple instances")
        try:
            app.run_polling(
                drop_pending_updates=True,
                allowed_updates=None,  # Allow all update types
                close_loop=False  # Don't close event loop on shutdown
            )
        except Conflict as e:
            logger.critical(
                f"[POLLING] Conflict error during startup: {e}\n"
                "This means another bot instance is already running.\n"
                "Please stop all other instances before starting this one."
            )
            raise

if __name__ == "__main__":
    main()
