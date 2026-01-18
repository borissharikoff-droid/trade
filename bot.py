import logging
import os
import random
import asyncio
import aiohttp
import json
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# #region agent log - Debug instrumentation
def debug_log(hypothesis_id: str, location: str, message: str, data: dict = None):
    """Write debug log via logger"""
    print(f"[DBG:{hypothesis_id}] {location} | {message} | {data}")
# #endregion

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, PreCheckoutQueryHandler, MessageHandler, filters
from telegram.error import BadRequest

from hedger import hedge_open, hedge_close, is_hedging_enabled, hedger
from smart_analyzer import (
    SmartAnalyzer, find_best_setup, record_trade_result, get_trading_state,
    TradeSetup, SetupQuality, MarketRegime, get_signal_stats, reset_signal_stats,
    increment_bybit_opened
)

load_dotenv()

# Умный анализатор v2.0 - единственный режим
smart = SmartAnalyzer()

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

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
    """Получить подключение к БД"""
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def run_sql(query: str, params: tuple = (), fetch: str = None):
    """
    Выполнить SQL запрос с автоматической конвертацией placeholder'ов
    fetch: None, 'one', 'all', 'id' (lastrowid)
    """
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
            # Для PostgreSQL используем RETURNING id
            result = c.fetchone()[0] if 'RETURNING' in query.upper() else None
        else:
            result = c.lastrowid
    
    conn.commit()
    conn.close()
    return result

def init_db():
    """Инициализация базы данных"""
    conn = get_connection()
    c = conn.cursor()
    
    if USE_POSTGRES:
        # PostgreSQL синтаксис
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance REAL DEFAULT 100.0,
            total_deposit REAL DEFAULT 100.0,
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
            balance REAL DEFAULT 100.0,
            total_deposit REAL DEFAULT 100.0,
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
        conn.commit()
        logger.info("[DB] Migration: system_settings and pending_invoices tables ensured")
    except Exception as e:
        logger.warning(f"[DB] Migration warning (system_settings): {e}")
    
    conn.close()
    db_type = "PostgreSQL" if USE_POSTGRES else f"SQLite ({DB_PATH})"
    logger.info(f"[DB] Initialized: {db_type}")

def db_get_user(user_id: int) -> Dict:
    """Получить пользователя из БД"""
    row = run_sql("""
        SELECT balance, total_deposit, total_profit, trading,
               auto_trade, auto_trade_max_daily, auto_trade_min_winrate,
               auto_trade_today, auto_trade_last_reset
        FROM users WHERE user_id = ?
    """, (user_id,), fetch="one")

    if not row:
        run_sql("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        logger.info(f"[DB] New user {user_id} created")
        return {
            'balance': 100.0, 'total_deposit': 100.0, 'total_profit': 0.0, 'trading': False,
            'auto_trade': False, 'auto_trade_max_daily': 10, 'auto_trade_min_winrate': 70,
            'auto_trade_today': 0, 'auto_trade_last_reset': None
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
        'auto_trade_last_reset': row['auto_trade_last_reset']
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
    return run_sql("SELECT * FROM positions WHERE user_id = ?", (user_id,), fetch="all")

def db_add_position(user_id: int, pos: Dict) -> int:
    """Добавить позицию"""
    if USE_POSTGRES:
        query = """INSERT INTO positions
            (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl, bybit_qty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id"""
    else:
        query = """INSERT INTO positions
            (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl, bybit_qty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    pos_id = run_sql(query,
        (user_id, pos['symbol'], pos['direction'], pos['entry'], pos['current'],
         pos['sl'], pos['tp'], pos['amount'], pos['commission'], pos.get('pnl', 0), pos.get('bybit_qty', 0)), fetch="id")
    logger.info(f"[DB] Position {pos_id} added for user {user_id}")
    return pos_id

def db_update_position(pos_id: int, **kwargs):
    """Обновить позицию"""
    for key, value in kwargs.items():
        run_sql(f"UPDATE positions SET {key} = ? WHERE id = ?", (value, pos_id))

def db_close_position(pos_id: int, exit_price: float, pnl: float, reason: str):
    """Закрыть позицию и перенести в историю"""
    # Получаем позицию
    pos = run_sql("SELECT * FROM positions WHERE id = ?", (pos_id,), fetch="one")
    if not pos:
        return
    
    # Переносим в историю
    run_sql("""INSERT INTO history 
        (user_id, symbol, direction, entry, exit_price, sl, tp, amount, commission, pnl, reason, opened_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (pos['user_id'], pos['symbol'], pos['direction'], pos['entry'], exit_price, 
         pos['sl'], pos['tp'], pos['amount'], pos['commission'], pnl, reason, pos['opened_at']))
    
    # Удаляем из активных
    run_sql("DELETE FROM positions WHERE id = ?", (pos_id,))
    
    # Записываем результат для статистики smart analyzer
    record_trade_result(pnl)
    
    logger.info(f"[DB] Position {pos_id} closed: {reason}, PnL: ${pnl:.2f}")

def db_get_history(user_id: int, limit: int = 20) -> List[Dict]:
    """Получить историю сделок"""
    return run_sql("SELECT * FROM history WHERE user_id = ? ORDER BY closed_at DESC LIMIT ?", (user_id, limit), fetch="all")

def db_get_user_stats(user_id: int) -> Dict:
    """Полная статистика пользователя по ВСЕМ сделкам"""
    row = run_sql("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
            SUM(pnl) as total_pnl
        FROM history WHERE user_id = ?
    """, (user_id,), fetch="one")
    
    if not row:
        return {'total': 0, 'wins': 0, 'losses': 0, 'winrate': 0, 'total_pnl': 0}
    
    total = int(row['total'] or 0)
    wins = int(row['wins'] or 0)
    losses = int(row['losses'] or 0)
    total_pnl = float(row['total_pnl'] or 0)
    winrate = int(wins / total * 100) if total > 0 else 0
    
    return {'total': total, 'wins': wins, 'losses': losses, 'winrate': winrate, 'total_pnl': total_pnl}

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

def db_add_referral_bonus(referrer_id: int, amount: float, from_user_id: int = 0) -> bool:
    """
    Добавить реферальный бонус с защитой от злоупотреблений
    Returns: True if bonus was added, False if blocked
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
    
    # Add bonus
    run_sql("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, referrer_id))
    
    # Обновляем кэш
    if referrer_id in users_cache:
        users_cache[referrer_id]['balance'] = sanitize_balance(users_cache[referrer_id]['balance'] + amount)
    
    # Mark as given
    if from_user_id:
        db_mark_referral_bonus_given(referrer_id, from_user_id)
    db_increment_daily_referral_bonus(referrer_id)
    
    logger.info(f"[REF] Bonus ${amount} added to {referrer_id} (from user {from_user_id})")
    return True

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
REFERRAL_BONUS = 5.0  # $5 бонус рефереру при депозите
COMMISSION_WITHDRAW_THRESHOLD = 10.0  # Авто-вывод комиссий при накоплении $10
ADMIN_CRYPTO_ID = os.getenv("ADMIN_CRYPTO_ID", "")  # CryptoBot ID админа для вывода комиссий

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
MAX_POSITIONS_PER_USER = 10  # Maximum open positions per user
MIN_BALANCE_RESERVE = 5.0    # Minimum balance to keep after trade
MAX_SINGLE_TRADE = 10000.0   # Maximum single trade amount
MAX_BALANCE = 1000000.0      # Maximum user balance (sanity check)

# Allowed trading symbols (whitelist) - РАСШИРЕННЫЙ СПИСОК
ALLOWED_SYMBOLS = {
    # === ОСНОВНЫЕ ===
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
    
    # === Прочие ликвидные ===
    'LINK/USDT', 'LTC/USDT', 'TRX/USDT', 'ORDI/USDT', 'BCH/USDT',
    'ETC/USDT', 'XLM/USDT', 'VET/USDT', 'THETA/USDT', 'EGLD/USDT'
}

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
    Validate trading symbol
    Returns: (is_valid: bool, error_message: str or None)
    """
    if not symbol or not isinstance(symbol, str):
        return False, "Invalid symbol"
    symbol = symbol.upper().strip()
    if symbol not in ALLOWED_SYMBOLS:
        return False, f"Symbol {symbol} not supported"
    return True, None

def validate_direction(direction: str) -> tuple:
    """
    Validate trade direction
    Returns: (is_valid: bool, error_message: str or None)
    """
    if direction not in ['LONG', 'SHORT']:
        return False, "Direction must be LONG or SHORT"
    return True, None

def sanitize_balance(balance: float) -> float:
    """Ensure balance stays within safe bounds"""
    return max(0.0, min(MAX_BALANCE, balance))

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

# Кэш цен для уменьшения запросов
price_cache: Dict[str, Dict] = {}  # {symbol: {'price': float, 'time': datetime}}
CACHE_TTL = 3  # секунд

async def get_cached_price(symbol: str) -> Optional[float]:
    """Получить цену с кэшированием"""
    now = datetime.now()
    
    if symbol in price_cache:
        cache = price_cache[symbol]
        age = (now - cache['time']).total_seconds()
        if age < CACHE_TTL:
            return cache['price']
    
    price = await get_real_price(symbol)
    if price:
        price_cache[symbol] = {'price': price, 'time': now}
    return price

# ==================== ДАННЫЕ (кэш в памяти) ====================
users_cache: Dict[int, Dict] = {}
positions_cache: Dict[int, List[Dict]] = {}
rate_limits: Dict[int, Dict] = {}  # {user_id: {'count': int, 'reset': datetime}}

# ==================== TRANSACTION LOCKS ====================
# Per-user locks to prevent race conditions on balance operations
_user_locks: Dict[int, asyncio.Lock] = {}

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
        new_balance = user['balance'] + delta
        
        if new_balance < 0:
            logger.warning(f"[BALANCE] Blocked: user {user_id} delta={delta:.2f} would result in negative balance")
            return False
        
        user['balance'] = sanitize_balance(new_balance)
        save_user(user_id)
        logger.info(f"[BALANCE] User {user_id}: {delta:+.2f} -> ${user['balance']:.2f} ({reason})")
        return True

# ==================== RATE LIMITING ====================
MAX_REQUESTS_PER_MINUTE = 30

def check_rate_limit(user_id: int) -> bool:
    """Проверка лимита запросов. Возвращает True если лимит превышен."""
    now = datetime.now()
    
    if user_id not in rate_limits:
        rate_limits[user_id] = {'count': 1, 'reset': now}
        return False
    
    user_limit = rate_limits[user_id]
    
    # Сброс каждую минуту
    if (now - user_limit['reset']).total_seconds() > 60:
        rate_limits[user_id] = {'count': 1, 'reset': now}
        return False
    
    user_limit['count'] += 1
    
    if user_limit['count'] > MAX_REQUESTS_PER_MINUTE:
        return True
    
    return False

# ==================== КОМИССИИ (АВТО-ВЫВОД) ====================
async def add_commission(amount: float):
    """Добавить комиссию и вывести при достижении порога (с персистентностью)"""
    global pending_commission
    pending_commission += amount
    save_pending_commission()  # Persist to DB
    
    logger.info(f"[COMMISSION] +${amount:.2f}, накоплено: ${pending_commission:.2f}")
    
    # Авто-вывод при достижении порога
    if pending_commission >= COMMISSION_WITHDRAW_THRESHOLD and ADMIN_CRYPTO_ID:
        await withdraw_commission()

async def withdraw_commission():
    """Вывести накопленные комиссии на кошелёк админа"""
    global pending_commission
    
    if pending_commission < 1:
        return False
    
    amount = pending_commission
    
    # CryptoBot Transfer API
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN", "")
    if not crypto_token or not ADMIN_CRYPTO_ID:
        logger.warning("[COMMISSION] CryptoBot не настроен для вывода")
        return False
    
    testnet = os.getenv("CRYPTO_TESTNET", "").lower() in ("true", "1", "yes")
    base_url = "https://testnet-pay.crypt.bot" if testnet else "https://pay.crypt.bot"
    
    try:
        async with aiohttp.ClientSession() as session:
            # Трансфер на CryptoBot ID админа
            async with session.post(
                f"{base_url}/api/transfer",
                headers={"Crypto-Pay-API-Token": crypto_token},
                json={
                    "user_id": int(ADMIN_CRYPTO_ID),
                    "asset": "USDT",
                    "amount": str(round(amount, 2)),
                    "spend_id": f"commission_{int(datetime.now().timestamp())}"
                }
            ) as resp:
                data = await resp.json()
                
                if data.get("ok"):
                    pending_commission = 0
                    save_pending_commission()  # Persist reset to DB
                    logger.info(f"[COMMISSION] ✅ Выведено ${amount:.2f} на CryptoBot ID {ADMIN_CRYPTO_ID}")
                    return True
                else:
                    logger.error(f"[COMMISSION] ❌ Ошибка вывода: {data}")
                    return False
    except Exception as e:
        logger.error(f"[COMMISSION] ❌ Ошибка: {e}")
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
    """Получить пользователя (с кэшированием)"""
    if user_id not in users_cache:
        users_cache[user_id] = db_get_user(user_id)
    return users_cache[user_id]

def save_user(user_id: int):
    """Сохранить пользователя в БД"""
    if user_id in users_cache:
        user = users_cache[user_id]
        db_update_user(user_id,
            balance=user['balance'],
            total_deposit=user['total_deposit'],
            total_profit=user['total_profit'],
            trading=user['trading'],
            auto_trade=user.get('auto_trade', False),
            auto_trade_max_daily=user.get('auto_trade_max_daily', 10),
            auto_trade_min_winrate=user.get('auto_trade_min_winrate', 70),
            auto_trade_today=user.get('auto_trade_today', 0),
            auto_trade_last_reset=user.get('auto_trade_last_reset')
        )

def get_positions(user_id: int) -> List[Dict]:
    """Получить позиции (с кэшированием)"""
    cache_hit = user_id in positions_cache
    if user_id not in positions_cache:
        positions_cache[user_id] = db_get_positions(user_id)
    # #region agent log
    debug_log("E", "get_positions", "Getting positions", {"user_id": user_id, "cache_hit": cache_hit, "count": len(positions_cache[user_id]), "position_ids": [p.get('id') for p in positions_cache[user_id]]})
    # #endregion
    return positions_cache[user_id]

# ==================== ГЛАВНЫЙ ЭКРАН ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    # Rate limiting
    if check_rate_limit(user_id):
        if update.callback_query:
            await update.callback_query.answer("⏳ Подожди немного", show_alert=True)
        return
    
    logger.info(f"[START] User {user_id}")
    
    # Обработка реферальной ссылки
    if context.args and len(context.args) > 0:
        ref_arg = context.args[0]
        if ref_arg.startswith("ref_"):
            try:
                referrer_id = int(ref_arg.replace("ref_", ""))
                if db_set_referrer(user_id, referrer_id):
                    logger.info(f"[REF] User {user_id} registered via referral from {referrer_id}")
            except ValueError:
                pass
    
    user = get_user(user_id)
    
    balance = user['balance']
    trading_status = "🟢" if user['trading'] else "🔴"
    auto_trade_status = "🟢" if user.get('auto_trade') else "🔴"
    
    # Получаем реальный winrate
    real_wr = db_get_real_winrate(min_trades=10)
    wr_text = f"{real_wr['winrate']:.0f}%" if real_wr['reliable'] else "~75%"
    
    text = f"""<b>💰 Баланс</b>

${balance:.2f}

Торговля: {trading_status}
Авто-трейд: {auto_trade_status}
Winrate: {wr_text}"""
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Выкл' if user['trading'] else '🟢 Вкл'}", callback_data="toggle"),
         InlineKeyboardButton(f"{'🟢' if user.get('auto_trade') else '🔴'} Авто-трейд", callback_data="auto_trade_menu")],
        [InlineKeyboardButton("💳 Пополнить", callback_data="deposit"), InlineKeyboardButton("📊 Сделки", callback_data="trades")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
        except Exception:
            await context.bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await context.bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode="HTML")

# ==================== ПОПОЛНЕНИЕ ====================
async def deposit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    logger.info(f"[DEPOSIT] User {update.effective_user.id}")
    await query.answer()
    
    text = f"""<b>💳 Пополнение</b>

Минимум: ${MIN_DEPOSIT}"""
    
    keyboard = [
        [InlineKeyboardButton("⭐ Telegram Stars", callback_data="pay_stars")],
        [InlineKeyboardButton("💎 Crypto (USDT/TON)", callback_data="pay_crypto")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def pay_stars_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = """<b>⭐ Через Stars</b>

Выбери сумму:"""
    
    # 50 stars = $1
    keyboard = [
        [
            InlineKeyboardButton("$1 (50⭐)", callback_data="stars_50"),
            InlineKeyboardButton("$5 (250⭐)", callback_data="stars_250")
        ],
        [
            InlineKeyboardButton("$10 (500⭐)", callback_data="stars_500"),
            InlineKeyboardButton("$25 (1250⭐)", callback_data="stars_1250")
        ],
        [
            InlineKeyboardButton("$50 (2500⭐)", callback_data="stars_2500"),
            InlineKeyboardButton("$100 (5000⭐)", callback_data="stars_5000")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="deposit")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def send_stars_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    stars_map = {"stars_50": 50, "stars_250": 250, "stars_500": 500, "stars_1250": 1250, "stars_2500": 2500, "stars_5000": 5000}
    stars = stars_map.get(query.data, 50)
    usd = stars // STARS_RATE
    
    try:
        await query.message.delete()
    except Exception:
        pass
    
    await context.bot.send_invoice(
        chat_id=update.effective_user.id,
        title=f"Пополнение ${usd}",
        description=f"Пополнение баланса на ${usd}",
        payload=f"deposit_{usd}",
        currency="XTR",
        prices=[LabeledPrice(label=f"${usd}", amount=stars)]
    )

async def precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    await query.answer(ok=True)

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    payment = update.message.successful_payment
    stars = payment.total_amount
    usd = stars // STARS_RATE
    
    # Проверяем первый депозит для реферального бонуса
    is_first_deposit = user['total_deposit'] == 100  # Начальный баланс
    
    user['balance'] += usd
    user['total_deposit'] += usd
    save_user(user_id)
    
    logger.info(f"[PAYMENT] User {user_id} deposited ${usd} via Stars")
    
    # Реферальный бонус при первом депозите
    if is_first_deposit:
        referrer_id = db_get_referrer(user_id)
        if referrer_id:
            bonus_added = db_add_referral_bonus(referrer_id, REFERRAL_BONUS, from_user_id=user_id)
            if bonus_added:
                try:
                    await context.bot.send_message(
                        referrer_id,
                        f"<b>📥 Реферал</b>\n\nТвой реферал сделал депозит.\nБонус: +${REFERRAL_BONUS}",
                        parse_mode="HTML"
                    )
                except:
                    pass
        
    text = f"""<b>✅ Оплата</b>

Зачислено: ${usd}
💰 ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

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
        [InlineKeyboardButton("🔙 Назад", callback_data="deposit")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def create_crypto_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    amount_map = {"crypto_1": 1, "crypto_5": 5, "crypto_10": 10, "crypto_25": 25, "crypto_50": 50, "crypto_100": 100}
    amount = amount_map.get(query.data, 1)
    user_id = update.effective_user.id
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    
    if not crypto_token:
        await query.edit_message_text(
            "❌ Crypto временно недоступен",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]])
        )
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

К оплате: ${amount} USDT"""
        
        keyboard = [
            [InlineKeyboardButton("💳 Оплатить", url=invoice['bot_invoice_url'])],
            [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_{invoice['invoice_id']}")],
            [InlineKeyboardButton("🔙 Отмена", callback_data="deposit")]
        ]
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        
    except Exception as e:
        logger.error(f"[CRYPTO] Error: {e}")
        await query.edit_message_text(
            "❌ Ошибка создания платежа",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]])
        )

async def check_crypto_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Проверяем...")
    
    try:
        invoice_id = int(query.data.split("_")[1])
    except (ValueError, IndexError):
        await query.answer("Ошибка данных", show_alert=True)
        return
    
    # Use persistent DB instead of context.bot_data
    pending_info = db_get_pending_invoice(invoice_id)
    if not pending_info:
        await query.answer("Платёж не найден или истёк", show_alert=True)
        return
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    if not crypto_token:
        await query.answer("Ошибка", show_alert=True)
        return
    
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
                    await query.answer("Платёж ещё не получен", show_alert=True)
                    return
                
                invoice = data["result"]["items"][0]
        
        if invoice.get("status") == "paid":
            # Remove from DB
            db_remove_pending_invoice(invoice_id)
            user_id = pending_info['user_id']
            amount = pending_info['amount']
            
            user = get_user(user_id)
            is_first_deposit = user['total_deposit'] == 100
            
            user['balance'] = sanitize_balance(user['balance'] + amount)
            user['total_deposit'] += amount
            save_user(user_id)
            
            logger.info(f"[CRYPTO] User {user_id} deposited ${amount}")
            
            # Реферальный бонус
            if is_first_deposit:
                referrer_id = db_get_referrer(user_id)
                if referrer_id:
                    bonus_added = db_add_referral_bonus(referrer_id, REFERRAL_BONUS, from_user_id=user_id)
                    if bonus_added:
                        try:
                            await context.bot.send_message(
                                referrer_id,
                                f"<b>📥 Реферал</b>\n\nТвой реферал сделал депозит.\nБонус: +${REFERRAL_BONUS}",
                                parse_mode="HTML"
                            )
                        except:
                            pass
            
            text = f"""<b>✅ Оплата</b>

Зачислено: ${amount}
💰 ${user['balance']:.2f}"""
            
            keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.answer("Платёж ещё не получен", show_alert=True)
            
    except Exception as e:
        logger.error(f"[CRYPTO] Check error: {e}")
        await query.answer("Ошибка проверки", show_alert=True)

# ==================== ТОРГОВЛЯ ====================
async def toggle_trading(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    logger.info(f"[TOGGLE] User {user_id}")
    
    # Принудительно читаем из БД чтобы избежать рассинхрона
    users_cache.pop(user_id, None)
    user = get_user(user_id)
    
    if not user['trading'] and user['balance'] < MIN_DEPOSIT:
        logger.info(f"[TOGGLE] User {user_id} - insufficient balance (${user['balance']:.2f})")
        await query.answer(f"❌ Недостаточно средств\n\nБаланс: ${user['balance']:.2f}\nМинимум: ${MIN_DEPOSIT}", show_alert=True)
        return
    
    new_state = not user['trading']
    user['trading'] = new_state
    
    # Сохраняем напрямую в БД
    db_update_user(user_id, trading=new_state)
    logger.info(f"[TOGGLE] User {user_id} trading = {new_state}")
    
    await start(update, context)

# ==================== АВТО-ТРЕЙД НАСТРОЙКИ ====================
async def auto_trade_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Меню настроек авто-трейда"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    users_cache.pop(user_id, None)  # Обновляем из БД
    user = get_user(user_id)
    
    auto_enabled = user.get('auto_trade', False)
    max_daily = user.get('auto_trade_max_daily', 10)
    min_wr = user.get('auto_trade_min_winrate', 70)
    today_count = user.get('auto_trade_today', 0)
    
    status = "🟢 ВКЛ" if auto_enabled else "🔴 ВЫКЛ"
    
    text = f"""<b>🤖 Авто-трейд</b>

Статус: {status}
Сделок сегодня: {today_count}/{max_daily}
Успешность: {min_wr}%

<i>Бот автоматически входит в сделки по сигналам.</i>"""
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Выключить' if auto_enabled else '🟢 Включить'}", callback_data="auto_trade_toggle")],
        [InlineKeyboardButton(f"📊 Сделок/день: {max_daily}", callback_data="auto_trade_daily_menu")],
        [InlineKeyboardButton(f"📈 Успешность: {min_wr}%", callback_data="auto_trade_winrate_menu")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

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
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

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
    
    text = f"""<b>📈 Успешность</b>

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
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

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
    Синхронизация позиций с Bybit - закрывает позиции которые закрылись на бирже
    Проверяет размер позиции, а не только наличие символа.

    Returns:
        Количество синхронизированных (закрытых) позиций
    """
    # #region agent log
    debug_log("A", "sync_bybit_positions:entry", "Sync started", {"user_id": user_id})
    # #endregion
    
    if not await is_hedging_enabled():
        # #region agent log
        debug_log("A", "sync_bybit_positions:skip", "Hedging disabled", {})
        # #endregion
        return 0

    user_positions = get_positions(user_id)
    if not user_positions:
        return 0

    user = get_user(user_id)
    synced = 0

    # Получаем все открытые позиции на Bybit
    bybit_positions = await hedger.get_all_positions()
    # Словарь: symbol -> size (размер позиции)
    bybit_sizes = {pos['symbol']: float(pos.get('size', 0)) for pos in bybit_positions}
    
    # #region agent log
    debug_log("A", "sync_bybit_positions:bybit_data", "Bybit positions fetched", {"bybit_count": len(bybit_positions), "bybit_sizes": bybit_sizes, "bot_positions_count": len(user_positions)})
    # #endregion

    # Получаем закрытые позиции за последние 7 дней
    closed_pnl = await hedger.get_closed_pnl(limit=100)

    for pos in user_positions[:]:
        bybit_symbol = pos['symbol'].replace("/", "")
        bybit_size = bybit_sizes.get(bybit_symbol, 0)
        expected_qty = pos.get('bybit_qty', 0)

        # Проверяем закрыта ли позиция:
        # 1. bybit_qty > 0 и размер на Bybit = 0 -> позиция закрылась на Bybit
        # 2. bybit_qty > 0 и размер сильно меньше ожидаемого -> частичное закрытие
        # 3. bybit_qty == 0 при включенном хеджировании -> "фейковая" позиция (ошибка при открытии)
        
        if expected_qty == 0:
            # Позиция без bybit_qty - возможно не открылась на Bybit
            # Помечаем как "orphan" и закрываем без PnL
            logger.warning(f"[SYNC] Orphan position {pos['id']}: {bybit_symbol} has no bybit_qty - closing")
            
            # Возвращаем только amount без PnL (позиция не была реально открыта)
            returned = pos['amount']
            user['balance'] = sanitize_balance(user['balance'] + returned)
            save_user(user_id)
            
            db_close_position(pos['id'], pos.get('entry', 0), 0, 'ORPHAN_SYNC')
            
            # Явно удаляем из кэша по ID
            pos_id_to_remove = pos['id']
            if user_id in positions_cache:
                positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]
            
            synced += 1
            logger.info(f"[SYNC] Orphan {pos_id_to_remove} removed from cache, remaining: {len(positions_cache.get(user_id, []))}")
            
            try:
                ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
                await context.bot.send_message(
                    user_id, 
                    f"<b>⚠️ Синхронизация</b>\n\n"
                    f"{ticker} закрыт (не был на Bybit)\n"
                    f"Возврат: ${returned:.0f}\n\n"
                    f"💰 ${user['balance']:.0f}",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"[SYNC] Failed to notify orphan: {e}")
            continue
        
        is_closed = bybit_size == 0 or (expected_qty > 0 and bybit_size < expected_qty * 0.1)

        if is_closed:
            # Позиция закрыта на Bybit - рассчитываем PnL локально
            # (Bybit PnL общий для всей позиции, не подходит для отдельных записей бота)
            real_pnl = pos.get('pnl', 0)
            
            # Пробуем найти закрытую позицию по символу для уточнения
            for closed in closed_pnl:
                if closed['symbol'] == bybit_symbol:
                    logger.info(f"[SYNC] Found closed position: {bybit_symbol}, Bybit PnL: ${closed['closed_pnl']:.2f}")
                    break

            logger.info(f"[SYNC] Closing {bybit_symbol}: bybit_size={bybit_size}, expected_qty={expected_qty}, PnL=${real_pnl:.2f}")

            # Закрываем позицию в боте
            returned = pos['amount'] + real_pnl
            user['balance'] = sanitize_balance(user['balance'] + returned)
            user['total_profit'] += real_pnl
            save_user(user_id)

            # Переносим в историю
            db_close_position(pos['id'], pos.get('current', pos['entry']), real_pnl, 'BYBIT_SYNC')
            
            # Явно удаляем из кэша по ID (надёжнее чем remove)
            pos_id_to_remove = pos['id']
            if user_id in positions_cache:
                positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]

            synced += 1
            logger.info(f"[SYNC] Position {pos_id_to_remove} synced: {pos['symbol']} PnL=${real_pnl:.2f}, cache remaining: {len(positions_cache.get(user_id, []))}")

            # Отправляем уведомление
            try:
                ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
                pnl_abs = abs(real_pnl)

                if real_pnl > 0:
                    text = f"""<b>📡 Bybit</b>

{ticker} закрыт
Итого: <b>+${pnl_abs:.0f}</b>

💰 ${user['balance']:.0f}"""
                else:
                    text = f"""<b>📡 Bybit</b>

{ticker} закрыт
Итого: <b>-${pnl_abs:.0f}</b>

💰 ${user['balance']:.0f}"""

                await context.bot.send_message(user_id, text, parse_mode="HTML")
            except Exception as e:
                logger.error(f"[SYNC] Failed to notify user {user_id}: {e}")

    if synced > 0:
        logger.info(f"[SYNC] User {user_id}: synced {synced} positions from Bybit")

    return synced


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
            
            stacked.append({
                'id': position_ids[0],  # Главный ID для отображения
                'position_ids': position_ids,  # Все ID для закрытия
                'symbol': symbol,
                'direction': direction,
                'entry': weighted_entry,
                'current': current,
                'amount': total_amount,
                'tp': tp,
                'sl': sl,
                'pnl': total_pnl,
                'commission': sum(p.get('commission', 0) for p in group),
                'stacked_count': len(group)  # Сколько позиций объединено
            })
    
    return stacked


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
    
    # Находим позиции по этому символу
    positions_to_close = [p for p in user_positions if p['symbol'] == symbol]
    
    if not positions_to_close:
        await query.edit_message_text(
            f"📭 Нет открытых позиций по {ticker}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
        )
        return
    
    await query.edit_message_text(f"⏳ Закрываем {ticker}...")
    
    # СНАЧАЛА закрываем на Bybit
    hedging_enabled = await is_hedging_enabled()
    failed_positions = []
    
    if hedging_enabled:
        for pos in positions_to_close:
            bybit_qty = pos.get('bybit_qty', 0)
            if bybit_qty > 0:
                hedge_result = await hedge_close(pos['id'], symbol, pos['direction'], bybit_qty)
                if not hedge_result:
                    logger.error(f"[CLOSE_SYMBOL] ❌ Failed to close {symbol} pos {pos['id']} on Bybit")
                    failed_positions.append(pos)
            else:
                hedge_result = await hedge_close(pos['id'], symbol, pos['direction'], None)
                if not hedge_result:
                    logger.error(f"[CLOSE_SYMBOL] ❌ Failed to close {symbol} pos {pos['id']} on Bybit")
                    failed_positions.append(pos)
    
    # Убираем позиции которые не удалось закрыть на Bybit
    if failed_positions and hedging_enabled:
        positions_to_close = [p for p in positions_to_close if p not in failed_positions]
        if not positions_to_close:
            await query.edit_message_text(
                f"<b>❌ Ошибка закрытия</b>\n\n"
                f"Не удалось закрыть позиции на Bybit.\n"
                f"Попробуйте ещё раз.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
            )
            return
    
    # Закрываем в БД и считаем PnL
    total_pnl = 0
    total_returned = 0
    
    for pos in positions_to_close:
        # Получаем реальную цену
        real_price = await get_cached_price(symbol)
        if not real_price:
            real_price = pos['current']
        
        # Пересчитываем PnL с реальной ценой
        if pos['direction'] == "LONG":
            pnl = (real_price - pos['entry']) / pos['entry'] * pos['amount'] * LEVERAGE
        else:
            pnl = (pos['entry'] - real_price) / pos['entry'] * pos['amount'] * LEVERAGE
        
        pnl -= pos.get('commission', 0)
        
        returned = pos['amount'] + pnl
        total_pnl += pnl
        total_returned += returned
        
        user['balance'] = sanitize_balance(user['balance'] + returned)
        user['total_profit'] += pnl
        
        db_close_position(pos['id'], real_price, pnl, 'MANUAL_CLOSE')
        # Явно удаляем из кэша по ID
        pos_id_to_remove = pos['id']
        if user_id in positions_cache:
            positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]
    
    save_user(user_id)
    
    pnl_sign = "+" if total_pnl >= 0 else ""
    pnl_emoji = "✅" if total_pnl >= 0 else "📉"
    
    text = f"""<b>{pnl_emoji} {ticker} закрыт</b>

PnL: {pnl_sign}${total_pnl:.2f}

💰 ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades"),
                 InlineKeyboardButton("🔙 Меню", callback_data="back")]]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    logger.info(f"[CLOSE_SYMBOL] User {user_id}: closed {ticker}, PnL=${total_pnl:.2f}")


async def close_all_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть все открытые позиции пользователя"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    if not user_positions:
        await query.edit_message_text(
            "📭 Нет открытых сделок",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
        )
        return
    
    await query.edit_message_text("⏳ Закрываем позиции...")
    
    # === ГРУППИРУЕМ ПОЗИЦИИ ПО СИМВОЛУ ДЛЯ ЗАКРЫТИЯ НА BYBIT ===
    # Bybit хранит одну позицию на символ, поэтому закрываем один раз за группу
    close_prices = {}  # (symbol, direction) -> close_price
    failed_symbols = []  # Символы которые не удалось закрыть
    hedging_enabled = await is_hedging_enabled()
    
    if hedging_enabled:
        by_symbol = {}
        for pos in user_positions:
            key = (pos['symbol'], pos['direction'])
            if key not in by_symbol:
                by_symbol[key] = []
            by_symbol[key].append(pos)
        
        # Закрываем на Bybit по символам и получаем реальные цены
        for (symbol, direction), positions in by_symbol.items():
            total_qty = sum(p.get('bybit_qty', 0) for p in positions)
            if total_qty > 0:
                hedge_result = await hedge_close(positions[0]['id'], symbol, direction, total_qty)
                if hedge_result:
                    logger.info(f"[CLOSE_ALL] Bybit closed {symbol} {direction} qty={total_qty}")
                else:
                    logger.error(f"[CLOSE_ALL] ❌ Failed to close {symbol} {direction} on Bybit")
                    failed_symbols.append((symbol, direction))
                    continue
            else:
                # Если bybit_qty не сохранён, закрываем всю позицию на Bybit
                hedge_result = await hedge_close(positions[0]['id'], symbol, direction, None)
                if hedge_result:
                    logger.info(f"[CLOSE_ALL] Bybit closed {symbol} {direction} (full)")
                else:
                    logger.error(f"[CLOSE_ALL] ❌ Failed to close {symbol} {direction} on Bybit")
                    failed_symbols.append((symbol, direction))
                    continue
            
            # Получаем реальную цену закрытия
            await asyncio.sleep(0.3)
            close_side = "Sell" if direction == "LONG" else "Buy"
            order_info = await hedger.get_last_order_price(symbol, close_side)
            if order_info and order_info.get('price'):
                close_prices[(symbol, direction)] = order_info['price']
                logger.info(f"[CLOSE_ALL] Real close price {symbol}: ${order_info['price']:.4f}")
    
    # Убираем позиции которые не удалось закрыть на Bybit
    positions_to_close = user_positions[:]
    if failed_symbols and hedging_enabled:
        positions_to_close = [p for p in user_positions if (p['symbol'], p['direction']) not in failed_symbols]
        if not positions_to_close:
            await query.edit_message_text(
                f"<b>❌ Ошибка закрытия</b>\n\n"
                f"Не удалось закрыть позиции на Bybit.\n"
                f"Попробуйте ещё раз.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
            )
            return
    
    # === ЗАКРЫВАЕМ ВСЕ ПОЗИЦИИ В БД ===
    total_pnl = 0
    total_returned = 0
    closed_count = 0
    winners = 0
    losers = 0
    
    for pos in positions_to_close:
        # Получаем реальную цену закрытия если есть
        close_price = close_prices.get((pos['symbol'], pos['direction']), pos.get('current', pos['entry']))
        
        # Пересчитываем PnL с реальной ценой
        if pos['direction'] == "LONG":
            pnl_percent = (close_price - pos['entry']) / pos['entry']
        else:
            pnl_percent = (pos['entry'] - close_price) / pos['entry']
        pnl = pos['amount'] * LEVERAGE * pnl_percent - pos.get('commission', 0)
        
        returned = pos['amount'] + pnl
        
        # Обновляем статистику
        total_pnl += pnl
        total_returned += returned
        closed_count += 1
        
        if pnl > 0:
            winners += 1
        elif pnl < 0:
            losers += 1
        
        # Закрываем в БД с реальной ценой
        db_close_position(pos['id'], close_price, pnl, 'CLOSE_ALL')
        # Явно удаляем из кэша по ID
        pos_id_to_remove = pos['id']
        if user_id in positions_cache:
            positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]
    
    # Обновляем баланс
    user['balance'] = sanitize_balance(user['balance'] + total_returned)
    user['total_profit'] += total_pnl
    save_user(user_id)
    
    # Формируем итоговое сообщение
    pnl_abs = abs(total_pnl)
    
    if total_pnl > 0:
        text = f"""<b>📊 Все сделки закрыты</b>

Закрыто: {closed_count}
✅ {winners} прибыльных
❌ {losers} убыточных

Итого: <b>+${pnl_abs:.0f}</b>
Хороший сет.

💰 ${user['balance']:.0f}"""
    elif total_pnl < 0:
        text = f"""<b>📊 Все сделки закрыты</b>

Закрыто: {closed_count}
✅ {winners} прибыльных
❌ {losers} убыточных

Итого: <b>-${pnl_abs:.0f}</b>
Следующий будет лучше.

💰 ${user['balance']:.0f}"""
    else:
        text = f"""<b>📊 Все сделки закрыты</b>

Закрыто: {closed_count}

Итого: $0
Капитал сохранён.

💰 ${user['balance']:.0f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Новые сигналы", callback_data="back")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    
    logger.info(f"[CLOSE_ALL] User {user_id}: closed {closed_count} positions, total PnL: ${total_pnl:.2f}")


async def show_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    logger.info(f"[TRADES] User {update.effective_user.id}")
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    # Логируем состояние кэша ДО синхронизации
    cache_before = len(positions_cache.get(user_id, []))
    cache_ids_before = [p.get('id') for p in positions_cache.get(user_id, [])]
    logger.info(f"[TRADES] Cache BEFORE sync: {cache_before} positions, IDs: {cache_ids_before}")
    
    # Синхронизация с Bybit при обновлении
    synced = await sync_bybit_positions(user_id, context)
    if synced > 0:
        logger.info(f"[TRADES] Synced {synced} positions from Bybit")
    
    # Логируем состояние кэша ПОСЛЕ синхронизации
    cache_after = len(positions_cache.get(user_id, []))
    cache_ids_after = [p.get('id') for p in positions_cache.get(user_id, [])]
    logger.info(f"[TRADES] Cache AFTER sync: {cache_after} positions, IDs: {cache_ids_after}")
    
    user_positions = get_positions(user_id)
    
    # Статистика побед - по ВСЕМ сделкам, не только последним 20
    stats = db_get_user_stats(user_id)
    wins = stats['wins']
    total_trades = stats['total']
    winrate = stats['winrate']
    total_profit = user.get('total_profit', 0)
    profit_str = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
    
    if not user_positions:
        text = f"""<b>💼 Нет позиций</b>

{wins}/{total_trades} ({winrate}%)

💰 ${user['balance']:.0f}"""
        
        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data="back"), InlineKeyboardButton("🔄 Обновить", callback_data="trades")]
        ]
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        except BadRequest:
            pass  # Сообщение не изменилось
        return
    
    # Стакаем одинаковые позиции для отображения
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
        
        # Показываем количество стакнутых позиций
        stack_info = f" x{pos['stacked_count']}" if pos.get('stacked_count', 1) > 1 else ""
        
        # Определяем какой TP активен
        tp1_hit = pos.get('tp1_hit', False)
        tp2_hit = pos.get('tp2_hit', False)
        if tp2_hit:
            tp_status = "TP3 🎯"
            current_tp = pos.get('tp3', pos['tp'])
        elif tp1_hit:
            tp_status = "TP2"
            current_tp = pos.get('tp2', pos['tp'])
        else:
            tp_status = "TP1"
            current_tp = pos.get('tp1', pos['tp'])
        
        text += f"<b>{ticker}</b> | {dir_text} | ${pos['amount']:.0f} | x{LEVERAGE}{stack_info} {emoji}\n"
        text += f"{format_price(current)} → {tp_status}: {format_price(current_tp)} | SL: {format_price(pos['sl'])}\n"
        text += f"PnL: {pnl_str}\n\n"
        
        # Для стакнутых позиций передаём все ID через запятую
        if pos.get('position_ids'):
            close_data = f"closestack_{','.join(str(pid) for pid in pos['position_ids'])}"
        else:
            close_data = f"close_{pos['id']}"
        
        keyboard.append([InlineKeyboardButton(f"❌ Закрыть {ticker}", callback_data=close_data)])
    
    # Общий PnL
    total_pnl = sum(p.get('pnl', 0) for p in user_positions)
    total_pnl_str = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"
    
    text += f"""───
Всего: <b>{total_pnl_str}</b>

💰 ${user['balance']:.2f} | {wins}/{total_trades} ({winrate}%)"""
    
    # Кнопка закрыть все (если больше 1 позиции)
    if len(user_positions) > 0:
        keyboard.append([InlineKeyboardButton("❌ Закрыть все", callback_data="close_all")])
    
    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="trades")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    except BadRequest:
        pass  # Сообщение не изменилось

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

def calculate_auto_bet(confidence: float, balance: float, atr_percent: float = 0) -> tuple:
    """
    Рассчитать размер ставки и плечо на основе уверенности и волатильности (ATR)
    
    Стратегия: 
    - Базовый размер от уверенности
    - Корректировка на волатильность (высокий ATR = меньше позиция)
    - Профессиональный риск-менеджмент
    
    Returns:
        (bet_amount, leverage)
    """
    # Базовое плечо (фиксированное для предсказуемости)
    leverage = LEVERAGE  # Используем глобальное плечо
    
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
    
    # Применяем корректировку
    bet_percent = bet_percent * volatility_multiplier
    
    bet = balance * bet_percent
    
    # Ограничения
    bet = max(AUTO_TRADE_MIN_BET, min(AUTO_TRADE_MAX_BET, bet))
    
    # Не ставить больше 20% баланса за раз (защита от слива)
    bet = min(bet, balance * 0.20)
    
    logger.info(f"[BET] Confidence={confidence}%, ATR={atr_percent:.2f}%, vol_mult={volatility_multiplier}, bet=${bet:.0f}")
    
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
    
    # #region agent log
    debug_log("B", "send_smart_signal:entry", "Smart signal job started", {"time": datetime.now().isoformat()})
    # #endregion
    
    logger.info("[SMART] ========== Smart Signal v2.0 ==========")
    
    # Получаем активных юзеров
    rows = run_sql("SELECT user_id, balance FROM users WHERE trading = 1 AND balance >= ?", (MIN_DEPOSIT,), fetch="all")
    active_users = [row['user_id'] for row in rows] if rows else []
    
    # #region agent log
    all_users_debug = run_sql("SELECT user_id, balance, trading FROM users LIMIT 10", fetch="all")
    debug_log("B", "send_smart_signal:db_query", "DB users query", {"MIN_DEPOSIT": MIN_DEPOSIT, "rows_found": len(rows) if rows else 0, "all_users_sample": [dict(u) for u in all_users_debug] if all_users_debug else []})
    # #endregion
    
    # Проверяем авто-трейд
    has_auto_trade = False
    auto_balance = 0
    if AUTO_TRADE_USER_ID and AUTO_TRADE_USER_ID != 0:
        auto_user_check = get_user(AUTO_TRADE_USER_ID)
        has_auto_trade = auto_user_check.get('auto_trade', False)
        auto_balance = auto_user_check.get('balance', 0)
    
    # #region agent log
    debug_log("B", "send_smart_signal:users_check", "Checking active users", {"active_users_count": len(active_users), "active_user_ids": [u.get('id') for u in active_users], "has_auto_trade": has_auto_trade, "AUTO_TRADE_USER_ID": AUTO_TRADE_USER_ID})
    # #endregion
    
    if not active_users and not has_auto_trade:
        logger.info("[SMART] Нет активных юзеров")
        return
    
    # Проверяем состояние торговли
    trading_state = get_trading_state()
    if trading_state['is_paused']:
        logger.info(f"[SMART] Торговля на паузе до {trading_state['pause_until']}")
        return
    
    logger.info(f"[SMART] Активных: {len(active_users)}, Авто-трейд: {'ВКЛ' if has_auto_trade else 'ВЫКЛ'}")
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
        
        # === АВТО-ТОРГОВЛЯ ===
        auto_trade_executed = False
        
        # #region agent log
        debug_log("B", "send_smart_signal:auto_trade_check", "Checking auto-trade conditions", {"has_auto_trade": has_auto_trade, "AUTO_TRADE_USER_ID": AUTO_TRADE_USER_ID})
        # #endregion
        
        if has_auto_trade and AUTO_TRADE_USER_ID:
            auto_user = get_user(AUTO_TRADE_USER_ID)
            auto_balance = auto_user.get('balance', 0)
            
            # Проверяем настройки пользователя
            user_auto_enabled = auto_user.get('auto_trade', False)
            user_min_winrate = auto_user.get('auto_trade_min_winrate', 70)
            user_max_daily = auto_user.get('auto_trade_max_daily', 10)
            user_today_count = auto_user.get('auto_trade_today', 0)
            
            # #region agent log
            debug_log("B", "send_smart_signal:auto_trade_settings", "Auto-trade settings", {"user_auto_enabled": user_auto_enabled, "auto_balance": auto_balance, "user_min_winrate": user_min_winrate, "user_max_daily": user_max_daily, "user_today_count": user_today_count, "confidence_percent": confidence_percent})
            # #endregion
            
            # Сброс счётчика
            from datetime import date as dt_date
            today = dt_date.today().isoformat()
            last_reset = auto_user.get('auto_trade_last_reset')
            if last_reset != today:
                user_today_count = 0
                auto_user['auto_trade_today'] = 0
                auto_user['auto_trade_last_reset'] = today
                db_update_user(AUTO_TRADE_USER_ID, auto_trade_today=0, auto_trade_last_reset=today)
            
            # Проверки
            skip_reason = None
            if not user_auto_enabled:
                skip_reason = "выключен"
            elif confidence_percent < user_min_winrate:
                skip_reason = f"confidence {confidence_percent}% < {user_min_winrate}%"
            elif user_today_count >= user_max_daily:
                skip_reason = f"лимит {user_today_count}/{user_max_daily}"
            elif auto_balance < AUTO_TRADE_MIN_BET:
                skip_reason = f"баланс ${auto_balance:.0f}"
            
            if skip_reason:
                logger.info(f"[SMART] Авто-трейд пропущен: {skip_reason}")
            else:
                # === VALIDATION FOR AUTO-TRADE ===
                auto_positions = get_positions(AUTO_TRADE_USER_ID)
                
                # Check max positions
                if len(auto_positions) >= MAX_POSITIONS_PER_USER:
                    logger.info(f"[SMART] Авто-трейд пропущен: лимит позиций ({len(auto_positions)})")
                    skip_reason = f"лимит позиций {len(auto_positions)}/{MAX_POSITIONS_PER_USER}"
                
                # Validate symbol
                valid, error = validate_symbol(symbol)
                if not valid:
                    logger.warning(f"[SMART] Invalid symbol for auto-trade: {symbol}")
                    skip_reason = f"invalid symbol: {error}"
                
                if skip_reason:
                    logger.info(f"[SMART] Авто-трейд пропущен (validation): {skip_reason}")
                
                if not skip_reason:
                    # Расчёт ставки на основе качества сетапа (только A+ и A)
                    quality_mult = {
                        SetupQuality.A_PLUS: 0.12,  # 12% баланса для идеального сетапа
                        SetupQuality.A: 0.10,        # 10% для отличного сетапа
                    }.get(setup.quality, 0.08)
                    
                    auto_bet = min(AUTO_TRADE_MAX_BET, max(AUTO_TRADE_MIN_BET, auto_balance * quality_mult))
                    auto_bet = min(auto_bet, auto_balance * 0.15)  # Не более 15% баланса
                    
                    # Ensure minimum balance reserve
                    if auto_balance - auto_bet < MIN_BALANCE_RESERVE:
                        auto_bet = max(0, auto_balance - MIN_BALANCE_RESERVE)
                        if auto_bet < AUTO_TRADE_MIN_BET:
                            logger.info(f"[SMART] Авто-трейд пропущен: недостаточно для минимальной ставки с резервом")
                            skip_reason = "недостаточно баланса с учётом резерва"
                    
                    ticker = symbol.split("/")[0]
                    
                    # === ОТКРЫТИЕ НА BYBIT ===
                    bybit_qty = 0
                    hedging_enabled = await is_hedging_enabled()
                    bybit_success = True
                    
                    if hedging_enabled:
                        hedge_amount = float(auto_bet * LEVERAGE)
                        hedge_result = await hedge_open(0, symbol, direction, hedge_amount, 
                                                       sl=float(sl), tp1=float(tp1), tp2=float(tp2), tp3=float(tp3))
                        
                        if hedge_result:
                            bybit_qty = hedge_result.get('qty', 0)
                            logger.info(f"[SMART] ✓ Bybit открыт: qty={bybit_qty}")
                            
                            # Верификация
                            await asyncio.sleep(0.5)
                            bybit_pos = await hedger.get_position_data(symbol)
                            if not bybit_pos or bybit_pos.get('size', 0) == 0:
                                logger.error("[SMART] ❌ Bybit не подтвердил позицию")
                                bybit_success = False
                            else:
                                increment_bybit_opened()
                        else:
                            logger.error("[SMART] ❌ Bybit ошибка")
                            bybit_success = False
                    
                    if bybit_success:
                        # Комиссия
                        commission = auto_bet * (COMMISSION_PERCENT / 100)
                        auto_user['balance'] -= auto_bet
                        auto_user['balance'] = sanitize_balance(auto_user['balance'])  # Security: ensure valid balance
                        new_balance = auto_user['balance']
                        save_user(AUTO_TRADE_USER_ID)
                        await add_commission(commission)
                        
                        # Создаём позицию
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
                            'bybit_qty': bybit_qty,
                            'original_amount': float(auto_bet)
                        }
                        
                        pos_id = db_add_position(AUTO_TRADE_USER_ID, position)
                        position['id'] = pos_id
                        
                        if AUTO_TRADE_USER_ID not in positions_cache:
                            positions_cache[AUTO_TRADE_USER_ID] = []
                        positions_cache[AUTO_TRADE_USER_ID].append(position)
                        
                        # Уведомление
                        reasoning_text = "\n".join([f"• {r}" for r in setup.reasoning[:4]])
                        warnings_text = "\n".join([f"• {w}" for w in setup.warnings[:2]]) if setup.warnings else ""
                        
                        auto_msg = f"""<b>🤖 {quality_emoji} | Smart Trade</b>

<b>{ticker}</b> | {direction} | ${auto_bet:.0f} | x{LEVERAGE}
{regime_text}

<b>Анализ:</b>
{reasoning_text}

<b>Вход:</b> {format_price(entry)}
<b>TP1:</b> {format_price(tp1)} (+{tp1_percent:.1f}%) — 50%
<b>TP2:</b> {format_price(tp2)} (+{tp2_percent:.1f}%) — 30%
<b>TP3:</b> {format_price(tp3)} (+{tp3_percent:.1f}%) — 20%
<b>SL:</b> {format_price(sl)} (-{sl_percent:.1f}%)
<b>R/R:</b> 1:{setup.risk_reward:.1f}"""
                        
                        if warnings_text:
                            auto_msg += f"\n\n⚠️ <b>Риски:</b>\n{warnings_text}"
                        
                        auto_msg += f"\n\n💰 ${new_balance:.0f}"
                        
                        auto_keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton(f"❌ Закрыть {ticker}", callback_data=f"close_symbol|{symbol}"),
                             InlineKeyboardButton("📊 Сделки", callback_data="trades")]
                        ])
                        
                        await context.bot.send_message(AUTO_TRADE_USER_ID, auto_msg, parse_mode="HTML", reply_markup=auto_keyboard)
                        logger.info(f"[SMART] ✓ Авто-сделка: {direction} {ticker} ${auto_bet:.0f}")
                        auto_trade_executed = True
                        
                        # Обновляем счётчики
                        auto_user['auto_trade_today'] = user_today_count + 1
                        db_update_user(AUTO_TRADE_USER_ID, auto_trade_today=user_today_count + 1)
        
        # === ОТПРАВКА АКТИВНЫМ ЮЗЕРАМ ===
        for user_id in active_users:
            if user_id == AUTO_TRADE_USER_ID and auto_trade_executed:
                continue
            
            user = get_user(user_id)
            balance = user['balance']
            
            if balance < 1:
                continue
            
            ticker = symbol.split("/")[0]
            d = 'L' if direction == "LONG" else 'S'
            
            # Форматируем анализ
            reasoning_text = "\n".join([f"• {r}" for r in setup.reasoning[:3]])
            
            text = f"""<b>📡 Smart Signal</b>

<b>{ticker}</b> | {direction} | x{LEVERAGE}
{quality_emoji} | {regime_text}

<b>Анализ:</b>
{reasoning_text}

<b>Вход:</b> {format_price(entry)}
<b>TP1:</b> {format_price(tp1)} (+{tp1_percent:.1f}%)
<b>TP2:</b> {format_price(tp2)} (+{tp2_percent:.1f}%)
<b>TP3:</b> {format_price(tp3)} (+{tp3_percent:.1f}%)
<b>SL:</b> {format_price(sl)} (-{sl_percent:.1f}%)
<b>R/R:</b> 1:{setup.risk_reward:.1f}

💰 ${balance:.0f}"""
            
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
                logger.info(f"[SMART] Sent to {user_id}")
            except Exception as e:
                logger.error(f"[SMART] Error sending to {user_id}: {e}")
    
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
    
    # #region agent log
    debug_log("E", "enter_trade:entry", "User entering trade", {"user_id": user_id, "balance": user.get('balance'), "positions_count": len(user_positions), "callback_data": query.data[:100]})
    # #endregion

    # e|SYM|D|ENTRY|SL|TP1|TP2|TP3|AMT|WINRATE
    data = query.data.split("|")
    if len(data) < 7:
        await query.edit_message_text("❌ Ошибка")
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
        await query.edit_message_text("❌ Ошибка данных")
        return

    # === INPUT VALIDATION ===
    # Validate symbol
    valid, error = validate_symbol(symbol)
    if not valid:
        await query.edit_message_text(f"❌ {error}")
        logger.warning(f"[SECURITY] User {user_id}: Invalid symbol {symbol}")
        return
    
    # Validate direction
    valid, error = validate_direction(direction)
    if not valid:
        await query.edit_message_text(f"❌ {error}")
        logger.warning(f"[SECURITY] User {user_id}: Invalid direction {direction}")
        return
    
    # Validate amount
    valid, error = validate_amount(amount, user['balance'])
    if not valid:
        await query.edit_message_text(f"❌ {error}")
        return
    
    # Check max positions limit
    if len(user_positions) >= MAX_POSITIONS_PER_USER:
        await query.edit_message_text(
            f"❌ Лимит позиций ({MAX_POSITIONS_PER_USER})\n\n"
            f"Закройте существующие сделки перед открытием новых."
        )
        logger.info(f"[LIMIT] User {user_id}: Max positions reached ({len(user_positions)})")
        return

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
    await query.edit_message_text(f"<b>⏳ Открываем</b>\n\n{ticker} | {direction} | ${amount:.0f}", parse_mode="HTML")

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
    user['balance'] -= amount
    user['balance'] = sanitize_balance(user['balance'])  # Security: ensure non-negative
    save_user(user_id)  # Сохраняем в БД

    # Добавляем комиссию в накопитель (авто-вывод)
    await add_commission(commission)

    # === ПРОВЕРЯЕМ ЕСТЬ ЛИ УЖЕ ПОЗИЦИЯ С ТАКИМ СИМВОЛОМ И НАПРАВЛЕНИЕМ ===
    existing = None
    for p in user_positions:
        if p['symbol'] == symbol and p['direction'] == direction:
            existing = p
            break

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
            'original_amount': amount  # Для расчёта частичных закрытий
        }

        pos_id = db_add_position(user_id, position)
        position['id'] = pos_id

        # Обновляем кэш
        if user_id not in positions_cache:
            positions_cache[user_id] = []
        positions_cache[user_id].append(position)
        
        logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount}, TP1={tp1:.4f}, TP2={tp2:.4f}, TP3={tp3:.4f}")
    
    dir_text = "LONG" if direction == "LONG" else "SHORT"
    tp1_percent = abs(tp1 - entry) / entry * 100
    tp2_percent = abs(tp2 - entry) / entry * 100
    tp3_percent = abs(tp3 - entry) / entry * 100
    sl_percent = abs(sl - entry) / entry * 100
    
    text = f"""<b>✅ Сделка открыта</b>

{ticker} | {dir_text} | ${amount:.0f} | x{LEVERAGE}
Winrate: {winrate}%

<b>Вход:</b> {format_price(entry)}

<b>TP1:</b> {format_price(tp1)} (+{tp1_percent:.1f}%) — 50%
<b>TP2:</b> {format_price(tp2)} (+{tp2_percent:.1f}%) — 30%
<b>TP3:</b> {format_price(tp3)} (+{tp3_percent:.1f}%) — 20%

<b>SL:</b> {format_price(sl)} (-{sl_percent:.1f}%)

💰 ${user['balance']:.0f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def close_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    try:
        pos_id = int(query.data.split("_")[1])
    except (ValueError, IndexError):
        await query.answer("Ошибка данных", show_alert=True)
        return
    
    pos = next((p for p in user_positions if p['id'] == pos_id), None)
    
    if not pos:
        await query.answer("Позиция не найдена", show_alert=True)
        return
    
    ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
    
    # Показываем статус
    await query.edit_message_text(f"<b>⏳ Закрываем {ticker}...</b>", parse_mode="HTML")
    
    # === ХЕДЖИРОВАНИЕ: СНАЧАЛА закрываем на Bybit ===
    close_price = pos.get('current', pos['entry'])
    hedging_enabled = await is_hedging_enabled()
    
    if hedging_enabled:
        bybit_qty = pos.get('bybit_qty', 0)
        if bybit_qty > 0:
            hedge_result = await hedge_close(pos_id, pos['symbol'], pos['direction'], bybit_qty)
            if hedge_result:
                logger.info(f"[HEDGE] ✓ Position {pos_id} closed on Bybit (qty={bybit_qty})")
                
                # Верификация: проверяем что позиция реально закрылась
                await asyncio.sleep(0.5)
                bybit_pos = await hedger.get_position_data(pos['symbol'])
                
                # Получаем реальную цену закрытия с Bybit
                close_side = "Sell" if pos['direction'] == "LONG" else "Buy"
                order_info = await hedger.get_last_order_price(pos['symbol'], close_side)
                if order_info and order_info.get('price'):
                    close_price = order_info['price']
                    logger.info(f"[HEDGE] Real close price: ${close_price:.4f}")
                
                # Если позиция ещё есть на Bybit - возможно частичное закрытие
                if bybit_pos and bybit_pos.get('size', 0) > 0:
                    remaining = bybit_pos['size']
                    logger.warning(f"[HEDGE] ⚠️ Position partially closed, remaining: {remaining}")
            else:
                # Bybit не закрыл - НЕ закрываем в боте
                logger.error(f"[HEDGE] ❌ Failed to close on Bybit - position kept open")
                await query.edit_message_text(
                    f"<b>❌ Ошибка закрытия</b>\n\n"
                    f"Не удалось закрыть позицию на Bybit.\n"
                    f"Позиция сохранена. Попробуйте ещё раз.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
                )
                return
    
    # Пересчитываем PnL с реальной ценой закрытия
    if pos['direction'] == "LONG":
        pnl_percent = (close_price - pos['entry']) / pos['entry']
    else:
        pnl_percent = (pos['entry'] - close_price) / pos['entry']
    pnl = pos['amount'] * LEVERAGE * pnl_percent - pos.get('commission', 0)
    
    returned = pos['amount'] + pnl
    
    user['balance'] = sanitize_balance(user['balance'] + returned)
    user['total_profit'] += pnl
    save_user(user_id)  # Сохраняем в БД
    
    # Закрываем в БД и удаляем из кэша
    db_close_position(pos_id, pos['current'], pnl, 'MANUAL')
    # Явно удаляем из кэша по ID
    if user_id in positions_cache:
        positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id]
    
    pnl_abs = abs(pnl)
    ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
    
    if pnl > 0:
        text = f"""<b>📈 Сделка закрыта</b>

{ticker} | +${pnl_abs:.0f}
Чистая работа.

💰 ${user['balance']:.0f}"""
    elif pnl == 0:
        text = f"""<b>➖ Безубыток</b>

{ticker} | $0
Вышли без потерь.

💰 ${user['balance']:.0f}"""
    else:
        text = f"""<b>📉 Сделка закрыта</b>

{ticker} | -${pnl_abs:.0f}
Часть стратегии.

💰 ${user['balance']:.0f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


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
    
    await query.edit_message_text("⏳ Закрываем позиции...")
    
    ticker = to_close[0]['symbol'].split("/")[0] if "/" in to_close[0]['symbol'] else to_close[0]['symbol']
    
    # === ГРУППИРУЕМ ПО СИМВОЛУ ДЛЯ BYBIT ===
    close_prices = {}  # symbol -> close_price
    failed_closes = []  # Позиции которые не удалось закрыть на Bybit
    hedging_enabled = await is_hedging_enabled()
    
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
                hedge_result = await hedge_close(positions[0]['id'], symbol, direction, total_qty)
                if hedge_result:
                    logger.info(f"[CLOSE_STACKED] Bybit closed {symbol} {direction} qty={total_qty}")
                    
                    # Получаем реальную цену закрытия
                    await asyncio.sleep(0.3)
                    close_side = "Sell" if direction == "LONG" else "Buy"
                    order_info = await hedger.get_last_order_price(symbol, close_side)
                    if order_info and order_info.get('price'):
                        close_prices[(symbol, direction)] = order_info['price']
                        logger.info(f"[CLOSE_STACKED] Real close price {symbol}: ${order_info['price']:.4f}")
                else:
                    # Не удалось закрыть на Bybit - помечаем эти позиции
                    logger.error(f"[CLOSE_STACKED] ❌ Failed to close {symbol} {direction} on Bybit")
                    failed_closes.extend(positions)
    
    # Убираем позиции которые не удалось закрыть на Bybit
    if failed_closes and hedging_enabled:
        to_close = [p for p in to_close if p not in failed_closes]
        if not to_close:
            await query.edit_message_text(
                f"<b>❌ Ошибка закрытия</b>\n\n"
                f"Не удалось закрыть позиции на Bybit.\n"
                f"Позиции сохранены. Попробуйте ещё раз.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
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
        if user_id in positions_cache:
            positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]
    
    # Обновляем баланс
    user['balance'] = sanitize_balance(user['balance'] + total_returned)
    user['total_profit'] += total_pnl
    save_user(user_id)
    
    pnl_abs = abs(total_pnl)
    
    if total_pnl > 0:
        text = f"""<b>📈 Сделки закрыты</b>

{ticker} | +${pnl_abs:.0f}
Закрыто: {len(to_close)}
Чистая работа.

💰 ${user['balance']:.0f}"""
    elif total_pnl == 0:
        text = f"""<b>➖ Безубыток</b>

{ticker} | $0
Закрыто: {len(to_close)}
Вышли без потерь.

💰 ${user['balance']:.0f}"""
    else:
        text = f"""<b>📉 Сделки закрыты</b>

{ticker} | -${pnl_abs:.0f}
Закрыто: {len(to_close)}
Часть стратегии.

💰 ${user['balance']:.0f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def custom_amount_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрос своей суммы"""
    query = update.callback_query
    await query.answer()
    
    # custom|SYM|D|ENTRY|SL|TP1|TP2|TP3|WINRATE (новый формат)
    # custom|SYM|D|ENTRY|SL|TP|WINRATE (старый формат)
    data = query.data.split("|")
    if len(data) < 6:
        await query.edit_message_text("❌ Ошибка")
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
    
    user = get_user(update.effective_user.id)
    
    text = f"""<b>💵 Своя сумма</b>

Минимум: $1
Баланс: ${user['balance']:.2f}

Введи сумму:"""

    keyboard = [
        [InlineKeyboardButton("❌ Отмена", callback_data="skip")],
        [InlineKeyboardButton("🏠 Домой", callback_data="back"), InlineKeyboardButton("📊 Сделки", callback_data="trades")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def handle_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка введённой суммы"""
    if 'pending_trade' not in context.user_data:
        return

    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)

    try:
        amount = float(update.message.text.replace(",", ".").replace("$", "").strip())
    except ValueError:
        await update.message.reply_text("❌ Введи число")
        return

    if amount < 1:
        await update.message.reply_text("❌ Минимум $1")
        return

    if amount > user['balance']:
        await update.message.reply_text(f"❌ Недостаточно средств (${user['balance']:.2f})\n\nВведи другую сумму:")
        return  # pending_trade сохраняется, можно ввести снова

    trade = context.user_data.pop('pending_trade')

    # Выполняем сделку
    symbol = trade['symbol']
    direction = "LONG" if trade['direction'] == 'L' else "SHORT"
    entry = float(trade['entry'])
    sl = float(trade['sl'])
    tp1 = float(trade.get('tp1', trade['tp']))
    tp2 = float(trade.get('tp2', entry + (tp1 - entry) * 2 if direction == "LONG" else entry - (entry - tp1) * 2))
    tp3 = float(trade.get('tp3', entry + (tp1 - entry) * 3.5 if direction == "LONG" else entry - (entry - tp1) * 3.5))
    tp = tp1  # для совместимости
    winrate = int(trade.get('winrate', 75))
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
    user['balance'] -= amount
    user['balance'] = sanitize_balance(user['balance'])  # Security: ensure non-negative
    save_user(user_id)

    # Добавляем комиссию в накопитель (авто-вывод)
    await add_commission(commission)

    # === ПРОВЕРЯЕМ ЕСТЬ ЛИ УЖЕ ПОЗИЦИЯ С ТАКИМ СИМВОЛОМ И НАПРАВЛЕНИЕМ ===
    existing = None
    for p in user_positions:
        if p['symbol'] == symbol and p['direction'] == direction:
            existing = p
            break

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
            'original_amount': amount
        }

        pos_id = db_add_position(user_id, position)
        position['id'] = pos_id

        # Обновляем кэш
        if user_id not in positions_cache:
            positions_cache[user_id] = []
        positions_cache[user_id].append(position)
        
        logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount} x{LEVERAGE} (custom), TP1/2/3={tp1:.4f}/{tp2:.4f}/{tp3:.4f}")
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_text = "LONG" if direction == "LONG" else "SHORT"
    tp1_percent = abs(tp1 - entry) / entry * 100
    tp2_percent = abs(tp2 - entry) / entry * 100
    tp3_percent = abs(tp3 - entry) / entry * 100
    sl_percent = abs(sl - entry) / entry * 100
    
    text = f"""<b>✅ Сделка открыта</b>

{ticker} | {dir_text} | ${amount:.0f} | x{LEVERAGE}
Winrate: {winrate}%

<b>Вход:</b> {format_price(entry)}

<b>TP1:</b> {format_price(tp1)} (+{tp1_percent:.1f}%) — 50%
<b>TP2:</b> {format_price(tp2)} (+{tp2_percent:.1f}%) — 30%
<b>TP3:</b> {format_price(tp3)} (+{tp3_percent:.1f}%) — 20%

<b>SL:</b> {format_price(sl)} (-{sl_percent:.1f}%)

💰 ${user['balance']:.0f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Сделки", callback_data="trades")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def skip_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    logger.info(f"[SKIP] User {update.effective_user.id}")
    
    # Очищаем pending trade если был
    if 'pending_trade' in context.user_data:
        del context.user_data['pending_trade']
    
    await query.answer("Пропущено")
    try:
        await query.message.delete()
    except:
        pass

async def unknown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ловим необработанные callbacks"""
    query = update.callback_query
    logger.warning(f"[UNKNOWN] User {update.effective_user.id}, data: {query.data}")
    await query.answer("Неизвестная команда")

# ==================== ОБНОВЛЕНИЕ ПОЗИЦИЙ ====================
async def update_positions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновление цен и PnL с реальными данными Bybit (если хеджирование) или Binance"""
    
    # #region agent log
    debug_log("C", "update_positions:entry", "Update positions job started", {"positions_cache_users": list(positions_cache.keys()), "total_positions": sum(len(p) for p in positions_cache.values())})
    # #endregion
    
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
            logger.warning(f"[BYBIT_SYNC] Ошибка получения позиций: {e}")
    
    for user_id, user_positions in positions_cache.items():
        user = get_user(user_id)
        
        # === ПРОВЕРКА: позиция закрылась на Bybit? ===
        # Проверяем если данные с Bybit получены успешно (даже если там 0 позиций)
        if bybit_sync_available:
            for pos in user_positions[:]:
                if pos.get('bybit_qty', 0) > 0:
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
                                max_reasonable_pnl = pos['amount'] * LEVERAGE * 0.5
                                current_time_ms = int(asyncio.get_event_loop().time() * 1000)
                                time_diff = (current_time_ms - bybit_time) / 1000 if bybit_time else 999999
                                
                                logger.info(f"[BYBIT_SYNC] Bybit data: pnl=${bybit_pnl:.2f}, time_diff={time_diff:.0f}s, max=${max_reasonable_pnl:.2f}")
                                
                                if abs(bybit_pnl) <= max_reasonable_pnl and time_diff < 120:  # 2 мин для sync
                                    real_pnl = bybit_pnl
                                    exit_price = bybit_exit
                                    reason = "TP" if real_pnl > 0 else "SL"
                                    logger.info(f"[BYBIT_SYNC] Using Bybit PnL: ${real_pnl:.2f}")
                                else:
                                    logger.warning(f"[BYBIT_SYNC] Bybit PnL ${bybit_pnl:.2f} seems wrong, using local ${pos['pnl']:.2f}")
                        except Exception as e:
                            logger.warning(f"[BYBIT_SYNC] Ошибка получения closed PnL: {e}")
                        
                        # Возвращаем деньги пользователю
                        returned = pos['amount'] + real_pnl
                        user['balance'] = sanitize_balance(user['balance'] + returned)
                        user['total_profit'] += real_pnl
                        save_user(user_id)
                        
                        # Закрываем в БД
                        db_close_position(pos['id'], exit_price, real_pnl, f'BYBIT_{reason}')
                        
                        # Явно удаляем из кэша по ID (надёжнее чем remove)
                        pos_id_to_remove = pos['id']
                        if user_id in positions_cache:
                            positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]
                        logger.info(f"[BYBIT_SYNC] Position {pos_id_to_remove} removed from cache, remaining: {len(positions_cache.get(user_id, []))}")
                        
                        # Уведомление
                        pnl_sign = "+" if real_pnl >= 0 else ""
                        pnl_emoji = "✅" if real_pnl >= 0 else "📉"
                        try:
                            await context.bot.send_message(
                                user_id,
                                f"<b>📡 Bybit: позиция закрыта</b>\n\n"
                                f"{ticker} | {pos['direction']} | {reason}\n"
                                f"{pnl_emoji} {pnl_sign}${real_pnl:.2f}\n\n"
                                f"💰 ${user['balance']:.0f}",
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
            
            # PnL - ВСЕГДА рассчитываем локально (Bybit PnL общий для всей позиции, не для отдельной записи бота)
            if pos['direction'] == "LONG":
                pnl_percent = (pos['current'] - pos['entry']) / pos['entry']
            else:
                pnl_percent = (pos['entry'] - pos['current']) / pos['entry']
            pos['pnl'] = pos['amount'] * LEVERAGE * pnl_percent - pos.get('commission', 0)
            
            # Обновляем в БД
            db_update_position(pos['id'], current=pos['current'], pnl=pos['pnl'])
            
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
            
            # #region agent log
            if hit_tp1 or hit_tp2 or hit_tp3 or hit_sl:
                debug_log("C", "update_positions:tp_sl_check", "TP/SL condition detected", {"pos_id": pos['id'], "symbol": pos['symbol'], "direction": pos['direction'], "current": pos['current'], "tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": pos['sl'], "hit_tp1": hit_tp1, "hit_tp2": hit_tp2, "hit_tp3": hit_tp3, "hit_sl": hit_sl, "tp1_hit_flag": pos.get('tp1_hit', False), "tp2_hit_flag": pos.get('tp2_hit', False), "pnl": pos['pnl']})
            # #endregion
            
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
                
                # PnL от частичного закрытия
                if pos['direction'] == "LONG":
                    partial_pnl = (pos['current'] - pos['entry']) / pos['entry'] * close_amount * LEVERAGE
                else:
                    partial_pnl = (pos['entry'] - pos['current']) / pos['entry'] * close_amount * LEVERAGE
                
                # Возвращаем часть и профит
                returned = close_amount + partial_pnl
                user['balance'] = sanitize_balance(user['balance'] + returned)
                user['total_profit'] += partial_pnl
                save_user(user_id)
                
                # Обновляем позицию
                pos['amount'] = remaining_amount
                pos['tp1_hit'] = True
                pos['sl'] = pos['entry'] * 1.001 if pos['direction'] == "LONG" else pos['entry'] * 0.999  # SL в безубыток
                pos['tp'] = tp2  # Следующий TP
                
                db_update_position(pos['id'], amount=remaining_amount, sl=pos['sl'], tp=pos['tp'], bybit_qty=pos['bybit_qty'])
                
                # Обновляем SL на Bybit
                if bybit_closed and await is_hedging_enabled():
                    await hedger.set_trading_stop(pos['symbol'].replace("/", ""), pos['direction'], tp=tp2, sl=pos['sl'])
                
                try:
                    await context.bot.send_message(user_id, f"""<b>✅ TP1 достигнут</b>

{ticker} | +${partial_pnl:.1f}
Закрыто 50%, SL → безубыток
Следующая цель: TP2

💰 ${user['balance']:.0f}""", parse_mode="HTML")
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
                
                if pos['direction'] == "LONG":
                    partial_pnl = (pos['current'] - pos['entry']) / pos['entry'] * close_amount * LEVERAGE
                else:
                    partial_pnl = (pos['entry'] - pos['current']) / pos['entry'] * close_amount * LEVERAGE
                
                returned = close_amount + partial_pnl
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
                
                db_update_position(pos['id'], amount=remaining_amount, sl=pos['sl'], tp=pos['tp'], bybit_qty=pos['bybit_qty'])
                
                # Обновляем SL на Bybit
                if bybit_closed and await is_hedging_enabled():
                    await hedger.set_trading_stop(pos['symbol'].replace("/", ""), pos['direction'], tp=tp3, sl=pos['sl'])
                
                try:
                    await context.bot.send_message(user_id, f"""<b>✅ TP2 достигнут</b>

{ticker} | +${partial_pnl:.1f}
Закрыто 80%, moonbag 20%
Цель: TP3

💰 ${user['balance']:.0f}""", parse_mode="HTML")
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
                                    max_reasonable_pnl = pos['amount'] * LEVERAGE * 0.5
                                    current_time_ms = int(asyncio.get_event_loop().time() * 1000)
                                    time_diff = (current_time_ms - bybit_time) / 1000 if bybit_time else 999999
                                    
                                    logger.info(f"[TP3/SL] Bybit data: pnl=${bybit_pnl:.2f}, qty={bybit_qty}, time_diff={time_diff:.0f}s")
                                    
                                    if abs(bybit_pnl) <= max_reasonable_pnl and time_diff < 60:
                                        real_pnl = bybit_pnl
                                        exit_price = bybit_exit
                                        logger.info(f"[TP3/SL] Using Bybit PnL: ${real_pnl:.2f}")
                                    else:
                                        logger.warning(f"[TP3/SL] Bybit PnL ${bybit_pnl:.2f} seems wrong, using local ${pos['pnl']:.2f}")
                            except Exception as e:
                                logger.warning(f"[TP3/SL] Failed to get real PnL: {e}")
                        else:
                            logger.error(f"[HEDGE] ❌ Failed to auto-close on Bybit - skipping local close")
                            continue  # Пропускаем - попробуем в следующем цикле
                
                returned = pos['amount'] + real_pnl
                user['balance'] = sanitize_balance(user['balance'] + returned)
                user['total_profit'] += real_pnl
                save_user(user_id)
                
                reason = 'TP3' if hit_tp3 else 'SL'
                db_close_position(pos['id'], exit_price, real_pnl, reason)
                # Явно удаляем из кэша по ID
                pos_id_to_remove = pos['id']
                if user_id in positions_cache:
                    positions_cache[user_id] = [p for p in positions_cache[user_id] if p.get('id') != pos_id_to_remove]
                
                pnl_abs = abs(real_pnl)
                
                if hit_tp3:
                    text = f"""<b>🎯 TP3 Runner</b>

{ticker} | +${pnl_abs:.2f}
{format_price(pos['entry'])} → {format_price(exit_price)}
Все цели достигнуты!

💰 ${user['balance']:.0f}"""
                elif real_pnl >= 0:
                    text = f"""<b>➖ Безубыток</b>

{ticker} | ${real_pnl:.2f}
Защитный стоп сработал.

💰 ${user['balance']:.0f}"""
                else:
                    text = f"""<b>📉 Stop Loss</b>

{ticker} | -${pnl_abs:.2f}
Стоп отработал.

💰 ${user['balance']:.0f}"""
                
                try:
                    await context.bot.send_message(
                        user_id, text,
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
                    )
                    logger.info(f"[AUTO-CLOSE] User {user_id} {reason} {ticker}: Real PnL=${real_pnl:.2f}, Balance: ${user['balance']:.2f}")
                except Exception as e:
                    logger.error(f"[AUTO-CLOSE] Failed to notify user {user_id}: {e}")

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

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Админ-панель"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
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

async def add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавить баланс пользователю (админ)"""
    admin_id = update.effective_user.id
    
    if admin_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    # /addbalance [user_id] [amount] или /addbalance [amount] (себе)
    if not context.args:
        await update.message.reply_text("Использование:\n/addbalance 100 — себе\n/addbalance 123456 100 — юзеру")
        return
    
    try:
        if len(context.args) == 1:
            target_id = admin_id
            amount = float(context.args[0])
        else:
            target_id = int(context.args[0])
            amount = float(context.args[1])
        
        # Обновляем баланс
        run_sql("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, target_id))
        user = db_get_user(target_id)
        
        if user:
            # Audit log
            audit_log(admin_id, "ADD_BALANCE", f"amount=${amount:.2f}, new_balance=${user['balance']:.2f}", target_user=target_id)
            await update.message.reply_text(f"✅ Добавлено ${amount:.2f} юзеру {target_id}\n💰 Новый баланс: ${user['balance']:.2f}")
        else:
            await update.message.reply_text(f"❌ Юзер {target_id} не найден")
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Неверный формат. Пример: /addbalance 100")

async def commission_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Статус и вывод комиссий (админ)"""
    admin_id = update.effective_user.id
    
    if admin_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    stats = db_get_stats()
    
    text = f"""💰 <b>КОМИССИИ</b>

📊 Всего заработано: <b>${stats['commissions']:.2f}</b>
⏳ В ожидании вывода: <b>${pending_commission:.2f}</b>
🎯 Порог вывода: ${COMMISSION_WITHDRAW_THRESHOLD}

CryptoBot ID: {ADMIN_CRYPTO_ID or '❌ Не настроен'}"""
    
    keyboard = []
    if pending_commission >= 1:
        keyboard.append([InlineKeyboardButton(f"💸 Вывести ${pending_commission:.2f}", callback_data="withdraw_commission")])
    
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

async def withdraw_commission_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback для вывода комиссий"""
    query = update.callback_query
    await query.answer()
    
    admin_id = update.effective_user.id
    if admin_id not in ADMIN_IDS:
        return
    
    amount_to_withdraw = pending_commission
    await query.edit_message_text("⏳ Выводим комиссию...")
    
    success = await withdraw_commission()
    
    if success:
        # Audit log
        audit_log(admin_id, "WITHDRAW_COMMISSION", f"amount=${amount_to_withdraw:.2f}")
        await query.edit_message_text(f"✅ Комиссия выведена на CryptoBot!")
    else:
        await query.edit_message_text("❌ Ошибка вывода. Проверь настройки CRYPTO_BOT_TOKEN и ADMIN_CRYPTO_ID")

async def test_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест генерации SMART сигнала"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    await update.message.reply_text("🔄 Ищу качественный SMART сетап...")
    
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
        await update.message.reply_text(f"❌ Ошибка: {e}")
    finally:
        await smart.close()


async def force_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Принудительная генерация тестового сигнала (игнорирует фильтры качества)"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    await update.message.reply_text("🔧 Генерирую принудительный тестовый сигнал BTC...")
    
    try:
        # Получаем текущую цену BTC
        symbol = "BTC/USDT"
        price = await get_real_price(symbol)
        
        if not price:
            await update.message.reply_text("❌ Не удалось получить цену BTC")
            return
        
        # Генерируем простой сигнал для тестирования
        direction = "LONG"  # Всегда LONG для теста
        atr_percent = 0.5  # 0.5% ATR
        atr = price * (atr_percent / 100)
        
        entry = price
        sl = entry - (atr * 1.5)  # SL 1.5 ATR
        tp1 = entry + (atr * 2.0)  # TP1 2 ATR
        tp2 = entry + (atr * 3.0)  # TP2 3 ATR  
        tp3 = entry + (atr * 4.0)  # TP3 4 ATR
        
        confidence = 75
        risk_reward = 2.0 / 1.5  # ~1.33
        
        text = f"""🔧 <b>FORCE TEST SIGNAL</b>

<b>{symbol}</b> | {direction}
Качество: TEST (игнорирует фильтры)
Confidence: {confidence}%
R/R: 1:{risk_reward:.1f}

<b>Вход:</b> {format_price(entry)}
<b>TP1:</b> {format_price(tp1)} (+{((tp1/entry)-1)*100:.2f}%)
<b>TP2:</b> {format_price(tp2)} (+{((tp2/entry)-1)*100:.2f}%)
<b>TP3:</b> {format_price(tp3)} (+{((tp3/entry)-1)*100:.2f}%)
<b>SL:</b> {format_price(sl)} ({((sl/entry)-1)*100:.2f}%)

⚠️ Это тестовый сигнал для проверки механики бота."""
        
        # Создаём кнопки для входа (сокращаем данные для лимита 64 байт)
        # Используем BTC вместо BTC/USDT и округляем цены
        short_symbol = "BTC"
        e = int(entry)  # Округляем до целых
        s = int(sl)
        t1 = int(tp1)
        t2 = int(tp2)
        t3 = int(tp3)
        
        keyboard = [
            [InlineKeyboardButton(f"✅ LONG $10", callback_data=f"e|{short_symbol}|L|{e}|{s}|{t1}|{t2}|{t3}|10|{confidence}")],
            [InlineKeyboardButton(f"✅ LONG $25", callback_data=f"e|{short_symbol}|L|{e}|{s}|{t1}|{t2}|{t3}|25|{confidence}")],
            [InlineKeyboardButton(f"✅ LONG $50", callback_data=f"e|{short_symbol}|L|{e}|{s}|{t1}|{t2}|{t3}|50|{confidence}")],
            [InlineKeyboardButton("❌ Пропустить", callback_data="skip")]
        ]
        
        await update.message.reply_text(
            text, 
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        # #region agent log
        debug_log("TEST", "force_signal:created", "Force signal created", {"symbol": symbol, "direction": direction, "entry": entry, "sl": sl, "tp1": tp1})
        # #endregion
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


async def whale_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Анализ китов на Hyperliquid: /whale [COIN]"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    if not ADVANCED_FEATURES:
        await update.message.reply_text("❌ Whale tracker не загружен")
        return
    
    coin = context.args[0].upper() if context.args else "BTC"
    
    await update.message.reply_text(f"🐋 Анализирую китов для {coin}...")
    
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
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def memes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сканер мемкоинов: /memes"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    if not ADVANCED_FEATURES:
        await update.message.reply_text("❌ Meme scanner не загружен")
        return
    
    await update.message.reply_text("🔍 Сканирую мемкоины...")
    
    try:
        opportunities = await get_meme_opportunities()
        
        if not opportunities:
            await update.message.reply_text("😴 Нет активных сигналов по мемам")
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
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def market_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Анализ рынка: /market"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    if not ADVANCED_FEATURES:
        await update.message.reply_text("❌ Market analyzer не загружен")
        return
    
    await update.message.reply_text("📊 Анализирую рынок...")
    
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
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def signal_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Статистика генерации сигналов: /signalstats [reset]"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    args = context.args
    
    if args and args[0].lower() == "reset":
        reset_signal_stats()
        await update.message.reply_text("✅ Статистика сброшена")
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


async def autotrade_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Управление авто-торговлей: /autotrade [on|off|status|balance AMOUNT]"""
    global AUTO_TRADE_ENABLED
    
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
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
Баланс: <b>${balance:.0f}</b>
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
        await update.message.reply_text("✅ Авто-торговля ВКЛЮЧЕНА")
    elif cmd == "off":
        AUTO_TRADE_ENABLED = False
        audit_log(user_id, "AUTO_TRADE_TOGGLE", "enabled=False")
        await update.message.reply_text("❌ Авто-торговля ВЫКЛЮЧЕНА")
    elif cmd == "balance" and len(args) > 1:
        try:
            new_balance = float(args[1])
            run_sql("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, AUTO_TRADE_USER_ID))
            # Обновляем кэш
            if AUTO_TRADE_USER_ID in users_cache:
                users_cache[AUTO_TRADE_USER_ID]['balance'] = new_balance
            audit_log(user_id, "SET_AUTO_TRADE_BALANCE", f"balance=${new_balance:.0f}", target_user=AUTO_TRADE_USER_ID)
            await update.message.reply_text(f"✅ Баланс установлен: ${new_balance:.0f}")
        except ValueError:
            await update.message.reply_text("❌ Неверная сумма")
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
        await update.message.reply_text("✅ ВСЯ БД очищена:\n• Позиции\n• История\n• Алерты\n• Статистика")
        logger.info(f"[ADMIN] User {user_id} cleared ALL database")
    else:
        await update.message.reply_text("❌ Неизвестная команда. Используй: on, off, balance AMOUNT, clear")

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Health check endpoint for monitoring"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
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
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    from hedger import hedger
    
    await update.message.reply_text("🔄 Проверяю Bybit...")
    
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
    
    await update.message.reply_text("🔧 BYBIT TEST\n\n" + "\n".join(status))

async def test_hedge(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест открытия/закрытия хеджа"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    await update.message.reply_text("🔄 Тестирую хеджирование на BTC...")
    
    # Пробуем открыть минимальную позицию
    result = await hedge_open(999999, "BTC/USDT", "LONG", 10.0)
    
    if result:
        qty = result.get('qty', 0)
        await update.message.reply_text(f"✅ Хедж ОТКРЫТ!\nOrder ID: {result.get('order_id')}\nQty: {qty}\n\n⏳ Закрываю через 5 сек...")
        await asyncio.sleep(5)
        # Тест: закрываем используя qty из открытия
        close_result = await hedge_close(999999, "BTC/USDT", "LONG", qty if qty > 0 else None)
        if close_result:
            await update.message.reply_text("✅ Хедж ЗАКРЫТ!")
        else:
            await update.message.reply_text("❌ Ошибка закрытия")
    else:
        await update.message.reply_text("❌ Не удалось открыть хедж. Проверь логи Railway.")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Рассылка всем пользователям"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    if not context.args:
        await update.message.reply_text("Использование: /broadcast <сообщение>")
        return
    
    message = " ".join(context.args)
    
    rows = run_sql("SELECT user_id FROM users", fetch="all")
    all_users = [row['user_id'] for row in rows] if rows else []
    
    sent = 0
    failed = 0
    
    for uid in all_users:
        try:
            await context.bot.send_message(uid, f"📢 {message}")
            sent += 1
        except:
            failed += 1
    
    await update.message.reply_text(f"<b>📢 Рассылка</b>\n\n✅ {sent} | ❌ {failed}", parse_mode="HTML")

async def reset_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сброс: закрыть все позиции и установить баланс"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
        return
    
    # /reset [user_id] [balance] или /reset [balance]
    if not context.args:
        await update.message.reply_text("Использование:\n/reset 1500 — себе\n/reset 123456 1500 — юзеру\n/reset all 0 — всем закрыть позиции")
        return
    
    try:
        if context.args[0].lower() == "all":
            # Закрыть все позиции у всех
            run_sql("DELETE FROM positions")
            positions_cache.clear()
            await update.message.reply_text("✅ Все позиции закрыты у всех пользователей")
            return
        
        if len(context.args) == 1:
            target_id = user_id
            balance = float(context.args[0])
        else:
            target_id = int(context.args[0])
            balance = float(context.args[1])
        
        # Закрыть позиции пользователя
        run_sql("DELETE FROM positions WHERE user_id = ?", (target_id,))
        if target_id in positions_cache:
            positions_cache[target_id] = []
        
        # Установить баланс
        db_update_user(target_id, balance=balance)
        if target_id in users_cache:
            users_cache[target_id]['balance'] = balance
        
        await update.message.reply_text(f"✅ Готово!\n\n👤 User: {target_id}\n💰 Баланс: ${balance:.0f}\n📊 Позиции: закрыты")
        
    except (ValueError, IndexError) as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def reset_everything(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Полный сброс ВСЕХ данных: пользователи, позиции, история, кэши"""
    user_id = update.effective_user.id

    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ закрыт")
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
        # Очищаем все таблицы
        run_sql("DELETE FROM positions")
        run_sql("DELETE FROM history")
        run_sql("DELETE FROM users")
        run_sql("DELETE FROM alerts")
        
        # Очищаем кэши
        positions_cache.clear()
        users_cache.clear()
        
        # Сбрасываем статистику сигналов (уже импортировано из smart_analyzer)
        reset_signal_stats()
        
        await update.message.reply_text(
            "✅ <b>Полный сброс выполнен!</b>\n\n"
            "🗑 Удалено:\n"
            "• Все пользователи\n"
            "• Все позиции\n"
            "• Вся история сделок\n"
            "• Все алерты\n"
            "• Все кэши\n"
            "• Статистика сигналов\n\n"
            "Бот готов к работе с нуля.",
            parse_mode="HTML"
        )
        logger.info(f"[ADMIN] Full reset executed by user {user_id}")

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        logger.error(f"[ADMIN] Reset error: {e}")

# ==================== РЕФЕРАЛЬНАЯ КОМАНДА ====================
async def referral_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Реферальная ссылка"""
    user_id = update.effective_user.id
    bot_username = (await context.bot.get_me()).username
    
    ref_count = db_get_referrals_count(user_id)
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    
    text = f"""🤝 Реферальная программа

Приглашай друзей и получай ${REFERRAL_BONUS} за каждого!

📊 Твои рефералы: {ref_count}
💰 Бонус за реферала: ${REFERRAL_BONUS}

🔗 Твоя ссылка:
{ref_link}"""
    
    await update.message.reply_text(text)

# ==================== АЛЕРТЫ КОМАНДЫ ====================
async def alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Создать или показать алерты. /alert BTC 100000 или /alert"""
    user_id = update.effective_user.id
    
    if not context.args or len(context.args) == 0:
        # Показать алерты
        alerts = db_get_user_alerts(user_id)
        if not alerts:
            await update.message.reply_text("<b>🔔 Нет алертов</b>\n\nСоздать: /alert BTC 100000", parse_mode="HTML")
            return
    
        text = "<b>🔔 Алерты</b>\n\n"
        for a in alerts:
            ticker = a['symbol'].split("/")[0] if "/" in a['symbol'] else a['symbol']
            direction = "⬆️" if a['direction'] == 'above' else "⬇️"
            text += f"#{a['id']} {ticker} {direction} ${a['target_price']:,.0f}\n"
        
        text += "\nУдалить: /delalert <id>"
        await update.message.reply_text(text, parse_mode="HTML")
        return
    
    # Создать алерт: /alert BTC 100000
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /alert BTC 100000")
        return
    
    ticker = context.args[0].upper()
    symbol = f"{ticker}/USDT"
    
    try:
        target_price = float(context.args[1].replace(",", ""))
    except ValueError:
        await update.message.reply_text("❌ Неверная цена")
        return
    
    # Получаем текущую цену
    current_price = await get_real_price(symbol)
    if not current_price:
        await update.message.reply_text(f"❌ Не найден {ticker}")
        return
    
    # Определяем направление
    direction = "above" if target_price > current_price else "below"
    
    alert_id = db_add_alert(user_id, symbol, target_price, direction)
    
    emoji = "⬆️" if direction == "above" else "⬇️"
    text = f"""<b>🔔 Алерт создан</b>

{ticker} {emoji} ${target_price:,.0f}
Сейчас: ${current_price:,.2f}"""
    
    await update.message.reply_text(text)

async def delete_alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удалить алерт: /delalert <id>"""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text("Использование: /delalert <id>")
        return
    
    try:
        alert_id = int(context.args[0].replace("#", ""))
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")
        return
    
    if db_delete_alert(alert_id, user_id):
        await update.message.reply_text(f"✅ Алерт #{alert_id} удалён")
    else:
        await update.message.reply_text("❌ Алерт не найден")

async def check_alerts(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job для проверки алертов"""
    alerts = db_get_active_alerts()
    
    if not alerts:
        return
    
    # Группируем по символам
    symbols = set(a['symbol'] for a in alerts)
    prices = {}
    
    for symbol in symbols:
        price = await get_real_price(symbol)
        if price:
            prices[symbol] = price
    
    for alert in alerts:
        symbol = alert['symbol']
        if symbol not in prices:
            continue
        
        current_price = prices[symbol]
        target = alert['target_price']
        direction = alert['direction']
        
        triggered = False
        if direction == 'above' and current_price >= target:
            triggered = True
        elif direction == 'below' and current_price <= target:
            triggered = True
        
        if triggered:
            db_trigger_alert(alert['id'])
            
            ticker = symbol.split("/")[0] if "/" in symbol else symbol
            emoji = "🚀" if direction == 'above' else "📉"
            
            text = f"""<b>🔔 Алерт</b>

{ticker} достиг ${target:,.0f}"""
            
            try:
                await context.bot.send_message(alert['user_id'], text, parse_mode="HTML")
                logger.info(f"[ALERT] Triggered #{alert['id']} for {alert['user_id']}")
            except:
                pass

# ==================== ИСТОРИЯ СДЕЛОК ====================
async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """История сделок пользователя"""
    user_id = update.effective_user.id
    trades = db_get_history(user_id, limit=10)
    
    if not trades:
        await update.message.reply_text("📜 История пуста")
        return
    
    text = "📜 Последние сделки:\n\n"
    for t in trades:
        emoji = "🟢" if t['pnl'] >= 0 else "🔴"
        pnl_str = f"+${t['pnl']:.2f}" if t['pnl'] >= 0 else f"-${abs(t['pnl']):.2f}"
        ticker = t['symbol'].split("/")[0] if "/" in t['symbol'] else t['symbol']
        text += f"{emoji} {ticker} {t['direction']} | {pnl_str} | {t['reason']}\n"
    
    await update.message.reply_text(text)

# ==================== MAIN ====================
def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN not set")
        return
    
    # #region agent log
    debug_log("INIT", "main:startup", "Bot starting", {"bybit_enabled": bool(os.getenv("BYBIT_API_KEY")), "demo_mode": os.getenv("BYBIT_DEMO", ""), "admin_ids": os.getenv("ADMIN_IDS", "")})
    # #endregion
    
    # Load persistent data from DB
    load_pending_commission()
    load_pending_invoices()
    
    app = Application.builder().token(token).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("health", health_check))
    app.add_handler(CommandHandler("addbalance", add_balance))
    app.add_handler(CommandHandler("commission", commission_cmd))
    app.add_handler(CommandHandler("testbybit", test_bybit))
    app.add_handler(CommandHandler("testhedge", test_hedge))
    app.add_handler(CommandHandler("testsignal", test_signal))
    app.add_handler(CommandHandler("forcesignal", force_signal))
    app.add_handler(CommandHandler("signalstats", signal_stats_cmd))
    app.add_handler(CommandHandler("whale", whale_cmd))
    app.add_handler(CommandHandler("memes", memes_cmd))
    app.add_handler(CommandHandler("market", market_cmd))
    app.add_handler(CommandHandler("autotrade", autotrade_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("reset", reset_all))
    app.add_handler(CommandHandler("resetall", reset_everything))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("ref", referral_cmd))
    app.add_handler(CommandHandler("alert", alert_cmd))
    app.add_handler(CommandHandler("delalert", delete_alert_cmd))
    
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
    app.add_handler(CallbackQueryHandler(send_stars_invoice, pattern="^stars_"))
    app.add_handler(CallbackQueryHandler(pay_crypto_menu, pattern="^pay_crypto$"))
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
    app.add_handler(CallbackQueryHandler(start, pattern="^back$"))
    
    # Обработка текста для своей суммы
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_amount))
    
    # Catch-all для неизвестных callbacks
    app.add_handler(CallbackQueryHandler(unknown_callback))
    
    # Jobs
    # Error handler
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.error(f"Exception: {context.error}", exc_info=context.error)
        if update and hasattr(update, 'effective_user'):
            try:
                await context.bot.send_message(
                    update.effective_user.id, 
                    "⚠️ Произошла ошибка. Попробуйте позже."
                )
            except:
                pass
    
    app.add_error_handler(error_handler)
    
    if app.job_queue:
        app.job_queue.run_repeating(update_positions, interval=5, first=5)
        # Интервал 300 сек (5 минут) - качество важнее количества
        app.job_queue.run_repeating(send_smart_signal, interval=120, first=10)  # 2 минуты
        app.job_queue.run_repeating(check_alerts, interval=30, first=15)
        logger.info("[JOBS] JobQueue configured (SMART only, interval=300s)")
    else:
        logger.warning("[JOBS] JobQueue NOT available!")
    
    # Устанавливаем меню команд
    async def post_init(application):
        from telegram import BotCommand
        commands = [
            BotCommand("start", "🏠 Главное меню"),
            BotCommand("history", "📜 История сделок"),
            BotCommand("ref", "👥 Реферальная программа"),
            BotCommand("alert", "🔔 Создать алерт"),
            BotCommand("delalert", "🔕 Удалить алерт"),
            BotCommand("autotrade", "🤖 Авто-трейд настройки"),
            BotCommand("signalstats", "📊 Статистика сигналов"),
        ]
        await application.bot.set_my_commands(commands)
        logger.info("[BOT] Commands menu set")
    
    app.post_init = post_init
    
    logger.info("=" * 40)
    logger.info("BOT STARTED")
    logger.info("=" * 40)
    
    # Graceful shutdown
    import signal as sig
    
    def shutdown(signum, frame):
        logger.info("Shutting down gracefully...")
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
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
