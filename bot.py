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
from telegram.error import BadRequest

from hedger import hedge_open, hedge_close, is_hedging_enabled, hedger

load_dotenv()

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

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
    conn.close()
    db_type = "PostgreSQL" if USE_POSTGRES else f"SQLite ({DB_PATH})"
    logger.info(f"[DB] Initialized: {db_type}")

def db_get_user(user_id: int) -> Dict:
    """Получить пользователя из БД"""
    row = run_sql("SELECT balance, total_deposit, total_profit, trading FROM users WHERE user_id = ?", (user_id,), fetch="one")
    
    if not row:
        run_sql("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        logger.info(f"[DB] New user {user_id} created")
        return {'balance': 100.0, 'total_deposit': 100.0, 'total_profit': 0.0, 'trading': False}
    
    return {
        'balance': row['balance'],
        'total_deposit': row['total_deposit'],
        'total_profit': row['total_profit'],
        'trading': bool(row['trading'])
    }

def db_update_user(user_id: int, **kwargs):
    """Обновить данные пользователя"""
    for key, value in kwargs.items():
        if key == 'trading':
            value = 1 if value else 0
        run_sql(f"UPDATE users SET {key} = ? WHERE user_id = ?", (value, user_id))

def db_get_positions(user_id: int) -> List[Dict]:
    """Получить открытые позиции"""
    return run_sql("SELECT * FROM positions WHERE user_id = ?", (user_id,), fetch="all")

def db_add_position(user_id: int, pos: Dict) -> int:
    """Добавить позицию"""
    if USE_POSTGRES:
        query = """INSERT INTO positions 
            (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id"""
    else:
        query = """INSERT INTO positions 
            (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    
    pos_id = run_sql(query,
        (user_id, pos['symbol'], pos['direction'], pos['entry'], pos['current'],
         pos['sl'], pos['tp'], pos['amount'], pos['commission'], pos.get('pnl', 0)), fetch="id")
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
    
    logger.info(f"[DB] Position {pos_id} closed: {reason}, PnL: ${pnl:.2f}")

def db_get_history(user_id: int, limit: int = 20) -> List[Dict]:
    """Получить историю сделок"""
    return run_sql("SELECT * FROM history WHERE user_id = ? ORDER BY closed_at DESC LIMIT ?", (user_id, limit), fetch="all")

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

def db_add_referral_bonus(referrer_id: int, amount: float):
    """Добавить реферальный бонус"""
    run_sql("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, referrer_id))
    
    # Обновляем кэш
    if referrer_id in users_cache:
        users_cache[referrer_id]['balance'] += amount
    
    logger.info(f"[REF] Bonus ${amount} added to {referrer_id}")

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

# Счётчик накопленных комиссий (в памяти, сбрасывается при выводе)
pending_commission = 0.0

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
    """Добавить комиссию и вывести при достижении порога"""
    global pending_commission
    pending_commission += amount
    
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
            trading=user['trading']
        )

def get_positions(user_id: int) -> List[Dict]:
    """Получить позиции (с кэшированием)"""
    if user_id not in positions_cache:
        positions_cache[user_id] = db_get_positions(user_id)
    return positions_cache[user_id]

# ==================== ГЛАВНЫЙ ЭКРАН ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    # Rate limiting
    if check_rate_limit(user_id):
        if update.callback_query:
            await update.callback_query.answer("⏳ Слишком много запросов", show_alert=True)
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
    
    text = f"""<b>💰 ${balance:.2f}</b>

Торговля: {trading_status}

Включи — получай сделки 75%+ winrate"""
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Выкл' if user['trading'] else '🟢 Вкл'}", callback_data="toggle")],
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
    
    text = f"""<b>💳 Пополнение баланса</b>

<b>Минимум:</b> ${MIN_DEPOSIT}

Выберите способ:"""
    
    keyboard = [
        [InlineKeyboardButton("⭐ Telegram Stars", callback_data="pay_stars")],
        [InlineKeyboardButton("💎 Crypto (USDT/TON)", callback_data="pay_crypto")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def pay_stars_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = """⭐ Пополнение через Stars

Выберите сумму:"""
    
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
            db_add_referral_bonus(referrer_id, REFERRAL_BONUS)
            try:
                await context.bot.send_message(
                    referrer_id,
                    f"🎉 Твой реферал сделал депозит!\nБонус: +${REFERRAL_BONUS}"
                )
            except:
                pass
        
    text = f"""✅ Оплата прошла!

Зачислено: ${usd}
Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

# ==================== CRYPTO ПОПОЛНЕНИЕ ====================
async def pay_crypto_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = """💎 Пополнение через Crypto

Выберите сумму:"""
    
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
            "❌ Crypto-оплата временно недоступна.",
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
        
        # Сохраняем invoice_id для проверки
        if 'pending_invoices' not in context.bot_data:
            context.bot_data['pending_invoices'] = {}
        context.bot_data['pending_invoices'][invoice['invoice_id']] = {
            'user_id': user_id,
            'amount': amount
        }
        
        text = f"""💎 Оплата ${amount} USDT

Нажмите кнопку для оплаты:"""
        
        keyboard = [
            [InlineKeyboardButton("💳 Оплатить", url=invoice['bot_invoice_url'])],
            [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_{invoice['invoice_id']}")],
            [InlineKeyboardButton("🔙 Отмена", callback_data="deposit")]
        ]
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        
    except Exception as e:
        logger.error(f"[CRYPTO] Error: {e}")
        await query.edit_message_text(
            "❌ Ошибка создания платежа.",
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
    
    pending = context.bot_data.get('pending_invoices', {})
    if invoice_id not in pending:
        await query.answer("Платёж не найден", show_alert=True)
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
            info = pending.pop(invoice_id)
            user_id = info['user_id']
            amount = info['amount']
            
            user = get_user(user_id)
            is_first_deposit = user['total_deposit'] == 100
            
            user['balance'] += amount
            user['total_deposit'] += amount
            save_user(user_id)
            
            logger.info(f"[CRYPTO] User {user_id} deposited ${amount}")
            
            # Реферальный бонус
            if is_first_deposit:
                referrer_id = db_get_referrer(user_id)
                if referrer_id:
                    db_add_referral_bonus(referrer_id, REFERRAL_BONUS)
                    try:
                        await context.bot.send_message(
                            referrer_id,
                            f"🎉 Твой реферал сделал депозит!\nБонус: +${REFERRAL_BONUS}"
                        )
                    except:
                        pass
            
            text = f"""✅ Оплата получена!

Зачислено: ${amount}
Баланс: ${user['balance']:.2f}"""
            
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
        await query.answer(f"❌ Недостаточно баланса!\n\nМинимум для торговли: ${MIN_DEPOSIT}\nВаш баланс: ${user['balance']:.2f}", show_alert=True)
        return
    
    new_state = not user['trading']
    user['trading'] = new_state
    
    # Сохраняем напрямую в БД
    db_update_user(user_id, trading=new_state)
    logger.info(f"[TOGGLE] User {user_id} trading = {new_state}")
    
    await start(update, context)

async def show_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    logger.info(f"[TRADES] User {update.effective_user.id}")
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = get_positions(user_id)
    
    # Статистика побед
    user_history = db_get_history(user_id)
    wins = len([t for t in user_history if t['pnl'] > 0])
    total_trades = len(user_history)
    winrate = int((wins / total_trades * 100)) if total_trades > 0 else 0
    total_profit = user.get('total_profit', 0)
    profit_str = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
    
    if not user_positions:
        text = f"""<b>💼 Позиции</b>

Нет сделок

💰 ${user['balance']:.0f} | {wins}/{total_trades} ({winrate}%)"""
        
        keyboard = [
            [InlineKeyboardButton("🔄", callback_data="trades"), InlineKeyboardButton("🔙", callback_data="back")]
        ]
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
        except BadRequest:
            pass  # Сообщение не изменилось
        return
    
    text = "<b>💼 Позиции</b>\n\n"
    
    keyboard = []
    for pos in user_positions:
        pnl = pos.get('pnl', 0)
        emoji = "🟢" if pnl >= 0 else "🔴"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
        dir_text = "L" if pos['direction'] == "LONG" else "S"
        current = pos.get('current', pos['entry'])
        
        text += f"<b>{ticker}</b> {dir_text} ${pos['amount']:.0f} {emoji}\n"
        text += f"📍 {format_price(current)} | TP: {format_price(pos['tp'])} | SL: {format_price(pos['sl'])}\n"
        text += f"PNL: {pnl_str}\n\n"
        keyboard.append([InlineKeyboardButton(f"❌ Закрыть {ticker}", callback_data=f"close_{pos['id']}")])
    
    text += f"""───────────────
💰 ${user['balance']:.2f} | {wins}/{total_trades} ({winrate}%)"""
    
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

async def send_signal(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка сигнала с реальной аналитикой"""
    from analyzer import MarketAnalyzer
    
    # Получаем активных юзеров из БД (не из кэша!)
    rows = run_sql("SELECT user_id, balance FROM users WHERE trading = 1 AND balance >= ?", (MIN_DEPOSIT,), fetch="all")
    active_users = [row['user_id'] for row in rows] if rows else []
    
    if not active_users:
        logger.info("[SIGNAL] Нет активных юзеров с включённой торговлей")
        return
    
    logger.info(f"[SIGNAL] Активных юзеров: {len(active_users)}")
    
    # Анализируем несколько пар (топ волатильные)
    symbols = [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
        "XRP/USDT", "DOGE/USDT", "AVAX/USDT", "LINK/USDT",
        "MATIC/USDT", "ARB/USDT", "OP/USDT", "APT/USDT"
    ]
    
    analyzer = MarketAnalyzer()
    best_signal = None
    
    try:
        # Ищем лучший сигнал
        for symbol in symbols:
            analysis = await analyzer.analyze_signal(symbol)
            if analysis:
                if best_signal is None or analysis['confidence'] > best_signal['confidence']:
                    best_signal = analysis
        
        if not best_signal:
            logger.info("[SIGNAL] Нет качественных сигналов")
            return
        
        # Получаем Entry, SL, TP
        price_data = await analyzer.calculate_entry_price(
            best_signal['symbol'], 
            best_signal['direction'],
            best_signal
        )
        
        symbol = best_signal['symbol']
        direction = best_signal['direction']
        entry = price_data['entry_price']
        sl = price_data['stop_loss']
        tp = price_data['take_profit']
        winrate = int(price_data['success_rate'])
        
        # === ПРОВЕРКА НА ДУБЛИКАТ СИГНАЛА ===
        now = datetime.now()
        if symbol in last_signals:
            last = last_signals[symbol]
            time_diff = (now - last['time']).total_seconds()
            price_diff = abs(entry - last['price']) / last['price']
            
            # Пропускаем если: тот же символ + направление + <5 мин + цена не изменилась на 0.5%+
            if (last['direction'] == direction and 
                time_diff < SIGNAL_COOLDOWN and 
                price_diff < PRICE_CHANGE_THRESHOLD):
                logger.info(f"[SIGNAL] Пропуск дубликата: {symbol} {direction} (прошло {time_diff:.0f}с, изменение {price_diff*100:.2f}%)")
                return
        
        # Сохраняем этот сигнал
        last_signals[symbol] = {
            'direction': direction,
            'price': entry,
            'time': now
        }
        
        # Потенциальный профит
        if direction == "LONG":
            potential_profit = ((tp - entry) / entry) * 100
        else:
            potential_profit = ((entry - tp) / entry) * 100
        
    finally:
        await analyzer.close()
    
    # Отправляем активным юзерам
    for user_id in active_users:
        user = get_user(user_id)
        balance = user['balance']
        
        if balance < 1:
            continue
        
        ticker = symbol.split("/")[0]
        d = 'L' if direction == "LONG" else 'S'
        dir_emoji = "🟢" if direction == "LONG" else "🔴"
        dir_text = "LONG" if direction == "LONG" else "SHORT"
        
        # Формат сигнала с TP/SL и плечом
        tp_percent = abs(tp - entry) / entry * 100
        sl_percent = abs(sl - entry) / entry * 100
        
        text = f"""🎯 <b>{winrate}%</b> | {ticker} {dir_text} x{LEVERAGE}

💵 Вход: <b>{format_price(entry)}</b>
✅ TP: {format_price(tp)} (+{tp_percent:.1f}%)
🛡 SL: {format_price(sl)} (-{sl_percent:.1f}%)

💰 ${balance:.0f}"""
        
        # Кнопки с суммами - включая малые для низких балансов
        if balance >= 100:
            amounts = [10, 25, 50, 100]
        elif balance >= 25:
            amounts = [5, 10, 25]
        elif balance >= 10:
            amounts = [3, 5, 10]
        else:
            amounts = [1, 2, 3]
        
        amounts = [a for a in amounts if a <= balance]
        
        # Форматируем цены с нужной точностью (не int для дешёвых монет!)
        entry_str = f"{entry:.4f}" if entry < 100 else f"{entry:.0f}"
        sl_str = f"{sl:.4f}" if sl < 100 else f"{sl:.0f}"
        tp_str = f"{tp:.4f}" if tp < 100 else f"{tp:.0f}"
        
        keyboard = []
        if amounts:
            row = [InlineKeyboardButton(f"${amt}", callback_data=f"e|{symbol}|{d}|{entry_str}|{sl_str}|{tp_str}|{amt}|{winrate}") for amt in amounts[:4]]
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("💵 Своя сумма", callback_data=f"custom|{symbol}|{d}|{entry_str}|{sl_str}|{tp_str}|{winrate}")])
        keyboard.append([InlineKeyboardButton("❌ Пропустить", callback_data="skip")])
        
        try:
            await context.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            logger.info(f"[SIGNAL] Sent {direction} {ticker} @ ${entry:.2f} (WR={winrate}%) to {user_id}")
        except Exception as e:
            logger.error(f"[SIGNAL] Error sending to {user_id}: {e}")

async def enter_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    # e|SYM|D|ENTRY|SL|TP|AMT|WINRATE
    data = query.data.split("|")
    if len(data) < 7:
        await query.edit_message_text("❌ Ошибка")
        return
    
    try:
        symbol = data[1]
        direction = "LONG" if data[2] == 'L' else "SHORT"
        entry = float(data[3])
        sl = float(data[4])
        tp = float(data[5])
        amount = float(data[6])
        winrate = int(data[7]) if len(data) > 7 else 75
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Ошибка данных")
        return
    
    # Проверка баланса
    if user['balance'] < amount:
        await query.answer("Недостаточно средств", show_alert=True)
        return
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
    
    # === ПОКАЗЫВАЕМ "ОТКРЫВАЕМ..." ===
    await query.edit_message_text(f"⏳ Открываем {dir_emoji} {ticker} на ${amount:.0f}...")
    
    # Комиссия за открытие
    commission = amount * (COMMISSION_PERCENT / 100)
    user['balance'] -= amount
    save_user(user_id)  # Сохраняем в БД
    
    # Добавляем комиссию в накопитель (авто-вывод)
    await add_commission(commission)
    
    position = {
        'symbol': symbol,
        'direction': direction,
        'amount': amount,
        'entry': entry,
        'current': entry,
        'sl': sl,
        'tp': tp,
        'pnl': -commission,
        'commission': commission
    }
    
    pos_id = db_add_position(user_id, position)
    position['id'] = pos_id
    
    # Обновляем кэш
    if user_id not in positions_cache:
        positions_cache[user_id] = []
    positions_cache[user_id].append(position)
    
    # === ХЕДЖИРОВАНИЕ: открываем реальную позицию на Bybit с TP/SL ===
    # Bybit сам применит x20 плечо (set_leverage вызывается в hedger)
    hedge_ok = False
    if await is_hedging_enabled():
        hedge_result = await hedge_open(pos_id, symbol, direction, amount, tp=tp, sl=sl)
        if hedge_result:
            hedge_ok = True
            logger.info(f"[HEDGE] ✓ Position {pos_id} hedged on Bybit: {hedge_result}")
        else:
            logger.warning(f"[HEDGE] ✗ Failed to hedge position {pos_id}")
    
    logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount}")
    
    dir_text = "LONG" if direction == "LONG" else "SHORT"
    tp_percent = abs(tp - entry) / entry * 100
    sl_percent = abs(sl - entry) / entry * 100
    
    text = f"""✅ <b>{winrate}%</b> | {ticker} {dir_text} x{LEVERAGE} | ${amount:.0f}

📍 Вход: {format_price(entry)}
✅ TP: {format_price(tp)} (+{tp_percent:.1f}%)
🛡 SL: {format_price(sl)} (-{sl_percent:.1f}%)

💰 Баланс: ${user['balance']:.0f}"""
    
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
    
    # === ХЕДЖИРОВАНИЕ: закрываем позицию на Bybit ===
    if await is_hedging_enabled():
        hedge_result = await hedge_close(pos_id, pos['symbol'], pos['direction'])
        if hedge_result:
            logger.info(f"[HEDGE] ✓ Position {pos_id} closed on Bybit")
        else:
            logger.warning(f"[HEDGE] ✗ Failed to close hedge for position {pos_id}")
    
    # Закрываем с текущим PnL
    pnl = pos.get('pnl', 0)
    returned = pos['amount'] + pnl
    
    user['balance'] += returned
    user['total_profit'] += pnl
    save_user(user_id)  # Сохраняем в БД
    
    # Закрываем в БД и удаляем из кэша
    db_close_position(pos_id, pos['current'], pnl, 'MANUAL')
    user_positions.remove(pos)
    
    pnl_abs = abs(pnl)
    ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
    
    if pnl > 0:
        text = f"""🎉 <b>Поздравляем!</b>

Вы заработали <b>+${pnl_abs:.0f}</b> на {ticker}! 🚀

💰 Баланс: <b>${user['balance']:.0f}</b>"""
    elif pnl == 0:
        text = f"""✅ <b>Сделка закрыта</b>

{ticker}: <b>$0</b> (в безубыток)

💰 Баланс: <b>${user['balance']:.0f}</b>"""
    else:
        text = f"""📉 <b>Сделка закрыта</b>

{ticker}: <b>-${pnl_abs:.0f}</b>

Не расстраивайтесь, следующая будет лучше! 💪
💰 Баланс: <b>${user['balance']:.0f}</b>"""
    
    keyboard = [[InlineKeyboardButton("📊 Новые сигналы", callback_data="back")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def custom_amount_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрос своей суммы"""
    query = update.callback_query
    await query.answer()
    
    # custom|SYM|D|ENTRY|SL|TP|WINRATE
    data = query.data.split("|")
    if len(data) < 6:
        await query.edit_message_text("❌ Ошибка")
        return
    
    # Сохраняем данные сигнала
    context.user_data['pending_trade'] = {
        'symbol': data[1],
        'direction': data[2],
        'entry': data[3],
        'sl': data[4],
        'tp': data[5],
        'winrate': data[6] if len(data) > 6 else '75'
    }
    
    user = get_user(update.effective_user.id)
    
    text = f"""💵 Введи сумму сделки

Минимум: $1
Твой баланс: ${user['balance']:.2f}

Отправь число (например: 15)"""
    
    keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="skip")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка введённой суммы"""
    if 'pending_trade' not in context.user_data:
        return
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    try:
        amount = float(update.message.text.replace(",", ".").replace("$", "").strip())
    except ValueError:
        await update.message.reply_text("❌ Введи число")
        return
    
    if amount < 1:
        await update.message.reply_text("❌ Минимум $1")
        return
    
    if amount > user['balance']:
        await update.message.reply_text(f"❌ Недостаточно средств. Баланс: ${user['balance']:.2f}")
        return
    
    trade = context.user_data.pop('pending_trade')
    
    # Выполняем сделку
    symbol = trade['symbol']
    direction = "LONG" if trade['direction'] == 'L' else "SHORT"
    entry = float(trade['entry'])
    sl = float(trade['sl'])
    tp = float(trade['tp'])
    winrate = int(trade.get('winrate', 75))
    
    # Комиссия за открытие
    commission = amount * (COMMISSION_PERCENT / 100)
    user['balance'] -= amount
    save_user(user_id)
    
    # Добавляем комиссию в накопитель (авто-вывод)
    await add_commission(commission)
    
    position = {
        'symbol': symbol,
        'direction': direction,
        'amount': amount,
        'entry': entry,
        'current': entry,
        'sl': sl,
        'tp': tp,
        'pnl': -commission,
        'commission': commission
    }
    
    pos_id = db_add_position(user_id, position)
    position['id'] = pos_id
    
    # Обновляем кэш
    if user_id not in positions_cache:
        positions_cache[user_id] = []
    positions_cache[user_id].append(position)
    
    # === ХЕДЖИРОВАНИЕ: открываем реальную позицию на Bybit с TP/SL ===
    # Bybit сам применит x20 плечо (set_leverage вызывается в hedger)
    if await is_hedging_enabled():
        hedge_result = await hedge_open(pos_id, symbol, direction, amount, tp=tp, sl=sl)
        if hedge_result:
            logger.info(f"[HEDGE] ✓ Position {pos_id} hedged on Bybit: {hedge_result}")
        else:
            logger.warning(f"[HEDGE] ✗ Failed to hedge position {pos_id}")
    
    logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount} x{LEVERAGE} (custom)")
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_text = "LONG" if direction == "LONG" else "SHORT"
    tp_percent = abs(tp - entry) / entry * 100
    sl_percent = abs(sl - entry) / entry * 100
    
    text = f"""✅ <b>{winrate}%</b> | {ticker} {dir_text} x{LEVERAGE} | ${amount:.0f}

📍 Вход: {format_price(entry)}
✅ TP: {format_price(tp)} (+{tp_percent:.1f}%)
🛡 SL: {format_price(sl)} (-{sl_percent:.1f}%)

💰 Баланс: ${user['balance']:.0f}"""
    
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
    """Обновление цен и PnL с реальными данными Binance"""
    for user_id, user_positions in positions_cache.items():
        user = get_user(user_id)
        
        for pos in user_positions[:]:  # копия для безопасного удаления
            # Получаем реальную цену с Binance
            real_price = await get_cached_price(pos['symbol'])
            
            if real_price:
                pos['current'] = real_price
            else:
                # Фоллбэк на симуляцию если API недоступен
                change = random.uniform(-0.003, 0.004)
                pos['current'] = pos['current'] * (1 + change)
            
            # PnL - берём реальный с Bybit если хеджирование включено
            if await is_hedging_enabled():
                bybit_pnl = await hedger.get_position_pnl(pos['symbol'])
                if bybit_pnl is not None:
                    # Используем реальный PNL с Bybit (учитывает комиссии биржи)
                    pos['pnl'] = bybit_pnl
                else:
                    # Фоллбэк на локальный расчёт
                    if pos['direction'] == "LONG":
                        pnl_percent = (pos['current'] - pos['entry']) / pos['entry']
                    else:
                        pnl_percent = (pos['entry'] - pos['current']) / pos['entry']
                    # PNL с учётом плеча: $500 × x20 × 1% = $100
                    pos['pnl'] = pos['amount'] * LEVERAGE * pnl_percent - pos['commission']
            else:
                # Локальный расчёт без Bybit
                if pos['direction'] == "LONG":
                    pnl_percent = (pos['current'] - pos['entry']) / pos['entry']
                else:
                    pnl_percent = (pos['entry'] - pos['current']) / pos['entry']
                # PNL с учётом плеча: $500 × x20 × 1% = $100
                pos['pnl'] = pos['amount'] * LEVERAGE * pnl_percent - pos['commission']
            
            # Обновляем в БД
            db_update_position(pos['id'], current=pos['current'], pnl=pos['pnl'])
            
            # Автозакрытие по TP/SL
            if pos['direction'] == "LONG":
                hit_tp = pos['current'] >= pos['tp']
                hit_sl = pos['current'] <= pos['sl']
            else:
                hit_tp = pos['current'] <= pos['tp']
                hit_sl = pos['current'] >= pos['sl']
            
            if hit_tp or hit_sl:
                # === ХЕДЖИРОВАНИЕ: закрываем позицию на Bybit ===
                if await is_hedging_enabled():
                    await hedge_close(pos['id'], pos['symbol'], pos['direction'])
                    logger.info(f"[HEDGE] Auto-closed position {pos['id']} on Bybit")
                
                returned = pos['amount'] + pos['pnl']
                user['balance'] += returned
                user['total_profit'] += pos['pnl']
                save_user(user_id)  # Сохраняем баланс в БД
                
                reason = 'TP' if hit_tp else 'SL'
                db_close_position(pos['id'], pos['current'], pos['pnl'], reason)
                user_positions.remove(pos)
                
                pnl_abs = abs(pos['pnl'])
                pnl_str = f"+${pos['pnl']:.2f}" if pos['pnl'] >= 0 else f"-${pnl_abs:.2f}"
                dir_emoji = "🟢 LONG" if pos['direction'] == "LONG" else "🔴 SHORT"
                
                ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
                
                if hit_tp:
                    text = f"""🎉 <b>Take Profit!</b>

Вы заработали <b>+${pnl_abs:.0f}</b> на {ticker}! 🚀

📍 {format_price(pos['entry'])} → {format_price(pos['current'])}
💰 Баланс: <b>${user['balance']:.0f}</b>"""
                elif pnl == 0:
                    text = f"""✅ <b>Сделка закрыта</b>

{ticker}: <b>$0</b> (в безубыток)

💰 Баланс: <b>${user['balance']:.0f}</b>"""
                else:
                    text = f"""📉 <b>Stop Loss</b>

{ticker}: <b>-${pnl_abs:.0f}</b>

Защитили от большего убытка. Следующая будет лучше! 💪
💰 Баланс: <b>${user['balance']:.0f}</b>"""
                
                try:
                    await context.bot.send_message(
                        user_id, text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
                    )
                except:
                    pass

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
        await update.message.reply_text("⛔ Доступ запрещён")
        return
    
    stats = db_get_stats()
    
    text = f"""📊 АДМИН-ПАНЕЛЬ

👥 Пользователи: {stats['users']}
🟢 Активных: {stats['active_traders']}

💰 Общий баланс: ${stats['total_balance']:.2f}
📥 Всего депозитов: ${stats['total_deposits']:.2f}
📈 Общий профит: ${stats['total_profit']:.2f}

📋 Открытых позиций: {stats['open_positions']}
✅ Всего сделок: {stats['total_trades']}
💵 Реализованный P&L: ${stats['realized_pnl']:.2f}

🏦 Заработано комиссий: ${stats['commissions']:.2f}"""
    
    await update.message.reply_text(text)

async def add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавить баланс пользователю (админ)"""
    admin_id = update.effective_user.id
    
    if admin_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён")
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
            await update.message.reply_text(f"✅ Добавлено ${amount:.2f} юзеру {target_id}\n💰 Новый баланс: ${user['balance']:.2f}")
        else:
            await update.message.reply_text(f"❌ Юзер {target_id} не найден")
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Неверный формат. Пример: /addbalance 100")

async def commission_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Статус и вывод комиссий (админ)"""
    admin_id = update.effective_user.id
    
    if admin_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён")
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
    
    if update.effective_user.id not in ADMIN_IDS:
        return
    
    await query.edit_message_text("⏳ Выводим комиссию...")
    
    success = await withdraw_commission()
    
    if success:
        await query.edit_message_text(f"✅ Комиссия выведена на CryptoBot!")
    else:
        await query.edit_message_text("❌ Ошибка вывода. Проверь настройки CRYPTO_BOT_TOKEN и ADMIN_CRYPTO_ID")

async def test_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест генерации сигнала"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён")
        return
    
    await update.message.reply_text("🔄 Генерирую тестовый сигнал...")
    
    from analyzer import MarketAnalyzer
    analyzer = MarketAnalyzer()
    
    try:
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        results = []
        
        for symbol in symbols:
            analysis = await analyzer.analyze_signal(symbol)
            if analysis:
                results.append(f"✅ {symbol}: {analysis['direction']} (conf: {analysis['confidence']:.2%})")
            else:
                results.append(f"❌ {symbol}: Нет сигнала")
        
        # Проверяем активных юзеров
        rows = run_sql("SELECT COUNT(*) as cnt FROM users WHERE trading = 1 AND balance >= ?", (MIN_DEPOSIT,), fetch="one")
        active_count = rows['cnt'] if rows else 0
        
        text = f"""🧪 ТЕСТ СИГНАЛОВ

{chr(10).join(results)}

👥 Активных юзеров: {active_count}
💰 Мин. депозит: ${MIN_DEPOSIT}

Интервал сигналов: 60 сек"""
        
        await update.message.reply_text(text)
    
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
    finally:
        await analyzer.close()

async def test_bybit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тест подключения к Bybit"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён")
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
        await update.message.reply_text("⛔ Доступ запрещён")
        return
    
    await update.message.reply_text("🔄 Тестирую хеджирование на BTC...")
    
    # Пробуем открыть минимальную позицию
    result = await hedge_open(999999, "BTC/USDT", "LONG", 10.0)
    
    if result:
        await update.message.reply_text(f"✅ Хедж ОТКРЫТ!\nOrder ID: {result}\n\n⏳ Закрываю через 5 сек...")
        await asyncio.sleep(5)
        close_result = await hedge_close(999999, "BTC/USDT", "LONG")
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
        await update.message.reply_text("⛔ Доступ запрещён")
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
    
    await update.message.reply_text(f"✅ Отправлено: {sent}\n❌ Ошибок: {failed}")

async def reset_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сброс: закрыть все позиции и установить баланс"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён")
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
            await update.message.reply_text("🔔 У тебя нет активных алертов\n\nСоздать: /alert BTC 100000")
        return
    
        text = "🔔 Твои алерты:\n\n"
        for a in alerts:
            ticker = a['symbol'].split("/")[0] if "/" in a['symbol'] else a['symbol']
            direction = "⬆️" if a['direction'] == 'above' else "⬇️"
            text += f"#{a['id']} {ticker} {direction} ${a['target_price']:,.0f}\n"
        
        text += "\nУдалить: /delalert <id>"
        await update.message.reply_text(text)
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
    text = f"""🔔 Алерт создан!

{ticker} {emoji} ${target_price:,.0f}
Сейчас: ${current_price:,.2f}

Уведомим когда цена достигнет цели."""
    
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
            
            text = f"""{emoji} АЛЕРТ!

{ticker} достиг ${target:,.0f}
Сейчас: ${current_price:,.2f}"""
            
            try:
                await context.bot.send_message(alert['user_id'], text)
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
    
    app = Application.builder().token(token).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("addbalance", add_balance))
    app.add_handler(CommandHandler("commission", commission_cmd))
    app.add_handler(CommandHandler("testbybit", test_bybit))
    app.add_handler(CommandHandler("testhedge", test_hedge))
    app.add_handler(CommandHandler("testsignal", test_signal))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("reset", reset_all))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("ref", referral_cmd))
    app.add_handler(CommandHandler("alert", alert_cmd))
    app.add_handler(CommandHandler("delalert", delete_alert_cmd))
    
    # Оплата Stars
    app.add_handler(PreCheckoutQueryHandler(precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    
    # Callbacks
    app.add_handler(CallbackQueryHandler(toggle_trading, pattern="^toggle$"))
    app.add_handler(CallbackQueryHandler(deposit_menu, pattern="^deposit$"))
    app.add_handler(CallbackQueryHandler(pay_stars_menu, pattern="^pay_stars$"))
    app.add_handler(CallbackQueryHandler(send_stars_invoice, pattern="^stars_"))
    app.add_handler(CallbackQueryHandler(pay_crypto_menu, pattern="^pay_crypto$"))
    app.add_handler(CallbackQueryHandler(create_crypto_invoice, pattern="^crypto_\\d+$"))
    app.add_handler(CallbackQueryHandler(check_crypto_payment, pattern="^check_"))
    app.add_handler(CallbackQueryHandler(show_trades, pattern="^(trades|my_positions|refresh_positions)$"))
    app.add_handler(CallbackQueryHandler(enter_trade, pattern="^e\\|"))
    app.add_handler(CallbackQueryHandler(custom_amount_prompt, pattern="^custom\\|"))
    app.add_handler(CallbackQueryHandler(close_trade, pattern="^close_"))
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
        app.job_queue.run_repeating(send_signal, interval=30, first=10)  # Каждые 30 сек (было 60)
        app.job_queue.run_repeating(check_alerts, interval=30, first=15)
        logger.info("[JOBS] JobQueue configured (positions, signals, alerts)")
    else:
        logger.warning("[JOBS] JobQueue NOT available!")
    
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
