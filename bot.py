import logging
import os
import random
import aiohttp
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, PreCheckoutQueryHandler, MessageHandler, filters
from telegram.error import BadRequest

load_dotenv()

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== DATABASE ====================
DB_PATH = os.environ.get("DB_PATH", "bot_data.db")

def init_db():
    """Инициализация SQLite базы"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
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
    logger.info(f"[DB] Initialized: {DB_PATH}")

def db_get_user(user_id: int) -> Dict:
    """Получить пользователя из БД"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT balance, total_deposit, total_profit, trading FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    
    if not row:
        c.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        logger.info(f"[DB] New user {user_id} created")
        row = (100.0, 100.0, 0.0, 0)
    
    conn.close()
    return {
        'balance': row[0],
        'total_deposit': row[1],
        'total_profit': row[2],
        'trading': bool(row[3])
    }

def db_update_user(user_id: int, **kwargs):
    """Обновить данные пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for key, value in kwargs.items():
        if key == 'trading':
            value = 1 if value else 0
        c.execute(f"UPDATE users SET {key} = ? WHERE user_id = ?", (value, user_id))
    
    conn.commit()
    conn.close()

def db_get_positions(user_id: int) -> List[Dict]:
    """Получить открытые позиции"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM positions WHERE user_id = ?", (user_id,))
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows

def db_add_position(user_id: int, pos: Dict) -> int:
    """Добавить позицию"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO positions 
        (user_id, symbol, direction, entry, current, sl, tp, amount, commission, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (user_id, pos['symbol'], pos['direction'], pos['entry'], pos['current'],
         pos['sl'], pos['tp'], pos['amount'], pos['commission'], pos.get('pnl', 0)))
    pos_id = c.lastrowid
    conn.commit()
    conn.close()
    logger.info(f"[DB] Position {pos_id} added for user {user_id}")
    return pos_id

def db_update_position(pos_id: int, **kwargs):
    """Обновить позицию"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for key, value in kwargs.items():
        c.execute(f"UPDATE positions SET {key} = ? WHERE id = ?", (value, pos_id))
    conn.commit()
    conn.close()

def db_close_position(pos_id: int, exit_price: float, pnl: float, reason: str):
    """Закрыть позицию и перенести в историю"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Получаем позицию
    c.execute("SELECT * FROM positions WHERE id = ?", (pos_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return
    
    # Переносим в историю
    c.execute("""INSERT INTO history 
        (user_id, symbol, direction, entry, exit_price, sl, tp, amount, commission, pnl, reason, opened_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (row[1], row[2], row[3], row[4], exit_price, row[6], row[7], row[8], row[9], pnl, reason, row[11]))
    
    # Удаляем из активных
    c.execute("DELETE FROM positions WHERE id = ?", (pos_id,))
    
    conn.commit()
    conn.close()
    logger.info(f"[DB] Position {pos_id} closed: {reason}, PnL: ${pnl:.2f}")

def db_get_history(user_id: int, limit: int = 20) -> List[Dict]:
    """Получить историю сделок"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM history WHERE user_id = ? ORDER BY closed_at DESC LIMIT ?", (user_id, limit))
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows

# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================
def db_set_referrer(user_id: int, referrer_id: int) -> bool:
    """Установить реферера для пользователя"""
    if user_id == referrer_id:
        return False
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Проверяем что у юзера ещё нет реферера
    c.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    if row and row[0]:
        conn.close()
        return False
    
    # Проверяем что реферер существует
    c.execute("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,))
    if not c.fetchone():
        conn.close()
        return False
    
    c.execute("UPDATE users SET referrer_id = ? WHERE user_id = ?", (referrer_id, user_id))
    conn.commit()
    conn.close()
    logger.info(f"[REF] User {user_id} referred by {referrer_id}")
    return True

def db_get_referrer(user_id: int) -> Optional[int]:
    """Получить реферера пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else None

def db_get_referrals_count(user_id: int) -> int:
    """Количество рефералов пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (user_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

def db_add_referral_bonus(referrer_id: int, amount: float):
    """Добавить реферальный бонус"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, referrer_id))
    conn.commit()
    conn.close()
    
    # Обновляем кэш
    if referrer_id in users_cache:
        users_cache[referrer_id]['balance'] += amount
    
    logger.info(f"[REF] Bonus ${amount} added to {referrer_id}")

# ==================== АЛЕРТЫ ====================
def db_add_alert(user_id: int, symbol: str, target_price: float, direction: str) -> int:
    """Добавить алерт"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO alerts (user_id, symbol, target_price, direction) VALUES (?, ?, ?, ?)",
        (user_id, symbol, target_price, direction)
    )
    alert_id = c.lastrowid
    conn.commit()
    conn.close()
    logger.info(f"[ALERT] Created #{alert_id} for {user_id}: {symbol} {direction} ${target_price}")
    return alert_id

def db_get_active_alerts() -> List[Dict]:
    """Получить все активные алерты"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM alerts WHERE triggered = 0")
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows

def db_get_user_alerts(user_id: int) -> List[Dict]:
    """Получить алерты пользователя"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM alerts WHERE user_id = ? AND triggered = 0", (user_id,))
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows

def db_trigger_alert(alert_id: int):
    """Пометить алерт как сработавший"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE alerts SET triggered = 1 WHERE id = ?", (alert_id,))
    conn.commit()
    conn.close()

def db_delete_alert(alert_id: int, user_id: int) -> bool:
    """Удалить алерт"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM alerts WHERE id = ? AND user_id = ?", (alert_id, user_id))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

# Инициализация БД при старте
init_db()

# ==================== КОНФИГ ====================
COMMISSION_PERCENT = 2.0  # Комиссия 2% за сделку
MIN_DEPOSIT = 1  # Минимальный депозит $1
STARS_RATE = 50  # 50 звёзд = $1
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]  # ID админов
REFERRAL_BONUS = 5.0  # $5 бонус рефереру при депозите

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

# ==================== УТИЛИТЫ ====================
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
    trading_status = "🟢 ВКЛ" if user['trading'] else "🔴 ВЫКЛ"
    
    text = f"""<b>💰 Баланс:</b> ${balance:.2f}

<b>📊 Авто-Торговля:</b> {trading_status}
Включив Авто-торговлю, вам будут приходить сделки.

Получайте сигналы с винрейтом 70-85%"""
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Выключить' if user['trading'] else '🟢 Включить'} торговлю", callback_data="toggle")],
        [InlineKeyboardButton("💳 Пополнить", callback_data="deposit")],
        [InlineKeyboardButton("📊 Мои сделки", callback_data="trades")]
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
    user = get_user(user_id)
    
    if not user['trading'] and user['balance'] < MIN_DEPOSIT:
        logger.info(f"[TOGGLE] User {user_id} - insufficient balance")
        await query.answer(f"Минимальный баланс ${MIN_DEPOSIT}", show_alert=True)
        return
    
    user['trading'] = not user['trading']
    save_user(user_id)  # Сохраняем в БД
    logger.info(f"[TOGGLE] User {user_id} trading = {user['trading']}")
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

Нет активных сделок

───────────────
<b>Баланс:</b> ${user['balance']:.2f}
<b>Профит:</b> {profit_str}
<b>Побед:</b> {wins}/{total_trades} ({winrate}%)"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="trades")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back")]
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
        text += f"{ticker}  ${pos['amount']:.0f}  →  PNL: {pnl_str}{emoji}\n"
        keyboard.append([InlineKeyboardButton(f"❌ Закрыть {ticker}", callback_data=f"close_{pos['id']}")])
    
    text += f"""
───────────────
<b>Баланс:</b> ${user['balance']:.2f}
<b>Профит:</b> {profit_str}
<b>Побед:</b> {wins}/{total_trades} ({winrate}%)"""
    
    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="trades")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    except BadRequest:
        pass  # Сообщение не изменилось

# ==================== СИГНАЛЫ ====================
async def send_signal(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка сигнала с реальной аналитикой"""
    from analyzer import MarketAnalyzer
    
    active_users = [uid for uid, u in users_cache.items() if u.get('trading') and u.get('balance', 0) >= MIN_DEPOSIT]
    if not active_users:
        return
    
    # Анализируем несколько пар
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    
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
        
        # Потенциальный профит
        if direction == "LONG":
            potential_profit = ((tp - entry) / entry) * 100
        else:
            potential_profit = ((entry - tp) / entry) * 100
        
    finally:
        await analyzer.close()
    
    # Получаем аналитику из сигнала
    reasoning = best_signal.get('reasoning', '')
    context_data = best_signal.get('market_context', {})
    conclusion = context_data.get('conclusion', '')
    
    # Отправляем активным юзерам
    for user_id in active_users:
        user = get_user(user_id)
        balance = user['balance']
        
        if balance < 1:
            continue
        
        ticker = symbol.split("/")[0]
        d = 'L' if direction == "LONG" else 'S'
        dir_emoji = "🟢 LONG" if direction == "LONG" else "🔴 SHORT"
        
        # Компактный формат с аналитикой
        text = f"""<b>📊 СИГНАЛ | {ticker} | {dir_emoji}</b>

🎯 Вин-рейт: <b>{winrate}%</b>
💰 TP: ${tp:,.0f} | SL: ${sl:,.0f}

{reasoning}

{conclusion}"""
        
        # Кнопки с суммами
        amounts = [10, 25, 50, 100]
        amounts = [a for a in amounts if a <= balance]
        
        keyboard = []
        for amt in amounts:
            keyboard.append([InlineKeyboardButton(
                f"${amt}",
                callback_data=f"e|{symbol}|{d}|{int(entry)}|{int(sl)}|{int(tp)}|{amt}|{winrate}"
            )])
        
        # Кнопка своей суммы
        keyboard.append([InlineKeyboardButton(
            "💵 Своя сумма",
            callback_data=f"custom|{symbol}|{d}|{int(entry)}|{int(sl)}|{int(tp)}|{winrate}"
        )])
        
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
    
    # Комиссия за открытие
    commission = amount * (COMMISSION_PERCENT / 100)
    user['balance'] -= amount
    save_user(user_id)  # Сохраняем в БД
    
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
    
    logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount}")
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_emoji = "🟢 LONG" if direction == "LONG" else "🔴 SHORT"
    
    text = f"""✅ Вы в сделке!

{dir_emoji} | {ticker}

Сумма: ${amount:.0f}
Шанс: {winrate}%

TP: ${tp:,.0f}
SL: ${sl:,.0f}

Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Мои сделки", callback_data="trades")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

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
    
    # Закрываем с текущим PnL
    pnl = pos.get('pnl', 0)
    returned = pos['amount'] + pnl
    
    user['balance'] += returned
    user['total_profit'] += pnl
    save_user(user_id)  # Сохраняем в БД
    
    # Закрываем в БД и удаляем из кэша
    db_close_position(pos_id, pos['current'], pnl, 'MANUAL')
    user_positions.remove(pos)
    
    result_emoji = "🟢" if pnl >= 0 else "🔴"
    pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
    dir_emoji = "🟢 LONG" if pos['direction'] == "LONG" else "🔴 SHORT"
    
    ticker = pos['symbol'].split("/")[0] if "/" in pos['symbol'] else pos['symbol']
    text = f"""{result_emoji} Сделка закрыта!

{ticker} {dir_emoji}
P&L: {pnl_str}

Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

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
    
    logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount} (custom)")
    
    ticker = symbol.split("/")[0] if "/" in symbol else symbol
    dir_emoji = "🟢 LONG" if direction == "LONG" else "🔴 SHORT"
    
    text = f"""✅ Вы в сделке!

{dir_emoji} | {ticker}

Сумма: ${amount:.2f}
Шанс: {winrate}%

TP: ${tp:,.0f}
SL: ${sl:,.0f}

Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Мои сделки", callback_data="trades")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

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
            
            # PnL
            if pos['direction'] == "LONG":
                pnl_percent = (pos['current'] - pos['entry']) / pos['entry']
            else:
                pnl_percent = (pos['entry'] - pos['current']) / pos['entry']
            
            pos['pnl'] = pos['amount'] * pnl_percent - pos['commission']
            
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
                    text = f"""🎯 +${pnl_abs:.0f} Take Profit!

{ticker} {dir_emoji}
P&L: {pnl_str}
Баланс: ${user['balance']:.2f}"""
                else:
                    text = f"""🛡️ -${pnl_abs:.0f} Stop Loss!

{ticker} {dir_emoji}
P&L: {pnl_str}
Баланс: ${user['balance']:.2f}"""
                
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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*), SUM(balance), SUM(total_deposit), SUM(total_profit) FROM users")
    row = c.fetchone()
    users_count = row[0] or 0
    total_balance = row[1] or 0
    total_deposits = row[2] or 0
    total_profit = row[3] or 0
    
    c.execute("SELECT COUNT(*) FROM users WHERE trading = 1")
    active_traders = c.fetchone()[0] or 0
    
    c.execute("SELECT COUNT(*) FROM positions")
    open_positions = c.fetchone()[0] or 0
    
    c.execute("SELECT COUNT(*), SUM(pnl) FROM history")
    row = c.fetchone()
    total_trades = row[0] or 0
    realized_pnl = row[1] or 0
    
    # Комиссии (2% от суммы всех сделок)
    c.execute("SELECT SUM(commission) FROM history")
    commissions = c.fetchone()[0] or 0
    c.execute("SELECT SUM(commission) FROM positions")
    commissions += c.fetchone()[0] or 0
    
    conn.close()
    
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
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT user_id FROM users")
    all_users = [row[0] for row in c.fetchall()]
    conn.close()
    
    sent = 0
    failed = 0
    
    for uid in all_users:
        try:
            await context.bot.send_message(uid, f"📢 {message}")
            sent += 1
        except:
            failed += 1
    
    await update.message.reply_text(f"✅ Отправлено: {sent}\n❌ Ошибок: {failed}")

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
    app.add_handler(CommandHandler("broadcast", broadcast))
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
        app.job_queue.run_repeating(send_signal, interval=60, first=10)
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
