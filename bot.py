import logging
import os
import random
from datetime import datetime
from typing import Dict, List
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, PreCheckoutQueryHandler, MessageHandler, filters

load_dotenv()

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== КОНФИГ ====================
COMMISSION_PERCENT = 2.0  # Комиссия 2% за сделку
MIN_DEPOSIT = 10  # Минимальный депозит $10
STARS_RATE = 50  # 50 звёзд = $1

# ==================== ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ ====================
users: Dict[int, Dict] = {}
positions: Dict[int, List[Dict]] = {}
history: Dict[int, List[Dict]] = {}

# ==================== УТИЛИТЫ ====================
def get_user(user_id: int) -> Dict:
    if user_id not in users:
        users[user_id] = {
            'balance': 0.0,
            'total_deposit': 0.0,
            'total_profit': 0.0,
            'trading': False
        }
    if user_id not in positions:
        positions[user_id] = []
    if user_id not in history:
        history[user_id] = []
    return users[user_id]

# ==================== ГЛАВНЫЙ ЭКРАН ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    balance = user['balance']
    trading_status = "🟢 ВКЛ" if user['trading'] else "🔴 ВЫКЛ"
    
    text = f"""💰 Баланс: ${balance:.2f}

📊 Торговля: {trading_status}

Получайте сигналы с винрейтом 70-85%
Комиссия: {COMMISSION_PERCENT}% за сделку"""
    
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Выключить' if user['trading'] else '🟢 Включить'} торговлю", callback_data="toggle")],
        [InlineKeyboardButton("💳 Пополнить", callback_data="deposit")],
        [InlineKeyboardButton("📊 Мои сделки", callback_data="trades")]
    ]
    
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        except:
            await context.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await context.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(keyboard))

# ==================== ПОПОЛНЕНИЕ ====================
async def deposit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = f"""💳 Пополнение баланса

Минимум: ${MIN_DEPOSIT}

Выберите способ:"""
    
    keyboard = [
        [InlineKeyboardButton("⭐ Telegram Stars", callback_data="pay_stars")],
        [InlineKeyboardButton("💎 Crypto (USDT/TON)", callback_data="pay_crypto")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def pay_stars_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    text = """⭐ Пополнение через Stars

Выберите сумму:"""
    
    # 50 stars = $1
    keyboard = [
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
    
    stars_map = {"stars_500": 500, "stars_1250": 1250, "stars_2500": 2500, "stars_5000": 5000}
    stars = stars_map.get(query.data, 500)
    usd = stars // STARS_RATE
    
    try:
        await query.message.delete()
    except:
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
    
    user['balance'] += usd
    user['total_deposit'] += usd
    
    logger.info(f"[PAYMENT] User {user_id} deposited ${usd} via Stars")
    
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
    
    amount_map = {"crypto_10": 10, "crypto_25": 25, "crypto_50": 50, "crypto_100": 100}
    amount = amount_map.get(query.data, 10)
    user_id = update.effective_user.id
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    
    if not crypto_token:
        await query.edit_message_text(
            "❌ Crypto-оплата временно недоступна.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="deposit")]])
        )
        return
    
    try:
        from aiosend import CryptoPay
        cp = CryptoPay(crypto_token)
        
        invoice = await cp.create_invoice(
            amount=amount,
            asset="USDT",
            description=f"Пополнение баланса ${amount}",
            payload=f"{user_id}_{amount}",
            expires_in=3600
        )
        
        # Сохраняем invoice_id для проверки
        if 'pending_invoices' not in context.bot_data:
            context.bot_data['pending_invoices'] = {}
        context.bot_data['pending_invoices'][invoice.invoice_id] = {
            'user_id': user_id,
            'amount': amount
        }
        
        text = f"""💎 Оплата ${amount} USDT

Нажмите кнопку для оплаты:"""
        
        keyboard = [
            [InlineKeyboardButton("💳 Оплатить", url=invoice.bot_invoice_url)],
            [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_{invoice.invoice_id}")],
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
    
    invoice_id = int(query.data.split("_")[1])
    
    pending = context.bot_data.get('pending_invoices', {})
    if invoice_id not in pending:
        await query.answer("Платёж не найден", show_alert=True)
        return
    
    crypto_token = os.getenv("CRYPTO_BOT_TOKEN")
    if not crypto_token:
        await query.answer("Ошибка", show_alert=True)
        return
    
    try:
        from aiosend import CryptoPay
        cp = CryptoPay(crypto_token)
        
        invoices = await cp.get_invoices(invoice_ids=[invoice_id])
        
        if invoices and invoices[0].status == "paid":
            info = pending.pop(invoice_id)
            user_id = info['user_id']
            amount = info['amount']
            
            user = get_user(user_id)
            user['balance'] += amount
            user['total_deposit'] += amount
            
            logger.info(f"[CRYPTO] User {user_id} deposited ${amount}")
            
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
    user = get_user(user_id)
    
    if not user['trading'] and user['balance'] < MIN_DEPOSIT:
        await query.answer(f"Минимальный баланс ${MIN_DEPOSIT}", show_alert=True)
        return
    
    user['trading'] = not user['trading']
    await start(update, context)

async def show_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    user_positions = positions.get(user_id, [])
    
    if not user_positions:
        total_profit = user.get('total_profit', 0)
        trades_count = len(history.get(user_id, []))
        pnl_str = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
        
        text = f"""📊 Мои сделки

Активных: 0
Всего сделок: {trades_count}
Общий профит: {pnl_str}"""
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    text = "📊 Активные сделки\n\n"
    
    keyboard = []
    for pos in user_positions:
        emoji = "🟢" if pos['pnl'] >= 0 else "🔴"
        pnl_str = f"+${pos['pnl']:.2f}" if pos['pnl'] >= 0 else f"-${abs(pos['pnl']):.2f}"
        text += f"{emoji} {pos['symbol']} | ${pos['amount']:.0f} | {pnl_str}\n"
        keyboard.append([InlineKeyboardButton(f"❌ Закрыть {pos['symbol']}", callback_data=f"close_{pos['id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

# ==================== СИГНАЛЫ ====================
async def send_signal(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка сигнала пользователям с торговлей"""
    
    active_users = [uid for uid, u in users.items() if u.get('trading') and u.get('balance', 0) >= MIN_DEPOSIT]
    if not active_users:
        return
    
    symbols = ["BTC", "ETH", "SOL", "BNB"]
    symbol = random.choice(symbols)
    direction = random.choice(["LONG", "SHORT"])
    winrate = random.randint(70, 85)
    
    prices = {"BTC": 95000, "ETH": 3300, "SOL": 200, "BNB": 700}
    entry = prices[symbol] * random.uniform(0.99, 1.01)
    
    if direction == "LONG":
        tp = entry * 1.03
        sl = entry * 0.985
    else:
        tp = entry * 0.97
        sl = entry * 1.015
    
    potential_profit = 3.0  # ~3% потенциал
    
    for user_id in active_users:
        user = get_user(user_id)
        balance = user['balance']
        
        # Предложить сумму от баланса
        suggested = min(balance * 0.3, 100)  # 30% баланса или $100 макс
        if suggested < 10:
            continue
        
        emoji = "🟢" if direction == "LONG" else "🔴"
        
        text = f"""{emoji} {direction} {symbol}

📊 Винрейт: {winrate}%
💰 Потенциал: +{potential_profit:.1f}%

Сумма сделки:"""
        
        # Кодируем: sym|dir|entry|sl|tp|amt
        d = 'L' if direction == "LONG" else 'S'
        amounts = [10, 25, 50, 100]
        amounts = [a for a in amounts if a <= balance]
        
        if not amounts:
            continue
        
        keyboard = []
        row = []
        for amt in amounts[:4]:
            profit = amt * (potential_profit / 100)
            commission = amt * (COMMISSION_PERCENT / 100)
            net = profit - commission
            row.append(InlineKeyboardButton(
                f"${amt} → +${net:.1f}",
                callback_data=f"e|{symbol}|{d}|{int(entry)}|{int(sl)}|{int(tp)}|{amt}"
            ))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("❌ Пропустить", callback_data="skip")])
        
        try:
            await context.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"[SIGNAL] Error sending to {user_id}: {e}")

async def enter_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    # e|SYM|D|ENTRY|SL|TP|AMT
    data = query.data.split("|")
    if len(data) < 7:
        await query.edit_message_text("❌ Ошибка")
        return
    
    symbol = data[1]
    direction = "LONG" if data[2] == 'L' else "SHORT"
    entry = float(data[3])
    sl = float(data[4])
    tp = float(data[5])
    amount = float(data[6])
    
    # Проверка баланса
    if user['balance'] < amount:
        await query.answer("Недостаточно средств", show_alert=True)
        return
    
    # Комиссия за открытие
    commission = amount * (COMMISSION_PERCENT / 100)
    user['balance'] -= amount
    
    pos_id = len(positions[user_id]) + 1
    position = {
        'id': pos_id,
        'symbol': symbol,
        'direction': direction,
        'amount': amount,
        'entry': entry,
        'current': entry,
        'sl': sl,
        'tp': tp,
        'pnl': -commission,  # Начинаем с минуса комиссии
        'commission': commission,
        'time': datetime.now()
    }
    
    positions[user_id].append(position)
    
    logger.info(f"[TRADE] User {user_id} opened {direction} {symbol} ${amount}")
    
    text = f"""✅ Сделка открыта!

{symbol} {direction}
Сумма: ${amount:.0f}
Комиссия: ${commission:.2f}

Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("📊 Мои сделки", callback_data="trades")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def close_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    user = get_user(user_id)
    
    pos_id = int(query.data.split("_")[1])
    pos = next((p for p in positions[user_id] if p['id'] == pos_id), None)
    
    if not pos:
        await query.answer("Позиция не найдена", show_alert=True)
        return
    
    # Закрываем с текущим PnL
    pnl = pos['pnl']
    returned = pos['amount'] + pnl
    
    user['balance'] += returned
    user['total_profit'] += pnl
    
    positions[user_id].remove(pos)
    
    pos['closed'] = datetime.now()
    pos['final_pnl'] = pnl
    history[user_id].append(pos)
    
    emoji = "🟢" if pnl >= 0 else "🔴"
    pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
    
    text = f"""{emoji} Сделка закрыта!

{pos['symbol']} {pos['direction']}
P&L: {pnl_str}

Баланс: ${user['balance']:.2f}"""
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="back")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def skip_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Пропущено")
    try:
        await query.message.delete()
    except:
        pass

# ==================== ОБНОВЛЕНИЕ ПОЗИЦИЙ ====================
async def update_positions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновление цен и PnL"""
    for user_id, user_positions in positions.items():
        user = get_user(user_id)
        
        for pos in user_positions[:]:  # копия для безопасного удаления
            # Случайное движение цены
            change = random.uniform(-0.005, 0.006)  # небольшой положительный bias
            pos['current'] = pos['entry'] * (1 + change)
            
            # PnL
            if pos['direction'] == "LONG":
                pnl_percent = (pos['current'] - pos['entry']) / pos['entry']
            else:
                pnl_percent = (pos['entry'] - pos['current']) / pos['entry']
            
            pos['pnl'] = pos['amount'] * pnl_percent - pos['commission']
            
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
                
                pos['closed'] = datetime.now()
                pos['final_pnl'] = pos['pnl']
                pos['reason'] = 'TP' if hit_tp else 'SL'
                history[user_id].append(pos)
                user_positions.remove(pos)
                
                emoji = "🎯" if hit_tp else "🛡️"
                result = "Take Profit" if hit_tp else "Stop Loss"
                pnl_str = f"+${pos['pnl']:.2f}" if pos['pnl'] >= 0 else f"-${abs(pos['pnl']):.2f}"
                
                text = f"""{emoji} {result}!

{pos['symbol']} {pos['direction']}
P&L: {pnl_str}
Баланс: ${user['balance']:.2f}"""
                
                try:
                    await context.bot.send_message(
                        user_id, text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 Сделки", callback_data="trades")]])
                    )
                except:
                    pass

# ==================== MAIN ====================
def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN not set")
        return
    
    app = Application.builder().token(token).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    
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
    app.add_handler(CallbackQueryHandler(show_trades, pattern="^trades$"))
    app.add_handler(CallbackQueryHandler(enter_trade, pattern="^e\\|"))
    app.add_handler(CallbackQueryHandler(close_trade, pattern="^close_"))
    app.add_handler(CallbackQueryHandler(skip_signal, pattern="^skip$"))
    app.add_handler(CallbackQueryHandler(start, pattern="^back$"))
    
    # Jobs
    if app.job_queue:
        app.job_queue.run_repeating(update_positions, interval=5, first=5)
        app.job_queue.run_repeating(send_signal, interval=60, first=10)
    
    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
