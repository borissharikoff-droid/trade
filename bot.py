import logging
import os
import random
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.helpers import escape_markdown

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("=" * 50)
logger.info("ИНИЦИАЛИЗАЦИЯ ЛОГГЕРА")
logger.info("=" * 50)

user_data: Dict[int, Dict] = {}
active_positions: Dict[int, List[Dict]] = {}
closed_positions: Dict[int, List[Dict]] = {}
pinned_messages: Dict[int, int] = {}


def escape_md(text: str) -> str:
    """Экранирование текста для MarkdownV2"""
    return escape_markdown(str(text), version=2)


def format_number(value: float, decimals: int = 2, with_sign: bool = False) -> str:
    """Форматирование числа с экранированием для MarkdownV2"""
    if with_sign:
        formatted = f"{value:+.{decimals}f}"
    else:
        formatted = f"{value:.{decimals}f}"
    # Экранируем . + -
    return formatted.replace('.', '\\.').replace('+', '\\+').replace('-', '\\-')


class TradeSignal:
    def __init__(self, symbol: str, direction: str, entry_price: float, 
                 stop_loss: float, take_profit: float, success_rate: float):
        self.symbol = symbol
        self.direction = direction
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.success_rate = success_rate
        self.timestamp = datetime.now()
        self.analysis = None


def init_user(user_id: int) -> None:
    if user_id not in user_data:
        user_data[user_id] = {'trading_enabled': False, 'notifications_enabled': True}
    if user_id not in active_positions:
        active_positions[user_id] = []
    if user_id not in closed_positions:
        closed_positions[user_id] = []


def calculate_pnl(position: Dict) -> tuple[float, float]:
    if position['direction'] == "LONG":
        pnl_percent = ((position['current_price'] - position['entry_price']) / position['entry_price']) * 100
    else:
        pnl_percent = ((position['entry_price'] - position['current_price']) / position['entry_price']) * 100
    pnl = (position['amount'] * pnl_percent) / 100
    return pnl, pnl_percent


def build_main_menu_text(user_id: int) -> str:
    """Построение текста главного меню"""
    init_user(user_id)
    
    closed = closed_positions.get(user_id, [])
    active = active_positions.get(user_id, [])
    total_pnl = sum(p.get('pnl', 0) for p in closed) + sum(p.get('pnl', 0) for p in active)
    total_trades = len(closed) + len(active)
    
    status_emoji = "🟢" if user_data[user_id]['trading_enabled'] else "🔴"
    status_text = "ВКЛЮЧЕНА" if user_data[user_id]['trading_enabled'] else "ВЫКЛЮЧЕНА"
    
    text = f"*🚀 FAST TRADE BOT*\n\n"
    text += f"*Торговля:* {status_emoji} {escape_md(status_text)}\n\n"
    
    if total_trades > 0:
        pnl_sign = "➕" if total_pnl >= 0 else "➖"
        text += f"*💰 Ваш баланс:*\n"
        text += f"{pnl_sign} P&L: *${format_number(total_pnl, 2, True)}*\n"
        text += f"📊 Сделок: *{total_trades}*\n\n"
    
    text += f"*📋 Быстрые действия:*"
    return text


def build_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Построение клавиатуры главного меню"""
    init_user(user_id)
    
    status_emoji = "🟢" if user_data[user_id]['trading_enabled'] else "🔴"
    status_text = "ВКЛЮЧЕНА" if user_data[user_id]['trading_enabled'] else "ВЫКЛЮЧЕНА"
    
    keyboard = [
        [InlineKeyboardButton(
            f"{status_emoji} Торговля: {status_text}",
            callback_data="toggle_trading"
        )],
        [
            InlineKeyboardButton("💼 Позиции", callback_data="my_positions"),
            InlineKeyboardButton("📊 Статистика", callback_data="show_stats")
        ],
        [InlineKeyboardButton("ℹ️ Помощь", callback_data="show_help")]
    ]
    return InlineKeyboardMarkup(keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    logger.info(f"[START] Команда /start от {user_id}")
    init_user(user_id)
    
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass
    
    text = build_main_menu_text(user_id)
    keyboard = build_main_menu_keyboard(user_id)
    
    msg = await context.bot.send_message(
        user_id,
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2
    )
    
    try:
        await context.bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        pinned_messages[user_id] = msg.message_id
        logger.info(f"[START] Меню закреплено для {user_id}")
    except Exception as e:
        logger.warning(f"[START] Не удалось закрепить: {e}")


async def show_help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    try:
        await query.message.delete()
    except Exception:
        pass
    
    await _show_help(update.effective_user.id, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await update.message.delete()
    except Exception:
        pass
    
    await _show_help(update.effective_user.id, context)


async def _show_help(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = "*📖 ПОМОЩЬ*\n\n"
    text += "*Как это работает:*\n"
    text += "1\\. Включите торговлю\n"
    text += "2\\. Получайте сигналы\n"
    text += "3\\. Выбирайте сумму и входите\n"
    text += "4\\. Следите за позициями\n"
    text += "5\\. Выходите в любой момент\n\n"
    text += "⏱ Позиции обновляются каждые 5 сек\n\n"
    text += "*Команды:*\n"
    text += "/start \\- Главное меню\n"
    text += "/positions \\- Активные позиции\n"
    text += "/stats \\- Статистика\n"
    text += "/help \\- Помощь"
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
    
    await context.bot.send_message(
        user_id,
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN_V2
    )


async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    init_user(user_id)
    
    try:
        await query.message.delete()
    except Exception:
        pass
    
    text = build_main_menu_text(user_id)
    keyboard = build_main_menu_keyboard(user_id)
    
    msg = await context.bot.send_message(
        user_id,
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2
    )
    
    try:
        if user_id in pinned_messages:
            try:
                await context.bot.unpin_chat_message(chat_id=user_id, message_id=pinned_messages[user_id])
            except Exception:
                pass
        
        await context.bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        pinned_messages[user_id] = msg.message_id
    except Exception as e:
        logger.warning(f"[MAIN_MENU] Не удалось закрепить: {e}")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    init_user(user_id)
    
    closed = closed_positions.get(user_id, [])
    active = active_positions.get(user_id, [])
    
    if not closed and not active:
        await update.message.reply_text(
            "📊 Статистика\n\n"
            "У вас пока нет сделок.\n"
            "Включите торговлю и начните торговать!"
        )
        return
    
    total_trades = len(closed)
    active_trades = len(active)
    total_pnl = sum(p.get('pnl', 0) for p in closed)
    total_invested = sum(p.get('amount', 0) for p in closed)
    
    profitable = [p for p in closed if p.get('pnl', 0) > 0]
    losing = [p for p in closed if p.get('pnl', 0) < 0]
    win_rate = (len(profitable) / total_trades * 100) if total_trades > 0 else 0
    
    avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
    avg_profit = sum(p.get('pnl', 0) for p in profitable) / len(profitable) if profitable else 0
    avg_loss = sum(p.get('pnl', 0) for p in losing) / len(losing) if losing else 0
    
    current_pnl = sum(p.get('pnl', 0) for p in active)
    current_invested = sum(p.get('amount', 0) for p in active)
    
    roi = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    text = "📊 Ваша статистика\n\n"
    text += "💰 Финансы:\n"
    text += f"Общий P&L: ${total_pnl:+.2f}\n"
    text += f"Инвестировано: ${total_invested:.2f}\n"
    text += f"ROI: {roi:+.2f}%\n"
    if current_invested > 0:
        text += f"Текущий P&L: ${current_pnl:+.2f}\n"
        text += f"В позициях: ${current_invested:.2f}\n"
    
    text += "\n📈 Сделки:\n"
    text += f"Всего закрыто: {total_trades}\n"
    text += f"Активных: {active_trades}\n"
    text += f"Всего сделок: {total_trades + active_trades}\n"
    
    text += "\n🎯 Результаты:\n"
    text += f"Прибыльных: {len(profitable)} 🟢\n"
    text += f"Убыточных: {len(losing)} 🔴\n"
    text += f"Винрейт: {win_rate:.1f}%\n"
    
    if total_trades > 0:
        text += "\n📊 Средние значения:\n"
        text += f"Средний P&L: ${avg_pnl:+.2f}\n"
        if profitable:
            text += f"Средняя прибыль: ${avg_profit:+.2f}\n"
        if losing:
            text += f"Средний убыток: ${avg_loss:+.2f}\n"
    
    if closed:
        best = max(closed, key=lambda x: x.get('pnl', 0))
        worst = min(closed, key=lambda x: x.get('pnl', 0))
        text += "\n🏆 Рекорды:\n"
        text += f"Лучшая: {best.get('symbol', '?')} ${best.get('pnl', 0):+.2f}\n"
        text += f"Худшая: {worst.get('symbol', '?')} ${worst.get('pnl', 0):+.2f}\n"
    
    await update.message.reply_text(text)


async def show_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    init_user(user_id)
    
    try:
        await query.message.delete()
    except Exception:
        pass
    
    closed = closed_positions.get(user_id, [])
    active = active_positions.get(user_id, [])
    
    if not closed and not active:
        text = "*📊 Статистика*\n\n"
        text += "У вас пока нет сделок\\.\n"
        text += "Включите торговлю и начните торговать\\!"
        
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        
        await context.bot.send_message(
            user_id,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    total_trades = len(closed)
    active_trades = len(active)
    total_pnl = sum(p.get('pnl', 0) for p in closed)
    total_invested = sum(p.get('amount', 0) for p in closed) if closed else 0
    
    profitable = [p for p in closed if p.get('pnl', 0) > 0]
    win_rate = (len(profitable) / total_trades * 100) if total_trades > 0 else 0
    roi = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    current_pnl = sum(p.get('pnl', 0) for p in active)
    
    text = "*📊 СТАТИСТИКА*\n\n"
    text += f"*💰 Финансы:*\n"
    text += f"Общий P&L: *${format_number(total_pnl, 2, True)}*\n"
    text += f"ROI: *{format_number(roi, 2, True)}%*\n"
    if active:
        text += f"Текущий P&L: *${format_number(current_pnl, 2, True)}*\n"
    text += f"\n*📈 Сделки:*\n"
    text += f"Закрыто: *{total_trades}*\n"
    text += f"Активных: *{active_trades}*\n"
    text += f"Винрейт: *{format_number(win_rate, 1)}%*\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
    
    await context.bot.send_message(
        user_id,
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN_V2
    )


async def toggle_trading(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    logger.info(f"[TOGGLE] Пользователь {user_id} переключает торговлю")
    init_user(user_id)
    
    user_data[user_id]['trading_enabled'] = not user_data[user_id]['trading_enabled']
    new_status = user_data[user_id]['trading_enabled']
    logger.info(f"[TOGGLE] Торговля для {user_id}: {new_status}")
    
    # Показываем обновленное главное меню
    text = build_main_menu_text(user_id)
    keyboard = build_main_menu_keyboard(user_id)
    
    try:
        await query.edit_message_text(
            text,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.warning(f"[TOGGLE] Ошибка редактирования: {e}")


async def show_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    init_user(user_id)
    
    if update.callback_query:
        try:
            await update.callback_query.message.delete()
        except Exception:
            pass
    
    if not active_positions.get(user_id):
        text = "*📊 Активные позиции*\n\n"
        text += "У вас нет активных позиций\\.\n"
        text += "Включите торговлю и получайте сигналы\\!"
        
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        
        await context.bot.send_message(
            user_id,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    text = build_positions_text(user_id)
    keyboard = build_positions_keyboard(user_id)
    
    msg = await context.bot.send_message(
        user_id,
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2
    )
    
    try:
        if user_id in pinned_messages:
            try:
                await context.bot.unpin_chat_message(chat_id=user_id, message_id=pinned_messages[user_id])
            except Exception:
                pass
        
        await context.bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        pinned_messages[user_id] = msg.message_id
        logger.info(f"[POSITIONS] Позиции закреплены для {user_id}")
    except Exception as e:
        logger.warning(f"[POSITIONS] Не удалось закрепить: {e}")


def build_positions_text(user_id: int) -> str:
    """Построение текста позиций"""
    positions = active_positions.get(user_id, [])
    
    text = "*💼 АКТИВНЫЕ ПОЗИЦИИ*\n\n"
    
    total_pnl = 0
    for pos in positions:
        pnl = pos.get('pnl', 0)
        pnl_percent = pos.get('pnl_percent', 0)
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        direction_emoji = "📈" if pos.get('direction') == "LONG" else "📉"
        
        text += f"{pnl_emoji} *Позиция \\#{pos.get('id', 0)}*\n"
        text += f"{direction_emoji} {escape_md(pos.get('symbol', '?'))} {pos.get('direction', '?')}\n"
        text += f"💰 Сумма: *${format_number(pos.get('amount', 0))}*\n"
        text += f"📥 Вход: *${format_number(pos.get('entry_price', 0), 4)}*\n"
        text += f"📊 Текущая: *${format_number(pos.get('current_price', 0), 4)}*\n"
        text += f"💵 P&L: *${format_number(pnl, 2, True)}* \\(*{format_number(pnl_percent, 2, True)}%*\\)\n"
        text += f"🛡️ SL: *${format_number(pos.get('stop_loss', 0), 4)}* \\| 🎯 TP: *${format_number(pos.get('take_profit', 0), 4)}*\n\n"
        
        total_pnl += pnl
    
    text += f"━━━━━━━━━━━━━━━━\n"
    text += f"*💰 Общий P&L: ${format_number(total_pnl, 2, True)}*"
    
    return text


def build_positions_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Построение клавиатуры позиций"""
    positions = active_positions.get(user_id, [])
    
    keyboard = []
    for pos in positions:
        keyboard.append([InlineKeyboardButton(
            f"❌ Закрыть {pos.get('id', 0)} ({pos.get('symbol', '?')})",
            callback_data=f"exit_{pos.get('id', 0)}"
        )])
    
    keyboard.append([
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_positions"),
        InlineKeyboardButton("🔙 Меню", callback_data="main_menu")
    ])
    
    return InlineKeyboardMarkup(keyboard)


async def handle_signal_notification(signal: TradeSignal, user_id: int, context: ContextTypes.DEFAULT_TYPE):
    direction_icon = "🟢" if signal.direction == "LONG" else "🔴"
    
    # Аналитика
    analysis = signal.analysis or {}
    confidence = analysis.get('confidence', 0.85) * 100
    components = analysis.get('components', {})
    indicators = analysis.get('indicators', {})
    sentiment_data = analysis.get('sentiment_data', {})
    
    tech = components.get('technical', 0.7) * 100
    sent = components.get('sentiment', 0.6) * 100
    
    # Индикаторы
    rsi = indicators.get('rsi', 50)
    adx = indicators.get('adx', 25)
    
    # Сентимент
    fng = sentiment_data.get('fear_greed', 50)
    funding = sentiment_data.get('funding_rate', 0) * 100
    lsr = sentiment_data.get('long_short_ratio', 1)
    
    symbol_escaped = escape_md(signal.symbol)
    
    text = f"""{direction_icon} *{signal.direction}*

*{symbol_escaped}*
Winrate: {signal.success_rate:.0f}%

_Аналитика_
├ Technical: {tech:.0f}%
│  ├ RSI: {rsi:.0f}
│  └ ADX: {adx:.0f}
└ Sentiment: {sent:.0f}%
   ├ Fear/Greed: {fng}
   ├ Funding: {format_number(funding, 4)}%
   └ L/S Ratio: {format_number(lsr, 2)}

Entry: \\${format_number(signal.entry_price)}
TP: \\${format_number(signal.take_profit)}
SL: \\${format_number(signal.stop_loss)}"""
    
    # Кодируем данные в callback (symbol|direction|entry|sl|tp|amount)
    sym = signal.symbol.split('/')[0]  # BTC, ETH, etc
    d = 'L' if signal.direction == "LONG" else 'S'
    e = int(signal.entry_price)
    sl = int(signal.stop_loss)
    tp = int(signal.take_profit)
    
    keyboard = [
        [
            InlineKeyboardButton("$50", callback_data=f"e|{sym}|{d}|{e}|{sl}|{tp}|50"),
            InlineKeyboardButton("$100", callback_data=f"e|{sym}|{d}|{e}|{sl}|{tp}|100"),
            InlineKeyboardButton("$250", callback_data=f"e|{sym}|{d}|{e}|{sl}|{tp}|250")
        ],
        [InlineKeyboardButton("✕ Пропустить", callback_data="skip")]
    ]
    
    await context.bot.send_message(
        user_id, 
        text, 
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN_V2
    )


async def enter_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    logger.info(f"[ENTER_TRADE] Пользователь {user_id} входит в сделку")
    
    # Формат: e|SYM|DIR|ENTRY|SL|TP|AMOUNT
    data = query.data.split("|")
    
    if len(data) < 7:
        logger.error(f"[ENTER_TRADE] Неверный формат: {query.data}")
        await query.edit_message_text("❌ Ошибка данных.")
        return
    
    try:
        sym = data[1]  # BTC, ETH, etc
        direction = "LONG" if data[2] == 'L' else "SHORT"
        entry_price = float(data[3])
        stop_loss = float(data[4])
        take_profit = float(data[5])
        amount = float(data[6])
    except (ValueError, IndexError) as e:
        logger.error(f"[ENTER_TRADE] Ошибка парсинга: {e}")
        await query.edit_message_text("❌ Ошибка данных.")
        return
    
    symbol = f"{sym}/USDT"
    init_user(user_id)
    
    position = {
        'id': len(active_positions[user_id]) + 1,
        'symbol': symbol,
        'direction': direction,
        'amount': amount,
        'entry_price': entry_price,
        'current_price': entry_price,
        'stop_loss': stop_loss,
        'take_profit': take_profit,
        'pnl': 0.0,
        'pnl_percent': 0.0,
        'entry_time': datetime.now()
    }
    
    active_positions[user_id].append(position)
    
    keyboard = [
        [InlineKeyboardButton("💼 Позиции", callback_data="my_positions")],
        [InlineKeyboardButton("❌ Выйти", callback_data=f"exit_{position['id']}")]
    ]
    
    text = f"""✅ ПОЗИЦИЯ ОТКРЫТА!

Позиция #{position['id']}
📊 {symbol} {direction}
💰 Сумма: ${amount:.2f}
📥 Вход: ${entry_price:.2f}
🛡️ SL: ${stop_loss:.2f}
🎯 TP: ${take_profit:.2f}"""
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def exit_trade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    try:
        position_id = int(query.data.split("_")[1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Ошибка данных.")
        return
    
    if user_id not in active_positions:
        await query.edit_message_text("❌ Позиция не найдена.")
        return
    
    position = next((p for p in active_positions[user_id] if p.get('id') == position_id), None)
    if not position:
        await query.edit_message_text("❌ Позиция не найдена.")
        return
    
    if user_id not in closed_positions:
        closed_positions[user_id] = []
    
    closed_position = position.copy()
    closed_position['close_time'] = datetime.now()
    closed_positions[user_id].append(closed_position)
    
    active_positions[user_id].remove(position)
    pnl_emoji = "🟢" if position.get('pnl', 0) >= 0 else "🔴"
    
    try:
        await query.message.delete()
    except Exception:
        pass
    
    entry_time = position.get('entry_time', datetime.now())
    time_in_position = (datetime.now() - entry_time).total_seconds()
    hours = int(time_in_position // 3600)
    minutes = int((time_in_position % 3600) // 60)
    
    text = f"""{pnl_emoji} ПОЗИЦИЯ ЗАКРЫТА!

Позиция #{position_id}
📊 {position.get('symbol', '?')} {position.get('direction', '?')}
💰 Сумма: ${position.get('amount', 0):.2f}
📥 Вход: ${position.get('entry_price', 0):.2f}
📤 Выход: ${position.get('current_price', 0):.2f}
💵 P&L: ${position.get('pnl', 0):+.2f} ({position.get('pnl_percent', 0):+.2f}%)
⏱ Время: {hours}ч {minutes}м"""
    
    keyboard = [
        [InlineKeyboardButton("💼 Позиции", callback_data="my_positions")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]
    ]
    
    await context.bot.send_message(
        user_id,
        text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def skip_signal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Пропущено")
    logger.info(f"[SKIP] Сигнал пропущен")
    
    try:
        await query.message.delete()
    except Exception:
        pass


async def my_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать позиции через callback"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    init_user(user_id)
    
    if not active_positions.get(user_id):
        text = "*📊 Активные позиции*\n\n"
        text += "У вас нет активных позиций\\.\n"
        text += "Включите торговлю и получайте сигналы\\!"
        
        keyboard = [[InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")]]
        
        try:
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except Exception as e:
            logger.warning(f"[MY_POSITIONS] Ошибка edit: {e}")
            try:
                await query.message.delete()
            except Exception:
                pass
            await context.bot.send_message(
                user_id,
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        return
    
    text = build_positions_text(user_id)
    keyboard = build_positions_keyboard(user_id)
    
    try:
        await query.edit_message_text(
            text,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.warning(f"[MY_POSITIONS] Ошибка edit: {e}")
        try:
            await query.message.delete()
        except Exception:
            pass
        await context.bot.send_message(
            user_id,
            text,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN_V2
        )


async def update_positions_live(context: ContextTypes.DEFAULT_TYPE):
    """Обновление позиций в реальном времени"""
    for user_id, positions in list(active_positions.items()):
        if not positions:
            continue
        
        for position in positions:
            price_change = random.uniform(-0.02, 0.02)
            position['current_price'] = position['entry_price'] * (1 + price_change)
            position['pnl'], position['pnl_percent'] = calculate_pnl(position)
        
        if user_id in pinned_messages:
            try:
                msg_id = pinned_messages[user_id]
                text = build_positions_text(user_id)
                keyboard = build_positions_keyboard(user_id)
                
                await context.bot.edit_message_text(
                    chat_id=user_id,
                    message_id=msg_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            except Exception as e:
                if "message is not modified" not in str(e).lower():
                    logger.warning(f"[UPDATE] Ошибка для {user_id}: {e}")
                    pinned_messages.pop(user_id, None)


async def send_test_signal(context: ContextTypes.DEFAULT_TYPE):
    logger.info("[TEST_SIGNAL] Генерация сигнала...")
    symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
    
    enabled_users = [uid for uid, data in list(user_data.items()) if data.get('trading_enabled', False)]
    
    if not enabled_users:
        logger.info("[TEST_SIGNAL] Нет пользователей с торговлей")
        return
    
    symbol = random.choice(symbols)
    
    try:
        from analyzer import MarketAnalyzer
        analyzer = MarketAnalyzer()
        
        analysis = await analyzer.analyze_signal(symbol)
        
        if analysis is None:
            raise Exception("Analysis returned None")
        
        prices = await analyzer.calculate_entry_price(symbol, analysis['direction'], analysis)
        
        signal = TradeSignal(
            symbol=symbol,
            direction=analysis['direction'],
            entry_price=prices['entry_price'],
            stop_loss=prices['stop_loss'],
            take_profit=prices['take_profit'],
            success_rate=prices['success_rate']
        )
        signal.analysis = analysis
        
        logger.info(f"[TEST_SIGNAL] Анализ: {analysis['direction']} {analysis['confidence']:.2%}")
        
    except Exception as e:
        logger.warning(f"[TEST_SIGNAL] Fallback: {e}")
        
        base_prices = {"BTC/USDT": 95000, "ETH/USDT": 3300, "BNB/USDT": 700, "SOL/USDT": 200}
        base_price = base_prices.get(symbol, 1000)
        
        current_price = base_price * random.uniform(0.98, 1.02)
        direction = random.choice(["LONG", "SHORT"])
        
        if direction == "LONG":
            entry = current_price
            stop_loss = entry * 0.98
            take_profit = entry * 1.04
        else:
            entry = current_price
            stop_loss = entry * 1.02
            take_profit = entry * 0.96
        
        signal = TradeSignal(
            symbol=symbol,
            direction=direction,
            entry_price=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            success_rate=random.uniform(85, 95)
        )
        
        signal.analysis = {
            'confidence': random.uniform(0.75, 0.95),
            'components': {
                'news': random.uniform(0.5, 0.8),
                'sentiment': random.uniform(0.5, 0.8),
                'twitter': random.uniform(0.5, 0.8),
                'macro': random.uniform(0.5, 0.8),
                'technical': random.uniform(0.5, 0.8)
            }
        }
        
        logger.info(f"[TEST_SIGNAL] Fallback сигнал: {symbol} {direction}")
    
    for user_id in enabled_users:
        for attempt in range(3):
            try:
                await handle_signal_notification(signal, user_id, context)
                logger.info(f"[TEST_SIGNAL] Сигнал отправлен {user_id}")
                break
            except Exception as e:
                logger.error(f"[TEST_SIGNAL] Попытка {attempt+1}/3: {e}")
                if attempt < 2:
                    import asyncio
                    await asyncio.sleep(2)


def main() -> None:
    logger.info("=" * 50)
    logger.info("ЗАПУСК БОТА")
    logger.info("=" * 50)
    
    token = os.getenv("BOT_TOKEN")
    
    if not token:
        logger.error("[MAIN] BOT_TOKEN не найден! Установите переменную окружения.")
        return
    
    logger.info(f"[MAIN] Токен: {token[:15]}...")
    
    try:
        application = (
            Application.builder()
            .token(token)
            .connect_timeout(30)
            .read_timeout(30)
            .write_timeout(30)
            .build()
        )
        logger.info("[MAIN] Application создан")
    except Exception as e:
        logger.error(f"[MAIN] Ошибка: {e}")
        return
    
    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("positions", show_positions))
    application.add_handler(CommandHandler("stats", stats_command))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(toggle_trading, pattern="^toggle_trading$"))
    application.add_handler(CallbackQueryHandler(enter_trade, pattern=r"^e\|"))
    application.add_handler(CallbackQueryHandler(exit_trade, pattern="^exit_"))
    application.add_handler(CallbackQueryHandler(my_positions, pattern="^(my_positions|refresh_positions)$"))
    application.add_handler(CallbackQueryHandler(skip_signal, pattern="^skip$"))
    application.add_handler(CallbackQueryHandler(show_stats_callback, pattern="^show_stats$"))
    application.add_handler(CallbackQueryHandler(show_help_callback, pattern="^show_help$"))
    application.add_handler(CallbackQueryHandler(main_menu, pattern="^main_menu$"))
    
    logger.info("[MAIN] Обработчики зарегистрированы")
    
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(update_positions_live, interval=5, first=5)
        job_queue.run_repeating(send_test_signal, interval=30, first=5)
        logger.info("[MAIN] Периодические задачи настроены")
    
    logger.info("=" * 50)
    logger.info("БОТ ЗАПУЩЕН!")
    logger.info("=" * 50)
    
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("[MAIN] Остановка")
    except Exception as e:
        logger.error(f"[MAIN] Ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
