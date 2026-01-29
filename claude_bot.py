"""
Claude Assistant Telegram Bot
Бот для общения с Claude AI через Telegram
"""

import os
import logging
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from anthropic import Anthropic

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TELEGRAM_TOKEN = os.getenv("CLAUDE_BOT_TOKEN", "8403919140:AAF-xVXgPsxOPVwADU_74J7OVpKzI2y44MI")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")  # Установи через переменную окружения на Railway

# Разрешённые пользователи (твой Telegram ID)
ALLOWED_USERS = set()  # Пустой = все могут использовать. Добавь свой ID для ограничения

# Контекст разговора для каждого пользователя
conversations = {}  # {user_id: [messages]}
MAX_CONTEXT_MESSAGES = 20  # Максимум сообщений в контексте

# Системный промпт
SYSTEM_PROMPT = """Ты - опытный Python разработчик и помощник по коду.
Ты помогаешь с:
- Исправлением багов
- Написанием кода
- Оптимизацией
- Архитектурными решениями

Отвечай кратко и по делу. Если нужен код - пиши его.
Если нужно больше контекста - спрашивай.

Проект над которым мы работаем: торговый Telegram бот (bot.py) с:
- Авто-торговлей криптовалютой
- Интеграцией с Bybit
- Системой депозитов через CryptoBot
- Реферальной системой
"""


def get_client():
    """Получить Anthropic клиент"""
    if not ANTHROPIC_API_KEY:
        return None
    return Anthropic(api_key=ANTHROPIC_API_KEY)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start"""
    user_id = update.effective_user.id
    
    # Проверка доступа
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ запрещён")
        return
    
    # Сброс контекста
    conversations[user_id] = []
    
    text = """👋 <b>Claude Assistant</b>

Я - Claude AI. Помогу с кодом, багами и вопросами.

<b>Команды:</b>
/start - начать заново (сбросить контекст)
/clear - очистить историю разговора
/context - показать текущий контекст

Просто напиши сообщение и я отвечу!"""
    
    await update.message.reply_text(text, parse_mode="HTML")
    logger.info(f"User {user_id} started bot")


async def clear_context(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Очистить контекст разговора"""
    user_id = update.effective_user.id
    conversations[user_id] = []
    await update.message.reply_text("✅ Контекст очищен")


async def show_context(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать текущий контекст"""
    user_id = update.effective_user.id
    history = conversations.get(user_id, [])
    
    if not history:
        await update.message.reply_text("📭 Контекст пуст")
        return
    
    text = f"📋 <b>Контекст ({len(history)} сообщений)</b>\n\n"
    for i, msg in enumerate(history[-5:], 1):  # Последние 5
        role = "👤" if msg["role"] == "user" else "🤖"
        content = msg["content"][:100] + "..." if len(msg["content"]) > 100 else msg["content"]
        text += f"{role} {content}\n\n"
    
    await update.message.reply_text(text, parse_mode="HTML")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка текстового сообщения"""
    user_id = update.effective_user.id
    user_message = update.message.text
    
    # Проверка доступа
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ запрещён")
        return
    
    # Проверка API ключа
    client = get_client()
    if not client:
        await update.message.reply_text(
            "❌ <b>API ключ не настроен</b>\n\n"
            "Установите переменную окружения:\n"
            "<code>export ANTHROPIC_API_KEY=your_key</code>",
            parse_mode="HTML"
        )
        return
    
    # Инициализация контекста если нужно
    if user_id not in conversations:
        conversations[user_id] = []
    
    # Добавляем сообщение пользователя
    conversations[user_id].append({
        "role": "user",
        "content": user_message
    })
    
    # Ограничиваем контекст
    if len(conversations[user_id]) > MAX_CONTEXT_MESSAGES:
        conversations[user_id] = conversations[user_id][-MAX_CONTEXT_MESSAGES:]
    
    # Показываем "печатает..."
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    
    try:
        # Запрос к Claude
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=conversations[user_id]
        )
        
        assistant_message = response.content[0].text
        
        # Добавляем ответ в контекст
        conversations[user_id].append({
            "role": "assistant",
            "content": assistant_message
        })
        
        # Telegram ограничивает сообщения до 4096 символов
        if len(assistant_message) > 4000:
            # Разбиваем на части
            parts = [assistant_message[i:i+4000] for i in range(0, len(assistant_message), 4000)]
            for part in parts:
                await update.message.reply_text(part, parse_mode=None)
        else:
            # Пробуем отправить как HTML, если не получится - как обычный текст
            try:
                await update.message.reply_text(assistant_message, parse_mode="HTML")
            except Exception:
                await update.message.reply_text(assistant_message, parse_mode=None)
        
        logger.info(f"User {user_id}: {user_message[:50]}... -> Response sent")
        
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        await update.message.reply_text(
            f"❌ <b>Ошибка</b>\n\n<code>{str(e)[:200]}</code>",
            parse_mode="HTML"
        )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка файла (код)"""
    user_id = update.effective_user.id
    
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        return
    
    document = update.message.document
    
    # Проверяем размер (макс 1MB)
    if document.file_size > 1024 * 1024:
        await update.message.reply_text("❌ Файл слишком большой (макс 1MB)")
        return
    
    try:
        file = await context.bot.get_file(document.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8')
        
        # Формируем сообщение с файлом
        caption = update.message.caption or "Проанализируй этот код:"
        full_message = f"{caption}\n\n```\n{text}\n```"
        
        # Создаём фейковый update с текстом
        update.message.text = full_message
        await handle_message(update, context)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка чтения файла: {e}")


def main():
    """Запуск бота"""
    if not TELEGRAM_TOKEN:
        print("❌ CLAUDE_BOT_TOKEN не установлен!")
        return
    
    if not ANTHROPIC_API_KEY:
        print("⚠️ ANTHROPIC_API_KEY не установлен! Бот запустится, но не сможет отвечать.")
        print("Установите: export ANTHROPIC_API_KEY=your_key")
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("clear", clear_context))
    app.add_handler(CommandHandler("context", show_context))
    
    # Сообщения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    print("🤖 Claude Bot запущен!")
    print(f"Token: {TELEGRAM_TOKEN[:20]}...")
    print(f"API Key: {'✅ Настроен' if ANTHROPIC_API_KEY else '❌ Не настроен'}")
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
