#!/usr/bin/env python3
"""
Скрипт для полного сброса базы данных
Удаляет ВСЕ данные: пользователей, позиции, историю, настройки

Запуск: python reset_db.py
"""

import os
import sys

# Загружаем переменные окружения из .env если есть
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL")
DB_PATH = os.environ.get("DB_PATH", "bot_data.db")
USE_POSTGRES = DATABASE_URL is not None

def reset_database():
    """Полный сброс базы данных"""
    
    print("=" * 50)
    print("⚠️  ПОЛНЫЙ СБРОС БАЗЫ ДАННЫХ")
    print("=" * 50)
    print()
    
    if USE_POSTGRES:
        print(f"📊 Тип БД: PostgreSQL")
        print(f"📍 URL: {DATABASE_URL[:50]}...")
    else:
        print(f"📊 Тип БД: SQLite")
        print(f"📍 Файл: {DB_PATH}")
    
    print()
    print("Будут удалены ВСЕ данные:")
    print("  - Все пользователи")
    print("  - Все позиции")
    print("  - Вся история сделок")
    print("  - Все алерты")
    print("  - Все настройки системы")
    print("  - Все pending invoices")
    print("  - Все реферальные записи")
    print("  - Все логи сделок")
    print()
    
    confirm = input("Введите 'RESET' для подтверждения: ")
    
    if confirm != "RESET":
        print("❌ Отменено")
        return False
    
    print()
    print("🔄 Очищаем базу данных...")
    
    try:
        if USE_POSTGRES:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            
            # Очищаем все таблицы
            tables = [
                'positions',
                'history', 
                'alerts',
                'pending_invoices',
                'referral_earnings',
                'system_settings',
                'trade_logs',
                'rate_limits',
                'users'  # users последним из-за foreign keys
            ]
            
            for table in tables:
                try:
                    cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                    print(f"  ✅ {table} - очищена")
                except Exception as e:
                    # Таблица может не существовать
                    conn.rollback()
                    try:
                        cursor.execute(f"DELETE FROM {table}")
                        print(f"  ✅ {table} - очищена (DELETE)")
                    except:
                        print(f"  ⚠️ {table} - не найдена или пустая")
                        conn.rollback()
            
            # Сбрасываем последовательности (auto-increment)
            sequences = [
                'positions_id_seq',
                'history_id_seq',
                'alerts_id_seq',
                'referral_earnings_id_seq',
                'trade_logs_id_seq'
            ]
            
            for seq in sequences:
                try:
                    cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
                    print(f"  🔄 {seq} - сброшена")
                except:
                    conn.rollback()
            
            conn.commit()
            conn.close()
            
        else:
            import sqlite3
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Очищаем все таблицы
            tables = [
                'positions',
                'history',
                'alerts', 
                'pending_invoices',
                'referral_earnings',
                'system_settings',
                'trade_logs',
                'rate_limits',
                'users'
            ]
            
            for table in tables:
                try:
                    cursor.execute(f"DELETE FROM {table}")
                    print(f"  ✅ {table} - очищена")
                except:
                    print(f"  ⚠️ {table} - не найдена")
            
            # Сбрасываем auto-increment
            cursor.execute("DELETE FROM sqlite_sequence")
            print("  🔄 sqlite_sequence - сброшена")
            
            # VACUUM для освобождения места
            conn.commit()
            cursor.execute("VACUUM")
            
            conn.close()
        
        print()
        print("=" * 50)
        print("✅ БАЗА ДАННЫХ ПОЛНОСТЬЮ ОЧИЩЕНА")
        print("=" * 50)
        print()
        print("Теперь можно перезапустить бота.")
        print("Все пользователи начнут с нуля.")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


if __name__ == "__main__":
    reset_database()
