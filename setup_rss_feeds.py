#!/usr/bin/env python3
"""
Скрипт для автоматического создания Twitter фидов в RSS.app
и добавления их в bundle YULA

Запуск: python setup_rss_feeds.py

Требует переменные окружения:
  RSS_APP_API_KEY, RSS_APP_API_SECRET, RSS_APP_BUNDLE_ID
"""

import os
import requests
import time

# RSS.app API credentials - from environment only
API_KEY = os.getenv("RSS_APP_API_KEY")
API_SECRET = os.getenv("RSS_APP_API_SECRET")
API_URL = "https://api.rss.app/v1"
BUNDLE_ID = os.getenv("RSS_APP_BUNDLE_ID")

# Все Twitter аккаунты для мониторинга
TWITTER_ACCOUNTS = {
    # P0 - Критически важные
    "US_politics": [
        "WhiteHouse", "POTUS", "VP", "PressSec", "realDonaldTrump", "EricTrump"
    ],
    "US_regulators": [
        "SECGov", "CFTC", "USTreasury", "federalreserve", "FDICgov", "TheJusticeDept"
    ],
    "Macro_data": [
        "BLS_gov", "BEA_News", "EIAgov"
    ],
    "Fast_wires": [
        "DeItaone", "FirstSquawk", "Reuters", "Bloomberg", "business"
    ],
    "Crypto_breaking": [
        "WuBlockchain", "CoinDesk", "TheBlock__", "Blockworks_", "Cointelegraph"
    ],
    "Onchain_flows": [
        "whale_alert", "lookonchain"
    ],
    "Exchanges": [
        "binance", "coinbase", "krakenfx", "okx", "Bybit_Official"
    ],
    "Stablecoins": [
        "Tether_to", "circle", "USDC"
    ],
    "Market_movers_people": [
        "elonmusk", "saylor", "cz_binance", "brian_armstrong", "VitalikButerin", "CryptoHayes"
    ],
    
    # P1 - Важные
    "Extra_news": [
        "WSJ", "FT", "CNBC", "cnnbrk", "BNONews", "KobeissiLetter", "zerohedge"
    ],
    "Onchain_analytics": [
        "nansen_ai", "glassnode", "cryptoquant_com", "santimentfeed", "coinmetrics", "MessariCrypto"
    ],
    "ETF_layer": [
        "EricBalchunas", "JSeyff", "NateGeraci", "BlackRock", "iShares", 
        "Grayscale", "ARKInvest", "vaneck_us", "BitwiseInvest", "ProShares"
    ],
    "Macro_commentary": [
        "LynAldenContact", "RaoulGMI", "krugermacro"
    ],
    "Extra_crypto_media": [
        "decryptmedia", "BitcoinMagazine", "CoinMarketCap", "coingecko"
    ]
}

def get_auth_header():
    if not API_KEY or not API_SECRET:
        raise ValueError("RSS_APP_API_KEY and RSS_APP_API_SECRET must be set in environment")
    return f"Bearer {API_KEY}:{API_SECRET}"

def list_existing_feeds():
    """Получить список существующих фидов"""
    print("\n📋 Получаем список существующих фидов...")
    
    resp = requests.get(
        f"{API_URL}/feeds?limit=100",
        headers={"Authorization": get_auth_header()},
        timeout=30
    )
    
    if resp.status_code == 200:
        data = resp.json()
        feeds = data.get('data', [])
        print(f"   Найдено {len(feeds)} существующих фидов")
        return {f.get('source_url', ''): f.get('id') for f in feeds}
    else:
        print(f"   ❌ Ошибка: {resp.status_code} - {resp.text[:100]}")
        return {}

def create_twitter_feed(username):
    """Создать фид для Twitter аккаунта"""
    twitter_url = f"https://x.com/{username}"
    
    resp = requests.post(
        f"{API_URL}/feeds",
        headers={
            "Authorization": get_auth_header(),
            "Content-Type": "application/json"
        },
        json={"url": twitter_url},
        timeout=60
    )
    
    if resp.status_code == 200:
        data = resp.json()
        feed_id = data.get('id')
        items_count = len(data.get('items', []))
        return feed_id, items_count
    elif resp.status_code == 429:
        return "RATE_LIMIT", 0
    else:
        return None, resp.status_code

def add_feed_to_bundle(feed_id):
    """Добавить фид в bundle"""
    if not BUNDLE_ID:
        return False
    resp = requests.put(
        f"{API_URL}/bundles/{BUNDLE_ID}/feeds/{feed_id}",
        headers={"Authorization": get_auth_header()},
        timeout=30
    )
    return resp.status_code == 200

def main():
    if not API_KEY or not API_SECRET:
        print("ERROR: Set RSS_APP_API_KEY and RSS_APP_API_SECRET in environment.")
        return
    if not BUNDLE_ID:
        print("ERROR: Set RSS_APP_BUNDLE_ID in environment.")
        return
    print("=" * 60)
    print("🚀 RSS.app Twitter Feeds Setup for YULA Bundle")
    print("=" * 60)
    
    # Получаем существующие фиды
    existing_feeds = list_existing_feeds()
    existing_urls = set(existing_feeds.keys())
    
    # Собираем все аккаунты
    all_accounts = []
    for category, accounts in TWITTER_ACCOUNTS.items():
        for acc in accounts:
            all_accounts.append((category, acc))
    
    print(f"\n📊 Всего аккаунтов для добавления: {len(all_accounts)}")
    print(f"⚠️  Лимит операций: ~1000/месяц")
    print(f"   Каждый новый фид = 1 операция\n")
    
    # Спрашиваем подтверждение
    confirm = input("Продолжить? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Отменено.")
        return
    
    created = 0
    skipped = 0
    failed = 0
    added_to_bundle = 0
    
    for i, (category, username) in enumerate(all_accounts):
        twitter_url = f"https://x.com/{username}"
        
        print(f"\n[{i+1}/{len(all_accounts)}] @{username} ({category})")
        
        # Проверяем существует ли уже
        if twitter_url in existing_urls:
            feed_id = existing_feeds[twitter_url]
            print(f"   ✓ Уже существует: {feed_id}")
            skipped += 1
            
            # Добавляем в bundle если есть
            if add_feed_to_bundle(feed_id):
                print(f"   ✓ Добавлен в bundle")
                added_to_bundle += 1
        else:
            # Создаём новый фид
            print(f"   ⏳ Создаём фид...")
            feed_id, items = create_twitter_feed(username)
            
            if feed_id == "RATE_LIMIT":
                print(f"   ❌ Rate limit! Подождите и запустите снова.")
                break
            elif feed_id:
                print(f"   ✅ Создан: {feed_id} ({items} постов)")
                created += 1
                
                # Добавляем в bundle
                if add_feed_to_bundle(feed_id):
                    print(f"   ✓ Добавлен в bundle")
                    added_to_bundle += 1
                    
                # Добавляем в existing для проверки дубликатов
                existing_urls.add(twitter_url)
            else:
                print(f"   ❌ Ошибка: {items}")
                failed += 1
        
        # Rate limiting
        time.sleep(1)
    
    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТ:")
    print(f"   ✅ Создано новых: {created}")
    print(f"   ⏭️  Пропущено (уже есть): {skipped}")
    print(f"   ❌ Ошибок: {failed}")
    print(f"   📦 Добавлено в bundle: {added_to_bundle}")
    print("=" * 60)
    print(f"\n🔗 Ваш bundle: https://rss.app/bundle/{BUNDLE_ID}")

if __name__ == "__main__":
    main()
