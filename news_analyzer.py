"""
News Analyzer v1.0 - Анализ новостей, Twitter и макро-событий для трейдинга
Отслеживает: Trump, крупных трейдеров, гос. органы США, крипто-новости

Функции:
1. Twitter/X мониторинг ключевых аккаунтов
2. RSS/API новости криптовалют
3. Macro-события (FOMC, CPI, NFP, тарифы)
4. Sentiment анализ
5. Генерация торговых сигналов на основе новостей
"""

import asyncio
import aiohttp
import logging
import re
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================

# Ключевые Twitter аккаунты для мониторинга
TWITTER_ACCOUNTS = {
    # === ПОЛИТИКИ ===
    'realDonaldTrump': {
        'name': 'Donald Trump',
        'type': 'politician',
        'impact': 'HIGH',
        'keywords': ['crypto', 'bitcoin', 'tariff', 'china', 'economy', 'fed', 'rate', 'dollar', 'trade']
    },
    'POTUS': {
        'name': 'President of the United States',
        'type': 'government',
        'impact': 'HIGH',
        'keywords': ['economy', 'trade', 'tariff', 'crypto', 'digital', 'regulation']
    },
    'WhiteHouse': {
        'name': 'White House',
        'type': 'government',
        'impact': 'HIGH',
        'keywords': ['economy', 'executive order', 'trade', 'china', 'policy']
    },
    'USTreasury': {
        'name': 'US Treasury',
        'type': 'government',
        'impact': 'HIGH',
        'keywords': ['sanctions', 'dollar', 'debt', 'crypto', 'stablecoin', 'regulation']
    },
    'SECGov': {
        'name': 'SEC',
        'type': 'regulator',
        'impact': 'CRITICAL',
        'keywords': ['crypto', 'bitcoin', 'ethereum', 'enforcement', 'etf', 'regulation', 'lawsuit']
    },
    'federalreserve': {
        'name': 'Federal Reserve',
        'type': 'central_bank',
        'impact': 'CRITICAL',
        'keywords': ['rate', 'inflation', 'fomc', 'powell', 'monetary', 'balance sheet']
    },
    
    # === ТОП КРИПТО-ТРЕЙДЕРЫ ===
    'CryptoCred': {
        'name': 'Crypto Cred',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['long', 'short', 'btc', 'eth', 'entry', 'target', 'stop']
    },
    'HsakaTrades': {
        'name': 'Hsaka',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['long', 'short', 'btc', 'eth', 'sol', 'position', 'tp', 'sl']
    },
    'CryptoKaleo': {
        'name': 'Kaleo',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['btc', 'eth', 'alt', 'degen', 'pump', 'moon']
    },
    'ColdBloodShill': {
        'name': 'ColdBloodShill',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['btc', 'eth', 'alts', 'chart', 'setup']
    },
    'inversebrah': {
        'name': 'InverseBrah',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['perp', 'long', 'short', 'liquidation', 'funding']
    },
    'GCRClassic': {
        'name': 'GCR',
        'type': 'trader',
        'impact': 'HIGH',
        'keywords': ['btc', 'macro', 'cycle', 'bear', 'bull']
    },
    'loomdart': {
        'name': 'Loomdart',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['btc', 'eth', 'trade', 'analysis']
    },
    'PeterLBrandt': {
        'name': 'Peter Brandt',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['btc', 'pattern', 'chart', 'target']
    },
    
    # === АЛЬФА-ГРУППЫ И СИГНАЛЬНЫЕ КАНАЛЫ (ДОБАВЛЕНО!) ===
    'MustStopMurad': {
        'name': 'Murad',
        'type': 'alpha',
        'impact': 'HIGH',
        'keywords': ['memecoin', 'meta', 'play', 'rotation', 'sol']
    },
    'blaborekek': {
        'name': 'Blknoiz06',
        'type': 'alpha',
        'impact': 'HIGH',
        'keywords': ['alpha', 'gem', 'early', 'narrative', 'meta']
    },
    'DefiIgnas': {
        'name': 'Ignas',
        'type': 'alpha',
        'impact': 'HIGH',
        'keywords': ['defi', 'yield', 'airdrop', 'strategy', 'alpha']
    },
    'Route2FI': {
        'name': 'Route2FI',
        'type': 'alpha',
        'impact': 'MEDIUM',
        'keywords': ['alpha', 'airdrop', 'strategy', 'yield']
    },
    'TheDeFinvestor': {
        'name': 'The DeFi Investor',
        'type': 'alpha',
        'impact': 'MEDIUM',
        'keywords': ['defi', 'yield', 'strategy', 'protocol']
    },
    'MilesDeutscher': {
        'name': 'Miles Deutscher',
        'type': 'alpha',
        'impact': 'HIGH',
        'keywords': ['narrative', 'rotation', 'alpha', 'gem', 'meta']
    },
    'AltcoinSherpa': {
        'name': 'Altcoin Sherpa',
        'type': 'trader',
        'impact': 'HIGH',
        'keywords': ['alt', 'chart', 'entry', 'setup', 'target']
    },
    'Pentosh1': {
        'name': 'Pentoshi',
        'type': 'trader',
        'impact': 'HIGH',
        'keywords': ['btc', 'long', 'short', 'macro', 'cycle']
    },
    'CryptoTony__': {
        'name': 'Crypto Tony',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['btc', 'eth', 'chart', 'analysis', 'setup']
    },
    'CryptoGodJohn': {
        'name': 'CryptoGodJohn',
        'type': 'alpha',
        'impact': 'HIGH',
        'keywords': ['memecoin', 'sol', 'degen', 'play', 'alpha']
    },
    'CryptoDonAlt': {
        'name': 'DonAlt',
        'type': 'trader',
        'impact': 'HIGH',
        'keywords': ['btc', 'macro', 'chart', 'cycle', 'bear', 'bull']
    },
    'crypto_birb': {
        'name': 'CryptoBirb',
        'type': 'trader',
        'impact': 'MEDIUM',
        'keywords': ['btc', 'eth', 'chart', 'pattern', 'target']
    },
    'SmartContracter': {
        'name': 'SmartContracter',
        'type': 'trader',
        'impact': 'HIGH',
        'keywords': ['btc', 'eth', 'setup', 'tp', 'sl', 'entry']
    },
    'CryptoCapo_': {
        'name': 'Capo',
        'type': 'trader',
        'impact': 'HIGH',
        'keywords': ['btc', 'macro', 'bear', 'bull', 'cycle']
    },
    'coaborekglass': {
        'name': 'Coinglass',
        'type': 'data',
        'impact': 'HIGH',
        'keywords': ['liquidation', 'funding', 'oi', 'long', 'short', 'ratio']
    },
    'LookOnChain': {
        'name': 'Lookonchain',
        'type': 'onchain',
        'impact': 'HIGH',
        'keywords': ['whale', 'transfer', 'deposit', 'withdraw', 'move']
    },
    'spoaborektonchain': {
        'name': 'Spot On Chain',
        'type': 'onchain',
        'impact': 'HIGH',
        'keywords': ['whale', 'wallet', 'transfer', 'accumulate', 'sell']
    },
    'EmberCN': {
        'name': 'Ember',
        'type': 'onchain',
        'impact': 'HIGH',
        'keywords': ['whale', 'smart money', 'flow', 'move']
    },
    'ai_9684xtpa': {
        'name': 'Ai_9684xtpa',
        'type': 'onchain',
        'impact': 'HIGH',
        'keywords': ['whale', 'deposit', 'withdraw', 'accumulate']
    },
    
    # === КРИПТО ИНСАЙДЕРЫ ===
    'caborek': {
        'name': 'Caborek',
        'type': 'insider',
        'impact': 'HIGH',
        'keywords': ['blackrock', 'etf', 'flow', 'institutional']
    },
    'WuBlockchain': {
        'name': 'Wu Blockchain',
        'type': 'news',
        'impact': 'HIGH',
        'keywords': ['china', 'mining', 'regulation', 'exchange', 'binance']
    },
    'FatManTerra': {
        'name': 'FatMan',
        'type': 'investigator',
        'impact': 'HIGH',
        'keywords': ['scam', 'fraud', 'warning', 'insolvency', 'hack']
    },
    'zachxbt': {
        'name': 'ZachXBT',
        'type': 'investigator',
        'impact': 'HIGH',
        'keywords': ['scam', 'hack', 'exploit', 'stolen', 'investigation']
    },
    
    # === БИРЖИ И ПРОЕКТЫ ===
    'binance': {
        'name': 'Binance',
        'type': 'exchange',
        'impact': 'HIGH',
        'keywords': ['listing', 'delist', 'maintenance', 'withdrawal', 'announcement']
    },
    'coinaborek': {
        'name': 'Coinbase',
        'type': 'exchange',
        'impact': 'HIGH',
        'keywords': ['listing', 'sec', 'legal', 'announcement']
    },
    'caborek': {
        'name': 'Grayscale',
        'type': 'fund',
        'impact': 'HIGH',
        'keywords': ['btc', 'eth', 'outflow', 'inflow', 'etf']
    },
    
    # === ЭКОНОМИКА ===
    'elaborianmusk': {
        'name': 'Elon Musk',
        'type': 'influencer',
        'impact': 'HIGH',
        'keywords': ['doge', 'bitcoin', 'crypto', 'tesla']
    },
    'michaeljsaylor': {
        'name': 'Michael Saylor',
        'type': 'bitcoin_bull',
        'impact': 'MEDIUM',
        'keywords': ['bitcoin', 'btc', 'microstrategy', 'buy', 'acquisition']
    }
}

# Крипто-специфические ключевые слова
BULLISH_KEYWORDS = [
    'bullish', 'moon', 'pump', 'breakout', 'ath', 'new high', 'adoption',
    'institutional', 'etf approved', 'blackrock', 'buy', 'long', 'accumulate',
    'bottom', 'reversal', 'support holding', 'green', 'rally', 'surge',
    'approval', 'partnership', 'integration', 'listing', 'mainnet', 'upgrade'
]

BEARISH_KEYWORDS = [
    'bearish', 'dump', 'crash', 'breakdown', 'new low', 'ban', 'regulation',
    'lawsuit', 'sec', 'enforcement', 'sell', 'short', 'distribute',
    'top', 'resistance', 'rejection', 'red', 'plunge', 'collapse',
    'hack', 'exploit', 'scam', 'insolvency', 'bankruptcy', 'delisting',
    'tariff', 'sanctions', 'war', 'recession'
]

NEUTRAL_HIGH_IMPACT = [
    'fomc', 'fed', 'cpi', 'nfp', 'gdp', 'inflation', 'rate decision',
    'powell', 'yellen', 'trump', 'executive order', 'announcement'
]

# Макро-события календарь (UTC)
MACRO_EVENTS = {
    'FOMC': {
        'impact': 'CRITICAL',
        'typical_days': [2, 3],  # Wed-Thu
        'typical_hours': [18, 19],  # 6-7 PM UTC
        'description': 'Federal Reserve Rate Decision'
    },
    'CPI': {
        'impact': 'CRITICAL',
        'typical_days': list(range(7)),  # Any day
        'typical_hours': [12, 13],  # 12-1 PM UTC
        'description': 'Consumer Price Index'
    },
    'NFP': {
        'impact': 'HIGH',
        'typical_days': [4],  # Friday
        'typical_hours': [12, 13],
        'description': 'Non-Farm Payrolls'
    },
    'GDP': {
        'impact': 'HIGH',
        'typical_days': list(range(7)),
        'typical_hours': [12, 13],
        'description': 'Gross Domestic Product'
    },
    'PCE': {
        'impact': 'HIGH',
        'typical_days': list(range(7)),
        'typical_hours': [12, 13],
        'description': 'Personal Consumption Expenditures'
    }
}

# News APIs
NEWS_SOURCES = {
    'cryptopanic': 'https://cryptopanic.com/api/v1/posts/?auth_token={api_key}&currencies=BTC,ETH,SOL&filter=hot',
    'coingecko_news': 'https://api.coingecko.com/api/v3/status_updates',
    'fear_greed': 'https://api.alternative.me/fng/?limit=1'
}

# === COINGLASS API (бесплатные эндпоинты) ===
COINGLASS_ENDPOINTS = {
    'funding_rates': 'https://open-api.coinglass.com/public/v2/funding',
    'liquidation_24h': 'https://open-api.coinglass.com/public/v2/liquidation_chart',
    'long_short_ratio': 'https://open-api.coinglass.com/public/v2/long_short_ratio',
    'open_interest': 'https://open-api.coinglass.com/public/v2/open_interest',
}

# DexScreener для отслеживания хайпа на DEX
DEXSCREENER_API = 'https://api.dexscreener.com/latest/dex'

# RSS.app API для Twitter фидов
RSS_APP_API_KEY = "c_xMtGIIcrdOZ8Nt"
RSS_APP_API_SECRET = "s_r8NiIDkqNcLUwMDiusRtqf"
RSS_APP_API_URL = "https://api.rss.app/v1"
RSS_APP_BUNDLE_ID = "_XzgeXtiahhlT8Vg5"  # YULA bundle с Twitter аккаунтами

# Кэш созданных фидов RSS.app {username: feed_id}
_rss_app_feeds_cache = {}

# Кэш работающего RSSHub инстанса
_working_rsshub_instance = None
_rsshub_last_check = None

# RSSHub instances для Twitter
RSSHUB_INSTANCES = [
    'https://rsshub.app',
    'https://rss.shab.fun',
    'https://rsshub.rssforever.com',
]

# Приоритетные аккаунты для мониторинга (самые важные)
PRIORITY_TWITTER_ACCOUNTS = [
    'realDonaldTrump', 'POTUS', 'SECGov', 'federalreserve',  # Политика/регуляторы
    'elonmusk', 'michaeljsaylor',  # Крипто-инфлюенсеры
    'binance', 'caborek',  # Биржи
    'zachxbt', 'WuBlockchain',  # Инсайдеры
    'Pentosh1', 'CryptoDonAlt', 'GCRClassic', 'AltcoinSherpa',  # Топ трейдеры
    'LookOnChain', 'EmberCN', 'ai_9684xtpa',  # On-chain аналитика
    'DefiIgnas', 'MilesDeutscher', 'MustStopMurad',  # Альфа
]

# Пороговые значения для сигналов
COINGLASS_THRESHOLDS = {
    'extreme_funding_long': 0.05,    # >0.05% = перегрев лонгов
    'extreme_funding_short': -0.03,  # <-0.03% = перегрев шортов
    'liquidation_spike': 50_000_000,  # >$50M ликвидаций = возможный разворот
    'long_short_extreme_long': 70,    # >70% лонгов = опасно
    'long_short_extreme_short': 30,   # <30% лонгов = возможен шорт-сквиз
    'oi_change_significant': 5,       # >5% изменение OI = важно
}


# ==================== ТИПЫ ДАННЫХ ====================

class NewsImpact(Enum):
    """Влияние новости на рынок"""
    CRITICAL = 5  # Может двинуть рынок на 5-10%+
    HIGH = 4      # 2-5% движение
    MEDIUM = 3    # 1-2% движение
    LOW = 2       # <1% движение
    NOISE = 1     # Шум, игнорировать


class NewsSentiment(Enum):
    """Сентимент новости"""
    VERY_BULLISH = 2
    BULLISH = 1
    NEUTRAL = 0
    BEARISH = -1
    VERY_BEARISH = -2


class NewsCategory(Enum):
    """Категория новости"""
    REGULATION = 'regulation'
    MACRO = 'macro'
    TARIFFS = 'tariffs'
    HACK_EXPLOIT = 'hack'
    LISTING = 'listing'
    PARTNERSHIP = 'partnership'
    WHALE_MOVE = 'whale'
    TRADER_CALL = 'trader_call'
    POLITICAL = 'political'
    TECHNICAL = 'technical'
    OTHER = 'other'


@dataclass
class NewsEvent:
    """Новостное событие"""
    id: str
    source: str
    author: str
    title: str
    content: str
    url: str
    timestamp: datetime
    sentiment: NewsSentiment
    impact: NewsImpact
    category: NewsCategory
    affected_coins: List[str]
    keywords_found: List[str]
    confidence: float  # 0-1
    trading_signal: Optional[str] = None  # 'LONG', 'SHORT', None
    reasoning: List[str] = field(default_factory=list)


@dataclass
class MacroEvent:
    """Макро-экономическое событие"""
    name: str
    description: str
    scheduled_time: datetime
    impact: NewsImpact
    actual_value: Optional[float] = None
    forecast_value: Optional[float] = None
    previous_value: Optional[float] = None
    surprise: Optional[float] = None  # actual - forecast


@dataclass
class TradingSignal:
    """Торговый сигнал на основе новостей"""
    direction: str  # 'LONG' или 'SHORT'
    confidence: float  # 0-1
    source: str  # Источник сигнала
    reasoning: List[str]
    affected_coins: List[str]
    time_sensitive: bool  # Нужно ли действовать немедленно
    expires_at: datetime  # Когда сигнал истекает
    impact: NewsImpact


# ==================== NEWS ANALYZER ====================

class NewsAnalyzer:
    """Анализатор новостей и Twitter для трейдинга"""
    
    def __init__(self, cryptopanic_api_key: str = None):
        self.cryptopanic_key = cryptopanic_api_key
        
        # Кэш новостей для избежания дублей
        self.seen_news: deque = deque(maxlen=1000)
        self.recent_events: deque = deque(maxlen=100)
        
        # Агрегированный сентимент
        self.market_sentiment = {
            'score': 0,  # -100 to +100
            'trend': 'NEUTRAL',  # BULLISH, BEARISH, NEUTRAL
            'last_update': None
        }
        
        # Кэш для API
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._cache_ttl = 60  # секунд
        
        logger.info("[NEWS] Analyzer initialized")
    
    def _get_news_hash(self, title: str, source: str) -> str:
        """Генерация уникального хэша новости"""
        content = f"{title}:{source}".lower()
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _is_cached(self, key: str) -> bool:
        """Проверка кэша"""
        if key in self._cache:
            data, timestamp = self._cache[key]
            if datetime.now() - timestamp < timedelta(seconds=self._cache_ttl):
                return True
        return False
    
    def _get_cached(self, key: str) -> Any:
        """Получить из кэша"""
        if key in self._cache:
            return self._cache[key][0]
        return None
    
    def _set_cache(self, key: str, value: Any):
        """Сохранить в кэш"""
        self._cache[key] = (value, datetime.now())
    
    # ==================== SENTIMENT ANALYSIS ====================
    
    def analyze_sentiment(self, text: str) -> Tuple[NewsSentiment, float, List[str]]:
        """
        Анализ сентимента текста
        Возвращает: (sentiment, confidence, found_keywords)
        """
        text_lower = text.lower()
        found_keywords = []
        
        bullish_score = 0
        bearish_score = 0
        
        # Проверяем бычьи ключевые слова
        for keyword in BULLISH_KEYWORDS:
            if keyword in text_lower:
                bullish_score += 1
                found_keywords.append(f"✅ {keyword}")
        
        # Проверяем медвежьи ключевые слова
        for keyword in BEARISH_KEYWORDS:
            if keyword in text_lower:
                bearish_score += 1
                found_keywords.append(f"❌ {keyword}")
        
        # Проверяем нейтральные высоко-импактные
        for keyword in NEUTRAL_HIGH_IMPACT:
            if keyword in text_lower:
                found_keywords.append(f"⚡ {keyword}")
        
        # Определяем сентимент
        total_score = bullish_score - bearish_score
        total_keywords = bullish_score + bearish_score
        
        if total_keywords == 0:
            return NewsSentiment.NEUTRAL, 0.3, found_keywords
        
        confidence = min(0.9, 0.4 + (total_keywords * 0.1))
        
        if total_score >= 3:
            return NewsSentiment.VERY_BULLISH, confidence, found_keywords
        elif total_score >= 1:
            return NewsSentiment.BULLISH, confidence, found_keywords
        elif total_score <= -3:
            return NewsSentiment.VERY_BEARISH, confidence, found_keywords
        elif total_score <= -1:
            return NewsSentiment.BEARISH, confidence, found_keywords
        else:
            return NewsSentiment.NEUTRAL, confidence, found_keywords
    
    def detect_category(self, text: str, source: str) -> NewsCategory:
        """Определить категорию новости"""
        text_lower = text.lower()
        
        if any(w in text_lower for w in ['sec', 'regulation', 'lawsuit', 'enforcement', 'ban', 'legal']):
            return NewsCategory.REGULATION
        elif any(w in text_lower for w in ['fomc', 'fed', 'cpi', 'nfp', 'inflation', 'rate', 'powell']):
            return NewsCategory.MACRO
        elif any(w in text_lower for w in ['tariff', 'trade war', 'china', 'sanctions']):
            return NewsCategory.TARIFFS
        elif any(w in text_lower for w in ['hack', 'exploit', 'stolen', 'breach', 'vulnerability']):
            return NewsCategory.HACK_EXPLOIT
        elif any(w in text_lower for w in ['listing', 'delist', 'launch', 'mainnet']):
            return NewsCategory.LISTING
        elif any(w in text_lower for w in ['partnership', 'integration', 'collaboration']):
            return NewsCategory.PARTNERSHIP
        elif any(w in text_lower for w in ['whale', 'large transfer', 'moved', 'billion']):
            return NewsCategory.WHALE_MOVE
        elif source in ['trader'] or any(w in text_lower for w in ['long', 'short', 'entry', 'target']):
            return NewsCategory.TRADER_CALL
        elif any(w in text_lower for w in ['trump', 'biden', 'congress', 'senate', 'executive order']):
            return NewsCategory.POLITICAL
        else:
            return NewsCategory.OTHER
    
    def extract_coins(self, text: str) -> List[str]:
        """Извлечь упомянутые монеты из текста"""
        coins = []
        text_upper = text.upper()
        
        # Основные монеты
        coin_patterns = [
            'BTC', 'BITCOIN', 'ETH', 'ETHEREUM', 'SOL', 'SOLANA',
            'XRP', 'RIPPLE', 'BNB', 'DOGE', 'DOGECOIN', 'ADA', 'CARDANO',
            'AVAX', 'DOT', 'MATIC', 'LINK', 'UNI', 'ATOM', 'LTC',
            'NEAR', 'APT', 'ARB', 'OP', 'SUI', 'SEI', 'INJ', 'TIA',
            'PEPE', 'SHIB', 'FLOKI', 'BONK', 'WIF', 'MEME',
            'FET', 'RNDR', 'TAO', 'WLD', 'ARKM'
        ]
        
        for pattern in coin_patterns:
            if pattern in text_upper:
                # Нормализуем название
                normalized = pattern.replace('BITCOIN', 'BTC').replace('ETHEREUM', 'ETH')
                normalized = normalized.replace('SOLANA', 'SOL').replace('RIPPLE', 'XRP')
                normalized = normalized.replace('DOGECOIN', 'DOGE').replace('CARDANO', 'ADA')
                if normalized not in coins:
                    coins.append(normalized)
        
        return coins if coins else ['BTC']  # По умолчанию BTC
    
    def calculate_impact(self, source_type: str, category: NewsCategory, 
                         sentiment_strength: int) -> NewsImpact:
        """Рассчитать влияние новости"""
        base_impact = 2  # LOW
        
        # По источнику
        if source_type in ['regulator', 'central_bank']:
            base_impact = 5  # CRITICAL
        elif source_type in ['government', 'politician']:
            base_impact = 4  # HIGH
        elif source_type in ['exchange', 'investigator']:
            base_impact = 4  # HIGH
        elif source_type in ['trader', 'insider']:
            base_impact = 3  # MEDIUM
        
        # По категории
        if category in [NewsCategory.REGULATION, NewsCategory.MACRO]:
            base_impact = max(base_impact, 4)
        elif category in [NewsCategory.HACK_EXPLOIT, NewsCategory.TARIFFS]:
            base_impact = max(base_impact, 4)
        
        # По силе сентимента
        if abs(sentiment_strength) >= 2:
            base_impact = min(5, base_impact + 1)
        
        return NewsImpact(min(5, max(1, base_impact)))
    
    # ==================== NEWS FETCHING ====================
    
    async def fetch_cryptopanic_news(self) -> List[NewsEvent]:
        """Получить новости с CryptoPanic"""
        if not self.cryptopanic_key:
            return []
        
        cache_key = 'cryptopanic'
        if self._is_cached(cache_key):
            return self._get_cached(cache_key)
        
        events = []
        
        try:
            url = f"https://cryptopanic.com/api/v1/posts/?auth_token={self.cryptopanic_key}&filter=hot&public=true"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return events
                    
                    data = await resp.json()
            
            for post in data.get('results', [])[:20]:
                title = post.get('title', '')
                news_hash = self._get_news_hash(title, 'cryptopanic')
                
                if news_hash in self.seen_news:
                    continue
                
                self.seen_news.append(news_hash)
                
                sentiment, confidence, keywords = self.analyze_sentiment(title)
                category = self.detect_category(title, 'news')
                coins = self.extract_coins(title)
                impact = self.calculate_impact('news', category, sentiment.value)
                
                event = NewsEvent(
                    id=news_hash,
                    source='CryptoPanic',
                    author=post.get('source', {}).get('title', 'Unknown'),
                    title=title,
                    content=title,
                    url=post.get('url', ''),
                    timestamp=datetime.fromisoformat(post.get('created_at', '').replace('Z', '+00:00')),
                    sentiment=sentiment,
                    impact=impact,
                    category=category,
                    affected_coins=coins,
                    keywords_found=keywords,
                    confidence=confidence
                )
                
                events.append(event)
            
            self._set_cache(cache_key, events)
            logger.info(f"[NEWS] Fetched {len(events)} news from CryptoPanic")
            
        except Exception as e:
            logger.warning(f"[NEWS] CryptoPanic error: {e}")
        
        return events
    
    async def fetch_fear_greed_index(self) -> Dict:
        """Получить индекс страха и жадности"""
        cache_key = 'fear_greed'
        if self._is_cached(cache_key):
            return self._get_cached(cache_key)
        
        result = {
            'value': 50,
            'classification': 'Neutral',
            'timestamp': datetime.now(timezone.utc)
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    'https://api.alternative.me/fng/?limit=1',
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('data'):
                            fg = data['data'][0]
                            result['value'] = int(fg.get('value', 50))
                            result['classification'] = fg.get('value_classification', 'Neutral')
            
            self._set_cache(cache_key, result)
            logger.info(f"[NEWS] Fear & Greed: {result['value']} ({result['classification']})")
            
        except Exception as e:
            logger.warning(f"[NEWS] Fear & Greed error: {e}")
        
        return result
    
    async def fetch_twitter_sentiment(self, accounts: List[str] = None) -> List[NewsEvent]:
        """
        Получить крипто-новости из нескольких РАБОТАЮЩИХ источников:
        1. CoinTelegraph RSS
        2. Decrypt RSS
        3. TheBlock RSS
        4. Bitcoin Magazine RSS
        5. CryptoSlate RSS
        """
        events = []
        
        # Работающие RSS источники крипто-новостей
        rss_sources = [
            {
                'url': 'https://cointelegraph.com/rss',
                'name': 'CoinTelegraph',
                'type': 'news',
                'impact': 'HIGH'
            },
            {
                'url': 'https://decrypt.co/feed',
                'name': 'Decrypt',
                'type': 'news',
                'impact': 'HIGH'
            },
            {
                'url': 'https://www.theblock.co/rss.xml',
                'name': 'TheBlock',
                'type': 'news',
                'impact': 'HIGH'
            },
            {
                'url': 'https://bitcoinmagazine.com/.rss/full/',
                'name': 'Bitcoin Magazine',
                'type': 'news',
                'impact': 'MEDIUM'
            },
            {
                'url': 'https://cryptoslate.com/feed/',
                'name': 'CryptoSlate',
                'type': 'news',
                'impact': 'MEDIUM'
            },
            {
                'url': 'https://www.coindesk.com/arc/outboundfeeds/rss/',
                'name': 'CoinDesk',
                'type': 'news',
                'impact': 'HIGH'
            }
        ]
        
        for source in rss_sources:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        source['url'],
                        timeout=aiohttp.ClientTimeout(total=10),
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                            'Accept': 'application/rss+xml, application/xml, text/xml'
                        }
                    ) as resp:
                        if resp.status != 200:
                            logger.debug(f"[NEWS] {source['name']} returned {resp.status}")
                            continue
                        
                        content = await resp.text()
                
                # Парсим RSS
                items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL | re.IGNORECASE)
                
                if not items:
                    # Попробуем альтернативный формат (Atom)
                    items = re.findall(r'<entry>(.*?)</entry>', content, re.DOTALL | re.IGNORECASE)
                
                for item in items[:10]:  # Последние 10 новостей
                    # Парсим title
                    title_match = re.search(r'<title[^>]*>(.*?)</title>', item, re.DOTALL)
                    if not title_match:
                        continue
                    
                    title = title_match.group(1)
                    title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title)  # Убираем CDATA
                    title = re.sub(r'<[^>]+>', '', title)  # Убираем HTML теги
                    title = title.strip()
                    
                    if not title or len(title) < 10:
                        continue
                    
                    news_hash = self._get_news_hash(title, source['name'])
                    if news_hash in self.seen_news:
                        continue
                    
                    self.seen_news.append(news_hash)
                    
                    # Парсим ссылку
                    link_match = re.search(r'<link[^>]*>([^<]+)</link>', item)
                    if not link_match:
                        link_match = re.search(r'<link[^>]*href=["\']([^"\']+)["\']', item)
                    url = link_match.group(1) if link_match else ''
                    
                    # Парсим описание для дополнительного контекста
                    desc_match = re.search(r'<description[^>]*>(.*?)</description>', item, re.DOTALL)
                    description = ''
                    if desc_match:
                        description = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', desc_match.group(1))
                        description = re.sub(r'<[^>]+>', '', description)[:200]
                    
                    # Анализ
                    full_text = f"{title} {description}"
                    sentiment, confidence, keywords = self.analyze_sentiment(full_text)
                    category = self.detect_category(full_text, source['type'])
                    coins = self.extract_coins(full_text)
                    impact = self.calculate_impact(source['type'], category, sentiment.value)
                    
                    # Парсим дату
                    timestamp = datetime.now(timezone.utc)
                    date_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                    if not date_match:
                        date_match = re.search(r'<published>(.*?)</published>', item)
                    if date_match:
                        try:
                            from email.utils import parsedate_to_datetime
                            timestamp = parsedate_to_datetime(date_match.group(1))
                        except:
                            try:
                                # ISO format fallback
                                timestamp = datetime.fromisoformat(date_match.group(1).replace('Z', '+00:00'))
                            except:
                                pass
                    
                    event = NewsEvent(
                        id=news_hash,
                        source=source['name'],
                        author=source['name'],
                        title=title[:200],
                        content=description or title,
                        url=url,
                        timestamp=timestamp,
                        sentiment=sentiment,
                        impact=impact,
                        category=category,
                        affected_coins=coins,
                        keywords_found=keywords,
                        confidence=confidence
                    )
                    
                    events.append(event)
                
                logger.debug(f"[NEWS] {source['name']}: parsed {len(items)} items")
                
            except Exception as e:
                logger.debug(f"[NEWS] {source['name']} error: {e}")
                continue
            
            await asyncio.sleep(0.3)  # Rate limiting
        
        # Сортируем по времени
        events.sort(key=lambda x: x.timestamp, reverse=True)
        
        logger.info(f"[NEWS] Fetched {len(events)} news from RSS sources")
        return events
    
    async def fetch_twitter_posts(self, accounts: List[str] = None) -> List[NewsEvent]:
        """
        Получить твиты от ключевых аккаунтов через RSS.app API
        Fallback: RSSHub
        """
        events = []
        accounts_to_fetch = accounts or PRIORITY_TWITTER_ACCOUNTS[:8]  # Топ 8 аккаунтов
        logger.info(f"[TWITTER] Starting fetch for {len(accounts_to_fetch)} accounts: {accounts_to_fetch[:3]}...")
        
        # Сначала пробуем RSS.app API
        try:
            rss_app_events = await self._fetch_twitter_via_rss_app(accounts_to_fetch)
            if rss_app_events:
                logger.info(f"[TWITTER] ✅ Got {len(rss_app_events)} tweets via RSS.app")
                return rss_app_events
            else:
                logger.warning("[TWITTER] RSS.app returned empty list")
        except Exception as e:
            logger.warning(f"[TWITTER] RSS.app failed: {e}")
        
        # Fallback на RSSHub
        global _working_rsshub_instance, _rsshub_last_check
        working_instance = None
        now = datetime.now()
        
        # Используем кэшированный инстанс если проверяли недавно
        if _working_rsshub_instance and _rsshub_last_check:
            if (now - _rsshub_last_check).total_seconds() < 300:
                working_instance = _working_rsshub_instance
        
        async with aiohttp.ClientSession() as session:
            # Ищем работающий RSSHub инстанс
            if not working_instance:
                for instance in RSSHUB_INSTANCES:
                    try:
                        test_url = f"{instance}/twitter/user/elonmusk"
                        async with session.get(
                            test_url,
                            timeout=aiohttp.ClientTimeout(total=8),
                            headers={'User-Agent': 'Mozilla/5.0', 'Accept': '*/*'},
                            allow_redirects=True
                        ) as resp:
                            if resp.status == 200:
                                content = await resp.text()
                                if '<item>' in content or '<entry>' in content:
                                    working_instance = instance
                                    _working_rsshub_instance = instance
                                    _rsshub_last_check = now
                                    logger.info(f"[TWITTER] ✅ Found working RSSHub: {instance}")
                                    break
                    except Exception as e:
                        logger.debug(f"[TWITTER] {instance} failed: {e}")
                        continue
            
            if not working_instance:
                logger.warning("[TWITTER] ❌ No working RSSHub instance found")
                return events
            
            # Парсим твиты от каждого аккаунта через RSSHub
            for username in accounts_to_fetch:
                try:
                    rss_url = f"{working_instance}/twitter/user/{username}"
                    
                    async with session.get(
                        rss_url,
                        timeout=aiohttp.ClientTimeout(total=10),
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                            'Accept': 'application/rss+xml, application/xml, text/xml'
                        }
                    ) as resp:
                        if resp.status != 200:
                            continue
                        
                        content = await resp.text()
                    
                    # Парсим RSS
                    items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL | re.IGNORECASE)
                    
                    # Информация об аккаунте
                    account_info = TWITTER_ACCOUNTS.get(username, {
                        'name': username,
                        'type': 'unknown',
                        'impact': 'MEDIUM',
                        'keywords': []
                    })
                    
                    for item in items[:5]:  # Последние 5 твитов от каждого
                        # Парсим title (текст твита)
                        title_match = re.search(r'<title[^>]*>(.*?)</title>', item, re.DOTALL)
                        if not title_match:
                            continue
                        
                        title = title_match.group(1)
                        title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title)
                        title = re.sub(r'<[^>]+>', '', title)
                        title = title.strip()
                        
                        # Пропускаем RT: и слишком короткие
                        if not title or len(title) < 15 or title.startswith('RT:'):
                            continue
                        
                        # Проверяем релевантность по ключевым словам аккаунта
                        account_keywords = account_info.get('keywords', [])
                        is_relevant = False
                        
                        title_lower = title.lower()
                        # Всегда релевантно если от важных источников
                        if account_info.get('type') in ['regulator', 'central_bank', 'government', 'politician']:
                            is_relevant = True
                        # Или если содержит ключевые слова
                        elif any(kw.lower() in title_lower for kw in account_keywords):
                            is_relevant = True
                        # Или общие крипто-ключевые слова
                        elif any(kw in title_lower for kw in ['btc', 'bitcoin', 'eth', 'crypto', 'sol', 'pump', 'dump', 'long', 'short']):
                            is_relevant = True
                        
                        if not is_relevant:
                            continue
                        
                        news_hash = self._get_news_hash(title, f"twitter_{username}")
                        if news_hash in self.seen_news:
                            continue
                        
                        self.seen_news.append(news_hash)
                        
                        # Парсим ссылку
                        link_match = re.search(r'<link[^>]*>([^<]+)</link>', item)
                        url = link_match.group(1).strip() if link_match else f"https://twitter.com/{username}"
                        # Заменяем rsshub URL на twitter
                        url = re.sub(r'https?://rsshub[^/]*/twitter/user/', 'https://twitter.com/', url)
                        
                        # Парсим дату
                        timestamp = datetime.now(timezone.utc)
                        date_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                        if date_match:
                            try:
                                from email.utils import parsedate_to_datetime
                                timestamp = parsedate_to_datetime(date_match.group(1))
                            except:
                                pass
                        
                        # Анализ сентимента
                        sentiment, confidence, keywords = self.analyze_sentiment(title)
                        category = self.detect_category(title, account_info.get('type', 'trader'))
                        coins = self.extract_coins(title)
                        
                        # Определяем impact на основе типа аккаунта
                        impact_str = account_info.get('impact', 'MEDIUM')
                        try:
                            base_impact = NewsImpact[impact_str]
                        except KeyError:
                            base_impact = NewsImpact.MEDIUM
                        
                        # Boost confidence для важных источников
                        if account_info.get('type') in ['regulator', 'central_bank', 'government']:
                            confidence = min(0.95, confidence + 0.2)
                        elif account_info.get('type') in ['politician', 'exchange']:
                            confidence = min(0.9, confidence + 0.1)
                        
                        event = NewsEvent(
                            id=news_hash,
                            source=f"🐦 @{username}",
                            author=account_info.get('name', username),
                            title=title[:200],
                            content=title,
                            url=url,
                            timestamp=timestamp,
                            sentiment=sentiment,
                            impact=base_impact,
                            category=category,
                            affected_coins=coins,
                            keywords_found=keywords,
                            confidence=confidence
                        )
                        
                        events.append(event)
                    
                except Exception as e:
                    logger.debug(f"[TWITTER] Error fetching @{username}: {e}")
                    continue
                
                await asyncio.sleep(0.5)  # Rate limiting между аккаунтами
        
        # Сортируем по времени
        events.sort(key=lambda x: x.timestamp, reverse=True)
        
        logger.info(f"[TWITTER] Fetched {len(events)} tweets from {len(accounts_to_fetch)} accounts")
        return events
    
    async def _fetch_twitter_via_rss_app(self, accounts: List[str]) -> List[NewsEvent]:
        """Получить Twitter/новости через RSS.app Bundle YULA"""
        global _rss_app_feeds_cache
        events = []
        
        auth_header = f"Bearer {RSS_APP_API_KEY}:{RSS_APP_API_SECRET}"
        logger.info(f"[RSS.APP] Fetching from YULA bundle...")
        
        async with aiohttp.ClientSession() as session:
            # 1. Получаем данные из bundle YULA
            try:
                async with session.get(
                    f"{RSS_APP_API_URL}/bundles/{RSS_APP_BUNDLE_ID}",
                    headers={'Authorization': auth_header},
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        bundle_name = data.get('name', 'YULA')
                        items = data.get('items', [])
                        feeds_count = len(data.get('feeds', []))
                        
                        logger.info(f"[RSS.APP] ✅ Bundle '{bundle_name}': {feeds_count} feeds, {len(items)} items")
                        
                        for item in items[:30]:  # Последние 30 постов
                            url = item.get('url', '')
                            is_twitter = 'twitter.com' in url or 'x.com' in url
                            source = '🐦 Twitter/X' if is_twitter else '📰 News'
                            
                            event = self._parse_rss_app_feed_item(item, source, is_twitter)
                            if event:
                                events.append(event)
                        
                        if events:
                            logger.info(f"[RSS.APP] Got {len(events)} items from bundle")
                            events.sort(key=lambda x: x.timestamp, reverse=True)
                            return events
                    elif resp.status == 404:
                        logger.warning(f"[RSS.APP] Bundle not found")
                    else:
                        resp_text = await resp.text()
                        logger.warning(f"[RSS.APP] Bundle error: {resp.status} - {resp_text[:100]}")
            except Exception as e:
                logger.warning(f"[RSS.APP] Error fetching bundle: {e}")
            
            # 2. Fallback: отдельные фиды из аккаунта
            try:
                async with session.get(
                    f"{RSS_APP_API_URL}/feeds?limit=30",
                    headers={'Authorization': auth_header},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        existing_feeds = data.get('data', [])
                        logger.info(f"[RSS.APP] Fallback: {len(existing_feeds)} feeds")
                        
                        for feed in existing_feeds[:10]:
                            feed_id = feed.get('id')
                            feed_title = feed.get('title', 'Unknown')
                            source_url = feed.get('source_url', '')
                            is_twitter = 'twitter.com' in source_url or 'x.com' in source_url
                            
                            if feed_id:
                                try:
                                    async with session.get(
                                        f"{RSS_APP_API_URL}/feeds/{feed_id}",
                                        headers={'Authorization': auth_header},
                                        timeout=aiohttp.ClientTimeout(total=10)
                                    ) as feed_resp:
                                        if feed_resp.status == 200:
                                            feed_data = await feed_resp.json()
                                            items = feed_data.get('items', [])
                                            for item in items[:5]:
                                                source = '🐦 Twitter/X' if is_twitter else f'📰 {feed_title[:20]}'
                                                event = self._parse_rss_app_feed_item(item, source, is_twitter)
                                                if event:
                                                    events.append(event)
                                except:
                                    pass
                            await asyncio.sleep(0.1)
                        
                        if events:
                            logger.info(f"[RSS.APP] Got {len(events)} from individual feeds")
                            events.sort(key=lambda x: x.timestamp, reverse=True)
                            return events
            except Exception as e:
                logger.warning(f"[RSS.APP] Fallback error: {e}")
            
            # 3. Keyword search fallback
            crypto_keywords = ["bitcoin crypto", "ethereum defi", "SEC crypto"]
            # Метод 1: Keyword search (точно работает по документации)
            for keyword in crypto_keywords[:3]:  # Лимитируем чтобы не тратить операции
                cache_key = f"keyword_{keyword.replace(' ', '_')}"
                feed_id = _rss_app_feeds_cache.get(cache_key)
                
                try:
                    if not feed_id:
                        logger.info(f"[RSS.APP] Creating feed for keyword: {keyword}")
                        async with session.post(
                            f"{RSS_APP_API_URL}/feeds",
                            headers={
                                'Authorization': auth_header,
                                'Content-Type': 'application/json'
                            },
                            json={
                                'keyword': keyword,
                                'region': 'US:en'
                            },
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                feed_id = data.get('id')
                                if feed_id:
                                    _rss_app_feeds_cache[cache_key] = feed_id
                                    logger.info(f"[RSS.APP] ✅ Created keyword feed: {feed_id}")
                                    
                                    items = data.get('items', [])
                                    logger.info(f"[RSS.APP] Keyword '{keyword}' has {len(items)} items")
                                    for item in items[:5]:
                                        event = self._parse_rss_app_keyword_item(item, keyword)
                                        if event:
                                            events.append(event)
                            elif resp.status == 429:
                                logger.warning("[RSS.APP] ❌ Rate limit!")
                                break
                            elif resp.status == 401:
                                logger.error("[RSS.APP] ❌ Unauthorized!")
                                break
                            else:
                                resp_text = await resp.text()
                                logger.warning(f"[RSS.APP] Keyword failed: {resp.status} - {resp_text[:100]}")
                    else:
                        # Получаем существующий фид
                        async with session.get(
                            f"{RSS_APP_API_URL}/feeds/{feed_id}",
                            headers={'Authorization': auth_header},
                            timeout=aiohttp.ClientTimeout(total=15)
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                items = data.get('items', [])
                                for item in items[:5]:
                                    event = self._parse_rss_app_keyword_item(item, keyword)
                                    if event:
                                        events.append(event)
                            elif resp.status == 404:
                                del _rss_app_feeds_cache[cache_key]
                                
                except Exception as e:
                    logger.warning(f"[RSS.APP] Error for keyword '{keyword}': {e}")
                    continue
                
                await asyncio.sleep(0.3)
        
        logger.info(f"[RSS.APP] Total news fetched: {len(events)}")
        events.sort(key=lambda x: x.timestamp, reverse=True)
        return events
    
    def _parse_rss_app_feed_item(self, item: dict, source: str, is_twitter: bool = False) -> Optional[NewsEvent]:
        """Парсинг item из существующего RSS.app фида"""
        title = item.get('title', '') or item.get('description_text', '')
        
        if not title or len(title) < 10:
            return None
        
        # Очищаем title
        title = re.sub(r'<[^>]+>', '', title).strip()
        
        news_hash = self._get_news_hash(title, f"rssapp_{source[:15]}")
        if news_hash in self.seen_news:
            return None
        self.seen_news.append(news_hash)
        
        # Анализ
        sentiment, confidence, keywords = self.analyze_sentiment(title)
        category = self.detect_category(title, 'twitter' if is_twitter else 'news')
        coins = self.extract_coins(title)
        
        # Impact выше для Twitter
        impact = NewsImpact.HIGH if is_twitter else NewsImpact.MEDIUM
        
        # Timestamp
        timestamp = datetime.now(timezone.utc)
        date_str = item.get('date_published')
        if date_str:
            try:
                timestamp = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                pass
        
        return NewsEvent(
            id=news_hash,
            source=source,
            author=item.get('authors', [{}])[0].get('name', '') if item.get('authors') else '',
            title=title[:200],
            content=item.get('description_text', title)[:500],
            url=item.get('url', ''),
            timestamp=timestamp,
            sentiment=sentiment,
            impact=impact,
            category=category,
            affected_coins=coins,
            keywords_found=keywords,
            confidence=confidence
        )
    
    def _parse_rss_app_keyword_item(self, item: dict, keyword: str) -> Optional[NewsEvent]:
        """Парсинг item из RSS.app keyword search в NewsEvent"""
        title = item.get('title', '') or item.get('description_text', '')
        
        if not title or len(title) < 15:
            return None
        
        # Очищаем title
        title = re.sub(r'<[^>]+>', '', title).strip()
        
        news_hash = self._get_news_hash(title, f"rssapp_{keyword[:10]}")
        if news_hash in self.seen_news:
            return None
        self.seen_news.append(news_hash)
        
        # Анализ
        sentiment, confidence, keywords = self.analyze_sentiment(title)
        category = self.detect_category(title, 'news')
        coins = self.extract_coins(title)
        
        # Timestamp
        timestamp = datetime.now(timezone.utc)
        date_str = item.get('date_published')
        if date_str:
            try:
                timestamp = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                pass
        
        # Определяем source
        source_url = item.get('url', '')
        if 'cointelegraph' in source_url.lower():
            source = '📰 CoinTelegraph'
        elif 'decrypt' in source_url.lower():
            source = '📰 Decrypt'
        elif 'coindesk' in source_url.lower():
            source = '📰 CoinDesk'
        elif 'twitter' in source_url.lower() or 'x.com' in source_url.lower():
            source = '🐦 Twitter/X'
        else:
            source = '📰 Crypto News'
        
        return NewsEvent(
            id=news_hash,
            source=source,
            author=keyword,
            title=title[:200],
            content=item.get('description_text', title)[:500],
            url=item.get('url', ''),
            timestamp=timestamp,
            sentiment=sentiment,
            impact=NewsImpact.MEDIUM,
            category=category,
            affected_coins=coins,
            keywords_found=keywords,
            confidence=confidence
        )
    
    def _parse_rss_app_item(self, item: dict, username: str) -> Optional[NewsEvent]:
        """Парсинг item из RSS.app в NewsEvent"""
        title = item.get('title', '') or item.get('description_text', '')
        
        if not title or len(title) < 10:
            return None
        
        # Очищаем title
        title = re.sub(r'https?://\S+', '', title).strip()
        title = re.sub(r'@\w+', '', title).strip()
        
        if len(title) < 10:
            return None
        
        news_hash = self._get_news_hash(title, f"twitter_{username}")
        if news_hash in self.seen_news:
            return None
        self.seen_news.append(news_hash)
        
        # Информация об аккаунте
        account_info = TWITTER_ACCOUNTS.get(username, {
            'name': username,
            'type': 'unknown',
            'impact': 'MEDIUM',
            'keywords': []
        })
        
        # Анализ
        sentiment, confidence, keywords = self.analyze_sentiment(title)
        category = self.detect_category(title, account_info.get('type', 'trader'))
        coins = self.extract_coins(title)
        
        # Impact
        try:
            impact = NewsImpact[account_info.get('impact', 'MEDIUM')]
        except:
            impact = NewsImpact.MEDIUM
        
        # Boost для важных аккаунтов
        if account_info.get('type') in ['regulator', 'central_bank', 'government']:
            confidence = min(0.95, confidence + 0.2)
        
        # Timestamp
        timestamp = datetime.now(timezone.utc)
        date_str = item.get('date_published')
        if date_str:
            try:
                timestamp = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                pass
        
        return NewsEvent(
            id=news_hash,
            source=f"🐦 @{username}",
            author=account_info.get('name', username),
            title=title[:200],
            content=title,
            url=item.get('url', f"https://twitter.com/{username}"),
            timestamp=timestamp,
            sentiment=sentiment,
            impact=impact,
            category=category,
            affected_coins=coins,
            keywords_found=keywords,
            confidence=confidence
        )
    
    async def _fetch_twitter_via_rsshub(self, session, accounts: List[str]) -> List[NewsEvent]:
        """Fallback: получить твиты через RSSHub"""
        events = []
        global _working_rsshub_instance, _rsshub_last_check
        
        working_rsshub = _working_rsshub_instance
        for instance in RSSHUB_INSTANCES:
            try:
                async with session.get(
                    f"{instance}/twitter/user/elonmusk",
                    timeout=aiohttp.ClientTimeout(total=8),
                    headers={'User-Agent': 'Mozilla/5.0'}
                ) as resp:
                    if resp.status == 200:
                        working_rsshub = instance
                        break
            except:
                continue
        
        if not working_rsshub:
            return events
        
        for username in accounts:
            try:
                url = f"{working_rsshub}/twitter/user/{username}"
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/xml'}
                ) as resp:
                    if resp.status != 200:
                        continue
                    content = await resp.text()
                
                items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
                account_info = TWITTER_ACCOUNTS.get(username, {'name': username, 'type': 'unknown', 'impact': 'MEDIUM', 'keywords': []})
                
                for item in items[:3]:
                    title_match = re.search(r'<title[^>]*>(.*?)</title>', item, re.DOTALL)
                    if not title_match:
                        continue
                    
                    title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title_match.group(1))
                    title = re.sub(r'<[^>]+>', '', title).strip()
                    
                    if not title or len(title) < 15:
                        continue
                    
                    news_hash = self._get_news_hash(title, f"twitter_{username}")
                    if news_hash in self.seen_news:
                        continue
                    self.seen_news.append(news_hash)
                    
                    sentiment, confidence, keywords = self.analyze_sentiment(title)
                    category = self.detect_category(title, account_info.get('type', 'trader'))
                    coins = self.extract_coins(title)
                    
                    try:
                        impact = NewsImpact[account_info.get('impact', 'MEDIUM')]
                    except:
                        impact = NewsImpact.MEDIUM
                    
                    event = NewsEvent(
                        id=news_hash,
                        source=f"🐦 @{username}",
                        author=account_info.get('name', username),
                        title=title[:200],
                        content=title,
                        url=f"https://twitter.com/{username}",
                        timestamp=datetime.now(timezone.utc),
                        sentiment=sentiment,
                        impact=impact,
                        category=category,
                        affected_coins=coins,
                        keywords_found=keywords,
                        confidence=confidence
                    )
                    events.append(event)
                    
            except Exception as e:
                logger.debug(f"[RSSHUB] Error fetching @{username}: {e}")
                continue
            
            await asyncio.sleep(0.3)
        
        return events
    
    async def fetch_coingecko_trending(self) -> List[NewsEvent]:
        """
        Получить trending монеты с CoinGecko - хороший индикатор хайпа
        """
        events = []
        
        try:
            async with aiohttp.ClientSession() as session:
                # Trending coins
                async with session.get(
                    'https://api.coingecko.com/api/v3/search/trending',
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'Accept': 'application/json'}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        for idx, coin_data in enumerate(data.get('coins', [])[:7]):
                            coin = coin_data.get('item', {})
                            name = coin.get('name', '')
                            symbol = coin.get('symbol', '').upper()
                            
                            if not symbol:
                                continue
                            
                            # Создаём новость о trending монете
                            title = f"🔥 {symbol} trending #{idx+1} on CoinGecko"
                            
                            news_hash = self._get_news_hash(f"trending_{symbol}", "coingecko")
                            if news_hash in self.seen_news:
                                continue
                            self.seen_news.append(news_hash)
                            
                            # Trending = потенциально bullish (хайп)
                            event = NewsEvent(
                                id=news_hash,
                                source='CoinGecko Trending',
                                author='CoinGecko',
                                title=title,
                                content=f"{name} ({symbol}) is trending",
                                url=f"https://www.coingecko.com/en/coins/{coin.get('id', '')}",
                                timestamp=datetime.now(timezone.utc),
                                sentiment=NewsSentiment.BULLISH,
                                impact=NewsImpact.MEDIUM,
                                category=NewsCategory.OTHER,
                                affected_coins=[symbol],
                                keywords_found=['trending', 'hype'],
                                confidence=0.5 + (0.05 * (7 - idx))  # Higher rank = higher confidence
                            )
                            events.append(event)
                
                logger.info(f"[NEWS] CoinGecko trending: {len(events)} coins")
                
        except Exception as e:
            logger.debug(f"[NEWS] CoinGecko error: {e}")
        
        return events
    
    # ==================== COINGLASS INTEGRATION ====================
    
    async def fetch_coinglass_funding(self) -> Dict[str, Any]:
        """
        Получить funding rates с Coinglass
        Экстремальные значения = сигнал на разворот
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    'https://open-api.coinglass.com/public/v2/funding',
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'Accept': 'application/json'}
                ) as resp:
                    if resp.status != 200:
                        return {}
                    data = await resp.json()
                    
                    result = {'extreme_long': [], 'extreme_short': [], 'neutral': []}
                    
                    for item in data.get('data', []):
                        symbol = item.get('symbol', '')
                        rate = float(item.get('uMarginList', [{}])[0].get('rate', 0) or 0)
                        
                        if rate > COINGLASS_THRESHOLDS['extreme_funding_long']:
                            result['extreme_long'].append({
                                'symbol': symbol,
                                'rate': rate,
                                'signal': 'SHORT'  # Много лонгов = возможен шорт
                            })
                            logger.info(f"[COINGLASS] 🔴 {symbol} EXTREME LONG funding: {rate:.4%}")
                        elif rate < COINGLASS_THRESHOLDS['extreme_funding_short']:
                            result['extreme_short'].append({
                                'symbol': symbol,
                                'rate': rate,
                                'signal': 'LONG'  # Много шортов = возможен шорт-сквиз
                            })
                            logger.info(f"[COINGLASS] 🟢 {symbol} EXTREME SHORT funding: {rate:.4%}")
                        else:
                            result['neutral'].append({'symbol': symbol, 'rate': rate})
                    
                    return result
                    
        except Exception as e:
            logger.debug(f"[COINGLASS] Funding error: {e}")
            return {}
    
    async def fetch_coinglass_liquidations(self) -> Dict[str, Any]:
        """
        Получить данные о ликвидациях с Coinglass
        Большие ликвидации = возможный разворот
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    'https://open-api.coinglass.com/public/v2/liquidation_chart?symbol=BTC',
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'Accept': 'application/json'}
                ) as resp:
                    if resp.status != 200:
                        return {}
                    data = await resp.json()
                    
                    result = {
                        'total_24h': 0,
                        'long_liquidations': 0,
                        'short_liquidations': 0,
                        'signal': None
                    }
                    
                    chart_data = data.get('data', [])
                    if chart_data:
                        # Суммируем ликвидации за последние 24 часа
                        for item in chart_data[-24:]:  # Последние 24 часа
                            result['long_liquidations'] += float(item.get('longLiquidationUsd', 0) or 0)
                            result['short_liquidations'] += float(item.get('shortLiquidationUsd', 0) or 0)
                        
                        result['total_24h'] = result['long_liquidations'] + result['short_liquidations']
                        
                        # Анализ
                        if result['total_24h'] > COINGLASS_THRESHOLDS['liquidation_spike']:
                            if result['long_liquidations'] > result['short_liquidations'] * 1.5:
                                result['signal'] = 'LONG'  # Лонги ликвидированы = возможен отскок
                                logger.info(f"[COINGLASS] 💥 LONG LIQUIDATIONS ${result['long_liquidations']/1e6:.1f}M - possible bounce")
                            elif result['short_liquidations'] > result['long_liquidations'] * 1.5:
                                result['signal'] = 'SHORT'  # Шорты ликвидированы = возможен откат
                                logger.info(f"[COINGLASS] 💥 SHORT LIQUIDATIONS ${result['short_liquidations']/1e6:.1f}M - possible pullback")
                    
                    return result
                    
        except Exception as e:
            logger.debug(f"[COINGLASS] Liquidations error: {e}")
            return {}
    
    async def fetch_coinglass_long_short_ratio(self, symbol: str = 'BTC') -> Dict[str, Any]:
        """
        Получить соотношение Long/Short с Coinglass
        Экстремальные значения = контр-сигнал
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'https://open-api.coinglass.com/public/v2/long_short_ratio?symbol={symbol}&interval=h1',
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'Accept': 'application/json'}
                ) as resp:
                    if resp.status != 200:
                        return {}
                    data = await resp.json()
                    
                    result = {
                        'symbol': symbol,
                        'long_ratio': 50,
                        'short_ratio': 50,
                        'signal': None
                    }
                    
                    ls_data = data.get('data', [])
                    if ls_data:
                        latest = ls_data[-1]
                        result['long_ratio'] = float(latest.get('longRatio', 50) or 50)
                        result['short_ratio'] = 100 - result['long_ratio']
                        
                        # Экстремальные значения = контр-сигнал
                        if result['long_ratio'] > COINGLASS_THRESHOLDS['long_short_extreme_long']:
                            result['signal'] = 'SHORT'  # Слишком много лонгов
                            logger.info(f"[COINGLASS] 📊 {symbol} EXTREME LONG RATIO: {result['long_ratio']:.1f}% - contrarian SHORT")
                        elif result['long_ratio'] < COINGLASS_THRESHOLDS['long_short_extreme_short']:
                            result['signal'] = 'LONG'  # Слишком много шортов = шорт-сквиз
                            logger.info(f"[COINGLASS] 📊 {symbol} EXTREME SHORT RATIO: {result['short_ratio']:.1f}% - possible SHORT SQUEEZE")
                    
                    return result
                    
        except Exception as e:
            logger.debug(f"[COINGLASS] Long/Short ratio error: {e}")
            return {}
    
    async def fetch_dexscreener_trending(self) -> List[Dict]:
        """
        Получить trending токены с DexScreener
        Хороший индикатор хайпа на DEX
        """
        try:
            async with aiohttp.ClientSession() as session:
                # Top gainers
                async with session.get(
                    'https://api.dexscreener.com/token-boosts/top/v1',
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={'Accept': 'application/json'}
                ) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()
                    
                    trending = []
                    for item in data[:20]:  # Top 20
                        symbol = item.get('tokenAddress', '')[:8]
                        name = item.get('description', '')
                        chain = item.get('chainId', '')
                        
                        trending.append({
                            'symbol': symbol,
                            'name': name,
                            'chain': chain,
                            'url': item.get('url', ''),
                            'signal': 'WATCH'  # Отслеживать хайп
                        })
                    
                    if trending:
                        logger.info(f"[DEXSCREENER] Found {len(trending)} trending tokens")
                    
                    return trending
                    
        except Exception as e:
            logger.debug(f"[DEXSCREENER] Error: {e}")
            return []
    
    async def get_coinglass_signals(self) -> Dict[str, Any]:
        """
        Агрегированный анализ данных с Coinglass
        Возвращает торговые сигналы на основе:
        - Funding rates
        - Liquidations
        - Long/Short ratio
        """
        signals = {
            'funding': {},
            'liquidations': {},
            'long_short': {},
            'overall_signal': None,
            'confidence': 0.5
        }
        
        try:
            # Параллельно получаем все данные
            funding, liquidations, ls_btc, ls_eth = await asyncio.gather(
                self.fetch_coinglass_funding(),
                self.fetch_coinglass_liquidations(),
                self.fetch_coinglass_long_short_ratio('BTC'),
                self.fetch_coinglass_long_short_ratio('ETH'),
                return_exceptions=True
            )
            
            if not isinstance(funding, Exception):
                signals['funding'] = funding
            if not isinstance(liquidations, Exception):
                signals['liquidations'] = liquidations
            if not isinstance(ls_btc, Exception):
                signals['long_short']['BTC'] = ls_btc
            if not isinstance(ls_eth, Exception):
                signals['long_short']['ETH'] = ls_eth
            
            # Анализируем сигналы
            long_votes = 0
            short_votes = 0
            
            # Funding signals
            if signals['funding'].get('extreme_short'):
                long_votes += len(signals['funding']['extreme_short'])
            if signals['funding'].get('extreme_long'):
                short_votes += len(signals['funding']['extreme_long'])
            
            # Liquidation signal
            if signals['liquidations'].get('signal') == 'LONG':
                long_votes += 2
            elif signals['liquidations'].get('signal') == 'SHORT':
                short_votes += 2
            
            # Long/Short ratio
            for ls_data in signals['long_short'].values():
                if isinstance(ls_data, dict):
                    if ls_data.get('signal') == 'LONG':
                        long_votes += 1
                    elif ls_data.get('signal') == 'SHORT':
                        short_votes += 1
            
            # Определяем общий сигнал
            total_votes = long_votes + short_votes
            if total_votes > 0:
                if long_votes > short_votes:
                    signals['overall_signal'] = 'LONG'
                    signals['confidence'] = 0.5 + (long_votes / total_votes) * 0.3
                elif short_votes > long_votes:
                    signals['overall_signal'] = 'SHORT'
                    signals['confidence'] = 0.5 + (short_votes / total_votes) * 0.3
            
            if signals['overall_signal']:
                logger.info(f"[COINGLASS] Overall signal: {signals['overall_signal']} (confidence: {signals['confidence']:.0%})")
            
        except Exception as e:
            logger.error(f"[COINGLASS] Aggregation error: {e}")
        
        return signals
    
    # ==================== END COINGLASS ====================
    
    async def fetch_binance_announcements(self) -> List[NewsEvent]:
        """
        Получить важные анонсы с Binance (листинги, делистинги)
        """
        events = []
        
        try:
            async with aiohttp.ClientSession() as session:
                # Binance announcements API
                async with session.get(
                    'https://www.binance.com/bapi/composite/v1/public/cms/article/list/query',
                    params={
                        'type': 1,
                        'pageNo': 1,
                        'pageSize': 20
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers={
                        'User-Agent': 'Mozilla/5.0',
                        'Accept': 'application/json'
                    }
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        articles = data.get('data', {}).get('catalogs', [])
                        
                        for catalog in articles:
                            for article in catalog.get('articles', [])[:10]:
                                title = article.get('title', '')
                                
                                if not title:
                                    continue
                                
                                news_hash = self._get_news_hash(title, "binance")
                                if news_hash in self.seen_news:
                                    continue
                                self.seen_news.append(news_hash)
                                
                                # Определяем тип анонса
                                title_lower = title.lower()
                                
                                if 'list' in title_lower and 'delist' not in title_lower:
                                    sentiment = NewsSentiment.VERY_BULLISH
                                    impact = NewsImpact.HIGH
                                    category = NewsCategory.LISTING
                                elif 'delist' in title_lower or 'remove' in title_lower:
                                    sentiment = NewsSentiment.VERY_BEARISH
                                    impact = NewsImpact.HIGH
                                    category = NewsCategory.LISTING
                                elif 'maintenance' in title_lower or 'suspend' in title_lower:
                                    sentiment = NewsSentiment.BEARISH
                                    impact = NewsImpact.MEDIUM
                                    category = NewsCategory.OTHER
                                else:
                                    sentiment = NewsSentiment.NEUTRAL
                                    impact = NewsImpact.LOW
                                    category = NewsCategory.OTHER
                                
                                coins = self.extract_coins(title)
                                
                                event = NewsEvent(
                                    id=news_hash,
                                    source='Binance',
                                    author='Binance',
                                    title=title[:200],
                                    content=title,
                                    url=f"https://www.binance.com/en/support/announcement",
                                    timestamp=datetime.now(timezone.utc),
                                    sentiment=sentiment,
                                    impact=impact,
                                    category=category,
                                    affected_coins=coins,
                                    keywords_found=[],
                                    confidence=0.7 if impact.value >= NewsImpact.HIGH.value else 0.5
                                )
                                events.append(event)
                
                logger.info(f"[NEWS] Binance announcements: {len(events)}")
                
        except Exception as e:
            logger.debug(f"[NEWS] Binance announcements error: {e}")
        
        return events
    
    # ==================== MACRO EVENTS ====================
    
    def get_upcoming_macro_events(self, hours_ahead: int = 24) -> List[MacroEvent]:
        """Получить предстоящие макро-события"""
        events = []
        now = datetime.now(timezone.utc)
        
        # Упрощённая логика: проверяем текущий день
        weekday = now.weekday()
        hour = now.hour
        
        for event_name, event_info in MACRO_EVENTS.items():
            if weekday in event_info['typical_days']:
                for event_hour in event_info['typical_hours']:
                    if hour <= event_hour < hour + hours_ahead:
                        scheduled = now.replace(hour=event_hour, minute=0, second=0)
                        events.append(MacroEvent(
                            name=event_name,
                            description=event_info['description'],
                            scheduled_time=scheduled,
                            impact=NewsImpact[event_info['impact']]
                        ))
        
        return events
    
    def is_macro_event_window(self) -> Tuple[bool, Optional[str]]:
        """Проверить, находимся ли мы в окне макро-события"""
        now = datetime.now(timezone.utc)
        weekday = now.weekday()
        hour = now.hour
        
        for event_name, event_info in MACRO_EVENTS.items():
            if weekday in event_info['typical_days']:
                for event_hour in event_info['typical_hours']:
                    # За 30 мин до и 30 мин после
                    if event_hour - 1 <= hour <= event_hour + 1:
                        return True, event_name
        
        return False, None
    
    # ==================== SIGNAL GENERATION ====================
    
    def generate_trading_signal(self, event: NewsEvent) -> Optional[TradingSignal]:
        """Генерировать торговый сигнал на основе новости"""
        
        # Фильтр по импакту
        if event.impact.value < NewsImpact.MEDIUM.value:
            return None
        
        # Фильтр по confidence
        if event.confidence < 0.5:
            return None
        
        reasoning = []
        
        # Определяем направление
        direction = None
        confidence = event.confidence
        
        if event.sentiment in [NewsSentiment.VERY_BULLISH, NewsSentiment.BULLISH]:
            direction = 'LONG'
            reasoning.append(f"📈 Позитивная новость: {event.sentiment.name}")
        elif event.sentiment in [NewsSentiment.VERY_BEARISH, NewsSentiment.BEARISH]:
            direction = 'SHORT'
            reasoning.append(f"📉 Негативная новость: {event.sentiment.name}")
        else:
            return None  # Нейтральные не торгуем
        
        # Модификаторы уверенности
        if event.impact == NewsImpact.CRITICAL:
            confidence = min(0.95, confidence + 0.2)
            reasoning.append(f"⚡ Критическое влияние")
        elif event.impact == NewsImpact.HIGH:
            confidence = min(0.9, confidence + 0.1)
            reasoning.append(f"🔥 Высокое влияние")
        
        # Категория
        reasoning.append(f"📁 Категория: {event.category.value}")
        
        # Источник
        reasoning.append(f"📰 Источник: {event.source}")
        
        # Время жизни сигнала
        if event.category in [NewsCategory.MACRO, NewsCategory.REGULATION]:
            expires_delta = timedelta(hours=4)
            time_sensitive = True
        elif event.category == NewsCategory.HACK_EXPLOIT:
            expires_delta = timedelta(hours=1)
            time_sensitive = True
        else:
            expires_delta = timedelta(hours=2)
            time_sensitive = False
        
        return TradingSignal(
            direction=direction,
            confidence=confidence,
            source=event.source,
            reasoning=reasoning,
            affected_coins=event.affected_coins,
            time_sensitive=time_sensitive,
            expires_at=datetime.now(timezone.utc) + expires_delta,
            impact=event.impact
        )
    
    async def get_aggregated_signals(self) -> List[TradingSignal]:
        """
        Получить агрегированные торговые сигналы из всех источников
        """
        all_events = []
        signals = []
        
        # Собираем новости из всех источников параллельно
        tasks = [
            self.fetch_cryptopanic_news(),        # CryptoPanic API
            self.fetch_twitter_sentiment(),        # RSS новостные ленты (CoinTelegraph, Decrypt, etc.)
            self.fetch_fear_greed_index(),         # Fear & Greed Index
            self.fetch_coingecko_trending(),       # Trending на CoinGecko
            self.fetch_binance_announcements(),    # Анонсы Binance
            self.get_coinglass_signals(),          # Coinglass (funding, liquidations, L/S ratio)
            self.fetch_dexscreener_trending(),     # DexScreener trending tokens
            self.fetch_twitter_posts()             # Twitter/X посты от ключевых аккаунтов
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # CryptoPanic
        if isinstance(results[0], list):
            all_events.extend(results[0])
        
        # RSS News (CoinTelegraph, Decrypt, etc.)
        if isinstance(results[1], list):
            all_events.extend(results[1])
        
        # CoinGecko Trending
        if isinstance(results[3], list):
            all_events.extend(results[3])
        
        # Twitter Posts (index 7)
        if isinstance(results[7], list):
            all_events.extend(results[7])
            logger.info(f"[TWITTER] Added {len(results[7])} tweets to news feed")
        
        # Binance Announcements
        if isinstance(results[4], list):
            all_events.extend(results[4])
        
        # === COINGLASS SIGNALS ===
        if isinstance(results[5], dict):
            cg = results[5]
            overall = cg.get('overall_signal')
            if overall:
                conf = cg.get('confidence', 0.5)
                reasons = []
                
                # Funding
                if cg.get('funding', {}).get('extreme_long'):
                    reasons.append(f"🔴 {len(cg['funding']['extreme_long'])} монет с экстремальным funding")
                if cg.get('funding', {}).get('extreme_short'):
                    reasons.append(f"🟢 {len(cg['funding']['extreme_short'])} монет с негативным funding")
                
                # Liquidations
                liq = cg.get('liquidations', {})
                if liq.get('total_24h', 0) > 50_000_000:
                    reasons.append(f"💥 Ликвидаций: ${liq['total_24h']/1e6:.1f}M за 24ч")
                
                # L/S Ratio
                for symbol, ls in cg.get('long_short', {}).items():
                    if isinstance(ls, dict) and ls.get('signal'):
                        reasons.append(f"📊 {symbol} L/S Ratio: {ls.get('long_ratio', 50):.1f}%")
                
                if reasons:
                    signals.append(TradingSignal(
                        direction=overall,
                        confidence=conf,
                        source='Coinglass Analytics',
                        reasoning=reasons,
                        affected_coins=['BTC', 'ETH'],
                        time_sensitive=True,
                        expires_at=datetime.now(timezone.utc) + timedelta(hours=4),
                        impact=NewsImpact.HIGH
                    ))
                    logger.info(f"[COINGLASS] Signal: {overall} ({conf:.0%}) - {reasons[0]}")
        
        # === DEXSCREENER TRENDING ===
        if isinstance(results[6], list) and results[6]:
            trending_coins = [t['symbol'][:10] for t in results[6][:5]]
            signals.append(TradingSignal(
                direction='WATCH',
                confidence=0.4,
                source='DexScreener Trending',
                reasoning=[f"🔥 DEX Trending: {', '.join(trending_coins)}"],
                affected_coins=trending_coins,
                time_sensitive=True,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
                impact=NewsImpact.MEDIUM
            ))
            logger.info(f"[DEXSCREENER] Trending: {trending_coins}")
        
        # Fear & Greed влияет на общий сентимент
        if isinstance(results[2], dict):
            fg = results[2]
            fg_value = fg.get('value', 50)
            
            if fg_value <= 25:  # Extreme Fear
                # Контр-сигнал: покупаем на страхе
                signals.append(TradingSignal(
                    direction='LONG',
                    confidence=0.6,
                    source='Fear & Greed Index',
                    reasoning=[
                        f"😱 Extreme Fear: {fg_value}",
                        "Контр-тренд: покупка на страхе",
                        f"Классификация: {fg.get('classification')}"
                    ],
                    affected_coins=['BTC', 'ETH'],
                    time_sensitive=False,
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                    impact=NewsImpact.MEDIUM
                ))
            elif fg_value >= 75:  # Extreme Greed
                # Контр-сигнал: осторожность на жадности
                signals.append(TradingSignal(
                    direction='SHORT',
                    confidence=0.5,
                    source='Fear & Greed Index',
                    reasoning=[
                        f"🤑 Extreme Greed: {fg_value}",
                        "Контр-тренд: осторожность на жадности",
                        f"Классификация: {fg.get('classification')}"
                    ],
                    affected_coins=['BTC', 'ETH'],
                    time_sensitive=False,
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                    impact=NewsImpact.MEDIUM
                ))
        
        # Генерируем сигналы из новостей
        for event in all_events:
            signal = self.generate_trading_signal(event)
            if signal:
                signals.append(signal)
        
        # Сортируем по confidence и impact
        signals.sort(key=lambda s: (s.impact.value, s.confidence), reverse=True)
        
        # Сохраняем события
        for event in all_events:
            self.recent_events.append(event)
        
        # Обновляем market sentiment
        self._update_market_sentiment(all_events)
        
        logger.info(f"[NEWS] Generated {len(signals)} trading signals")
        return signals
    
    def _update_market_sentiment(self, events: List[NewsEvent]):
        """Обновить агрегированный рыночный сентимент"""
        if not events:
            return
        
        total_score = 0
        total_weight = 0
        
        for event in events:
            weight = event.impact.value * event.confidence
            score = event.sentiment.value * 25  # -50 to +50
            total_score += score * weight
            total_weight += weight
        
        if total_weight > 0:
            final_score = total_score / total_weight
            self.market_sentiment['score'] = max(-100, min(100, final_score))
            
            if final_score > 20:
                self.market_sentiment['trend'] = 'BULLISH'
            elif final_score < -20:
                self.market_sentiment['trend'] = 'BEARISH'
            else:
                self.market_sentiment['trend'] = 'NEUTRAL'
            
            self.market_sentiment['last_update'] = datetime.now(timezone.utc)
    
    def get_market_sentiment(self) -> Dict:
        """Получить текущий рыночный сентимент"""
        return self.market_sentiment.copy()
    
    # ==================== MANIPULATION DETECTION ====================
    
    async def detect_manipulation_news(self) -> List[Dict]:
        """
        Детекция манипуляций на основе новостного потока:
        1. Внезапный поток FUD
        2. Координированные pump-посты
        3. Фейковые новости
        """
        alerts = []
        
        # Анализируем последние события
        recent = list(self.recent_events)[-50:]
        
        if len(recent) < 5:
            return alerts
        
        # 1. Проверяем на FUD-атаку (много негатива за короткое время)
        last_hour = datetime.now(timezone.utc) - timedelta(hours=1)
        recent_negative = [e for e in recent 
                          if e.timestamp > last_hour 
                          and e.sentiment.value < 0]
        
        if len(recent_negative) >= 5:
            alerts.append({
                'type': 'FUD_ATTACK',
                'severity': 'HIGH',
                'description': f'Обнаружено {len(recent_negative)} негативных новостей за час',
                'recommendation': 'Возможная манипуляция. Не паниковать, проверить источники.'
            })
        
        # 2. Проверяем на pump-координацию
        recent_positive = [e for e in recent 
                          if e.timestamp > last_hour 
                          and e.sentiment.value > 0
                          and e.category == NewsCategory.TRADER_CALL]
        
        if len(recent_positive) >= 3:
            # Проверяем, говорят ли о одной монете
            coin_counts = {}
            for e in recent_positive:
                for coin in e.affected_coins:
                    coin_counts[coin] = coin_counts.get(coin, 0) + 1
            
            for coin, count in coin_counts.items():
                if count >= 3:
                    alerts.append({
                        'type': 'COORDINATED_PUMP',
                        'severity': 'MEDIUM',
                        'description': f'{count} трейдеров говорят о {coin}',
                        'recommendation': f'Возможный pump {coin}. Осторожно с входом.'
                    })
        
        return alerts
    
    # ==================== API FUNCTIONS ====================
    
    async def get_news_for_coin(self, coin: str) -> List[NewsEvent]:
        """Получить новости для конкретной монеты"""
        all_events = list(self.recent_events)
        
        # Фильтруем по монете
        coin_upper = coin.upper().replace('USDT', '').replace('/USDT', '')
        
        relevant = [e for e in all_events if coin_upper in e.affected_coins]
        
        return sorted(relevant, key=lambda x: x.timestamp, reverse=True)[:10]
    
    async def should_avoid_trading(self) -> Tuple[bool, Optional[str]]:
        """
        Проверить, стоит ли избегать торговли сейчас
        Возвращает: (should_avoid, reason)
        """
        # 1. Проверяем макро-события
        is_macro, macro_event = self.is_macro_event_window()
        if is_macro:
            return True, f"Окно макро-события: {macro_event}"
        
        # 2. Проверяем манипуляции
        manipulations = await self.detect_manipulation_news()
        if any(m['severity'] == 'HIGH' for m in manipulations):
            return True, "Обнаружена возможная манипуляция"
        
        # 3. Проверяем extreme sentiment
        sentiment = self.get_market_sentiment()
        if abs(sentiment['score']) > 80:
            return False, f"⚠️ Экстремальный сентимент: {sentiment['score']}"
        
        return False, None


# ==================== ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР ====================

news_analyzer = NewsAnalyzer()


# ==================== API ФУНКЦИИ ====================

async def get_news_signals() -> List[TradingSignal]:
    """Получить торговые сигналы на основе новостей"""
    return await news_analyzer.get_aggregated_signals()


async def get_market_sentiment() -> Dict:
    """Получить текущий рыночный сентимент"""
    # Обновляем если нужно
    if not news_analyzer.market_sentiment.get('last_update'):
        await news_analyzer.get_aggregated_signals()
    return news_analyzer.get_market_sentiment()


async def get_news_for_coin(coin: str) -> List[NewsEvent]:
    """Получить новости для монеты"""
    return await news_analyzer.get_news_for_coin(coin)


async def should_trade_now() -> Tuple[bool, Optional[str]]:
    """Проверить, можно ли торговать сейчас"""
    should_avoid, reason = await news_analyzer.should_avoid_trading()
    return not should_avoid, reason


async def get_upcoming_events() -> List[MacroEvent]:
    """Получить предстоящие макро-события"""
    return news_analyzer.get_upcoming_macro_events()


async def detect_manipulations() -> List[Dict]:
    """Проверить на манипуляции"""
    return await news_analyzer.detect_manipulation_news()


# ==================== ИНТЕГРАЦИЯ С SMART ANALYZER ====================

async def enhance_setup_with_news(setup: Any, coin: str) -> Any:
    """
    Улучшить торговый сетап данными из новостей
    
    Args:
        setup: TradeSetup объект из smart_analyzer
        coin: Символ монеты (например 'BTC')
    
    Returns:
        Модифицированный setup с учётом новостей
    """
    if setup is None:
        return None
    
    try:
        # Получаем новости для монеты
        news_events = await get_news_for_coin(coin)
        
        if not news_events:
            return setup
        
        # Считаем сентимент последних новостей
        bullish_count = sum(1 for e in news_events[:5] if e.sentiment.value > 0)
        bearish_count = sum(1 for e in news_events[:5] if e.sentiment.value < 0)
        
        # Модифицируем confidence
        setup_direction = setup.direction.upper()
        
        if setup_direction == 'LONG' and bullish_count > bearish_count:
            # Новости подтверждают лонг
            boost = min(0.1, bullish_count * 0.02)
            setup.confidence = min(0.95, setup.confidence + boost)
            setup.reasoning.insert(0, f"📰 Новости подтверждают ({bullish_count} позитивных)")
            logger.info(f"[NEWS] {coin}: News boost +{boost:.0%} for LONG")
            
        elif setup_direction == 'SHORT' and bearish_count > bullish_count:
            # Новости подтверждают шорт
            boost = min(0.1, bearish_count * 0.02)
            setup.confidence = min(0.95, setup.confidence + boost)
            setup.reasoning.insert(0, f"📰 Новости подтверждают ({bearish_count} негативных)")
            logger.info(f"[NEWS] {coin}: News boost +{boost:.0%} for SHORT")
            
        elif (setup_direction == 'LONG' and bearish_count > bullish_count + 2) or \
             (setup_direction == 'SHORT' and bullish_count > bearish_count + 2):
            # Новости ПРОТИВОРЕЧАТ сетапу
            penalty = 0.1
            setup.confidence = max(0.3, setup.confidence - penalty)
            setup.reasoning.insert(0, f"⚠️ Новости противоречат сетапу")
            logger.warning(f"[NEWS] {coin}: News penalty -{penalty:.0%}")
        
        # Добавляем важные новости в reasoning
        critical_news = [e for e in news_events[:3] if e.impact.value >= NewsImpact.HIGH.value]
        for news in critical_news[:2]:
            setup.reasoning.append(f"📰 {news.title[:50]}...")
        
    except Exception as e:
        logger.warning(f"[NEWS] Error enhancing setup: {e}")
    
    return setup


async def get_news_trading_opportunities() -> List[Dict]:
    """
    Получить торговые возможности на основе новостей
    Возвращает список потенциальных сделок
    """
    signals = await get_news_signals()
    
    opportunities = []
    
    for signal in signals[:5]:  # Топ-5 сигналов
        if signal.confidence >= 0.6:
            opportunities.append({
                'direction': signal.direction,
                'coins': signal.affected_coins,
                'confidence': signal.confidence,
                'reasoning': signal.reasoning,
                'source': signal.source,
                'time_sensitive': signal.time_sensitive,
                'expires_at': signal.expires_at.isoformat()
            })
    
    return opportunities
