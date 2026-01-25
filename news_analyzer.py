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
            self.fetch_binance_announcements()     # Анонсы Binance
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
        
        # Binance Announcements
        if isinstance(results[4], list):
            all_events.extend(results[4])
        
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
