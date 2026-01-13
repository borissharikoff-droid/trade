import logging
import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
from binance.client import Client

logger = logging.getLogger(__name__)

# === СТАТИСТИКА ОТКЛОНЕНИЙ СИГНАЛОВ ===
signal_stats = {
    'analyzed': 0,
    'accepted': 0,
    'rejected': 0,
    'reasons': {
        'low_liquidity': 0,
        'manipulation': 0,
        'weak_score': 0,
        'context_conflict': 0,
        'mtf_conflict': 0,
        'low_factors': 0,
        'low_confidence': 0,
        'weak_trend': 0,
        'low_volume': 0,
        'whale_against': 0,
        'cvd_against': 0,
        'orderbook_against': 0,
        'btc_against': 0
    }
}

def get_signal_stats() -> dict:
    """Получить статистику сигналов"""
    return signal_stats.copy()

def reset_signal_stats():
    """Сбросить статистику"""
    global signal_stats
    signal_stats['analyzed'] = 0
    signal_stats['accepted'] = 0
    signal_stats['rejected'] = 0
    for key in signal_stats['reasons']:
        signal_stats['reasons'][key] = 0

# Оптимальные часы для торговли (UTC)
# Лондон: 8-16, Нью-Йорк: 13-21
# Лучшее время: EU+US overlap 13-16 UTC
OPTIMAL_TRADING_HOURS = list(range(5, 23))  # 5:00 - 23:00 UTC (расширенные часы)
LOW_LIQUIDITY_HOURS = [0, 1, 2, 3, 4]  # Только глубокая ночь UTC


class TechnicalIndicators:
    """Технические индикаторы"""
    
    @staticmethod
    def rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def macd(prices: List[float]) -> Tuple[float, float, float]:
        if len(prices) < 26:
            return 0.0, 0.0, 0.0
        
        df = pd.DataFrame({'close': prices})
        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        macd_line = ema12 - ema26
        signal = macd_line.ewm(span=9).mean()
        histogram = macd_line - signal
        
        return float(macd_line.iloc[-1]), float(signal.iloc[-1]), float(histogram.iloc[-1])
    
    @staticmethod
    def bollinger_bands(prices: List[float], period: int = 20) -> Tuple[float, float, float]:
        if len(prices) < period:
            return 0, 0, 0
        
        df = pd.DataFrame({'close': prices})
        sma = df['close'].rolling(period).mean()
        std = df['close'].rolling(period).std()
        
        upper = sma + (std * 2)
        lower = sma - (std * 2)
        
        return float(upper.iloc[-1]), float(sma.iloc[-1]), float(lower.iloc[-1])
    
    @staticmethod
    def stochastic(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Tuple[float, float]:
        if len(closes) < period:
            return 50.0, 50.0
        
        lowest_low = min(lows[-period:])
        highest_high = max(highs[-period:])
        
        if highest_high == lowest_low:
            return 50.0, 50.0
        
        k = ((closes[-1] - lowest_low) / (highest_high - lowest_low)) * 100
        
        # %D = 3-period SMA of %K
        k_values = []
        for i in range(3):
            if len(closes) >= period + i:
                ll = min(lows[-(period+i):len(lows)-i] if i > 0 else lows[-period:])
                hh = max(highs[-(period+i):len(highs)-i] if i > 0 else highs[-period:])
                if hh != ll:
                    k_values.append(((closes[-(i+1)] - ll) / (hh - ll)) * 100)
        
        d = np.mean(k_values) if k_values else k
        
        return k, d
    
    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        if len(highs) < period + 1:
            return 0.0
        
        true_ranges = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            true_ranges.append(tr)
        
        return np.mean(true_ranges[-period:])
    
    @staticmethod
    def adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        """Average Directional Index - сила тренда"""
        if len(highs) < period + 1:
            return 25.0
        
        plus_dm = []
        minus_dm = []
        tr_list = []
        
        for i in range(1, len(highs)):
            high_diff = highs[i] - highs[i-1]
            low_diff = lows[i-1] - lows[i]
            
            plus_dm.append(high_diff if high_diff > low_diff and high_diff > 0 else 0)
            minus_dm.append(low_diff if low_diff > high_diff and low_diff > 0 else 0)
            
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            tr_list.append(tr)
        
        if len(tr_list) < period:
            return 25.0
        
        atr = np.mean(tr_list[-period:])
        if atr == 0:
            return 25.0
        
        plus_di = (np.mean(plus_dm[-period:]) / atr) * 100
        minus_di = (np.mean(minus_dm[-period:]) / atr) * 100
        
        if plus_di + minus_di == 0:
            return 25.0
        
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100
        return dx


class MarketAnalyzer:
    """Анализатор рынка с реальными данными"""
    
    def __init__(self):
        self.client = None
        self.session = None
        self.cache = {}
        self.cache_ttl = 60  # секунд
        
        try:
            self.client = Client()
            logger.info("[ANALYZER] Binance клиент инициализирован")
        except Exception as e:
            logger.warning(f"[ANALYZER] Binance недоступен: {e}")
    
    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def _fetch_json(self, url: str, cache_key: str = None) -> Optional[Dict]:
        """Универсальный HTTP запрос с кешированием"""
        if cache_key and cache_key in self.cache:
            cached_time, data = self.cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.cache_ttl:
                return data
        
        try:
            session = await self._get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if cache_key:
                        self.cache[cache_key] = (datetime.now(), data)
                    return data
        except Exception as e:
            logger.warning(f"[FETCH] Ошибка {url}: {e}")
        return None
    
    # ==================== ДАННЫЕ С BINANCE ====================
    
    async def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List:
        """Свечи с Binance"""
        try:
            binance_symbol = symbol.replace('/', '')
            if self.client:
                return self.client.get_klines(symbol=binance_symbol, interval=interval, limit=limit)
        except Exception as e:
            logger.warning(f"[KLINES] Ошибка: {e}")
        return []
    
    async def get_price(self, symbol: str) -> float:
        """Текущая цена"""
        try:
            binance_symbol = symbol.replace('/', '')
            if self.client:
                ticker = self.client.get_symbol_ticker(symbol=binance_symbol)
                return float(ticker['price'])
        except Exception as e:
            logger.warning(f"[PRICE] Ошибка: {e}")
        
            # Fallback
        defaults = {'BTC/USDT': 95000, 'ETH/USDT': 3300, 'BNB/USDT': 700, 'SOL/USDT': 200}
        return defaults.get(symbol, 1000)
    
    async def get_funding_rate(self, symbol: str) -> float:
        """Funding Rate с Binance Futures"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={binance_symbol}&limit=1"
            data = await self._fetch_json(url, f"funding_{binance_symbol}")
            if data and len(data) > 0:
                rate = float(data[0]['fundingRate'])
                logger.info(f"[FUNDING] {symbol}: {rate:.6f}")
                return rate
        except Exception as e:
            logger.warning(f"[FUNDING] Ошибка: {e}")
        return 0.0
    
    async def get_open_interest(self, symbol: str) -> Dict:
        """Open Interest с Binance Futures"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={binance_symbol}"
            data = await self._fetch_json(url, f"oi_{binance_symbol}")
            if data:
                oi = float(data['openInterest'])
                logger.info(f"[OI] {symbol}: {oi:.2f}")
                return {'value': oi, 'symbol': symbol}
        except Exception as e:
            logger.warning(f"[OI] Ошибка: {e}")
        return {'value': 0, 'symbol': symbol}
    
    async def get_long_short_ratio(self, symbol: str) -> float:
        """Long/Short Ratio"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={binance_symbol}&period=1h&limit=1"
            data = await self._fetch_json(url, f"lsr_{binance_symbol}")
            if data and len(data) > 0:
                ratio = float(data[0]['longShortRatio'])
                logger.info(f"[LSR] {symbol}: {ratio:.4f}")
                return ratio
        except Exception as e:
            logger.warning(f"[LSR] Ошибка: {e}")
        return 1.0
    
    # ==================== ORDER BOOK IMBALANCE ====================
    
    async def get_order_book_imbalance(self, symbol: str) -> Dict:
        """Анализ дисбаланса ордербука - давление покупателей/продавцов"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://api.binance.com/api/v3/depth?symbol={binance_symbol}&limit=100"
            data = await self._fetch_json(url, f"depth_{binance_symbol}")
            
            if data:
                bids = data.get('bids', [])
                asks = data.get('asks', [])
                
                # Суммарный объём на покупку и продажу
                bid_volume = sum(float(b[1]) for b in bids[:50])
                ask_volume = sum(float(a[1]) for a in asks[:50])
                
                total = bid_volume + ask_volume
                if total == 0:
                    return {'imbalance': 0, 'signal': 'NEUTRAL', 'bid_volume': 0, 'ask_volume': 0}
                
                # Imbalance: положительный = больше покупателей
                imbalance = (bid_volume - ask_volume) / total
                
                # Сигнал
                if imbalance > 0.15:
                    signal = 'STRONG_BUY'
                elif imbalance > 0.05:
                    signal = 'BUY'
                elif imbalance < -0.15:
                    signal = 'STRONG_SELL'
                elif imbalance < -0.05:
                    signal = 'SELL'
                else:
                    signal = 'NEUTRAL'
                
                logger.info(f"[ORDERBOOK] {symbol}: Imbalance={imbalance:.2%}, Bid={bid_volume:.0f}, Ask={ask_volume:.0f}")
                
                return {
                    'imbalance': imbalance,
                    'signal': signal,
                    'bid_volume': bid_volume,
                    'ask_volume': ask_volume,
                    'ratio': bid_volume / ask_volume if ask_volume > 0 else 1
                }
        except Exception as e:
            logger.warning(f"[ORDERBOOK] Ошибка: {e}")
        
        return {'imbalance': 0, 'signal': 'NEUTRAL', 'bid_volume': 0, 'ask_volume': 0}
    
    # ==================== OPEN INTEREST CHANGE ====================
    
    async def get_open_interest_change(self, symbol: str) -> Dict:
        """Изменение Open Interest - рост OI + рост цены = сильный тренд"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={binance_symbol}&period=1h&limit=24"
            data = await self._fetch_json(url, f"oi_hist_{binance_symbol}")
            
            if data and len(data) >= 2:
                current_oi = float(data[-1]['sumOpenInterest'])
                prev_oi = float(data[-2]['sumOpenInterest'])
                oi_24h_ago = float(data[0]['sumOpenInterest']) if len(data) >= 24 else prev_oi
                
                # Изменение за час и за 24 часа
                change_1h = (current_oi - prev_oi) / prev_oi if prev_oi > 0 else 0
                change_24h = (current_oi - oi_24h_ago) / oi_24h_ago if oi_24h_ago > 0 else 0
                
                # Интерпретация
                # OI растёт + цена растёт = бычий тренд усиливается
                # OI растёт + цена падает = медвежий тренд усиливается
                # OI падает + цена растёт = шорт-сквиз
                # OI падает + цена падает = лонг-ликвидации
                
                logger.info(f"[OI_CHANGE] {symbol}: 1h={change_1h:.2%}, 24h={change_24h:.2%}")
                
                return {
                    'current': current_oi,
                    'change_1h': change_1h,
                    'change_24h': change_24h,
                    'rising': change_1h > 0.01,
                    'falling': change_1h < -0.01
                }
        except Exception as e:
            logger.warning(f"[OI_CHANGE] Ошибка: {e}")
        
        return {'current': 0, 'change_1h': 0, 'change_24h': 0, 'rising': False, 'falling': False}
    
    # ==================== CVD (Cumulative Volume Delta) ====================
    
    async def get_cvd(self, symbol: str) -> Dict:
        """CVD - реальный спрос vs предложение на основе тиковых данных"""
        try:
            binance_symbol = symbol.replace('/', '')
            # Получаем последние сделки
            url = f"https://api.binance.com/api/v3/aggTrades?symbol={binance_symbol}&limit=1000"
            data = await self._fetch_json(url, f"trades_{binance_symbol}")
            
            if data:
                buy_volume = 0
                sell_volume = 0
                
                for trade in data:
                    qty = float(trade['q'])
                    # isBuyerMaker = True означает, что покупатель был мейкером (лимитка)
                    # т.е. продавец был тейкером (маркет ордер на продажу)
                    if trade['m']:  # Buyer was maker = sell aggressor
                        sell_volume += qty
                    else:
                        buy_volume += qty
                
                total = buy_volume + sell_volume
                delta = buy_volume - sell_volume
                delta_percent = delta / total if total > 0 else 0
                
                # Сигнал
                if delta_percent > 0.1:
                    signal = 'STRONG_BUY'
                elif delta_percent > 0.03:
                    signal = 'BUY'
                elif delta_percent < -0.1:
                    signal = 'STRONG_SELL'
                elif delta_percent < -0.03:
                    signal = 'SELL'
                else:
                    signal = 'NEUTRAL'
                
                logger.info(f"[CVD] {symbol}: Delta={delta_percent:.2%}, Buy={buy_volume:.0f}, Sell={sell_volume:.0f}")
                
                return {
                    'delta': delta,
                    'delta_percent': delta_percent,
                    'buy_volume': buy_volume,
                    'sell_volume': sell_volume,
                    'signal': signal
                }
        except Exception as e:
            logger.warning(f"[CVD] Ошибка: {e}")
        
        return {'delta': 0, 'delta_percent': 0, 'buy_volume': 0, 'sell_volume': 0, 'signal': 'NEUTRAL'}
    
    # ==================== WHALE ALERTS ====================
    
    async def check_whale_activity(self, symbol: str) -> Dict:
        """Проверка крупных транзакций (киты)"""
        try:
            binance_symbol = symbol.replace('/', '')
            # Получаем последние сделки
            url = f"https://api.binance.com/api/v3/aggTrades?symbol={binance_symbol}&limit=500"
            data = await self._fetch_json(url, f"whale_{binance_symbol}")
            
            if data:
                # Считаем средний размер сделки
                quantities = [float(t['q']) for t in data]
                avg_qty = np.mean(quantities)
                std_qty = np.std(quantities)
                
                # Ищем сделки > 3 стандартных отклонений (киты)
                whale_threshold = avg_qty + 3 * std_qty
                whale_trades = [t for t in data if float(t['q']) > whale_threshold]
                
                whale_buy = sum(float(t['q']) for t in whale_trades if not t['m'])
                whale_sell = sum(float(t['q']) for t in whale_trades if t['m'])
                
                # Активность китов
                whale_activity = len(whale_trades) / len(data) if data else 0
                whale_bias = 'BUY' if whale_buy > whale_sell * 1.5 else ('SELL' if whale_sell > whale_buy * 1.5 else 'NEUTRAL')
                
                logger.info(f"[WHALE] {symbol}: {len(whale_trades)} whale trades, Bias={whale_bias}")
                
                return {
                    'whale_trades_count': len(whale_trades),
                    'whale_buy_volume': whale_buy,
                    'whale_sell_volume': whale_sell,
                    'whale_activity': whale_activity,
                    'bias': whale_bias,
                    'threshold': whale_threshold
                }
        except Exception as e:
            logger.warning(f"[WHALE] Ошибка: {e}")
        
        return {'whale_trades_count': 0, 'whale_buy_volume': 0, 'whale_sell_volume': 0, 'whale_activity': 0, 'bias': 'NEUTRAL'}
    
    # ==================== LIQUIDATION ESTIMATE ====================
    
    async def estimate_liquidation_levels(self, symbol: str) -> Dict:
        """Оценка уровней ликвидаций на основе OI и цены"""
        try:
            current_price = await self.get_price(symbol)
            
            # Типичные плечи: 5x, 10x, 20x, 50x, 100x
            # Ликвидация лонга при падении на: 20%, 10%, 5%, 2%, 1%
            # Ликвидация шорта при росте на: 20%, 10%, 5%, 2%, 1%
            
            leverages = [5, 10, 20, 50, 100]
            liq_drops = [0.20, 0.10, 0.05, 0.02, 0.01]
            
            long_liquidations = []
            short_liquidations = []
            
            for lev, drop in zip(leverages, liq_drops):
                long_liq = current_price * (1 - drop)
                short_liq = current_price * (1 + drop)
                long_liquidations.append({'leverage': lev, 'price': long_liq})
                short_liquidations.append({'leverage': lev, 'price': short_liq})
            
            # Ближайшие уровни
            nearest_long_liq = current_price * 0.98  # -2% (x50 лонги)
            nearest_short_liq = current_price * 1.02  # +2% (x50 шорты)
            
            # Магнит - цена часто идёт к уровням ликвидаций
            # Если ближе к шортовым ликвидациям = магнит вверх
            # Если ближе к лонговым ликвидациям = магнит вниз
            
            dist_to_long_liq = (current_price - nearest_long_liq) / current_price
            dist_to_short_liq = (nearest_short_liq - current_price) / current_price
            
            if dist_to_short_liq < dist_to_long_liq:
                magnet = 'UP'  # Шортовые ликвидации ближе
            else:
                magnet = 'DOWN'  # Лонговые ликвидации ближе
            
            logger.info(f"[LIQ] {symbol}: Magnet={magnet}, Long@${nearest_long_liq:.0f}, Short@${nearest_short_liq:.0f}")
            
            return {
                'long_liquidations': long_liquidations,
                'short_liquidations': short_liquidations,
                'nearest_long_liq': nearest_long_liq,
                'nearest_short_liq': nearest_short_liq,
                'magnet': magnet,
                'current_price': current_price
            }
        except Exception as e:
            logger.warning(f"[LIQ] Ошибка: {e}")
        
        return {'magnet': 'NEUTRAL', 'long_liquidations': [], 'short_liquidations': []}
    
    # ==================== CRYPTO NEWS & SENTIMENT (MULTI-SOURCE) ====================
    
    # Ключевые слова для анализа
    BULLISH_KEYWORDS = [
        'surge', 'surges', 'surging', 'rally', 'rallies', 'bullish', 'breakout', 'breaks out',
        'adoption', 'approval', 'approved', 'etf approved', 'partnership', 'upgrade',
        'all-time high', 'ath', 'new high', 'record high', 'moon', 'pump', 'pumping',
        'trump crypto', 'trump bitcoin', 'trump supports', 'institutional buy',
        'accumulating', 'accumulation', 'whale buy', 'whales buying', 'massive buy',
        'bullish signal', 'golden cross', 'breakout confirmed', 'support holds',
        'positive', 'growth', 'growing', 'soars', 'soaring', 'explodes', 'skyrockets'
    ]
    
    BEARISH_KEYWORDS = [
        'crash', 'crashes', 'crashing', 'dump', 'dumps', 'dumping', 'bearish', 'plunge',
        'ban', 'bans', 'banned', 'regulation', 'regulatory crackdown', 'sec', 'lawsuit',
        'hack', 'hacked', 'exploit', 'exploited', 'scam', 'fraud', 'bankruptcy', 'bankrupt',
        'sell-off', 'selloff', 'selling', 'fear', 'panic', 'investigation', 'warning',
        'concern', 'risk', 'risky', 'collapse', 'collapses', 'tank', 'tanks', 'tanking',
        'death cross', 'breakdown', 'support breaks', 'resistance rejected', 'rejected',
        'whale sell', 'whales selling', 'massive sell', 'liquidation', 'liquidated',
        'fud', 'negative', 'decline', 'declining', 'falls', 'falling', 'drops', 'dropping'
    ]
    
    HIGH_IMPACT_KEYWORDS = [
        'trump', 'biden', 'president', 'white house', 'congress', 'senate',
        'sec', 'cftc', 'fed', 'federal reserve', 'powell', 'gensler',
        'regulation', 'regulatory', 'law', 'legislation', 'bill passed',
        'etf approved', 'spot etf', 'bitcoin etf', 'eth etf',
        'china ban', 'russia', 'us government',
        'ban crypto', 'banned', 'illegal',
        'blackrock', 'fidelity', 'grayscale', 'microstrategy', 'tesla',
        'hack', 'hacked', 'exploit', 'stolen', 'million stolen',
        'breaking:', 'just in:', 'urgent:', 'emergency'
    ]
    
    URGENCY_KEYWORDS = [
        'breaking', 'just in', 'just now', 'happening now', 'urgent', 'alert',
        'moments ago', 'minutes ago', 'live', 'developing', 'confirmed'
    ]
    
    async def get_crypto_news_sentiment(self, symbol: str) -> Dict:
        """
        Мульти-источниковый анализ новостей:
        1. CryptoPanic API (агрегатор)
        2. CryptoCompare News
        3. RSS фиды (CoinDesk, CoinTelegraph)
        4. Twitter/X через Nitter RSS
        """
        ticker = symbol.split("/")[0] if "/" in symbol else symbol.replace("USDT", "")
        
        news_sentiment = {
            'score': 0.5,
            'impact': 'LOW',
            'urgency': 'NORMAL',
            'bias': 'NEUTRAL',
            'headlines': [],
            'warnings': [],
            'sources': [],
            'bullish_count': 0,
            'bearish_count': 0,
            'breaking_news': False,
            'trade_recommendation': 'NORMAL'  # NORMAL, CAUTION, PAUSE, AGGRESSIVE
        }
        
        all_news = []
        
        # === 1. CryptoPanic API (бесплатный, быстрый) ===
        try:
            cryptopanic_url = f"https://cryptopanic.com/api/v1/posts/?auth_token=free&currencies={ticker}&kind=news&filter=hot"
            data = await self._fetch_json(cryptopanic_url, f"cryptopanic_{ticker}")
            
            if data and 'results' in data:
                for item in data['results'][:15]:
                    all_news.append({
                        'title': item.get('title', ''),
                        'source': item.get('source', {}).get('title', 'CryptoPanic'),
                        'url': item.get('url', ''),
                        'votes': item.get('votes', {}),
                        'time': item.get('published_at', '')
                    })
                news_sentiment['sources'].append('CryptoPanic')
                logger.info(f"[NEWS] CryptoPanic: {len(data['results'])} items for {ticker}")
        except Exception as e:
            logger.warning(f"[NEWS] CryptoPanic error: {e}")
        
        # === 2. CryptoCompare News ===
        try:
            cc_url = f"https://min-api.cryptocompare.com/data/v2/news/?categories={ticker},BTC,Regulation&lang=EN"
            data = await self._fetch_json(cc_url, f"cc_news_{ticker}")
            
            if data and 'Data' in data:
                for item in data['Data'][:10]:
                    all_news.append({
                        'title': item.get('title', ''),
                        'body': item.get('body', '')[:500],
                        'source': item.get('source', 'CryptoCompare'),
                        'time': item.get('published_on', '')
                    })
                news_sentiment['sources'].append('CryptoCompare')
        except Exception as e:
            logger.warning(f"[NEWS] CryptoCompare error: {e}")
        
        # === 3. Twitter/X мониторинг через публичные источники ===
        try:
            # Проверяем упоминания через альтернативные источники
            # Используем социальные метрики с CryptoCompare
            social_url = f"https://min-api.cryptocompare.com/data/social/coin/latest?coinId=1182"  # BTC
            social_data = await self._fetch_json(social_url, "social_btc")
            
            if social_data and 'Data' in social_data:
                twitter_data = social_data['Data'].get('Twitter', {})
                if twitter_data:
                    followers = twitter_data.get('followers', 0)
                    # Если резкий рост активности - это сигнал
                    news_sentiment['social_activity'] = 'HIGH' if followers > 1000000 else 'NORMAL'
        except Exception as e:
            logger.warning(f"[NEWS] Social data error: {e}")
        
        # === АНАЛИЗ СОБРАННЫХ НОВОСТЕЙ ===
        bullish_count = 0
        bearish_count = 0
        high_impact = False
        is_breaking = False
        urgency_score = 0
        
        for news in all_news:
            title = news.get('title', '').lower()
            body = news.get('body', '').lower() if 'body' in news else ''
            combined = title + " " + body
            
            # Проверка на срочность
            for keyword in self.URGENCY_KEYWORDS:
                if keyword in combined:
                    is_breaking = True
                    urgency_score += 2
                    break
            
            # Проверка на высокий импакт
            impact_count = 0
            for keyword in self.HIGH_IMPACT_KEYWORDS:
                if keyword in combined:
                    impact_count += 1
                    high_impact = True
                    if len(news_sentiment['headlines']) < 5:
                        news_sentiment['headlines'].append(news.get('title', '')[:120])
            
            # Bullish keywords
            bull_score = 0
            for keyword in self.BULLISH_KEYWORDS:
                if keyword in combined:
                    bull_score += 1
            if bull_score > 0:
                bullish_count += min(bull_score, 3)  # Макс 3 за одну новость
            
            # Bearish keywords
            bear_score = 0
            for keyword in self.BEARISH_KEYWORDS:
                if keyword in combined:
                    bear_score += 1
                    # Критические предупреждения
                    if keyword in ['hack', 'hacked', 'exploit', 'stolen', 'ban', 'sec lawsuit']:
                        if len(news_sentiment['warnings']) < 3:
                            news_sentiment['warnings'].append(f"🚨 {news.get('title', '')[:100]}")
            if bear_score > 0:
                bearish_count += min(bear_score, 3)
            
            # Голоса сообщества (CryptoPanic)
            votes = news.get('votes', {})
            if votes:
                if votes.get('positive', 0) > votes.get('negative', 0) + 5:
                    bullish_count += 1
                elif votes.get('negative', 0) > votes.get('positive', 0) + 5:
                    bearish_count += 1
        
        # === РАСЧЁТ ФИНАЛЬНОГО SENTIMENT ===
        news_sentiment['bullish_count'] = bullish_count
        news_sentiment['bearish_count'] = bearish_count
        news_sentiment['breaking_news'] = is_breaking
        
        total = bullish_count + bearish_count
        if total > 0:
            # Score от 0 (bearish) до 1 (bullish)
            news_sentiment['score'] = bullish_count / total
        else:
            news_sentiment['score'] = 0.5
        
        # Impact level
        if is_breaking and high_impact:
            news_sentiment['impact'] = 'CRITICAL'
            news_sentiment['urgency'] = 'IMMEDIATE'
        elif high_impact or urgency_score >= 3:
            news_sentiment['impact'] = 'HIGH'
            news_sentiment['urgency'] = 'HIGH'
        elif total >= 5:
            news_sentiment['impact'] = 'MEDIUM'
        else:
            news_sentiment['impact'] = 'LOW'
        
        # Bias
        diff = bullish_count - bearish_count
        if diff >= 5:
            news_sentiment['bias'] = 'STRONG_BULLISH'
        elif diff >= 2:
            news_sentiment['bias'] = 'BULLISH'
        elif diff <= -5:
            news_sentiment['bias'] = 'STRONG_BEARISH'
        elif diff <= -2:
            news_sentiment['bias'] = 'BEARISH'
        else:
            news_sentiment['bias'] = 'NEUTRAL'
        
        # === ТОРГОВАЯ РЕКОМЕНДАЦИЯ ===
        # PAUSE только при CRITICAL + сильно противоречивых новостях (много bull И много bear)
        if news_sentiment['impact'] == 'CRITICAL':
            if news_sentiment['bias'] in ['STRONG_BULLISH', 'STRONG_BEARISH']:
                news_sentiment['trade_recommendation'] = 'AGGRESSIVE'  # Торгуем по тренду новостей
            elif news_sentiment['bias'] in ['BULLISH', 'BEARISH']:
                news_sentiment['trade_recommendation'] = 'CAUTION'  # Есть направление, но осторожно
            else:
                # NEUTRAL = новости не дают направления, торгуем нормально
                news_sentiment['trade_recommendation'] = 'NORMAL'
        elif news_sentiment['impact'] == 'HIGH':
            news_sentiment['trade_recommendation'] = 'CAUTION'  # Уменьшенный размер
        else:
            news_sentiment['trade_recommendation'] = 'NORMAL'
        
        logger.info(f"[NEWS] {ticker}: Bull={bullish_count}, Bear={bearish_count}, "
                   f"Impact={news_sentiment['impact']}, Bias={news_sentiment['bias']}, "
                   f"Breaking={is_breaking}, Rec={news_sentiment['trade_recommendation']}")
        
        if news_sentiment['headlines']:
            logger.info(f"[NEWS] Headlines: {news_sentiment['headlines'][:2]}")
        
        return news_sentiment
    
    # ==================== MANIPULATION DETECTION ====================
    
    async def detect_manipulation(self, symbol: str) -> Dict:
        """Детекция возможных манипуляций рынком"""
        manipulation = {
            'detected': False,
            'type': None,
            'severity': 'LOW',
            'signals': [],
            'recommendation': 'TRADE'
        }
        
        try:
            # Собираем данные
            klines_5m = await self.get_klines(symbol, '5m', 50)
            klines_1h = await self.get_klines(symbol, '1h', 24)
            funding = await self.get_funding_rate(symbol)
            oi_change = await self.get_open_interest_change(symbol)
            orderbook = await self.get_order_book_imbalance(symbol)
            
            if not klines_5m or not klines_1h:
                return manipulation
            
            closes_5m = [float(k[4]) for k in klines_5m]
            volumes_5m = [float(k[5]) for k in klines_5m]
            closes_1h = [float(k[4]) for k in klines_1h]
            
            # === 1. VOLUME SPIKE DETECTION ===
            avg_vol = np.mean(volumes_5m[:-5])  # Средний объём без последних 5 свечей
            recent_vol = np.mean(volumes_5m[-5:])  # Последние 5 свечей
            
            if avg_vol > 0 and recent_vol > avg_vol * 3:
                manipulation['signals'].append(f"📊 Объём в 3x+ выше нормы ({recent_vol/avg_vol:.1f}x)")
                manipulation['detected'] = True
            
            # === 2. SUDDEN PRICE MOVE (без объёма = манипуляция) ===
            price_change_5m = (closes_5m[-1] - closes_5m[-6]) / closes_5m[-6] * 100
            if abs(price_change_5m) > 1.5 and recent_vol < avg_vol * 1.5:
                manipulation['signals'].append(f"🎭 Резкое движение ({price_change_5m:.1f}%) без объёма - возможная манипуляция")
                manipulation['detected'] = True
                manipulation['type'] = 'PUMP_DUMP' if price_change_5m > 0 else 'DUMP_PUMP'
            
            # === 3. FUNDING RATE EXTREME ===
            if abs(funding) > 0.001:  # >0.1% за 8 часов = экстремально
                direction = "лонгов" if funding > 0 else "шортов"
                manipulation['signals'].append(f"💰 Экстремальный Funding ({funding:.4f}) - переизбыток {direction}")
                manipulation['detected'] = True
            
            # === 4. OI + PRICE DIVERGENCE ===
            # Цена растёт, но OI падает = ликвидации шортов (не органический рост)
            if oi_change['falling'] and price_change_5m > 0.5:
                manipulation['signals'].append("📉 Рост цены при падении OI - возможно шорт-сквиз")
                manipulation['type'] = 'SHORT_SQUEEZE'
            elif oi_change['falling'] and price_change_5m < -0.5:
                manipulation['signals'].append("📉 Падение цены при падении OI - ликвидации лонгов")
                manipulation['type'] = 'LONG_LIQUIDATION'
            
            # === 5. ORDERBOOK WALL (большая стена - возможно спуфинг) ===
            if abs(orderbook['imbalance']) > 0.4:
                side = "покупок" if orderbook['imbalance'] > 0 else "продаж"
                manipulation['signals'].append(f"🧱 Сильный дисбаланс ордербука в сторону {side} - возможный спуфинг")
            
            # === 6. WASH TRADING DETECTION ===
            # Много сделок одинакового размера подряд
            
            # === SEVERITY ===
            if len(manipulation['signals']) >= 3:
                manipulation['severity'] = 'HIGH'
                manipulation['recommendation'] = 'AVOID'
            elif len(manipulation['signals']) >= 2:
                manipulation['severity'] = 'MEDIUM'
                manipulation['recommendation'] = 'CAUTION'
            elif manipulation['detected']:
                manipulation['severity'] = 'LOW'
                manipulation['recommendation'] = 'MONITOR'
            
            if manipulation['detected']:
                logger.warning(f"[MANIPULATION] {symbol}: {manipulation['type']}, Severity={manipulation['severity']}")
                for sig in manipulation['signals']:
                    logger.warning(f"[MANIPULATION] {sig}")
            
        except Exception as e:
            logger.warning(f"[MANIPULATION] Ошибка: {e}")
        
        return manipulation
    
    # ==================== BTC CORRELATION ====================
    
    async def get_btc_correlation(self, symbol: str) -> Dict:
        """Корреляция с BTC - если BTC падает, альты падают сильнее"""
        if 'BTC' in symbol:
            return {'correlation': 1.0, 'btc_trend': 'SELF', 'impact': 'NONE'}
        
        try:
            # Получаем свечи BTC и альта
            btc_klines = await self.get_klines('BTC/USDT', '1h', 24)
            alt_klines = await self.get_klines(symbol, '1h', 24)
            
            if not btc_klines or not alt_klines or len(btc_klines) < 20 or len(alt_klines) < 20:
                return {'correlation': 0.8, 'btc_trend': 'UNKNOWN', 'impact': 'NEUTRAL'}
            
            # Изменения цены
            btc_changes = [float(btc_klines[i][4]) / float(btc_klines[i-1][4]) - 1 for i in range(1, len(btc_klines))]
            alt_changes = [float(alt_klines[i][4]) / float(alt_klines[i-1][4]) - 1 for i in range(1, len(alt_klines))]
            
            # Корреляция
            correlation = np.corrcoef(btc_changes[-20:], alt_changes[-20:])[0, 1]
            
            # Тренд BTC
            btc_change_24h = (float(btc_klines[-1][4]) - float(btc_klines[0][4])) / float(btc_klines[0][4])
            
            if btc_change_24h > 0.02:
                btc_trend = 'BULLISH'
            elif btc_change_24h < -0.02:
                btc_trend = 'BEARISH'
            else:
                btc_trend = 'NEUTRAL'
            
            # Влияние на альт
            # Если корреляция высокая и BTC падает = негатив для альта
            if correlation > 0.7:
                if btc_trend == 'BEARISH':
                    impact = 'NEGATIVE'
                elif btc_trend == 'BULLISH':
                    impact = 'POSITIVE'
                else:
                    impact = 'NEUTRAL'
            else:
                impact = 'LOW'  # Низкая корреляция, BTC мало влияет
            
            logger.info(f"[CORR] {symbol}: Corr={correlation:.2f}, BTC={btc_trend}, Impact={impact}")
            
            return {
                'correlation': correlation,
                'btc_trend': btc_trend,
                'btc_change_24h': btc_change_24h,
                'impact': impact
            }
        except Exception as e:
            logger.warning(f"[CORR] Ошибка: {e}")
        
        return {'correlation': 0.8, 'btc_trend': 'UNKNOWN', 'impact': 'NEUTRAL'}
    
    # ==================== TIME FILTER ====================
    
    def check_trading_time(self) -> Dict:
        """Проверка оптимального времени для торговли"""
        now = datetime.now(timezone.utc)
        hour = now.hour
        
        is_optimal = hour in OPTIMAL_TRADING_HOURS
        is_low_liquidity = hour in LOW_LIQUIDITY_HOURS
        
        # Сессии
        if 8 <= hour < 16:
            session = 'LONDON'
        elif 13 <= hour < 21:
            session = 'NEW_YORK'
        elif 0 <= hour < 8:
            session = 'ASIA'
        else:
            session = 'LATE'
        
        # Overlap (самая высокая ликвидность)
        is_overlap = 13 <= hour < 16  # London + NY overlap
        
        logger.info(f"[TIME] Hour={hour} UTC, Session={session}, Optimal={is_optimal}, Overlap={is_overlap}")
        
        return {
            'hour': hour,
            'session': session,
            'is_optimal': is_optimal,
            'is_low_liquidity': is_low_liquidity,
            'is_overlap': is_overlap,
            'recommendation': 'TRADE' if is_optimal else ('AVOID' if is_low_liquidity else 'CAUTION')
        }
    
    # ==================== ADAPTIVE TP/SL ====================
    
    async def calculate_adaptive_tpsl(self, symbol: str, direction: str, confidence: float) -> Dict:
        """Адаптивные TP/SL на основе ATR (волатильности)"""
        klines = await self.get_klines(symbol, '15m', 50)
        
        if not klines or len(klines) < 20:
            # Fallback к фиксированным
            return {
                'sl_percent': 0.004,
                'tp_percent': 0.007,
                'atr': 0,
                'volatility': 'UNKNOWN'
            }
        
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]
        
        # ATR
        ind = TechnicalIndicators()
        atr = ind.atr(highs, lows, closes, 14)
        current_price = closes[-1]
        
        # ATR как % от цены
        atr_percent = atr / current_price
        
        # Классификация волатильности
        if atr_percent > 0.015:
            volatility = 'HIGH'
            sl_mult = 1.5
            tp_mult = 2.0
        elif atr_percent > 0.008:
            volatility = 'MEDIUM'
            sl_mult = 1.2
            tp_mult = 1.5
        else:
            volatility = 'LOW'
            sl_mult = 1.0
            tp_mult = 1.2
        
        # Базовые значения для скальпинга x20
        base_sl = 0.003  # 0.3%
        base_tp = 0.006  # 0.6%
        
        # Адаптация под confidence
        confidence_factor = 0.8 + confidence * 0.4  # 0.8-1.2
        
        sl_percent = base_sl * sl_mult
        tp_percent = base_tp * tp_mult * confidence_factor
        
        # Risk/Reward ratio check (минимум 1.5)
        if tp_percent / sl_percent < 1.5:
            tp_percent = sl_percent * 1.5
        
        logger.info(f"[ADAPTIVE] {symbol}: ATR={atr_percent:.3%}, Vol={volatility}, SL={sl_percent:.3%}, TP={tp_percent:.3%}")
        
        return {
            'sl_percent': sl_percent,
            'tp_percent': tp_percent,
            'atr': atr,
            'atr_percent': atr_percent,
            'volatility': volatility,
            'risk_reward': tp_percent / sl_percent
        }
    
    # ==================== FEAR & GREED INDEX ====================
    
    async def get_fear_greed_index(self) -> Dict:
        """Fear & Greed Index от alternative.me"""
        url = "https://api.alternative.me/fng/?limit=1"
        data = await self._fetch_json(url, "fng")
        
        if data and 'data' in data and len(data['data']) > 0:
            fng = data['data'][0]
            value = int(fng['value'])
            classification = fng['value_classification']
            logger.info(f"[FNG] Fear & Greed: {value} ({classification})")
            return {'value': value, 'classification': classification}
        
        return {'value': 50, 'classification': 'Neutral'}
    
    # ==================== МУЛЬТИ-ТАЙМФРЕЙМ АНАЛИЗ ====================
    
    async def analyze_multi_timeframe(self, symbol: str) -> Dict:
        """Анализ нескольких таймфреймов для подтверждения тренда"""
        timeframes = ['15m', '1h', '4h']
        tf_results = {}
        
        for tf in timeframes:
            klines = await self.get_klines(symbol, tf, 50)
            if not klines or len(klines) < 30:
                continue
            
            closes = [float(k[4]) for k in klines]
            
            # Тренд по SMA
            sma10 = np.mean(closes[-10:])
            sma30 = np.mean(closes[-30:])
            current = closes[-1]
            
            if current > sma10 > sma30:
                trend = "BULLISH"
                score = 0.7
            elif current < sma10 < sma30:
                trend = "BEARISH"
                score = 0.3
            else:
                trend = "NEUTRAL"
                score = 0.5
            
            # RSI
            ind = TechnicalIndicators()
            rsi = ind.rsi(closes)
            
            tf_results[tf] = {
                'trend': trend,
                'score': score,
                'rsi': rsi,
                'price_vs_sma': (current / sma10 - 1) * 100
            }
        
        # Проверка согласованности таймфреймов
        trends = [r['trend'] for r in tf_results.values()]
        bullish_count = trends.count("BULLISH")
        bearish_count = trends.count("BEARISH")
        
        confluence = "NONE"
        if bullish_count >= 2:
            confluence = "BULLISH"
        elif bearish_count >= 2:
            confluence = "BEARISH"
        
        # Средний скор
        avg_score = np.mean([r['score'] for r in tf_results.values()]) if tf_results else 0.5
        
        logger.info(f"[MTF] {symbol}: {trends}, Confluence: {confluence}")
        
        return {
            'timeframes': tf_results,
            'confluence': confluence,
            'score': avg_score,
            'aligned': bullish_count == 3 or bearish_count == 3
        }
    
    async def detect_divergence(self, symbol: str) -> Dict:
        """Обнаружение дивергенций RSI"""
        klines = await self.get_klines(symbol, '1h', 50)
        if not klines or len(klines) < 30:
            return {'divergence': None}
        
        closes = [float(k[4]) for k in klines]
        ind = TechnicalIndicators()
        
        # Вычисляем RSI для каждой свечи
        rsi_values = []
        for i in range(20, len(closes)):
            rsi_values.append(ind.rsi(closes[:i+1]))
        
        prices = closes[-len(rsi_values):]
        
        # Ищем дивергенции на последних 10 свечах
        divergence = None
        
        # Bullish divergence: цена делает новый минимум, RSI делает более высокий минимум
        if len(prices) >= 10:
            recent_price_low_idx = np.argmin(prices[-10:])
            recent_rsi_low_idx = np.argmin(rsi_values[-10:])
            
            prev_price_low_idx = np.argmin(prices[-20:-10])
            prev_rsi_low_idx = np.argmin(rsi_values[-20:-10])
            
            recent_price_low = prices[-10:][recent_price_low_idx]
            prev_price_low = prices[-20:-10][prev_price_low_idx]
            recent_rsi_low = rsi_values[-10:][recent_rsi_low_idx]
            prev_rsi_low = rsi_values[-20:-10][prev_rsi_low_idx]
            
            # Бычья дивергенция
            if recent_price_low < prev_price_low and recent_rsi_low > prev_rsi_low:
                divergence = {
                    'type': 'BULLISH',
                    'strength': abs(recent_rsi_low - prev_rsi_low),
                    'description': 'Цена ниже, но RSI выше - разворот вверх'
                }
            
            # Медвежья дивергенция
            recent_price_high = max(prices[-10:])
            prev_price_high = max(prices[-20:-10])
            recent_rsi_high = max(rsi_values[-10:])
            prev_rsi_high = max(rsi_values[-20:-10])
            
            if recent_price_high > prev_price_high and recent_rsi_high < prev_rsi_high:
                divergence = {
                    'type': 'BEARISH',
                    'strength': abs(recent_rsi_high - prev_rsi_high),
                    'description': 'Цена выше, но RSI ниже - разворот вниз'
                }
        
        if divergence:
            logger.info(f"[DIV] {symbol}: {divergence['type']} дивергенция обнаружена")
        
        return {'divergence': divergence}
    
    async def find_support_resistance(self, symbol: str) -> Dict:
        """Определение уровней поддержки и сопротивления"""
        klines = await self.get_klines(symbol, '4h', 100)
        if not klines or len(klines) < 50:
            return {'supports': [], 'resistances': []}
        
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        current = float(klines[-1][4])
        
        # Найти локальные максимумы и минимумы
        resistances = []
        supports = []
        
        for i in range(5, len(klines) - 5):
            # Локальный максимум
            if highs[i] == max(highs[i-5:i+6]):
                resistances.append(highs[i])
            # Локальный минимум
            if lows[i] == min(lows[i-5:i+6]):
                supports.append(lows[i])
        
        # Кластеризация близких уровней
        def cluster_levels(levels, tolerance=0.02):
            if not levels:
                return []
            levels = sorted(levels)
            clusters = [[levels[0]]]
            for level in levels[1:]:
                if (level - clusters[-1][-1]) / clusters[-1][-1] < tolerance:
                    clusters[-1].append(level)
                else:
                    clusters.append([level])
            return [np.mean(c) for c in clusters]
        
        supports = cluster_levels(supports)
        resistances = cluster_levels(resistances)
        
        # Ближайшие уровни к текущей цене
        nearest_support = max([s for s in supports if s < current], default=None)
        nearest_resistance = min([r for r in resistances if r > current], default=None)
        
        return {
            'supports': supports[-3:] if supports else [],
            'resistances': resistances[:3] if resistances else [],
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'distance_to_support': ((current - nearest_support) / current * 100) if nearest_support else None,
            'distance_to_resistance': ((nearest_resistance - current) / current * 100) if nearest_resistance else None
        }
    
    # ==================== ТЕХНИЧЕСКИЙ АНАЛИЗ ====================
    
    async def analyze_technical(self, symbol: str) -> Dict:
        """Полный технический анализ"""
        klines = await self.get_klines(symbol, '1h', 100)
        
        if not klines or len(klines) < 50:
            return {'score': 0.5, 'signal': 'NEUTRAL', 'indicators': {}}
        
        closes = [float(k[4]) for k in klines]
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        
        ind = TechnicalIndicators()
        
        # RSI
        rsi = ind.rsi(closes)
        rsi_signal = 1 if rsi < 30 else (-1 if rsi > 70 else 0)
        
        # MACD
        macd_val, signal_val, hist = ind.macd(closes)
        macd_signal = 1 if hist > 0 and macd_val > signal_val else (-1 if hist < 0 else 0)
        
        # Bollinger Bands
        upper, middle, lower = ind.bollinger_bands(closes)
        current = closes[-1]
        bb_signal = 1 if current < lower else (-1 if current > upper else 0)
        
        # Stochastic
        k, d = ind.stochastic(highs, lows, closes)
        stoch_signal = 1 if k < 20 else (-1 if k > 80 else 0)
        
        # ADX (сила тренда)
        adx = ind.adx(highs, lows, closes)
        trend_strength = 'STRONG' if adx > 25 else 'WEAK'
        
        # Volume analysis
        avg_vol = np.mean(volumes[-20:])
        current_vol = volumes[-1]
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1
        
        # MA trend
        sma20 = np.mean(closes[-20:])
        sma50 = np.mean(closes[-50:])
        ma_signal = 1 if current > sma20 > sma50 else (-1 if current < sma20 < sma50 else 0)
        
        # Composite score
        signals = [rsi_signal, macd_signal, bb_signal, stoch_signal, ma_signal]
        weights = [0.2, 0.25, 0.15, 0.15, 0.25]
        
        score = sum(s * w for s, w in zip(signals, weights))
        
        # Normalize to 0-1
        normalized_score = (score + 1) / 2
        
        # Determine signal
        if normalized_score > 0.6:
            signal = 'LONG'
        elif normalized_score < 0.4:
            signal = 'SHORT'
        else:
            signal = 'NEUTRAL'
        
        indicators = {
            'rsi': rsi,
            'macd': macd_val,
            'macd_hist': hist,
            'stoch_k': k,
            'stoch_d': d,
            'adx': adx,
            'volume_ratio': vol_ratio,
            'price_vs_sma20': (current / sma20 - 1) * 100,
            'trend_strength': trend_strength
        }
        
        logger.info(f"[TECH] {symbol}: RSI={rsi:.1f}, MACD_hist={hist:.2f}, Stoch={k:.1f}, ADX={adx:.1f}")
        logger.info(f"[TECH] Score: {normalized_score:.2f}, Signal: {signal}")
        
        return {
            'score': normalized_score,
            'signal': signal,
            'indicators': indicators
        }
    
    # ==================== SENTIMENT ANALYSIS ====================
    
    async def analyze_sentiment(self, symbol: str) -> Dict:
        """Анализ сентимента на основе реальных данных"""
        
        # Fear & Greed
        fng = await self.get_fear_greed_index()
        fng_score = fng['value'] / 100  # 0-1
        
        # Funding Rate
        funding = await self.get_funding_rate(symbol)
        # Положительный funding = много лонгов = перекуплено
        # Отрицательный funding = много шортов = перепродано
        funding_signal = -1 if funding > 0.0005 else (1 if funding < -0.0005 else 0)
        
        # Long/Short Ratio
        lsr = await self.get_long_short_ratio(symbol)
        # LSR > 1.5 = много лонгов = bearish signal (contrarian)
        # LSR < 0.7 = много шортов = bullish signal (contrarian)
        lsr_signal = -1 if lsr > 1.5 else (1 if lsr < 0.7 else 0)
        
        # Combine
        sentiment_score = (
            fng_score * 0.4 +
            (0.5 + funding_signal * 0.2) * 0.3 +
            (0.5 + lsr_signal * 0.2) * 0.3
        )
        
        logger.info(f"[SENTIMENT] FnG={fng['value']}, Funding={funding:.6f}, LSR={lsr:.2f}")
        logger.info(f"[SENTIMENT] Score: {sentiment_score:.2f}")
        
        return {
            'score': sentiment_score,
            'fear_greed': fng,
            'funding_rate': funding,
            'long_short_ratio': lsr
        }
    
    # ==================== ГЛУБОКИЙ АНАЛИЗ ====================
    
    def _analyze_market_context(self, indicators: Dict, sentiment: Dict) -> Dict:
        """Анализ контекста рынка и генерация выводов"""
        insights = []
        warnings = []
        bullish_factors = 0
        bearish_factors = 0
        
        rsi = indicators.get('rsi', 50)
        macd_hist = indicators.get('macd_hist', 0)
        adx = indicators.get('adx', 25)
        volume_ratio = indicators.get('volume_ratio', 1)
        stoch_k = indicators.get('stoch_k', 50)
        price_vs_sma = indicators.get('price_vs_sma20', 0)
        
        fng = sentiment.get('fear_greed', {}).get('value', 50)
        funding = sentiment.get('funding_rate', 0)
        lsr = sentiment.get('long_short_ratio', 1)
        
        # === RSI АНАЛИЗ ===
        if rsi < 30:
            insights.append("📉 RSI в зоне перепроданности — потенциал отскока вверх")
            bullish_factors += 2
        elif rsi > 70:
            insights.append("📈 RSI в зоне перекупленности — риск коррекции")
            bearish_factors += 2
        elif 40 <= rsi <= 60:
            insights.append("⚖️ RSI нейтрален — рынок в равновесии")
        elif rsi < 40:
            insights.append("📊 RSI показывает слабость, но ещё не перепродан")
            bullish_factors += 1
        else:
            insights.append("📊 RSI показывает силу, но ещё не перекуплен")
            bearish_factors += 1
        
        # === MACD АНАЛИЗ ===
        if macd_hist > 0:
            if macd_hist > 50:
                insights.append("🚀 MACD сильно бычий — моментум на стороне покупателей")
                bullish_factors += 2
            else:
                insights.append("📈 MACD положительный — бычий моментум")
                bullish_factors += 1
        else:
            if macd_hist < -50:
                insights.append("💥 MACD сильно медвежий — давление продавцов")
                bearish_factors += 2
            else:
                insights.append("📉 MACD отрицательный — медвежий моментум")
                bearish_factors += 1
        
        # === ТРЕНД (ADX) ===
        if adx > 40:
            insights.append("💪 ADX > 40 — очень сильный тренд, можно торговать по тренду")
        elif adx > 25:
            insights.append("📊 ADX умеренный — тренд присутствует")
        else:
            warnings.append("⚠️ ADX < 25 — слабый тренд, высокий риск флэта")
        
        # === ОБЪЁМ ===
        if volume_ratio > 1.5:
            insights.append("📊 Объём выше среднего на 50%+ — подтверждение движения")
        elif volume_ratio < 0.7:
            warnings.append("⚠️ Низкий объём — движение может быть ложным")
        
        # === СТОХАСТИК ===
        if stoch_k < 20 and rsi < 35:
            insights.append("🎯 Стохастик + RSI оба в зоне перепроданности — сильный сигнал на покупку")
            bullish_factors += 2
        elif stoch_k > 80 and rsi > 65:
            insights.append("🎯 Стохастик + RSI оба в зоне перекупленности — сильный сигнал на продажу")
            bearish_factors += 2
        
        # === ДИВЕРГЕНЦИЯ RSI/ЦЕНА ===
        if price_vs_sma > 2 and rsi < 50:
            warnings.append("⚠️ Возможная медвежья дивергенция: цена растёт, RSI падает")
            bearish_factors += 1
        elif price_vs_sma < -2 and rsi > 50:
            insights.append("💡 Возможная бычья дивергенция: цена падает, RSI растёт")
            bullish_factors += 1
        
        # === FEAR & GREED ===
        if fng < 25:
            insights.append(f"😱 Extreme Fear ({fng}) — рынок в панике, исторически хорошее время для покупок")
            bullish_factors += 2
        elif fng < 40:
            insights.append(f"😰 Fear ({fng}) — осторожный оптимизм для покупок")
            bullish_factors += 1
        elif fng > 75:
            insights.append(f"🤑 Extreme Greed ({fng}) — эйфория на рынке, риск коррекции")
            bearish_factors += 2
        elif fng > 60:
            insights.append(f"😊 Greed ({fng}) — оптимизм, но осторожно с лонгами")
            bearish_factors += 1
        
        # === FUNDING RATE ===
        if funding > 0.0003:
            insights.append("💰 Высокий Funding Rate — много лонгов, возможен шорт-сквиз наоборот")
            bearish_factors += 1
        elif funding < -0.0003:
            insights.append("💰 Отрицательный Funding — много шортов, возможен шорт-сквиз")
            bullish_factors += 1
        
        # === LONG/SHORT RATIO ===
        if lsr > 1.5:
            warnings.append(f"⚠️ L/S Ratio {lsr:.2f} — слишком много лонгов (контрарный сигнал)")
            bearish_factors += 1
        elif lsr < 0.7:
            insights.append(f"💡 L/S Ratio {lsr:.2f} — много шортов, потенциал сквиза вверх")
            bullish_factors += 1
        
        # === ИТОГОВЫЙ ВЫВОД ===
        if bullish_factors >= bearish_factors + 3:
            conclusion = "🟢 СИЛЬНЫЙ БЫЧИЙ СЕТАП — множество факторов указывают на рост"
            bias = "STRONG_LONG"
        elif bullish_factors >= bearish_factors + 1:
            conclusion = "🟢 Умеренно бычий сетап — перевес в сторону покупок"
            bias = "LONG"
        elif bearish_factors >= bullish_factors + 3:
            conclusion = "🔴 СИЛЬНЫЙ МЕДВЕЖИЙ СЕТАП — множество факторов указывают на падение"
            bias = "STRONG_SHORT"
        elif bearish_factors >= bullish_factors + 1:
            conclusion = "🔴 Умеренно медвежий сетап — перевес в сторону продаж"
            bias = "SHORT"
        else:
            conclusion = "⚖️ Нейтральный рынок — нет явного преимущества"
            bias = "NEUTRAL"
        
        return {
            'insights': insights,
            'warnings': warnings,
            'conclusion': conclusion,
            'bias': bias,
            'bullish_factors': bullish_factors,
            'bearish_factors': bearish_factors
        }
    
    def _generate_trade_reasoning(self, direction: str, context: Dict, indicators: Dict) -> str:
        """Генерация человекочитаемого обоснования сделки с глубоким анализом"""
        
        reasoning_parts = []
        
        # Заголовок
        if direction == "LONG":
            reasoning_parts.append("📈 <b>Анализ: LONG</b>")
        else:
            reasoning_parts.append("📉 <b>Анализ: SHORT</b>")
        
        # Топ причины (максимум 4)
        insights = context.get('insights', [])[:4]
        for insight in insights:
            # Укорачиваем длинные инсайты
            if len(insight) > 60:
                insight = insight[:57] + "..."
            reasoning_parts.append(f"• {insight}")
        
        # MTF confluence
        mtf = context.get('mtf')
        if mtf:
            if mtf.get('aligned'):
                reasoning_parts.append(f"• ✅ Таймфреймы согласованы")
            elif mtf.get('confluence') != "NONE":
                reasoning_parts.append(f"• 📊 MTF: {mtf['confluence']}")
        
        # Дивергенция
        div = context.get('divergence')
        if div:
            reasoning_parts.append(f"• 💎 {div['type']} дивергенция")
        
        # S/R
        sr = context.get('sr_levels', {})
        if direction == "LONG" and sr.get('nearest_support'):
            dist = sr.get('distance_to_support', 999)
            if dist < 3:
                reasoning_parts.append(f"• 🛡️ Близко к поддержке")
        elif direction == "SHORT" and sr.get('nearest_resistance'):
            dist = sr.get('distance_to_resistance', 999)
            if dist < 3:
                reasoning_parts.append(f"• 🧱 Близко к сопротивлению")
        
        # Предупреждения (максимум 2)
        warnings = context.get('warnings', [])[:2]
        if warnings:
            reasoning_parts.append("\n⚠️ <b>Риски:</b>")
            for warning in warnings:
                # Укорачиваем
                if len(warning) > 50:
                    warning = warning[:47] + "..."
                reasoning_parts.append(f"• {warning}")
        
        # Ключевые метрики
        rsi = indicators.get('rsi', 50)
        adx = indicators.get('adx', 25)
        vol = indicators.get('volume_ratio', 1)
        
        metrics = f"\n📊 RSI {rsi:.0f} | ADX {adx:.0f}"
        if vol > 1.3:
            metrics += " | Vol ↑"
        elif vol < 0.7:
            metrics += " | Vol ↓"
        reasoning_parts.append(metrics)
        
        # Сила сигнала
        bf = context.get('bullish_factors', 0)
        bef = context.get('bearish_factors', 0)
        strength = abs(bf - bef)
        if strength >= 5:
            reasoning_parts.append("💪 Сила: ★★★★★")
        elif strength >= 3:
            reasoning_parts.append("💪 Сила: ★★★☆☆")
        else:
            reasoning_parts.append("💪 Сила: ★★☆☆☆")
        
        return "\n".join(reasoning_parts)
    
    # ==================== ГЛАВНЫЙ АНАЛИЗ ====================
    
    async def analyze_signal(self, symbol: str) -> Optional[Dict]:
        """Комплексный анализ для генерации сигнала с глубоким анализом"""
        global signal_stats
        signal_stats['analyzed'] += 1
        
        logger.info(f"[ANALYZER] ========== Глубокий анализ {symbol} ==========")
        
        # === TIME FILTER === (проверяем сразу)
        time_check = self.check_trading_time()
        if time_check['is_low_liquidity']:
            logger.info(f"[ANALYZER] ⏰ Низкая ликвидность ({time_check['hour']}:00 UTC) - пропуск")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['low_liquidity'] += 1
            return None
        
        # Параллельный сбор ВСЕХ данных (расширенный + новости + манипуляции)
        tech_task = self.analyze_technical(symbol)
        sentiment_task = self.analyze_sentiment(symbol)
        price_task = self.get_price(symbol)
        mtf_task = self.analyze_multi_timeframe(symbol)
        div_task = self.detect_divergence(symbol)
        sr_task = self.find_support_resistance(symbol)
        orderbook_task = self.get_order_book_imbalance(symbol)
        oi_task = self.get_open_interest_change(symbol)
        cvd_task = self.get_cvd(symbol)
        whale_task = self.check_whale_activity(symbol)
        liq_task = self.estimate_liquidation_levels(symbol)
        btc_corr_task = self.get_btc_correlation(symbol)
        news_task = self.get_crypto_news_sentiment(symbol)
        manipulation_task = self.detect_manipulation(symbol)
        
        results = await asyncio.gather(
            tech_task, sentiment_task, price_task, mtf_task, div_task, sr_task,
            orderbook_task, oi_task, cvd_task, whale_task, liq_task, btc_corr_task,
            news_task, manipulation_task
        )
        
        tech, sentiment, current_price, mtf, divergence, sr_levels = results[:6]
        orderbook, oi_change, cvd, whale, liquidations, btc_corr = results[6:12]
        news_sentiment, manipulation = results[12:14]
        
        # === ПРОВЕРКА МАНИПУЛЯЦИЙ - ОТКЛОНЯЕМ ЕСЛИ ВЫСОКИЙ РИСК ===
        if manipulation['recommendation'] == 'AVOID':
            logger.warning(f"[ANALYZER] ❌ Обнаружены манипуляции - пропуск сигнала")
            for sig in manipulation['signals']:
                logger.warning(f"[ANALYZER] {sig}")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['manipulation'] += 1
            return None
        
        # === ПРОВЕРКА НОВОСТНОГО ФОНА (РАСШИРЕННАЯ) ===
        news_trade_rec = news_sentiment.get('trade_recommendation', 'NORMAL')
        news_impact = news_sentiment.get('impact', 'LOW')
        news_bias = news_sentiment.get('bias', 'NEUTRAL')
        is_breaking = news_sentiment.get('breaking_news', False)
        
        # CRITICAL: Если рекомендация PAUSE - не торгуем
        if news_trade_rec == 'PAUSE':
            logger.warning(f"[ANALYZER] ⛔ Критические новости - торговля приостановлена")
            if news_sentiment.get('headlines'):
                logger.warning(f"[ANALYZER] Headlines: {news_sentiment['headlines'][:2]}")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['manipulation'] += 1  # Используем существующий счётчик
            return None
        
        # Логируем важные новости
        if news_impact in ['HIGH', 'CRITICAL']:
            logger.info(f"[ANALYZER] 📰 {news_impact} Impact News: {news_bias}")
            if news_sentiment.get('headlines'):
                for h in news_sentiment['headlines'][:2]:
                    logger.info(f"[ANALYZER] → {h}")
            if news_sentiment.get('warnings'):
                for w in news_sentiment['warnings'][:2]:
                    logger.warning(f"[ANALYZER] {w}")
        
        # Breaking news alert
        if is_breaking:
            logger.warning(f"[ANALYZER] 🚨 BREAKING NEWS detected! Bias: {news_bias}")
        
        # === ГЛУБОКИЙ АНАЛИЗ КОНТЕКСТА ===
        market_context = self._analyze_market_context(
            tech['indicators'],
            {'fear_greed': sentiment['fear_greed'], 
             'funding_rate': sentiment['funding_rate'],
             'long_short_ratio': sentiment['long_short_ratio']}
        )
        
        # === НОВЫЕ ДАННЫЕ: Order Book, CVD, OI, Whales ===
        
        # Order Book Imbalance
        if orderbook['signal'] == 'STRONG_BUY':
            market_context['insights'].append(f"📗 Order Book: сильное давление покупателей ({orderbook['imbalance']:.1%})")
            market_context['bullish_factors'] += 2
        elif orderbook['signal'] == 'BUY':
            market_context['bullish_factors'] += 1
        elif orderbook['signal'] == 'STRONG_SELL':
            market_context['insights'].append(f"📕 Order Book: сильное давление продавцов ({orderbook['imbalance']:.1%})")
            market_context['bearish_factors'] += 2
        elif orderbook['signal'] == 'SELL':
            market_context['bearish_factors'] += 1
        
        # CVD (Cumulative Volume Delta)
        if cvd['signal'] == 'STRONG_BUY':
            market_context['insights'].append(f"💹 CVD: агрессивные покупки ({cvd['delta_percent']:.1%})")
            market_context['bullish_factors'] += 2
        elif cvd['signal'] == 'BUY':
            market_context['bullish_factors'] += 1
        elif cvd['signal'] == 'STRONG_SELL':
            market_context['insights'].append(f"💹 CVD: агрессивные продажи ({cvd['delta_percent']:.1%})")
            market_context['bearish_factors'] += 2
        elif cvd['signal'] == 'SELL':
            market_context['bearish_factors'] += 1
        
        # Open Interest Change
        if oi_change['rising'] and oi_change['change_1h'] > 0.02:
            market_context['insights'].append(f"📈 OI растёт +{oi_change['change_1h']:.1%} — новые позиции открываются")
        elif oi_change['falling'] and oi_change['change_1h'] < -0.02:
            market_context['warnings'].append(f"⚠️ OI падает {oi_change['change_1h']:.1%} — ликвидации или закрытия")
        
        # Whale Activity
        if whale['bias'] == 'BUY' and whale['whale_trades_count'] > 5:
            market_context['insights'].append(f"🐋 Киты покупают ({whale['whale_trades_count']} крупных сделок)")
            market_context['bullish_factors'] += 2
        elif whale['bias'] == 'SELL' and whale['whale_trades_count'] > 5:
            market_context['insights'].append(f"🐋 Киты продают ({whale['whale_trades_count']} крупных сделок)")
            market_context['bearish_factors'] += 2
        
        # Liquidation Magnet
        if liquidations.get('magnet') == 'UP':
            market_context['insights'].append("🧲 Ликвидации шортов близко — магнит вверх")
            market_context['bullish_factors'] += 1
        elif liquidations.get('magnet') == 'DOWN':
            market_context['insights'].append("🧲 Ликвидации лонгов близко — магнит вниз")
            market_context['bearish_factors'] += 1
        
        # BTC Correlation
        if btc_corr['impact'] == 'NEGATIVE':
            market_context['warnings'].append(f"⚠️ BTC падает, альт коррелирует ({btc_corr['correlation']:.0%}) — риск")
            market_context['bearish_factors'] += 1
        elif btc_corr['impact'] == 'POSITIVE':
            market_context['insights'].append(f"📈 BTC растёт, альт коррелирует ({btc_corr['correlation']:.0%}) — попутный ветер")
            market_context['bullish_factors'] += 1
        
        # === NEWS SENTIMENT (РАСШИРЕННЫЙ) ===
        news_bullish = news_sentiment.get('bullish_count', 0)
        news_bearish = news_sentiment.get('bearish_count', 0)
        
        if news_bias == 'STRONG_BULLISH':
            market_context['insights'].insert(0, f"📰 СИЛЬНЫЕ бычьи новости ({news_bullish} упоминаний)")
            market_context['bullish_factors'] += 4
        elif news_bias == 'BULLISH':
            market_context['insights'].append(f"📰 Положительный новостной фон ({news_bullish} упоминаний)")
            market_context['bullish_factors'] += 2
        elif news_bias == 'STRONG_BEARISH':
            market_context['warnings'].insert(0, f"📰 СИЛЬНЫЕ медвежьи новости ({news_bearish} упоминаний)")
            market_context['bearish_factors'] += 4
        elif news_bias == 'BEARISH':
            market_context['warnings'].append(f"📰 Негативный новостной фон ({news_bearish} упоминаний)")
            market_context['bearish_factors'] += 2
        
        # Breaking news бонус
        if is_breaking:
            if news_bias in ['BULLISH', 'STRONG_BULLISH']:
                market_context['insights'].insert(0, "🚨 BREAKING: Срочные бычьи новости!")
                market_context['bullish_factors'] += 2
            elif news_bias in ['BEARISH', 'STRONG_BEARISH']:
                market_context['warnings'].insert(0, "🚨 BREAKING: Срочные медвежьи новости!")
                market_context['bearish_factors'] += 2
        
        # Предупреждения из новостей (все важные)
        for warning in news_sentiment.get('warnings', [])[:3]:
            market_context['warnings'].append(warning)
        
        # Сохраняем данные новостей для использования в TP/SL
        market_context['news_data'] = news_sentiment
        
        # === MANIPULATION WARNING ===
        if manipulation['detected']:
            market_context['warnings'].append(f"🎭 Возможные манипуляции: {manipulation['type'] or 'подозрительная активность'}")
            if manipulation['severity'] == 'MEDIUM':
                # Уменьшаем факторы если есть манипуляции средней тяжести
                market_context['bullish_factors'] = max(0, market_context['bullish_factors'] - 1)
                market_context['bearish_factors'] = max(0, market_context['bearish_factors'] - 1)
        
        # Time bonus
        if time_check['is_overlap']:
            market_context['insights'].append("⏰ London/NY overlap — максимальная ликвидность")
        
        # Добавляем MTF анализ в контекст
        if mtf['confluence'] == "BULLISH" and mtf['aligned']:
            market_context['insights'].insert(0, "🎯 ВСЕ таймфреймы (15m, 1h, 4h) бычьи — сильное подтверждение")
            market_context['bullish_factors'] += 3
        elif mtf['confluence'] == "BEARISH" and mtf['aligned']:
            market_context['insights'].insert(0, "🎯 ВСЕ таймфреймы (15m, 1h, 4h) медвежьи — сильное подтверждение")
            market_context['bearish_factors'] += 3
        elif mtf['confluence'] != "NONE":
            market_context['insights'].append(f"📊 Мульти-TF: {mtf['confluence']} (частичное согласование)")
            if mtf['confluence'] == "BULLISH":
                market_context['bullish_factors'] += 1
            else:
                market_context['bearish_factors'] += 1
        else:
            market_context['warnings'].append("⚠️ Таймфреймы не согласованы — конфликт сигналов")
        
        # Добавляем дивергенцию
        if divergence.get('divergence'):
            div = divergence['divergence']
            if div['type'] == "BULLISH":
                market_context['insights'].insert(0, f"💎 Бычья дивергенция RSI — {div['description']}")
                market_context['bullish_factors'] += 2
            elif div['type'] == "BEARISH":
                market_context['insights'].insert(0, f"💎 Медвежья дивергенция RSI — {div['description']}")
                market_context['bearish_factors'] += 2
        
        # Добавляем уровни S/R
        if sr_levels.get('distance_to_support') and sr_levels['distance_to_support'] < 1:
            market_context['insights'].append(f"🛡️ Цена у сильной поддержки (${sr_levels['nearest_support']:.0f})")
            market_context['bullish_factors'] += 1
        if sr_levels.get('distance_to_resistance') and sr_levels['distance_to_resistance'] < 1:
            market_context['insights'].append(f"🧱 Цена у сопротивления (${sr_levels['nearest_resistance']:.0f})")
            market_context['bearish_factors'] += 1
        
        logger.info(f"[ANALYZER] Контекст: {market_context['bias']}")
        logger.info(f"[ANALYZER] MTF: {mtf['confluence']}, Divergence: {divergence.get('divergence')}")
        logger.info(f"[ANALYZER] Bullish: {market_context['bullish_factors']}, Bearish: {market_context['bearish_factors']}")
        
        # Пересчитываем bias после добавления MTF и дивергенций
        bf = market_context['bullish_factors']
        bef = market_context['bearish_factors']
        if bf >= bef + 4:
            market_context['bias'] = "STRONG_LONG"
            market_context['conclusion'] = "🟢 ОЧЕНЬ СИЛЬНЫЙ БЫЧИЙ СЕТАП — комплексный анализ подтверждает рост"
        elif bf >= bef + 2:
            market_context['bias'] = "LONG"
            market_context['conclusion'] = "🟢 Бычий сетап — перевес факторов в сторону покупок"
        elif bef >= bf + 4:
            market_context['bias'] = "STRONG_SHORT"
            market_context['conclusion'] = "🔴 ОЧЕНЬ СИЛЬНЫЙ МЕДВЕЖИЙ СЕТАП — комплексный анализ подтверждает падение"
        elif bef >= bf + 2:
            market_context['bias'] = "SHORT"
            market_context['conclusion'] = "🔴 Медвежий сетап — перевес факторов в сторону продаж"
        else:
            market_context['bias'] = "NEUTRAL"
            market_context['conclusion'] = "⚖️ Нейтральный рынок — рекомендуем ждать"
        
        # Веса компонентов
        tech_weight = 0.35
        sentiment_weight = 0.2
        context_weight = 0.25
        mtf_weight = 0.2
        
        # Контекстный скор
        context_score = 0.5
        if market_context['bias'] == "STRONG_LONG":
            context_score = 0.9
        elif market_context['bias'] == "LONG":
            context_score = 0.7
        elif market_context['bias'] == "STRONG_SHORT":
            context_score = 0.1
        elif market_context['bias'] == "SHORT":
            context_score = 0.3
        
        # Общий скор с MTF
        total_score = (tech['score'] * tech_weight + 
                      sentiment['score'] * sentiment_weight + 
                      context_score * context_weight +
                      mtf['score'] * mtf_weight)
        
        # === СБАЛАНСИРОВАННЫЕ ПОРОГИ: качество + достаточное количество ===
        if total_score > 0.52:
            direction = "LONG"
        elif total_score < 0.48:
            direction = "SHORT"
        else:
            logger.info(f"[ANALYZER] ❌ Недостаточно сильный сигнал (score={total_score:.2f}, требуется >0.52 или <0.48)")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['weak_score'] += 1
            return None
        
        # === СТРОГАЯ ПРОВЕРКА СОГЛАСОВАННОСТИ ===
        # Сигнал должен совпадать с контекстом
        if direction == "LONG" and market_context['bias'] in ["STRONG_SHORT", "SHORT"]:
            logger.info(f"[ANALYZER] ❌ Конфликт: сигнал LONG, но контекст медвежий ({market_context['bias']})")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['context_conflict'] += 1
            return None
        if direction == "SHORT" and market_context['bias'] in ["STRONG_LONG", "LONG"]:
            logger.info(f"[ANALYZER] ❌ Конфликт: сигнал SHORT, но контекст бычий ({market_context['bias']})")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['context_conflict'] += 1
            return None
        
        # === MTF ДОЛЖЕН ПОДТВЕРЖДАТЬ ===
        if mtf['confluence'] != "NONE":
            if direction == "LONG" and mtf['confluence'] == "BEARISH":
                logger.info(f"[ANALYZER] ❌ MTF не подтверждает LONG (confluence={mtf['confluence']})")
                signal_stats['rejected'] += 1
                signal_stats['reasons']['mtf_conflict'] += 1
                return None
            if direction == "SHORT" and mtf['confluence'] == "BULLISH":
                logger.info(f"[ANALYZER] ❌ MTF не подтверждает SHORT (confluence={mtf['confluence']})")
                signal_stats['rejected'] += 1
                signal_stats['reasons']['mtf_conflict'] += 1
                return None
        
        # === МИНИМУМ ФАКТОРОВ В НАШУ СТОРОНУ ===
        bf = market_context['bullish_factors']
        bef = market_context['bearish_factors']
        if direction == "LONG" and bf < bef + 1:
            logger.info(f"[ANALYZER] ❌ Недостаточно бычьих факторов для LONG (bull={bf}, bear={bef})")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['low_factors'] += 1
            return None
        if direction == "SHORT" and bef < bf + 1:
            logger.info(f"[ANALYZER] ❌ Недостаточно медвежьих факторов для SHORT (bull={bf}, bear={bef})")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['low_factors'] += 1
            return None
        
        # Confidence с учётом силы контекста и MTF
        base_confidence = abs(total_score - 0.5) * 2
        context_bonus = 0.2 if "STRONG" in market_context['bias'] else 0.1
        mtf_bonus = 0.15 if mtf['aligned'] else (0.05 if mtf['confluence'] != "NONE" else 0)
        div_bonus = 0.15 if divergence.get('divergence') and divergence['divergence']['type'] == ("BULLISH" if direction == "LONG" else "BEARISH") else 0
        confidence = min(0.95, base_confidence + context_bonus + mtf_bonus + div_bonus)
        
        # === ПОРОГ УВЕРЕННОСТИ ===
        if confidence < 0.22:
            logger.info(f"[ANALYZER] ❌ Низкая уверенность ({confidence:.2%}, требуется >22%)")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['low_confidence'] += 1
            return None
        
        # === ADX: НУЖЕН ТРЕНД ===
        adx = tech['indicators'].get('adx', 20)
        if adx < 15:
            logger.info(f"[ANALYZER] ❌ Слабый тренд (ADX={adx:.1f}, требуется >15)")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['weak_trend'] += 1
            return None
        
        # === ОБЪЁМ ДОЛЖЕН ПОДТВЕРЖДАТЬ ===
        vol_ratio = tech['indicators'].get('volume_ratio', 1)
        if vol_ratio < 0.5:
            logger.info(f"[ANALYZER] ❌ Низкий объём ({vol_ratio:.2f}x от среднего)")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['low_volume'] += 1
            return None
        
        # === WHALE CONFIRMATION: Киты должны быть на нашей стороне ===
        if whale['whale_trades_count'] >= 5:
            if direction == "LONG" and whale['bias'] == "SELL":
                logger.info(f"[ANALYZER] ❌ Киты продают ({whale['whale_trades_count']} сделок) - пропуск LONG")
                signal_stats['rejected'] += 1
                signal_stats['reasons']['whale_against'] += 1
                return None
            if direction == "SHORT" and whale['bias'] == "BUY":
                logger.info(f"[ANALYZER] ❌ Киты покупают ({whale['whale_trades_count']} сделок) - пропуск SHORT")
                signal_stats['rejected'] += 1
                signal_stats['reasons']['whale_against'] += 1
                return None
        
        # === CVD MOMENTUM: Реальное давление должно подтверждать ===
        cvd_delta = cvd.get('delta_percent', 0)
        if direction == "LONG" and cvd_delta < -15:
            logger.info(f"[ANALYZER] ❌ CVD сильно негативный ({cvd_delta:.1f}%) - пропуск LONG")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['cvd_against'] += 1
            return None
        if direction == "SHORT" and cvd_delta > 15:
            logger.info(f"[ANALYZER] ❌ CVD сильно позитивный ({cvd_delta:.1f}%) - пропуск SHORT")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['cvd_against'] += 1
            return None
        
        # === ORDER BOOK: Не должен сильно противоречить ===
        if direction == "LONG" and orderbook['signal'] == 'STRONG_SELL':
            logger.info(f"[ANALYZER] ❌ Order book сильно против LONG ({orderbook['imbalance']:.1%})")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['orderbook_against'] += 1
            return None
        if direction == "SHORT" and orderbook['signal'] == 'STRONG_BUY':
            logger.info(f"[ANALYZER] ❌ Order book сильно против SHORT ({orderbook['imbalance']:.1%})")
            signal_stats['rejected'] += 1
            signal_stats['reasons']['orderbook_against'] += 1
            return None
        
        # === BTC TREND FILTER: Не торгуем альты против BTC ===
        if symbol != "BTC/USDT" and btc_corr['correlation'] > 0.7:
            if direction == "LONG" and btc_corr.get('btc_trend') == "BEARISH":
                logger.info(f"[ANALYZER] ❌ BTC падает, {symbol} коррелирует ({btc_corr['correlation']:.0%}) - пропуск LONG")
                signal_stats['rejected'] += 1
                signal_stats['reasons']['btc_against'] += 1
                return None
            if direction == "SHORT" and btc_corr.get('btc_trend') == "BULLISH":
                logger.info(f"[ANALYZER] ❌ BTC растёт, {symbol} коррелирует ({btc_corr['correlation']:.0%}) - пропуск SHORT")
                signal_stats['rejected'] += 1
                signal_stats['reasons']['btc_against'] += 1
                return None
        
        # Генерация обоснования с учётом всех данных
        market_context['mtf'] = mtf
        market_context['divergence'] = divergence.get('divergence')
        market_context['sr_levels'] = sr_levels
        reasoning = self._generate_trade_reasoning(direction, market_context, tech['indicators'])
        
        analysis = {
            'symbol': symbol,
            'direction': direction,
            'confidence': confidence,
            'total_score': total_score,
            'current_price': current_price,
            'components': {
                'technical': tech['score'],
                'sentiment': sentiment['score'],
                'context': context_score,
                'orderbook': orderbook['imbalance'],
                'cvd': cvd['delta_percent'],
                'mtf': mtf['score'],
                'news': news_sentiment.get('score', 0.5)
            },
            'indicators': tech['indicators'],
            'sentiment_data': {
                'fear_greed': sentiment['fear_greed']['value'],
                'funding_rate': sentiment['funding_rate'],
                'long_short_ratio': sentiment['long_short_ratio']
            },
            'news_sentiment': news_sentiment,  # Полные данные новостей
            'advanced_data': {
                'orderbook': orderbook,
                'cvd': cvd,
                'oi_change': oi_change,
                'whale': whale,
                'liquidations': liquidations,
                'btc_correlation': btc_corr,
                'time': time_check,
                'news': news_sentiment
            },
            'market_context': market_context,
            'reasoning': reasoning,
            'timestamp': datetime.now()
        }
        
        logger.info(f"[ANALYZER] ✓ Сигнал: {direction}, Confidence: {confidence:.2%}")
        logger.info(f"[ANALYZER] Вывод: {market_context['conclusion']}")
        
        signal_stats['accepted'] += 1
        return analysis
    
    async def calculate_entry_price(self, symbol: str, direction: str, analysis: Dict) -> Dict:
        """
        АГРЕССИВНЫЙ расчёт Entry, SL, TP с частичными тейками:
        
        Философия:
        - SL близко (0.3-0.5%) - быстро режем убытки
        - TP1 (40% позиции) - быстрый профит 0.4-0.6%
        - TP2 (40% позиции) - средний профит 0.8-1.2%
        - TP3 (20% позиции) - runner с трейлингом
        - После TP1 двигаем SL в безубыток
        """
        
        current_price = analysis.get('current_price', await self.get_price(symbol))
        confidence = analysis.get('confidence', 0.5)
        sr_levels = analysis.get('market_context', {}).get('sr_levels', {})
        advanced_data = analysis.get('advanced_data', {})
        news_data = analysis.get('news_sentiment', {})
        
        # === ПОЛУЧАЕМ ATR ДЛЯ ВОЛАТИЛЬНОСТИ ===
        klines = await self.get_klines(symbol, '5m', 30)  # 5-минутки для скальпинга
        
        if klines and len(klines) >= 20:
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # ATR на 5-минутках
            atr = TechnicalIndicators.atr(highs, lows, closes, 14)
            atr_percent = atr / current_price
            
            # Классификация волатильности
            if atr_percent > 0.003:  # >0.3% за 5 мин = высокая
                volatility = 'HIGH'
            elif atr_percent > 0.0015:
                volatility = 'MEDIUM'
            else:
                volatility = 'LOW'
        else:
            atr_percent = 0.002  # Default 0.2%
            volatility = 'MEDIUM'
        
        entry = current_price
        
        # === АГРЕССИВНЫЕ БАЗОВЫЕ УРОВНИ (ближе к цене!) ===
        # SL: 1-1.5 ATR (0.2-0.5%)
        # TP1: 1.5 ATR (быстрый профит)
        # TP2: 3 ATR (средний)
        # TP3: 5+ ATR (runner)
        
        if volatility == 'HIGH':
            sl_mult = 1.2   # Чуть шире при высокой волатильности
            tp1_mult = 1.0
            tp2_mult = 2.0
            tp3_mult = 4.0
        elif volatility == 'MEDIUM':
            sl_mult = 1.0
            tp1_mult = 1.2
            tp2_mult = 2.5
            tp3_mult = 5.0
        else:  # LOW
            sl_mult = 0.8
            tp1_mult = 1.5
            tp2_mult = 3.0
            tp3_mult = 6.0
        
        # Минимальные проценты (не меньше чем)
        min_sl_percent = 0.002   # 0.2%
        min_tp1_percent = 0.003  # 0.3%
        
        # Расчёт уровней
        sl_distance = max(atr_percent * sl_mult, min_sl_percent)
        tp1_distance = max(atr_percent * tp1_mult, min_tp1_percent)
        tp2_distance = atr_percent * tp2_mult
        tp3_distance = atr_percent * tp3_mult
        
        # === КОРРЕКТИРОВКА ПО S/R УРОВНЯМ ===
        if direction == "LONG":
            # SL ниже поддержки если она близко
            nearest_support = sr_levels.get('nearest_support')
            if nearest_support:
                support_distance = (current_price - nearest_support) / current_price
                if 0 < support_distance < sl_distance * 1.5:
                    # Поддержка близко - ставим SL чуть ниже неё
                    sl_distance = support_distance + 0.001
            
            stop_loss = current_price * (1 - sl_distance)
            tp1 = current_price * (1 + tp1_distance)
            tp2 = current_price * (1 + tp2_distance)
            tp3 = current_price * (1 + tp3_distance)
            
            # Корректируем TP по сопротивлениям
            nearest_resistance = sr_levels.get('nearest_resistance')
            if nearest_resistance:
                # Если сопротивление ближе чем TP1 - проблема
                resistance_distance = (nearest_resistance - current_price) / current_price
                if resistance_distance < tp1_distance:
                    # TP1 чуть до сопротивления
                    tp1 = nearest_resistance * 0.998
                    tp1_distance = (tp1 - current_price) / current_price
                    # Масштабируем остальные
                    tp2 = current_price * (1 + tp1_distance * 2)
                    tp3 = current_price * (1 + tp1_distance * 3.5)
        else:
            # SHORT
            nearest_resistance = sr_levels.get('nearest_resistance')
            if nearest_resistance:
                resistance_distance = (nearest_resistance - current_price) / current_price
                if 0 < resistance_distance < sl_distance * 1.5:
                    sl_distance = resistance_distance + 0.001
            
            stop_loss = current_price * (1 + sl_distance)
            tp1 = current_price * (1 - tp1_distance)
            tp2 = current_price * (1 - tp2_distance)
            tp3 = current_price * (1 - tp3_distance)
            
            nearest_support = sr_levels.get('nearest_support')
            if nearest_support:
                support_distance = (current_price - nearest_support) / current_price
                if support_distance < tp1_distance:
                    tp1 = nearest_support * 1.002
                    tp1_distance = (current_price - tp1) / current_price
                    tp2 = current_price * (1 - tp1_distance * 2)
                    tp3 = current_price * (1 - tp1_distance * 3.5)
        
        # === НОВОСТНОЙ БОНУС/ШТРАФ ===
        # Если сильные бычьи новости и мы в LONG - можно шире TP
        # Если сильные медвежьи и мы в SHORT - тоже
        news_bias = news_data.get('bias', 'NEUTRAL') if news_data else 'NEUTRAL'
        news_aligned = (direction == "LONG" and news_bias in ['BULLISH', 'STRONG_BULLISH']) or \
                       (direction == "SHORT" and news_bias in ['BEARISH', 'STRONG_BEARISH'])
        
        if news_aligned and news_data.get('impact') in ['HIGH', 'CRITICAL']:
            # Расширяем TP3 на 50% - новости в нашу сторону
            if direction == "LONG":
                tp3 = current_price * (1 + tp3_distance * 1.5)
            else:
                tp3 = current_price * (1 - tp3_distance * 1.5)
            logger.info(f"[TPSL] Новости усиливают позицию - TP3 расширен")
        
        # === ФИНАЛЬНЫЕ РАСЧЁТЫ ===
        sl_percent = abs(stop_loss - entry) / entry
        tp1_percent = abs(tp1 - entry) / entry
        tp2_percent = abs(tp2 - entry) / entry
        tp3_percent = abs(tp3 - entry) / entry
        
        # R/R для каждого TP
        rr1 = tp1_percent / sl_percent if sl_percent > 0 else 0
        rr2 = tp2_percent / sl_percent if sl_percent > 0 else 0
        rr3 = tp3_percent / sl_percent if sl_percent > 0 else 0
        
        # Минимальный R/R для TP1 = 1.2
        if rr1 < 1.2:
            tp1_percent = sl_percent * 1.2
            if direction == "LONG":
                tp1 = entry * (1 + tp1_percent)
            else:
                tp1 = entry * (1 - tp1_percent)
            rr1 = 1.2
        
        # Win rate оценка
        base_winrate = 72
        confidence_bonus = confidence * 12
        vol_bonus = 3 if volatility == 'LOW' else (-3 if volatility == 'HIGH' else 0)
        news_bonus = 5 if news_aligned else 0
        
        success_rate = min(90, base_winrate + confidence_bonus + vol_bonus + news_bonus)
        
        # === СТРАТЕГИЯ ЧАСТИЧНЫХ ТЕЙКОВ ===
        take_profit_strategy = {
            'tp1': {
                'price': tp1,
                'percent': tp1_percent,
                'close_percent': 40,  # Закрываем 40% позиции
                'move_sl_to_be': True  # После TP1 двигаем SL в безубыток
            },
            'tp2': {
                'price': tp2,
                'percent': tp2_percent,
                'close_percent': 40,  # Ещё 40%
                'trailing_start': True  # Начинаем трейлинг
            },
            'tp3': {
                'price': tp3,
                'percent': tp3_percent,
                'close_percent': 20,  # Последние 20%
                'is_runner': True  # Runner позиция
            }
        }
        
        logger.info(f"[TPSL] {symbol} {direction} Entry=${entry:.4f}")
        logger.info(f"[TPSL] SL=${stop_loss:.4f} ({sl_percent*100:.2f}%)")
        logger.info(f"[TPSL] TP1=${tp1:.4f} ({tp1_percent*100:.2f}%) R/R={rr1:.1f} [40%]")
        logger.info(f"[TPSL] TP2=${tp2:.4f} ({tp2_percent*100:.2f}%) R/R={rr2:.1f} [40%]")
        logger.info(f"[TPSL] TP3=${tp3:.4f} ({tp3_percent*100:.2f}%) R/R={rr3:.1f} [20%]")
        logger.info(f"[TPSL] Vol={volatility}, WinRate={success_rate:.0f}%")
        
        return {
            'entry_price': entry,
            'stop_loss': stop_loss,
            'take_profit': tp1,  # Основной TP (для совместимости)
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'tp_strategy': take_profit_strategy,
            'success_rate': success_rate,
            'sl_percent': sl_percent,
            'tp_percent': tp1_percent,  # Для совместимости
            'tp1_percent': tp1_percent,
            'tp2_percent': tp2_percent,
            'tp3_percent': tp3_percent,
            'volatility': volatility,
            'risk_reward': rr1,
            'rr1': rr1,
            'rr2': rr2,
            'rr3': rr3,
            'news_aligned': news_aligned
        }
    
    async def analyze_position_adjustment(self, symbol: str, direction: str, entry: float, 
                                          current_sl: float, current_tp: float) -> Dict:
        """
        Анализ необходимости сдвига SL/TP для открытой позиции
        
        Логика:
        1. Если цена идёт к TP и видим давление в нашу сторону - можно расширить TP
        2. Если цена идёт к SL но это манипуляция - можно временно сдвинуть SL
        3. Trailing stop logic
        """
        
        current_price = await self.get_price(symbol)
        adjustment = {
            'should_adjust_sl': False,
            'should_adjust_tp': False,
            'new_sl': current_sl,
            'new_tp': current_tp,
            'reason': None,
            'action': 'HOLD',
            'urgency': 'LOW',
            'should_flip': False,  # Переворот позиции
            'flip_direction': None,
            'pnl_percent': 0  # Текущий PnL для защиты от добавления
        }
        
        try:
            # Собираем данные
            manipulation = await self.detect_manipulation(symbol)
            orderbook = await self.get_order_book_imbalance(symbol)
            cvd = await self.get_cvd(symbol)
            oi_change = await self.get_open_interest_change(symbol)
            
            # Прогресс к TP и SL
            if direction == "LONG":
                pnl_percent = (current_price - entry) / entry * 100
                progress_to_tp = (current_price - entry) / (current_tp - entry) * 100 if current_tp != entry else 0
                progress_to_sl = (entry - current_price) / (entry - current_sl) * 100 if entry != current_sl else 0
            else:
                pnl_percent = (entry - current_price) / entry * 100
                progress_to_tp = (entry - current_price) / (entry - current_tp) * 100 if current_tp != entry else 0
                progress_to_sl = (current_price - entry) / (current_sl - entry) * 100 if current_sl != entry else 0
            
            logger.info(f"[POSITION_MONITOR] {symbol} {direction}: PnL={pnl_percent:.2f}%, ToTP={progress_to_tp:.0f}%, ToSL={progress_to_sl:.0f}%")
            
            # Сохраняем PnL для защиты от добавления в убыток
            adjustment['pnl_percent'] = pnl_percent
            
            # === АГРЕССИВНЫЙ TRAILING STOP ===
            # Начинаем защищать профит РАНО и МНОГО
            # Лучше забрать меньше, чем потерять всё
            
            trailing_applied = False
            trailing_percent = 0  # Какую долю профита защищаем
            partial_close = None  # Частичное закрытие
            
            if pnl_percent > 1.5:
                # Отличный профит: защищаем 80% + закрываем часть
                trailing_percent = 0.80
                trailing_applied = True
                if pnl_percent > 2.0:
                    partial_close = {'percent': 30, 'reason': 'TP2 zone reached'}
            elif pnl_percent > 1.0:
                # Хороший профит: защищаем 70%
                trailing_percent = 0.70
                trailing_applied = True
            elif pnl_percent > 0.6:
                # Средний профит: защищаем 60%
                trailing_percent = 0.60
                trailing_applied = True
            elif pnl_percent > 0.4:
                # Небольшой профит: защищаем 50%
                trailing_percent = 0.50
                trailing_applied = True
            elif pnl_percent > 0.25:
                # Минимальный профит: в безубыток + 30%
                trailing_percent = 0.30
                trailing_applied = True
            elif pnl_percent > 0.15:
                # Едва в плюсе: в безубыток
                trailing_percent = 0.0
                trailing_applied = True  # Просто BE
            
            if trailing_applied:
                if direction == "LONG":
                    profit_distance = current_price - entry
                    new_trailing_sl = entry + profit_distance * trailing_percent
                    # SL минимум в безубыток
                    new_trailing_sl = max(new_trailing_sl, entry * 1.0005)
                    
                    if new_trailing_sl > current_sl:
                        adjustment['should_adjust_sl'] = True
                        adjustment['new_sl'] = new_trailing_sl
                        protected_profit = (new_trailing_sl - entry) / entry * 100
                        adjustment['reason'] = f"Trailing: защита {protected_profit:.2f}% ({trailing_percent*100:.0f}% от {pnl_percent:.1f}%)"
                        adjustment['action'] = 'ADJUST_SL'
                        
                        # Частичное закрытие?
                        if partial_close:
                            adjustment['partial_close'] = partial_close
                            adjustment['action'] = 'PARTIAL_CLOSE_AND_TRAIL'
                else:  # SHORT
                    profit_distance = entry - current_price
                    new_trailing_sl = entry - profit_distance * trailing_percent
                    # SL минимум в безубыток
                    new_trailing_sl = min(new_trailing_sl, entry * 0.9995)
                    
                    if new_trailing_sl < current_sl:
                        adjustment['should_adjust_sl'] = True
                        adjustment['new_sl'] = new_trailing_sl
                        protected_profit = (entry - new_trailing_sl) / entry * 100
                        adjustment['reason'] = f"Trailing: защита {protected_profit:.2f}% ({trailing_percent*100:.0f}% от {pnl_percent:.1f}%)"
                        adjustment['action'] = 'ADJUST_SL'
                        
                        if partial_close:
                            adjustment['partial_close'] = partial_close
                            adjustment['action'] = 'PARTIAL_CLOSE_AND_TRAIL'
            
            # === МАНИПУЛЯЦИЯ В НАШУ СТОРОНУ: можно расширить TP ===
            if progress_to_tp > 70:  # Близко к TP
                favorable_pressure = False
                if direction == "LONG" and (orderbook['signal'] in ['STRONG_BUY', 'BUY'] or cvd['signal'] in ['STRONG_BUY', 'BUY']):
                    favorable_pressure = True
                elif direction == "SHORT" and (orderbook['signal'] in ['STRONG_SELL', 'SELL'] or cvd['signal'] in ['STRONG_SELL', 'SELL']):
                    favorable_pressure = True
                
                if favorable_pressure and not manipulation['detected']:
                    # Расширяем TP на 30%
                    tp_distance = abs(current_tp - entry)
                    if direction == "LONG":
                        adjustment['new_tp'] = current_tp + tp_distance * 0.3
                    else:
                        adjustment['new_tp'] = current_tp - tp_distance * 0.3
                    adjustment['should_adjust_tp'] = True
                    adjustment['reason'] = "Давление в нашу сторону - расширяем TP"
                    adjustment['action'] = 'EXTEND_TP'
            
            # === МАНИПУЛЯЦИЯ ПРОТИВ НАС: не паникуем, анализируем ===
            if progress_to_sl > 60 and manipulation['detected']:
                # Проверяем тип манипуляции
                if manipulation['type'] in ['PUMP_DUMP', 'DUMP_PUMP', 'SHORT_SQUEEZE', 'LONG_LIQUIDATION']:
                    # Если это явная манипуляция - можно временно расширить SL
                    # но только если OI падает (значит это ликвидации, а не новый тренд)
                    if oi_change['falling']:
                        sl_distance = abs(current_sl - entry)
                        if direction == "LONG":
                            adjustment['new_sl'] = current_sl - sl_distance * 0.3
                        else:
                            adjustment['new_sl'] = current_sl + sl_distance * 0.3
                        adjustment['should_adjust_sl'] = True
                        adjustment['reason'] = f"Манипуляция ({manipulation['type']}) + падение OI - временно расширяем SL"
                        adjustment['action'] = 'WIDEN_SL'
                        adjustment['urgency'] = 'HIGH'
            
            # === КРИТИЧЕСКАЯ СИТУАЦИЯ: близко к SL без манипуляций ===
            if progress_to_sl > 80 and not manipulation['detected']:
                # Проверяем давление
                unfavorable_pressure = False
                strong_opposite_signal = False
                
                if direction == "LONG":
                    if orderbook['signal'] in ['STRONG_SELL'] or cvd['signal'] in ['STRONG_SELL']:
                        unfavorable_pressure = True
                    # Очень сильный сигнал на переворот
                    if orderbook['signal'] == 'STRONG_SELL' and cvd['signal'] == 'STRONG_SELL':
                        strong_opposite_signal = True
                elif direction == "SHORT":
                    if orderbook['signal'] in ['STRONG_BUY'] or cvd['signal'] in ['STRONG_BUY']:
                        unfavorable_pressure = True
                    if orderbook['signal'] == 'STRONG_BUY' and cvd['signal'] == 'STRONG_BUY':
                        strong_opposite_signal = True
                
                if unfavorable_pressure:
                    adjustment['reason'] = "Сильное давление против позиции"
                    adjustment['action'] = 'CLOSE_EARLY'
                    adjustment['urgency'] = 'CRITICAL'
                    
                    # === FLIP LOGIC ===
                    # Если оба сигнала (orderbook + CVD) сильно против нас - предлагаем переворот
                    if strong_opposite_signal and not oi_change['falling']:
                        # OI не падает = это новый тренд, а не просто ликвидации
                        adjustment['should_flip'] = True
                        adjustment['flip_direction'] = "SHORT" if direction == "LONG" else "LONG"
                        adjustment['reason'] = f"Сильный разворот рынка - переворот в {adjustment['flip_direction']}"
                        logger.info(f"[FLIP] {symbol}: Рекомендуем переворот {direction} -> {adjustment['flip_direction']}")
            
            if adjustment['action'] != 'HOLD':
                logger.info(f"[POSITION_MONITOR] Рекомендация: {adjustment['action']} - {adjustment['reason']}")
            
        except Exception as e:
            logger.error(f"[POSITION_MONITOR] Ошибка: {e}")
        
        return adjustment
    
    async def close(self):
        """Закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
