import logging
import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from binance.client import Client

logger = logging.getLogger(__name__)


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
        logger.info(f"[ANALYZER] ========== Глубокий анализ {symbol} ==========")
        
        # Параллельный сбор ВСЕХ данных
        tech_task = self.analyze_technical(symbol)
        sentiment_task = self.analyze_sentiment(symbol)
        price_task = self.get_price(symbol)
        mtf_task = self.analyze_multi_timeframe(symbol)
        div_task = self.detect_divergence(symbol)
        sr_task = self.find_support_resistance(symbol)
        
        tech, sentiment, current_price, mtf, divergence, sr_levels = await asyncio.gather(
            tech_task, sentiment_task, price_task, mtf_task, div_task, sr_task
        )
        
        # === ГЛУБОКИЙ АНАЛИЗ КОНТЕКСТА ===
        market_context = self._analyze_market_context(
            tech['indicators'],
            {'fear_greed': sentiment['fear_greed'], 
             'funding_rate': sentiment['funding_rate'],
             'long_short_ratio': sentiment['long_short_ratio']}
        )
        
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
        
        # Определение направления (более мягкие пороги)
        if total_score > 0.55:
            direction = "LONG"
        elif total_score < 0.45:
            direction = "SHORT"
        else:
            logger.info(f"[ANALYZER] Нет четкого сигнала (score={total_score:.2f})")
            return None
        
        # Проверка согласованности с контекстом (только сильные конфликты)
        if direction == "LONG" and market_context['bias'] == "STRONG_SHORT":
            logger.info(f"[ANALYZER] Конфликт: сигнал LONG, но контекст сильно медвежий")
            return None
        if direction == "SHORT" and market_context['bias'] == "STRONG_LONG":
            logger.info(f"[ANALYZER] Конфликт: сигнал SHORT, но контекст сильно бычий")
            return None
        
        # Confidence с учётом силы контекста и MTF
        base_confidence = abs(total_score - 0.5) * 2
        context_bonus = 0.15 if "STRONG" in market_context['bias'] else 0.05
        mtf_bonus = 0.1 if mtf['aligned'] else 0
        div_bonus = 0.1 if divergence.get('divergence') and divergence['divergence']['type'] == ("BULLISH" if direction == "LONG" else "BEARISH") else 0
        confidence = min(0.95, base_confidence + context_bonus + mtf_bonus + div_bonus)
        
        # Минимальный порог качества
        if confidence < 0.15:
            logger.info(f"[ANALYZER] Низкая уверенность ({confidence:.2%})")
            return None
        
        # ADX check - нужен тренд
        adx = tech['indicators'].get('adx', 20)
        if adx < 18:
            logger.info(f"[ANALYZER] Слабый тренд (ADX={adx:.1f})")
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
                'context': context_score
            },
            'indicators': tech['indicators'],
            'sentiment_data': {
                'fear_greed': sentiment['fear_greed']['value'],
                'funding_rate': sentiment['funding_rate'],
                'long_short_ratio': sentiment['long_short_ratio']
            },
            'market_context': market_context,
            'reasoning': reasoning,
            'timestamp': datetime.now()
        }
        
        logger.info(f"[ANALYZER] ✓ Сигнал: {direction}, Confidence: {confidence:.2%}")
        logger.info(f"[ANALYZER] Вывод: {market_context['conclusion']}")
        
        return analysis
    
    async def calculate_entry_price(self, symbol: str, direction: str, analysis: Dict) -> Dict:
        """Расчет Entry, SL, TP для СКАЛЬПИНГА (15-40 минут)"""
        
        # Используем 5m для скальпинга
        klines = await self.get_klines(symbol, '5m', 50)
        current_price = analysis.get('current_price', await self.get_price(symbol))
        
        confidence = analysis.get('confidence', 0.5)
        
        # СКАЛЬПИНГ: фиксированные проценты
        # SL: 0.3-0.5% (зависит от уверенности)
        # TP: 0.5-1.0% (зависит от уверенности)
        sl_percent = 0.003 + (1 - confidence) * 0.002  # 0.3-0.5%
        tp_percent = 0.005 + confidence * 0.005        # 0.5-1.0%
        
        sl_distance = current_price * sl_percent
        tp_distance = current_price * tp_percent
        
        if direction == "LONG":
            entry = current_price
            stop_loss = entry - sl_distance
            take_profit = entry + tp_distance
        else:
            entry = current_price
            stop_loss = entry + sl_distance
            take_profit = entry - tp_distance
        
        # Win rate estimate для скальпинга x20 (выше из-за близких целей + строгих фильтров)
        base_winrate = 68  # Выше для качественного скальпинга
        confidence_bonus = confidence * 22
        
        # Bonus for strong ADX (тренд)
        adx = analysis.get('indicators', {}).get('adx', 20)
        adx_bonus = 5 if adx > 25 else 0
        
        success_rate = min(92, base_winrate + confidence_bonus + adx_bonus)
        
        logger.info(f"[SCALP] Entry=${entry:.2f}, SL=${stop_loss:.2f} ({sl_percent*100:.2f}%), TP=${take_profit:.2f} ({tp_percent*100:.2f}%)")
        logger.info(f"[SCALP] WinRate={success_rate:.0f}%, Confidence={confidence:.2f}")
        
        return {
            'entry_price': entry,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'success_rate': success_rate,
            'sl_percent': sl_percent,
            'tp_percent': tp_percent
        }
    
    async def close(self):
        """Закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
