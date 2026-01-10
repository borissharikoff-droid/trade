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
    
    # ==================== ГЛАВНЫЙ АНАЛИЗ ====================
    
    async def analyze_signal(self, symbol: str) -> Optional[Dict]:
        """Комплексный анализ для генерации сигнала"""
        logger.info(f"[ANALYZER] ========== Анализ {symbol} ==========")
        
        # Параллельный сбор данных
        tech_task = self.analyze_technical(symbol)
        sentiment_task = self.analyze_sentiment(symbol)
        price_task = self.get_price(symbol)
        
        tech, sentiment, current_price = await asyncio.gather(
            tech_task, sentiment_task, price_task
        )
        
        # Веса компонентов
        tech_weight = 0.6
        sentiment_weight = 0.4
        
        # Общий скор
        total_score = tech['score'] * tech_weight + sentiment['score'] * sentiment_weight
        
        # Определение направления
        if total_score > 0.58:
            direction = "LONG"
        elif total_score < 0.42:
            direction = "SHORT"
        else:
            logger.info(f"[ANALYZER] Нет четкого сигнала (score={total_score:.2f})")
            return None
        
        # Confidence
        confidence = abs(total_score - 0.5) * 2
        
        # Минимальный порог
        if confidence < 0.15:
            logger.info(f"[ANALYZER] Низкая уверенность ({confidence:.2%})")
            return None
        
        # ADX check - нужен тренд
        adx = tech['indicators'].get('adx', 20)
        if adx < 20:
            logger.info(f"[ANALYZER] Слабый тренд (ADX={adx:.1f})")
            return None
        
        analysis = {
            'symbol': symbol,
            'direction': direction,
            'confidence': confidence,
            'total_score': total_score,
            'current_price': current_price,
            'components': {
                'technical': tech['score'],
                'sentiment': sentiment['score']
            },
            'indicators': tech['indicators'],
            'sentiment_data': {
                'fear_greed': sentiment['fear_greed']['value'],
                'funding_rate': sentiment['funding_rate'],
                'long_short_ratio': sentiment['long_short_ratio']
            },
            'timestamp': datetime.now()
        }
        
        logger.info(f"[ANALYZER] ✓ Сигнал: {direction}, Confidence: {confidence:.2%}")
        
        return analysis
    
    async def calculate_entry_price(self, symbol: str, direction: str, analysis: Dict) -> Dict:
        """Расчет Entry, SL, TP на основе ATR"""
        
        klines = await self.get_klines(symbol, '1h', 50)
        current_price = analysis.get('current_price', await self.get_price(symbol))
        
        # ATR для волатильности
        if klines and len(klines) >= 20:
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            atr = TechnicalIndicators.atr(highs, lows, closes)
        else:
            atr = current_price * 0.02
        
        # SL = 1.5 ATR, TP = 3 ATR (Risk:Reward = 1:2)
        confidence = analysis.get('confidence', 0.5)
        
        sl_multiplier = 1.5
        tp_multiplier = 3.0 + confidence  # 3-4 ATR based on confidence
        
        if direction == "LONG":
            entry = current_price
            stop_loss = entry - (atr * sl_multiplier)
            take_profit = entry + (atr * tp_multiplier)
        else:
            entry = current_price
            stop_loss = entry + (atr * sl_multiplier)
            take_profit = entry - (atr * tp_multiplier)
        
        # Win rate estimate
        base_winrate = 55
        confidence_bonus = confidence * 30
        
        # Bonus for strong ADX
        adx = analysis.get('indicators', {}).get('adx', 20)
        adx_bonus = 5 if adx > 30 else 0
        
        success_rate = min(95, base_winrate + confidence_bonus + adx_bonus)
        
        logger.info(f"[PRICE] Entry=${entry:.2f}, SL=${stop_loss:.2f}, TP=${take_profit:.2f}")
        logger.info(f"[PRICE] ATR=${atr:.2f}, WinRate={success_rate:.0f}%")
        
        return {
            'entry_price': entry,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'success_rate': success_rate,
            'atr': atr
        }
    
    async def close(self):
        """Закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
