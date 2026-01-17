"""
Smart Trading Analyzer v2.0
Умная торговая система на основе Price Action и Market Structure

Ключевые принципы:
1. Качество > Количество (1-3 сделки в день максимум)
2. Торговля только по тренду
3. Вход только на откатах к ключевым уровням
4. Динамические TP/SL на основе структуры рынка
5. Защита капитала: max drawdown, cooldown после убытков
"""

import logging
import asyncio
import aiohttp
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


# ==================== ENUMS ====================
class MarketRegime(Enum):
    """Режим рынка"""
    STRONG_UPTREND = "STRONG_UPTREND"      # Сильный восходящий тренд
    UPTREND = "UPTREND"                     # Восходящий тренд
    RANGING = "RANGING"                     # Боковик/флэт
    DOWNTREND = "DOWNTREND"                 # Нисходящий тренд
    STRONG_DOWNTREND = "STRONG_DOWNTREND"  # Сильный нисходящий тренд
    HIGH_VOLATILITY = "HIGH_VOLATILITY"    # Высокая волатильность (не торгуем)
    UNKNOWN = "UNKNOWN"


class SetupQuality(Enum):
    """Качество торгового сетапа (числовой порядок для сравнения)"""
    A_PLUS = 5   # Идеальный сетап (берём обязательно)
    A = 4        # Отличный сетап
    B = 3        # Хороший сетап (берём при хорошем рынке)
    C = 2        # Средний (пропускаем)
    D = 1        # Плохой (пропускаем)


class SignalType(Enum):
    """Тип сигнала"""
    TREND_CONTINUATION = "TREND_CONTINUATION"  # Продолжение тренда
    TREND_REVERSAL = "TREND_REVERSAL"          # Разворот тренда
    BREAKOUT = "BREAKOUT"                       # Пробой уровня
    PULLBACK = "PULLBACK"                       # Откат к уровню
    NONE = "NONE"


# ==================== DATA CLASSES ====================
@dataclass
class SwingPoint:
    """Точка свинга (локальный экстремум)"""
    price: float
    index: int
    type: str  # 'HH', 'HL', 'LH', 'LL'
    strength: int  # Сколько свечей слева/справа подтверждают


@dataclass 
class KeyLevel:
    """Ключевой уровень поддержки/сопротивления"""
    price: float
    type: str  # 'support', 'resistance'
    touches: int  # Сколько раз цена касалась
    strength: float  # 0-1, сила уровня
    last_touch_index: int


@dataclass
class CandlePattern:
    """Свечной паттерн"""
    name: str
    type: str  # 'bullish', 'bearish', 'neutral'
    strength: float  # 0-1
    index: int


@dataclass
class TradeSetup:
    """Торговый сетап"""
    symbol: str
    direction: str  # 'LONG', 'SHORT'
    entry: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    quality: SetupQuality
    signal_type: SignalType
    risk_reward: float
    confidence: float
    reasoning: List[str]
    warnings: List[str]
    market_regime: MarketRegime
    timestamp: datetime


# ==================== TRADING STATE ====================
class TradingState:
    """Состояние торговли для защиты капитала"""
    
    def __init__(self):
        self.consecutive_losses = 0
        self.daily_trades = 0
        self.daily_pnl = 0.0
        self.last_trade_time: Optional[datetime] = None
        self.last_reset_date: Optional[str] = None
        self.is_paused = False
        self.pause_until: Optional[datetime] = None
        
        # Настройки защиты
        self.MAX_CONSECUTIVE_LOSSES = 4  # После 4 убытков подряд - пауза
        self.MAX_DAILY_TRADES = 10       # Максимум сделок в день
        self.MAX_DAILY_LOSS_PERCENT = 10 # Макс убыток в день 10%
        self.MIN_TIME_BETWEEN_TRADES = 10 # Минут между сделками
        self.PAUSE_AFTER_LOSSES_HOURS = 2 # Часов паузы после серии убытков
    
    def reset_daily(self):
        """Сброс дневных счётчиков"""
        today = datetime.now(timezone.utc).date().isoformat()
        if self.last_reset_date != today:
            self.daily_trades = 0
            self.daily_pnl = 0.0
            self.last_reset_date = today
            logger.info("[STATE] Daily counters reset")
    
    def record_trade(self, pnl: float):
        """Записать результат сделки"""
        self.daily_trades += 1
        self.daily_pnl += pnl
        self.last_trade_time = datetime.now(timezone.utc)
        
        if pnl < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.MAX_CONSECUTIVE_LOSSES:
                self.pause_trading(self.PAUSE_AFTER_LOSSES_HOURS)
        else:
            self.consecutive_losses = 0
    
    def pause_trading(self, hours: int):
        """Поставить торговлю на паузу"""
        self.is_paused = True
        self.pause_until = datetime.now(timezone.utc) + timedelta(hours=hours)
        logger.warning(f"[STATE] Trading paused for {hours} hours after {self.consecutive_losses} consecutive losses")
    
    def can_trade(self, balance: float) -> Tuple[bool, str]:
        """Можно ли открывать новую сделку"""
        self.reset_daily()
        
        now = datetime.now(timezone.utc)
        
        # Проверка паузы
        if self.is_paused:
            if self.pause_until and now < self.pause_until:
                remaining = (self.pause_until - now).seconds // 60
                return False, f"Пауза после убытков ({remaining} мин осталось)"
            else:
                self.is_paused = False
                self.consecutive_losses = 0
        
        # Проверка лимита сделок
        if self.daily_trades >= self.MAX_DAILY_TRADES:
            return False, f"Лимит сделок в день ({self.MAX_DAILY_TRADES})"
        
        # Проверка дневного убытка
        if balance > 0:
            daily_loss_percent = abs(min(0, self.daily_pnl)) / balance * 100
            if daily_loss_percent >= self.MAX_DAILY_LOSS_PERCENT:
                return False, f"Дневной лимит убытков ({self.MAX_DAILY_LOSS_PERCENT}%)"
        
        # Проверка времени между сделками
        if self.last_trade_time:
            minutes_since_last = (now - self.last_trade_time).seconds // 60
            if minutes_since_last < self.MIN_TIME_BETWEEN_TRADES:
                return False, f"Cooldown ({self.MIN_TIME_BETWEEN_TRADES - minutes_since_last} мин)"
        
        return True, "OK"


# ==================== SMART ANALYZER ====================
class SmartAnalyzer:
    """
    Умный анализатор рынка v2.0
    
    Принципы:
    1. Определяем режим рынка (тренд/флэт/волатильность)
    2. Торгуем ТОЛЬКО по тренду
    3. Ищем вход на откатах к ключевым уровням
    4. Подтверждение свечными паттернами
    5. Строгий risk management
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache = {}
        self.cache_ttl = 30  # секунд
        self.state = TradingState()
        
        # Настройки качества - баланс между качеством и количеством
        self.MIN_QUALITY = SetupQuality.B  # Минимум B-сетап (A+, A, B)
        self.MIN_RISK_REWARD = 2.0         # Минимальное соотношение R/R 1:2
        self.MIN_CONFIDENCE = 0.55         # Минимальная уверенность 55%
        
        # Торговые сессии (UTC)
        self.LONDON_OPEN = 7
        self.LONDON_CLOSE = 16
        self.NY_OPEN = 13
        self.NY_CLOSE = 21
        
        logger.info("[SMART] Analyzer initialized")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    # ==================== DATA FETCHING ====================
    
    async def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List:
        """Получить свечи с Binance"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://api.binance.com/api/v3/klines?symbol={binance_symbol}&interval={interval}&limit={limit}"
            
            session = await self._get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.warning(f"[KLINES] Error {symbol}: {e}")
        return []
    
    async def get_price(self, symbol: str) -> float:
        """Текущая цена"""
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_symbol}"
            
            session = await self._get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data['price'])
        except Exception as e:
            logger.warning(f"[PRICE] Error {symbol}: {e}")
        return 0
    
    # ==================== MARKET STRUCTURE ====================
    
    def find_swing_points(self, highs: List[float], lows: List[float], 
                          lookback: int = 5) -> List[SwingPoint]:
        """
        Найти свинг-точки (локальные экстремумы)
        
        Свинг-хай: high[i] > high[i-lookback:i] и high[i] > high[i+1:i+lookback]
        Свинг-лоу: аналогично для low
        """
        swings = []
        n = len(highs)
        
        if n < lookback * 2 + 1:
            return swings
        
        for i in range(lookback, n - lookback):
            # Проверка swing high
            is_swing_high = True
            for j in range(1, lookback + 1):
                if highs[i] <= highs[i - j] or highs[i] <= highs[i + j]:
                    is_swing_high = False
                    break
            
            if is_swing_high:
                swings.append(SwingPoint(
                    price=highs[i],
                    index=i,
                    type='HIGH',
                    strength=lookback
                ))
            
            # Проверка swing low
            is_swing_low = True
            for j in range(1, lookback + 1):
                if lows[i] >= lows[i - j] or lows[i] >= lows[i + j]:
                    is_swing_low = False
                    break
            
            if is_swing_low:
                swings.append(SwingPoint(
                    price=lows[i],
                    index=i,
                    type='LOW',
                    strength=lookback
                ))
        
        # Сортируем по индексу
        swings.sort(key=lambda x: x.index)
        
        # Определяем тип: HH, HL, LH, LL
        labeled_swings = self._label_swing_points(swings)
        
        return labeled_swings
    
    def _label_swing_points(self, swings: List[SwingPoint]) -> List[SwingPoint]:
        """
        Пометить свинги как HH/HL/LH/LL
        
        HH (Higher High) - новый максимум выше предыдущего
        HL (Higher Low) - новый минимум выше предыдущего
        LH (Lower High) - новый максимум ниже предыдущего
        LL (Lower Low) - новый минимум ниже предыдущего
        """
        last_high: Optional[SwingPoint] = None
        last_low: Optional[SwingPoint] = None
        
        for swing in swings:
            if swing.type == 'HIGH':
                if last_high is not None:
                    if swing.price > last_high.price:
                        swing.type = 'HH'
                    else:
                        swing.type = 'LH'
                last_high = swing
            else:  # LOW
                if last_low is not None:
                    if swing.price > last_low.price:
                        swing.type = 'HL'
                    else:
                        swing.type = 'LL'
                last_low = swing
        
        return swings
    
    def determine_market_regime(self, swings: List[SwingPoint], 
                                 atr_percent: float,
                                 price_change_24h: float) -> MarketRegime:
        """
        Определить режим рынка на основе структуры
        
        UPTREND: HH + HL последовательно
        DOWNTREND: LH + LL последовательно
        RANGING: Нет чёткой структуры
        HIGH_VOLATILITY: ATR > 3%
        """
        
        # Высокая волатильность - не торгуем
        if atr_percent > 3.0:
            return MarketRegime.HIGH_VOLATILITY
        
        if len(swings) < 4:
            return MarketRegime.UNKNOWN
        
        # Анализируем последние 6-8 свингов
        recent_swings = swings[-8:]
        
        hh_count = sum(1 for s in recent_swings if s.type == 'HH')
        hl_count = sum(1 for s in recent_swings if s.type == 'HL')
        lh_count = sum(1 for s in recent_swings if s.type == 'LH')
        ll_count = sum(1 for s in recent_swings if s.type == 'LL')
        
        bullish_structure = hh_count + hl_count
        bearish_structure = lh_count + ll_count
        
        logger.info(f"[REGIME] HH={hh_count}, HL={hl_count}, LH={lh_count}, LL={ll_count}")
        
        # Сильный тренд: 4+ свингов в одном направлении
        if bullish_structure >= 5 and price_change_24h > 2:
            return MarketRegime.STRONG_UPTREND
        elif bullish_structure >= 3 and price_change_24h > 0.5:
            return MarketRegime.UPTREND
        elif bearish_structure >= 5 and price_change_24h < -2:
            return MarketRegime.STRONG_DOWNTREND
        elif bearish_structure >= 3 and price_change_24h < -0.5:
            return MarketRegime.DOWNTREND
        else:
            return MarketRegime.RANGING
    
    # ==================== KEY LEVELS ====================
    
    def find_key_levels(self, highs: List[float], lows: List[float], 
                        closes: List[float], touches_required: int = 2) -> List[KeyLevel]:
        """
        Найти ключевые уровни поддержки/сопротивления
        
        Метод: Кластеризация свинг-точек + подсчёт касаний
        """
        # Все потенциальные уровни
        swing_points = self.find_swing_points(highs, lows, lookback=3)
        
        if not swing_points:
            return []
        
        # Группируем близкие уровни (в пределах 0.3%)
        levels: List[KeyLevel] = []
        tolerance = 0.003  # 0.3%
        
        for swing in swing_points:
            price = swing.price
            merged = False
            
            for level in levels:
                if abs(level.price - price) / level.price < tolerance:
                    # Объединяем уровни
                    level.touches += 1
                    level.price = (level.price + price) / 2  # Средняя цена
                    level.last_touch_index = max(level.last_touch_index, swing.index)
                    merged = True
                    break
            
            if not merged:
                current_price = closes[-1]
                level_type = 'support' if price < current_price else 'resistance'
                levels.append(KeyLevel(
                    price=price,
                    type=level_type,
                    touches=1,
                    strength=0.5,
                    last_touch_index=swing.index
                ))
        
        # Фильтруем по количеству касаний
        levels = [l for l in levels if l.touches >= touches_required]
        
        # Рассчитываем силу уровня
        for level in levels:
            # Сила зависит от касаний и свежести
            recency = level.last_touch_index / len(closes)
            level.strength = min(1.0, 0.3 + level.touches * 0.15 + recency * 0.3)
        
        # Сортируем по силе
        levels.sort(key=lambda x: x.strength, reverse=True)
        
        return levels[:10]  # Топ 10 уровней
    
    # ==================== CANDLE PATTERNS ====================
    
    def detect_candle_patterns(self, opens: List[float], highs: List[float],
                                lows: List[float], closes: List[float]) -> List[CandlePattern]:
        """
        Обнаружение свечных паттернов
        
        Паттерны:
        - Engulfing (бычье/медвежье поглощение)
        - Pin Bar (пин бар)
        - Inside Bar (внутренний бар)
        - Morning/Evening Star
        """
        patterns = []
        n = len(closes)
        
        if n < 3:
            return patterns
        
        # Последние 3 свечи для анализа
        for i in range(max(2, n - 5), n):
            # === ENGULFING ===
            if i >= 1:
                prev_body = abs(closes[i-1] - opens[i-1])
                curr_body = abs(closes[i] - opens[i])
                prev_bullish = closes[i-1] > opens[i-1]
                curr_bullish = closes[i] > opens[i]
                
                # Бычье поглощение
                if not prev_bullish and curr_bullish:
                    if curr_body > prev_body * 1.3:
                        if opens[i] <= closes[i-1] and closes[i] >= opens[i-1]:
                            patterns.append(CandlePattern(
                                name="Bullish Engulfing",
                                type="bullish",
                                strength=0.75,
                                index=i
                            ))
                
                # Медвежье поглощение
                if prev_bullish and not curr_bullish:
                    if curr_body > prev_body * 1.3:
                        if opens[i] >= closes[i-1] and closes[i] <= opens[i-1]:
                            patterns.append(CandlePattern(
                                name="Bearish Engulfing",
                                type="bearish",
                                strength=0.75,
                                index=i
                            ))
            
            # === PIN BAR ===
            body = abs(closes[i] - opens[i])
            upper_wick = highs[i] - max(opens[i], closes[i])
            lower_wick = min(opens[i], closes[i]) - lows[i]
            total_range = highs[i] - lows[i]
            
            if total_range > 0:
                # Бычий пин бар (молот)
                if lower_wick > body * 2 and lower_wick > upper_wick * 2:
                    if body / total_range < 0.3:
                        patterns.append(CandlePattern(
                            name="Bullish Pin Bar",
                            type="bullish",
                            strength=0.8,
                            index=i
                        ))
                
                # Медвежий пин бар (падающая звезда)
                if upper_wick > body * 2 and upper_wick > lower_wick * 2:
                    if body / total_range < 0.3:
                        patterns.append(CandlePattern(
                            name="Bearish Pin Bar",
                            type="bearish",
                            strength=0.8,
                            index=i
                        ))
            
            # === INSIDE BAR ===
            if i >= 1:
                if highs[i] < highs[i-1] and lows[i] > lows[i-1]:
                    # Inside bar - паттерн консолидации
                    patterns.append(CandlePattern(
                        name="Inside Bar",
                        type="neutral",
                        strength=0.5,
                        index=i
                    ))
            
            # === DOJI ===
            if total_range > 0 and body / total_range < 0.1:
                patterns.append(CandlePattern(
                    name="Doji",
                    type="neutral",
                    strength=0.4,
                    index=i
                ))
        
        return patterns
    
    # ==================== MOMENTUM & TREND ====================
    
    def calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Exponential Moving Average"""
        if len(prices) < period:
            return [prices[-1]] * len(prices) if prices else []
        
        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        
        for price in prices[period:]:
            ema.append((price - ema[-1]) * multiplier + ema[-1])
        
        # Паддинг начала
        result = [ema[0]] * period + ema
        return result[-len(prices):]
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """RSI индикатор"""
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
    
    def calculate_atr(self, highs: List[float], lows: List[float], 
                      closes: List[float], period: int = 14) -> float:
        """Average True Range"""
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
    
    def calculate_volume_profile(self, volumes: List[float]) -> Dict:
        """Анализ объёма"""
        if len(volumes) < 20:
            return {'ratio': 1.0, 'trend': 'NEUTRAL'}
        
        avg_volume = np.mean(volumes[-20:])
        recent_volume = np.mean(volumes[-3:])
        ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        
        # Тренд объёма
        vol_sma_5 = np.mean(volumes[-5:])
        vol_sma_20 = np.mean(volumes[-20:])
        
        if vol_sma_5 > vol_sma_20 * 1.2:
            trend = 'INCREASING'
        elif vol_sma_5 < vol_sma_20 * 0.8:
            trend = 'DECREASING'
        else:
            trend = 'NEUTRAL'
        
        return {'ratio': ratio, 'trend': trend, 'current': recent_volume, 'average': avg_volume}
    
    # ==================== ДИСБАЛАНС И ЭКСТРЕМАЛЬНЫЕ ДВИЖЕНИЯ ====================
    
    async def detect_extreme_move(self, symbol: str) -> Dict:
        """
        Обнаружение экстремальных движений за последние 5-15 минут
        
        Ищем:
        - Резкое падение > 2% за 5-15 мин (перепроданность)
        - Резкий рост > 2% за 5-15 мин (перекупленность)
        - Это создаёт дисбаланс и возможность для отката
        """
        klines_5m = await self.get_klines(symbol, '5m', 20)
        
        if not klines_5m or len(klines_5m) < 10:
            return {'extreme': False, 'type': None, 'change': 0, 'signal': None}
        
        closes = [float(k[4]) for k in klines_5m]
        highs = [float(k[2]) for k in klines_5m]
        lows = [float(k[3]) for k in klines_5m]
        volumes = [float(k[5]) for k in klines_5m]
        
        current_price = closes[-1]
        
        # Изменение за последние 3 свечи (15 минут)
        price_15m_ago = closes[-4] if len(closes) >= 4 else closes[0]
        change_15m = (current_price - price_15m_ago) / price_15m_ago * 100
        
        # Изменение за последние 2 свечи (10 минут)
        price_10m_ago = closes[-3] if len(closes) >= 3 else closes[0]
        change_10m = (current_price - price_10m_ago) / price_10m_ago * 100
        
        # Изменение за 1 свечу (5 минут)
        price_5m_ago = closes[-2] if len(closes) >= 2 else closes[0]
        change_5m = (current_price - price_5m_ago) / price_5m_ago * 100
        
        # Средний объём vs текущий
        avg_volume = np.mean(volumes[:-3]) if len(volumes) > 3 else np.mean(volumes)
        recent_volume = np.mean(volumes[-3:])
        volume_spike = recent_volume / avg_volume if avg_volume > 0 else 1
        
        result = {
            'extreme': False,
            'type': None,
            'change_5m': change_5m,
            'change_10m': change_10m,
            'change_15m': change_15m,
            'volume_spike': volume_spike,
            'signal': None,
            'strength': 0,
            'reasoning': []
        }
        
        # === ЭКСТРЕМАЛЬНОЕ ПАДЕНИЕ (перепроданность) ===
        if change_15m < -1.5 or change_10m < -1.2 or change_5m < -0.8:
            result['extreme'] = True
            result['type'] = 'OVERSOLD'
            result['signal'] = 'LONG'  # Покупаем на перепроданности
            
            # Сила сигнала
            strength = 0
            if change_15m < -3:
                strength += 3
                result['reasoning'].append(f"🔥 Обвал -{abs(change_15m):.1f}% за 15 мин")
            elif change_15m < -2:
                strength += 2
                result['reasoning'].append(f"📉 Сильное падение -{abs(change_15m):.1f}% за 15 мин")
            elif change_15m < -1.5:
                strength += 1
                result['reasoning'].append(f"📉 Падение -{abs(change_15m):.1f}% за 15 мин")
            
            if change_5m < -0.8:
                strength += 1
                result['reasoning'].append(f"⚡ Резкое движение -{abs(change_5m):.1f}% за 5 мин")
            
            # Объём подтверждает
            if volume_spike > 1.5:
                strength += 2
                result['reasoning'].append(f"📊 Всплеск объёма x{volume_spike:.1f}")
            elif volume_spike > 1.2:
                strength += 1
            
            result['strength'] = min(5, strength)
            
        # === ЭКСТРЕМАЛЬНЫЙ РОСТ (перекупленность) ===
        elif change_15m > 1.5 or change_10m > 1.2 or change_5m > 0.8:
            result['extreme'] = True
            result['type'] = 'OVERBOUGHT'
            result['signal'] = 'SHORT'  # Продаём на перекупленности
            
            # Сила сигнала
            strength = 0
            if change_15m > 3:
                strength += 3
                result['reasoning'].append(f"🚀 Памп +{change_15m:.1f}% за 15 мин")
            elif change_15m > 2:
                strength += 2
                result['reasoning'].append(f"📈 Сильный рост +{change_15m:.1f}% за 15 мин")
            elif change_15m > 1.5:
                strength += 1
                result['reasoning'].append(f"📈 Рост +{change_15m:.1f}% за 15 мин")
            
            if change_5m > 0.8:
                strength += 1
                result['reasoning'].append(f"⚡ Резкое движение +{change_5m:.1f}% за 5 мин")
            
            # Объём подтверждает
            if volume_spike > 1.5:
                strength += 2
                result['reasoning'].append(f"📊 Всплеск объёма x{volume_spike:.1f}")
            elif volume_spike > 1.2:
                strength += 1
            
            result['strength'] = min(5, strength)
        
        if result['extreme']:
            logger.info(f"[EXTREME] {symbol}: {result['type']} change_15m={change_15m:.2f}% vol_spike={volume_spike:.1f}x strength={result['strength']}")
        
        return result
    
    def detect_rsi_divergence(self, prices: List[float], rsi_values: List[float]) -> Dict:
        """
        Обнаружение дивергенции RSI
        
        Бычья дивергенция: Цена делает новый минимум, RSI - нет (сигнал на покупку)
        Медвежья дивергенция: Цена делает новый максимум, RSI - нет (сигнал на продажу)
        """
        if len(prices) < 20 or len(rsi_values) < 20:
            return {'divergence': False, 'type': None, 'signal': None}
        
        # Ищем последние 2 локальных минимума/максимума
        lookback = 10
        
        # Для бычьей дивергенции: ищем 2 минимума цены
        price_lows = []
        rsi_at_lows = []
        for i in range(lookback, len(prices) - 1):
            # Локальный минимум цены
            if prices[i] < prices[i-1] and prices[i] < prices[i+1]:
                if prices[i] < min(prices[i-lookback:i]):
                    price_lows.append((i, prices[i]))
                    rsi_at_lows.append(rsi_values[i] if i < len(rsi_values) else 50)
        
        # Для медвежьей дивергенции: ищем 2 максимума цены
        price_highs = []
        rsi_at_highs = []
        for i in range(lookback, len(prices) - 1):
            # Локальный максимум цены
            if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
                if prices[i] > max(prices[i-lookback:i]):
                    price_highs.append((i, prices[i]))
                    rsi_at_highs.append(rsi_values[i] if i < len(rsi_values) else 50)
        
        result = {'divergence': False, 'type': None, 'signal': None, 'strength': 0}
        
        # Проверяем бычью дивергенцию (последние 2 минимума)
        if len(price_lows) >= 2 and len(rsi_at_lows) >= 2:
            # Цена упала (новый минимум ниже), но RSI вырос
            if price_lows[-1][1] < price_lows[-2][1] and rsi_at_lows[-1] > rsi_at_lows[-2]:
                result['divergence'] = True
                result['type'] = 'BULLISH'
                result['signal'] = 'LONG'
                result['strength'] = min(3, int((rsi_at_lows[-1] - rsi_at_lows[-2]) / 5))
                logger.info(f"[DIVERGENCE] Bullish: Price {price_lows[-2][1]:.2f} -> {price_lows[-1][1]:.2f}, RSI {rsi_at_lows[-2]:.0f} -> {rsi_at_lows[-1]:.0f}")
        
        # Проверяем медвежью дивергенцию (последние 2 максимума)
        if len(price_highs) >= 2 and len(rsi_at_highs) >= 2:
            # Цена выросла (новый максимум выше), но RSI упал
            if price_highs[-1][1] > price_highs[-2][1] and rsi_at_highs[-1] < rsi_at_highs[-2]:
                result['divergence'] = True
                result['type'] = 'BEARISH'
                result['signal'] = 'SHORT'
                result['strength'] = min(3, int((rsi_at_highs[-2] - rsi_at_highs[-1]) / 5))
                logger.info(f"[DIVERGENCE] Bearish: Price {price_highs[-2][1]:.2f} -> {price_highs[-1][1]:.2f}, RSI {rsi_at_highs[-2]:.0f} -> {rsi_at_highs[-1]:.0f}")
        
        return result
    
    def calculate_stochastic(self, highs: List[float], lows: List[float], 
                             closes: List[float], k_period: int = 14, d_period: int = 3) -> Dict:
        """
        Стохастик - определяет перекупленность/перепроданность
        
        %K < 20 = перепроданность (покупка)
        %K > 80 = перекупленность (продажа)
        """
        if len(closes) < k_period:
            return {'k': 50, 'd': 50, 'signal': 'NEUTRAL', 'extreme': False}
        
        # %K = (Close - Lowest Low) / (Highest High - Lowest Low) * 100
        lowest_low = min(lows[-k_period:])
        highest_high = max(highs[-k_period:])
        
        if highest_high == lowest_low:
            k = 50
        else:
            k = (closes[-1] - lowest_low) / (highest_high - lowest_low) * 100
        
        # %D = SMA(%K, d_period) - упрощённо берём текущее значение
        d = k  # В реальности нужно SMA последних K
        
        result = {
            'k': k,
            'd': d,
            'signal': 'NEUTRAL',
            'extreme': False,
            'strength': 0
        }
        
        # Экстремальные значения
        if k < 20:
            result['signal'] = 'LONG'
            result['extreme'] = True
            result['strength'] = min(3, int((20 - k) / 5))
        elif k > 80:
            result['signal'] = 'SHORT'
            result['extreme'] = True
            result['strength'] = min(3, int((k - 80) / 5))
        elif k < 30:
            result['signal'] = 'LONG'
            result['strength'] = 1
        elif k > 70:
            result['signal'] = 'SHORT'
            result['strength'] = 1
        
        return result
    
    async def get_funding_rate(self, symbol: str) -> Dict:
        """
        Получить funding rate с Bybit
        
        Высокий положительный funding (>0.05%) = много лонгов, возможен шорт
        Высокий отрицательный funding (<-0.05%) = много шортов, возможен лонг
        """
        try:
            bybit_symbol = symbol.replace('/', '')
            url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={bybit_symbol}"
            
            session = await self._get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('retCode') == 0:
                        ticker = data['result']['list'][0] if data['result']['list'] else {}
                        funding_rate = float(ticker.get('fundingRate', '0')) * 100  # В процентах
                        
                        result = {
                            'rate': funding_rate,
                            'signal': 'NEUTRAL',
                            'extreme': False,
                            'reasoning': None
                        }
                        
                        # Экстремальный funding
                        if funding_rate > 0.05:
                            result['signal'] = 'SHORT'
                            result['extreme'] = True
                            result['reasoning'] = f"💰 Высокий фандинг +{funding_rate:.3f}% (много лонгов)"
                        elif funding_rate < -0.05:
                            result['signal'] = 'LONG'
                            result['extreme'] = True
                            result['reasoning'] = f"💰 Низкий фандинг {funding_rate:.3f}% (много шортов)"
                        elif funding_rate > 0.03:
                            result['signal'] = 'SHORT'
                            result['reasoning'] = f"💰 Повышенный фандинг +{funding_rate:.3f}%"
                        elif funding_rate < -0.03:
                            result['signal'] = 'LONG'
                            result['reasoning'] = f"💰 Пониженный фандинг {funding_rate:.3f}%"
                        
                        return result
        except Exception as e:
            logger.warning(f"[FUNDING] Error {symbol}: {e}")
        
        return {'rate': 0, 'signal': 'NEUTRAL', 'extreme': False, 'reasoning': None}
    
    async def get_order_book_imbalance(self, symbol: str, depth: int = 25) -> Dict:
        """
        Дисбаланс стакана заявок
        
        Много бидов vs асков = покупатели сильнее
        Много асков vs бидов = продавцы сильнее
        """
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://api.binance.com/api/v3/depth?symbol={binance_symbol}&limit={depth}"
            
            session = await self._get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    bids = data.get('bids', [])
                    asks = data.get('asks', [])
                    
                    # Сумма объёмов бидов и асков
                    bid_volume = sum(float(b[1]) for b in bids[:depth])
                    ask_volume = sum(float(a[1]) for a in asks[:depth])
                    
                    total_volume = bid_volume + ask_volume
                    if total_volume == 0:
                        return {'imbalance': 0, 'signal': 'NEUTRAL', 'bid_ratio': 0.5}
                    
                    bid_ratio = bid_volume / total_volume
                    imbalance = (bid_volume - ask_volume) / total_volume * 100  # В процентах
                    
                    result = {
                        'imbalance': imbalance,
                        'bid_ratio': bid_ratio,
                        'bid_volume': bid_volume,
                        'ask_volume': ask_volume,
                        'signal': 'NEUTRAL',
                        'strength': 0,
                        'reasoning': None
                    }
                    
                    # Сильный дисбаланс
                    if imbalance > 30:  # Много покупателей
                        result['signal'] = 'LONG'
                        result['strength'] = min(3, int(imbalance / 15))
                        result['reasoning'] = f"📗 Стакан: покупатели {bid_ratio:.0%} (дисбаланс +{imbalance:.0f}%)"
                    elif imbalance < -30:  # Много продавцов
                        result['signal'] = 'SHORT'
                        result['strength'] = min(3, int(abs(imbalance) / 15))
                        result['reasoning'] = f"📕 Стакан: продавцы {1-bid_ratio:.0%} (дисбаланс {imbalance:.0f}%)"
                    elif imbalance > 15:
                        result['signal'] = 'LONG'
                        result['strength'] = 1
                    elif imbalance < -15:
                        result['signal'] = 'SHORT'
                        result['strength'] = 1
                    
                    return result
                    
        except Exception as e:
            logger.warning(f"[ORDERBOOK] Error {symbol}: {e}")
        
        return {'imbalance': 0, 'signal': 'NEUTRAL', 'bid_ratio': 0.5, 'strength': 0}
    
    def calculate_bollinger_bands(self, closes: List[float], period: int = 20, std_dev: float = 2.0) -> Dict:
        """
        Bollinger Bands - определяют волатильность и экстремумы
        
        Цена у нижней полосы = потенциальная покупка
        Цена у верхней полосы = потенциальная продажа
        """
        if len(closes) < period:
            return {'upper': closes[-1], 'middle': closes[-1], 'lower': closes[-1], 
                    'signal': 'NEUTRAL', 'percent_b': 0.5}
        
        sma = np.mean(closes[-period:])
        std = np.std(closes[-period:])
        
        upper = sma + std_dev * std
        lower = sma - std_dev * std
        
        current = closes[-1]
        
        # %B = (Price - Lower) / (Upper - Lower)
        band_width = upper - lower
        percent_b = (current - lower) / band_width if band_width > 0 else 0.5
        
        result = {
            'upper': upper,
            'middle': sma,
            'lower': lower,
            'percent_b': percent_b,
            'signal': 'NEUTRAL',
            'extreme': False,
            'reasoning': None
        }
        
        # Экстремальные значения
        if percent_b < 0:  # Ниже нижней полосы
            result['signal'] = 'LONG'
            result['extreme'] = True
            result['reasoning'] = f"📉 Цена НИЖЕ Bollinger ({percent_b:.0%})"
        elif percent_b > 1:  # Выше верхней полосы
            result['signal'] = 'SHORT'
            result['extreme'] = True
            result['reasoning'] = f"📈 Цена ВЫШЕ Bollinger ({percent_b:.0%})"
        elif percent_b < 0.1:
            result['signal'] = 'LONG'
            result['reasoning'] = f"📉 Цена у нижней Bollinger ({percent_b:.0%})"
        elif percent_b > 0.9:
            result['signal'] = 'SHORT'
            result['reasoning'] = f"📈 Цена у верхней Bollinger ({percent_b:.0%})"
        
        return result
    
    def calculate_macd(self, closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """
        MACD - определяет momentum и развороты
        """
        if len(closes) < slow + signal:
            return {'macd': 0, 'signal_line': 0, 'histogram': 0, 'trend': 'NEUTRAL'}
        
        ema_fast = self.calculate_ema(closes, fast)
        ema_slow = self.calculate_ema(closes, slow)
        
        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = self.calculate_ema(macd_line, signal)
        
        histogram = macd_line[-1] - signal_line[-1]
        prev_histogram = macd_line[-2] - signal_line[-2] if len(macd_line) > 1 else histogram
        
        result = {
            'macd': macd_line[-1],
            'signal_line': signal_line[-1],
            'histogram': histogram,
            'trend': 'NEUTRAL',
            'crossover': None
        }
        
        # Определяем тренд и пересечения
        if macd_line[-1] > signal_line[-1]:
            result['trend'] = 'BULLISH'
            # Бычье пересечение
            if len(macd_line) > 1 and macd_line[-2] <= signal_line[-2]:
                result['crossover'] = 'BULLISH'
        else:
            result['trend'] = 'BEARISH'
            # Медвежье пересечение
            if len(macd_line) > 1 and macd_line[-2] >= signal_line[-2]:
                result['crossover'] = 'BEARISH'
        
        # Усиление/ослабление momentum
        if histogram > 0 and histogram > prev_histogram:
            result['momentum'] = 'STRENGTHENING_UP'
        elif histogram < 0 and histogram < prev_histogram:
            result['momentum'] = 'STRENGTHENING_DOWN'
        elif histogram > 0 and histogram < prev_histogram:
            result['momentum'] = 'WEAKENING_UP'
        elif histogram < 0 and histogram > prev_histogram:
            result['momentum'] = 'WEAKENING_DOWN'
        else:
            result['momentum'] = 'NEUTRAL'
        
        return result
    
    async def get_open_interest_change(self, symbol: str) -> Dict:
        """
        Изменение Open Interest
        
        Рост OI + рост цены = сильный тренд вверх
        Рост OI + падение цены = сильный тренд вниз
        Падение OI = закрытие позиций (возможен разворот)
        """
        try:
            bybit_symbol = symbol.replace('/', '')
            url = f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={bybit_symbol}&intervalTime=5min&limit=12"
            
            session = await self._get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('retCode') == 0:
                        oi_list = data['result']['list']
                        if len(oi_list) >= 2:
                            current_oi = float(oi_list[0]['openInterest'])
                            prev_oi = float(oi_list[-1]['openInterest'])
                            
                            change_percent = (current_oi - prev_oi) / prev_oi * 100 if prev_oi > 0 else 0
                            
                            result = {
                                'current': current_oi,
                                'change_percent': change_percent,
                                'trend': 'NEUTRAL',
                                'reasoning': None
                            }
                            
                            if change_percent > 5:
                                result['trend'] = 'INCREASING'
                                result['reasoning'] = f"📈 OI растёт +{change_percent:.1f}% (новые позиции)"
                            elif change_percent < -5:
                                result['trend'] = 'DECREASING'
                                result['reasoning'] = f"📉 OI падает {change_percent:.1f}% (закрытие позиций)"
                            
                            return result
        except Exception as e:
            logger.warning(f"[OI] Error {symbol}: {e}")
        
        return {'current': 0, 'change_percent': 0, 'trend': 'NEUTRAL', 'reasoning': None}
    
    # ==================== SIGNAL GENERATION ====================
    
    def evaluate_setup_quality(self, 
                               market_regime: MarketRegime,
                               direction: str,
                               at_key_level: bool,
                               pattern_confirmation: bool,
                               volume_confirmation: bool,
                               momentum_aligned: bool,
                               risk_reward: float,
                               bullish_signals: int = 0,
                               bearish_signals: int = 0,
                               has_extreme_move: bool = False,
                               has_divergence: bool = False) -> Tuple[SetupQuality, float]:
        """
        Оценка качества сетапа с учётом ДИСБАЛАНСА
        
        A+ сетап (берём всегда):
        - Сильный тренд в направлении сделки
        - Цена у ключевого уровня
        - Свечной паттерн подтверждает
        - Объём растёт
        - R/R >= 3
        - ИЛИ: Сильный дисбаланс (>= 6 сигналов)
        
        A сетап:
        - Тренд в направлении
        - 3 из 4 подтверждений
        - R/R >= 2.5
        
        B сетап:
        - Тренд или нейтрально
        - 2 из 4 подтверждений
        - R/R >= 2
        
        C/D - пропускаем
        """
        score = 0
        max_score = 100
        
        # === ДИСБАЛАНС БОНУС (НОВОЕ!) ===
        # Сильный дисбаланс может улучшить скор
        signal_count = bullish_signals if direction == "LONG" else bearish_signals
        opposite_count = bearish_signals if direction == "LONG" else bullish_signals
        
        # Чистый дисбаланс в нашу сторону
        net_imbalance = signal_count - opposite_count
        
        if net_imbalance >= 6:
            score += 25  # Очень сильный дисбаланс
            logger.info(f"[QUALITY] Strong imbalance bonus +25 (net={net_imbalance})")
        elif net_imbalance >= 4:
            score += 15  # Сильный дисбаланс
        elif net_imbalance >= 2:
            score += 8   # Умеренный дисбаланс
        elif net_imbalance < 0:
            score -= 15  # Дисбаланс против нас!
            logger.info(f"[QUALITY] Imbalance AGAINST us! penalty -15 (net={net_imbalance})")
        
        # Экстремальное движение бонус
        if has_extreme_move:
            score += 10
            logger.info(f"[QUALITY] Extreme move bonus +10")
        
        # Дивергенция бонус
        if has_divergence:
            score += 8
        
        # 1. Режим рынка (30 баллов)
        if direction == "LONG":
            if market_regime == MarketRegime.STRONG_UPTREND:
                score += 30
            elif market_regime == MarketRegime.UPTREND:
                score += 25
            elif market_regime == MarketRegime.RANGING:
                score += 10
            elif market_regime in [MarketRegime.DOWNTREND, MarketRegime.STRONG_DOWNTREND]:
                # Против тренда - но если сильный дисбаланс, меньший штраф
                if net_imbalance >= 5:
                    score -= 5  # Меньший штраф
                else:
                    score -= 20  # Полный штраф
        else:  # SHORT
            if market_regime == MarketRegime.STRONG_DOWNTREND:
                score += 30
            elif market_regime == MarketRegime.DOWNTREND:
                score += 25
            elif market_regime == MarketRegime.RANGING:
                score += 10
            elif market_regime in [MarketRegime.UPTREND, MarketRegime.STRONG_UPTREND]:
                if net_imbalance >= 5:
                    score -= 5
                else:
                    score -= 20
        
        # Высокая волатильность - не торгуем
        if market_regime == MarketRegime.HIGH_VOLATILITY:
            return SetupQuality.D, 0.0
        
        # 2. Ключевой уровень (20 баллов)
        if at_key_level:
            score += 20
        
        # 3. Свечной паттерн (20 баллов)
        if pattern_confirmation:
            score += 20
        
        # 4. Объём (15 баллов)
        if volume_confirmation:
            score += 15
        
        # 5. Моментум (15 баллов)
        if momentum_aligned:
            score += 15
        
        # 6. Risk/Reward бонус
        if risk_reward >= 3:
            score += 10
        elif risk_reward >= 2.5:
            score += 5
        elif risk_reward < 2:
            score -= 10
        
        # Нормализуем confidence
        confidence = min(0.95, max(0.3, score / max_score))
        
        # Определяем качество
        if score >= 85:
            quality = SetupQuality.A_PLUS
        elif score >= 70:
            quality = SetupQuality.A
        elif score >= 55:
            quality = SetupQuality.B
        elif score >= 40:
            quality = SetupQuality.C
        else:
            quality = SetupQuality.D
        
        logger.info(f"[QUALITY] Score={score}, Quality={quality.name}, Confidence={confidence:.0%}, NetImbalance={net_imbalance}")
        
        return quality, confidence
    
    def calculate_dynamic_levels(self, 
                                  entry: float,
                                  direction: str,
                                  atr: float,
                                  key_levels: List[KeyLevel],
                                  swings: List[SwingPoint]) -> Dict:
        """
        Расчёт динамических TP/SL на основе структуры рынка
        
        SL: За ближайшим свингом + буфер
        TP1: До ближайшего уровня сопротивления (50% позиции)
        TP2: До следующего уровня (30% позиции)
        TP3: Трейлинг или далёкий уровень (20% позиции)
        """
        
        # Буфер = 0.5 ATR
        buffer = atr * 0.5
        
        if direction == "LONG":
            # SL: под последним swing low
            recent_lows = [s.price for s in swings if s.type in ['HL', 'LL', 'LOW']][-3:]
            if recent_lows:
                sl = min(recent_lows) - buffer
            else:
                sl = entry - atr * 1.5
            
            # TP: ближайшие уровни сопротивления
            resistances = sorted([l.price for l in key_levels 
                                  if l.type == 'resistance' and l.price > entry])
            
            if len(resistances) >= 3:
                tp1 = resistances[0]
                tp2 = resistances[1]
                tp3 = resistances[2]
            elif len(resistances) >= 1:
                tp1 = resistances[0]
                tp2 = entry + atr * 2.5
                tp3 = entry + atr * 4
            else:
                tp1 = entry + atr * 1.5
                tp2 = entry + atr * 2.5
                tp3 = entry + atr * 4
                
        else:  # SHORT
            # SL: над последним swing high
            recent_highs = [s.price for s in swings if s.type in ['HH', 'LH', 'HIGH']][-3:]
            if recent_highs:
                sl = max(recent_highs) + buffer
            else:
                sl = entry + atr * 1.5
            
            # TP: ближайшие уровни поддержки
            supports = sorted([l.price for l in key_levels 
                              if l.type == 'support' and l.price < entry], reverse=True)
            
            if len(supports) >= 3:
                tp1 = supports[0]
                tp2 = supports[1]
                tp3 = supports[2]
            elif len(supports) >= 1:
                tp1 = supports[0]
                tp2 = entry - atr * 2.5
                tp3 = entry - atr * 4
            else:
                tp1 = entry - atr * 1.5
                tp2 = entry - atr * 2.5
                tp3 = entry - atr * 4
        
        # Расчёт R/R
        risk = abs(entry - sl)
        reward = abs(tp1 - entry)
        risk_reward = reward / risk if risk > 0 else 0
        
        return {
            'stop_loss': sl,
            'take_profit_1': tp1,
            'take_profit_2': tp2,
            'take_profit_3': tp3,
            'risk': risk,
            'reward': reward,
            'risk_reward': risk_reward
        }
    
    # ==================== MAIN ANALYSIS ====================
    
    async def analyze(self, symbol: str, balance: float = 0) -> Optional[TradeSetup]:
        """
        Главный метод анализа
        
        Возвращает TradeSetup если найден качественный сетап, иначе None
        """
        global _signal_stats
        
        logger.info(f"[SMART] ========== Analyzing {symbol} ==========")
        _signal_stats['analyzed'] += 1
        
        # 1. Проверка состояния торговли
        can_trade, reason = self.state.can_trade(balance)
        if not can_trade:
            logger.info(f"[SMART] Skip: {reason}")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['state_blocked'] += 1
            return None
        
        # 2. Проверка торговой сессии
        if not self._is_good_trading_time():
            logger.info("[SMART] Skip: Outside trading hours")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['outside_hours'] += 1
            return None
        
        # 3. Загружаем данные
        klines_1h = await self.get_klines(symbol, '1h', 100)
        klines_4h = await self.get_klines(symbol, '4h', 50)
        klines_15m = await self.get_klines(symbol, '15m', 50)
        
        if not klines_1h or len(klines_1h) < 50:
            logger.warning(f"[SMART] Insufficient data for {symbol}")
            return None
        
        # Парсим данные
        opens_1h = [float(k[1]) for k in klines_1h]
        highs_1h = [float(k[2]) for k in klines_1h]
        lows_1h = [float(k[3]) for k in klines_1h]
        closes_1h = [float(k[4]) for k in klines_1h]
        volumes_1h = [float(k[5]) for k in klines_1h]
        
        current_price = closes_1h[-1]
        
        # 4. Анализ структуры рынка
        swings = self.find_swing_points(highs_1h, lows_1h, lookback=5)
        logger.info(f"[SMART] Found {len(swings)} swing points")
        
        # 5. Определение режима рынка
        atr = self.calculate_atr(highs_1h, lows_1h, closes_1h)
        atr_percent = (atr / current_price) * 100
        
        price_change_24h = (closes_1h[-1] - closes_1h[-24]) / closes_1h[-24] * 100 if len(closes_1h) >= 24 else 0
        
        market_regime = self.determine_market_regime(swings, atr_percent, price_change_24h)
        logger.info(f"[SMART] Market Regime: {market_regime.value}, ATR: {atr_percent:.2f}%")
        
        # Не торгуем в высокой волатильности или неопределённости
        if market_regime in [MarketRegime.HIGH_VOLATILITY, MarketRegime.UNKNOWN]:
            logger.info(f"[SMART] Skip: Bad market regime")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['bad_regime'] += 1
            return None
        
        # 6. Ключевые уровни
        key_levels = self.find_key_levels(highs_1h, lows_1h, closes_1h, touches_required=2)
        logger.info(f"[SMART] Found {len(key_levels)} key levels")
        
        # 7. Свечные паттерны (на 1H)
        patterns = self.detect_candle_patterns(opens_1h, highs_1h, lows_1h, closes_1h)
        recent_patterns = [p for p in patterns if p.index >= len(closes_1h) - 3]
        logger.info(f"[SMART] Recent patterns: {[p.name for p in recent_patterns]}")
        
        # 8. Базовые индикаторы
        rsi = self.calculate_rsi(closes_1h)
        ema_20 = self.calculate_ema(closes_1h, 20)
        ema_50 = self.calculate_ema(closes_1h, 50)
        volume_data = self.calculate_volume_profile(volumes_1h)
        
        logger.info(f"[SMART] RSI={rsi:.1f}, Price vs EMA20={((current_price/ema_20[-1])-1)*100:.2f}%")
        
        # 9. НОВЫЕ ИНДИКАТОРЫ: Дисбаланс и экстремальные движения
        extreme_move = await self.detect_extreme_move(symbol)
        stochastic = self.calculate_stochastic(highs_1h, lows_1h, closes_1h)
        bollinger = self.calculate_bollinger_bands(closes_1h)
        macd = self.calculate_macd(closes_1h)
        funding = await self.get_funding_rate(symbol)
        orderbook = await self.get_order_book_imbalance(symbol)
        oi_change = await self.get_open_interest_change(symbol)
        
        # Логируем дисбаланс
        if extreme_move['extreme']:
            logger.info(f"[SMART] 🔥 EXTREME MOVE: {extreme_move['type']} change_15m={extreme_move['change_15m']:.2f}%")
        if stochastic['extreme']:
            logger.info(f"[SMART] 📊 STOCHASTIC EXTREME: {stochastic['signal']} K={stochastic['k']:.0f}")
        if bollinger['extreme']:
            logger.info(f"[SMART] 📉 BOLLINGER EXTREME: {bollinger['signal']}")
        if funding['extreme']:
            logger.info(f"[SMART] 💰 FUNDING EXTREME: {funding['rate']:.3f}%")
        if orderbook['strength'] >= 2:
            logger.info(f"[SMART] 📗 ORDERBOOK: imbalance={orderbook['imbalance']:.0f}%")
        
        # 10. Определяем направление сигнала
        direction = None
        signal_type = SignalType.NONE
        reasoning = []
        warnings = []
        
        # === СЧЁТЧИКИ СИГНАЛОВ ===
        bullish_signals = 0
        bearish_signals = 0
        
        # === ЛОГИКА СИГНАЛА ===
        
        # Проверка на откат к уровню
        at_support = any(abs(current_price - l.price) / l.price < 0.005 
                        for l in key_levels if l.type == 'support')
        at_resistance = any(abs(current_price - l.price) / l.price < 0.005 
                           for l in key_levels if l.type == 'resistance')
        
        # Бычий паттерн на уровне
        bullish_pattern = any(p.type == 'bullish' and p.strength >= 0.7 for p in recent_patterns)
        bearish_pattern = any(p.type == 'bearish' and p.strength >= 0.7 for p in recent_patterns)
        
        # Моментум
        bullish_momentum = rsi > 40 and rsi < 70 and current_price > ema_20[-1]
        bearish_momentum = rsi < 60 and rsi > 30 and current_price < ema_20[-1]
        
        # Объём подтверждает
        volume_confirms = volume_data['ratio'] > 1.2 or volume_data['trend'] == 'INCREASING'
        
        # === НОВАЯ ЛОГИКА: ЭКСТРЕМАЛЬНЫЕ ДВИЖЕНИЯ И ДИСБАЛАНС ===
        
        # 1. Экстремальное движение (резкое падение/рост)
        if extreme_move['extreme'] and extreme_move['strength'] >= 3:
            if extreme_move['signal'] == 'LONG':
                bullish_signals += extreme_move['strength']
                reasoning.extend(extreme_move['reasoning'])
            elif extreme_move['signal'] == 'SHORT':
                bearish_signals += extreme_move['strength']
                reasoning.extend(extreme_move['reasoning'])
        
        # 2. Стохастик (перепроданность/перекупленность)
        if stochastic['extreme']:
            if stochastic['signal'] == 'LONG':
                bullish_signals += stochastic['strength'] + 1
                reasoning.append(f"📊 Стохастик перепродан (K={stochastic['k']:.0f})")
            elif stochastic['signal'] == 'SHORT':
                bearish_signals += stochastic['strength'] + 1
                reasoning.append(f"📊 Стохастик перекуплен (K={stochastic['k']:.0f})")
        
        # 3. Bollinger Bands
        if bollinger['extreme']:
            if bollinger['signal'] == 'LONG':
                bullish_signals += 2
                reasoning.append(bollinger['reasoning'])
            elif bollinger['signal'] == 'SHORT':
                bearish_signals += 2
                reasoning.append(bollinger['reasoning'])
        
        # 4. Фандинг рейт (экстремальный)
        if funding['extreme']:
            if funding['signal'] == 'LONG':
                bullish_signals += 2
                reasoning.append(funding['reasoning'])
            elif funding['signal'] == 'SHORT':
                bearish_signals += 2
                reasoning.append(funding['reasoning'])
        
        # 5. Дисбаланс стакана
        if orderbook['strength'] >= 2:
            if orderbook['signal'] == 'LONG':
                bullish_signals += orderbook['strength']
                reasoning.append(orderbook['reasoning'])
            elif orderbook['signal'] == 'SHORT':
                bearish_signals += orderbook['strength']
                reasoning.append(orderbook['reasoning'])
        
        # 6. MACD crossover
        if macd['crossover'] == 'BULLISH':
            bullish_signals += 2
            reasoning.append("📈 MACD бычье пересечение")
        elif macd['crossover'] == 'BEARISH':
            bearish_signals += 2
            reasoning.append("📉 MACD медвежье пересечение")
        
        # 7. Open Interest
        if oi_change['reasoning']:
            if oi_change['trend'] == 'INCREASING':
                # Рост OI усиливает текущий тренд
                if bullish_signals > bearish_signals:
                    bullish_signals += 1
                else:
                    bearish_signals += 1
                reasoning.append(oi_change['reasoning'])
        
        logger.info(f"[SMART] Signals: Bullish={bullish_signals}, Bearish={bearish_signals}")
        
        # === КЛАССИЧЕСКАЯ ЛОГИКА (по тренду) ===
        
        # === LONG SETUP ===
        if market_regime in [MarketRegime.STRONG_UPTREND, MarketRegime.UPTREND]:
            if at_support or (current_price > ema_50[-1] and current_price < ema_20[-1] * 1.01):
                # Откат к поддержке в восходящем тренде
                direction = "LONG"
                signal_type = SignalType.PULLBACK
                reasoning.insert(0, "📈 Восходящий тренд")
                reasoning.insert(1, "🎯 Откат к поддержке/EMA")
                bullish_signals += 3
                
                if bullish_pattern:
                    bullish_signals += 2
                    reasoning.append(f"🕯️ {[p.name for p in recent_patterns if p.type == 'bullish']}")
        
        elif market_regime == MarketRegime.RANGING:
            if at_support and bullish_pattern and rsi < 40:
                # Отскок от поддержки в рейндже
                direction = "LONG"
                signal_type = SignalType.TREND_REVERSAL
                reasoning.insert(0, "⚖️ Рейндж: отскок от поддержки")
                reasoning.insert(1, f"📊 RSI перепродан ({rsi:.0f})")
                bullish_signals += 2
        
        # === SHORT SETUP ===
        if direction is None:  # Если ещё не определили направление
            if market_regime in [MarketRegime.STRONG_DOWNTREND, MarketRegime.DOWNTREND]:
                if at_resistance or (current_price < ema_50[-1] and current_price > ema_20[-1] * 0.99):
                    direction = "SHORT"
                    signal_type = SignalType.PULLBACK
                    reasoning.insert(0, "📉 Нисходящий тренд")
                    reasoning.insert(1, "🎯 Откат к сопротивлению/EMA")
                    bearish_signals += 3
                    
                    if bearish_pattern:
                        bearish_signals += 2
                        reasoning.append(f"🕯️ {[p.name for p in recent_patterns if p.type == 'bearish']}")
            
            elif market_regime == MarketRegime.RANGING:
                if at_resistance and bearish_pattern and rsi > 60:
                    direction = "SHORT"
                    signal_type = SignalType.TREND_REVERSAL
                    reasoning.insert(0, "⚖️ Рейндж: отскок от сопротивления")
                    reasoning.insert(1, f"📊 RSI перекуплен ({rsi:.0f})")
                    bearish_signals += 2
        
        # === ДИСБАЛАНС-ЛОГИКА: Если нет сигнала по тренду, но есть сильный дисбаланс ===
        if direction is None and (bullish_signals >= 4 or bearish_signals >= 4):
            # Сильный дисбаланс может создать сигнал даже без тренда
            if bullish_signals >= 4 and bullish_signals > bearish_signals * 1.3:
                direction = "LONG"
                signal_type = SignalType.TREND_REVERSAL
                reasoning.insert(0, "🔥 ДИСБАЛАНС: Сильная перепроданность")
                logger.info(f"[SMART] IMBALANCE LONG: {bullish_signals} vs {bearish_signals}")
            elif bearish_signals >= 4 and bearish_signals > bullish_signals * 1.3:
                direction = "SHORT"
                signal_type = SignalType.TREND_REVERSAL
                reasoning.insert(0, "🔥 ДИСБАЛАНС: Сильная перекупленность")
                logger.info(f"[SMART] IMBALANCE SHORT: {bearish_signals} vs {bullish_signals}")
        
        # Нет сигнала
        if direction is None:
            logger.info("[SMART] No valid setup found")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['no_setup'] += 1
            return None
        
        # 10. Расчёт уровней
        levels = self.calculate_dynamic_levels(
            entry=current_price,
            direction=direction,
            atr=atr,
            key_levels=key_levels,
            swings=swings
        )
        
        # Проверка минимального R/R
        if levels['risk_reward'] < self.MIN_RISK_REWARD:
            logger.info(f"[SMART] Skip: R/R too low ({levels['risk_reward']:.2f})")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['bad_rr'] += 1
            return None
        
        # 11. Оценка качества
        quality, confidence = self.evaluate_setup_quality(
            market_regime=market_regime,
            direction=direction,
            at_key_level=at_support or at_resistance,
            pattern_confirmation=bullish_pattern if direction == "LONG" else bearish_pattern,
            volume_confirmation=volume_confirms,
            momentum_aligned=bullish_momentum if direction == "LONG" else bearish_momentum,
            risk_reward=levels['risk_reward'],
            bullish_signals=bullish_signals,
            bearish_signals=bearish_signals,
            has_extreme_move=extreme_move['extreme'],
            has_divergence=False  # TODO: добавить дивергенцию позже
        )
        
        # Фильтр по качеству (A_PLUS=5, A=4, B=3, C=2, D=1)
        # Если quality < MIN_QUALITY, то качество слишком низкое
        if quality.value < self.MIN_QUALITY.value:
            logger.info(f"[SMART] Skip: Quality too low ({quality.name})")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['low_quality'] += 1
            return None
        
        if confidence < self.MIN_CONFIDENCE:
            logger.info(f"[SMART] Skip: Confidence too low ({confidence:.0%})")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['low_confidence'] += 1
            return None
        
        # 12. Добавляем предупреждения
        if volume_data['ratio'] < 0.8:
            warnings.append("⚠️ Низкий объём")
        
        if market_regime == MarketRegime.RANGING:
            warnings.append("⚠️ Рейндж - выше риск ложного пробоя")
        
        if atr_percent > 2:
            warnings.append(f"⚠️ Высокая волатильность ({atr_percent:.1f}%)")
        
        # 13. Формируем сетап
        setup = TradeSetup(
            symbol=symbol,
            direction=direction,
            entry=current_price,
            stop_loss=levels['stop_loss'],
            take_profit_1=levels['take_profit_1'],
            take_profit_2=levels['take_profit_2'],
            take_profit_3=levels['take_profit_3'],
            quality=quality,
            signal_type=signal_type,
            risk_reward=levels['risk_reward'],
            confidence=confidence,
            reasoning=reasoning,
            warnings=warnings,
            market_regime=market_regime,
            timestamp=datetime.now(timezone.utc)
        )
        
        logger.info(f"[SMART] ✅ Setup found: {direction} {symbol}")
        logger.info(f"[SMART] Quality: {quality.name}, Confidence: {confidence:.0%}, R/R: {levels['risk_reward']:.2f}")
        logger.info(f"[SMART] Entry: {current_price:.4f}, SL: {levels['stop_loss']:.4f}, TP1: {levels['take_profit_1']:.4f}")
        logger.info(f"[SMART] Signals: Bullish={bullish_signals}, Bearish={bearish_signals}")
        
        _signal_stats['accepted'] += 1
        
        # Отслеживаем дисбаланс-сделки
        if extreme_move['extreme']:
            _signal_stats['extreme_moves_detected'] += 1
        if signal_type == SignalType.TREND_REVERSAL and (bullish_signals >= 4 or bearish_signals >= 4):
            _signal_stats['imbalance_trades'] += 1
        
        return setup
    
    def _is_good_trading_time(self) -> bool:
        """Проверка торгового времени - крипта 24/7, всегда разрешено"""
        # Крипта торгуется 24/7, убираем ограничение по часам
        # Но избегаем только очень низколиквидных часов (3-5 UTC)
        hour = datetime.now(timezone.utc).hour
        
        # Очень низкая ликвидность только 3-5 UTC (азиатская ночь)
        if 3 <= hour < 5:
            logger.debug(f"[TIME] Low liquidity hours ({hour} UTC) - but still allowed")
            # Всё равно разрешаем, но логируем
        
        return True  # Крипта 24/7
    
    # ==================== COIN SELECTION ====================
    
    async def select_best_coins(self, top_n: int = 5) -> List[str]:
        """
        Выбор лучших монет для торговли
        
        Критерии:
        - Высокая ликвидность (>$50M оборот)
        - Умеренная волатильность (1-4%)
        - Чёткий тренд
        """
        
        try:
            session = await self._get_session()
            url = "https://api.bybit.com/v5/market/tickers?category=linear"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return self._default_coins()
                data = await resp.json()
            
            if data.get('retCode') != 0:
                return self._default_coins()
            
            tickers = data.get('result', {}).get('list', [])
            candidates = []
            
            for ticker in tickers:
                symbol = ticker.get('symbol', '')
                
                if not symbol.endswith('USDT'):
                    continue
                
                # Фильтруем стейблы
                skip = ['USDC', 'BUSD', 'TUSD', 'DAI', 'FDUSD']
                if any(s in symbol for s in skip):
                    continue
                
                try:
                    turnover = float(ticker.get('turnover24h', '0'))
                    price_change = abs(float(ticker.get('price24hPcnt', '0'))) * 100
                    
                    # Фильтры
                    if turnover < 50_000_000:  # Минимум $50M оборот
                        continue
                    if price_change < 0.5 or price_change > 8:  # 0.5-8% движение
                        continue
                    
                    base = symbol.replace('USDT', '')
                    our_symbol = f"{base}/USDT"
                    
                    # Скор = ликвидность * умеренная волатильность
                    # Лучше: высокий оборот, волатильность 2-4%
                    vol_score = 1.0 if 2 <= price_change <= 4 else 0.7
                    score = (turnover / 1_000_000_000) * vol_score
                    
                    candidates.append({
                        'symbol': our_symbol,
                        'score': score,
                        'turnover': turnover,
                        'change': price_change
                    })
                    
                except (ValueError, TypeError):
                    continue
            
            # Сортируем по скору
            candidates.sort(key=lambda x: x['score'], reverse=True)
            
            # Берём топ
            result = [c['symbol'] for c in candidates[:top_n]]
            
            # Всегда включаем BTC и ETH
            for coin in ['BTC/USDT', 'ETH/USDT']:
                if coin not in result:
                    result.insert(0, coin)
            
            logger.info(f"[COINS] Selected: {result[:top_n]}")
            return result[:top_n]
            
        except Exception as e:
            logger.error(f"[COINS] Error: {e}")
            return self._default_coins()
    
    def _default_coins(self) -> List[str]:
        """Дефолтный список"""
        return ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']


# ==================== GLOBAL INSTANCE ====================
smart_analyzer = SmartAnalyzer()


async def find_best_setup(balance: float = 0) -> Optional[TradeSetup]:
    """
    Найти лучший торговый сетап
    
    Возвращает только качественные сетапы (A+, A, B)
    """
    # Выбираем монеты
    coins = await smart_analyzer.select_best_coins(top_n=10)
    
    best_setup: Optional[TradeSetup] = None
    
    for symbol in coins:
        try:
            setup = await smart_analyzer.analyze(symbol, balance)
            
            if setup is not None:
                # Берём первый качественный сетап
                if best_setup is None:
                    best_setup = setup
                # Или заменяем на лучший
                elif setup.confidence > best_setup.confidence:
                    best_setup = setup
                    
        except Exception as e:
            logger.error(f"[FIND] Error analyzing {symbol}: {e}")
            continue
    
    await smart_analyzer.close()
    return best_setup


def record_trade_result(pnl: float):
    """Записать результат сделки для статистики"""
    smart_analyzer.state.record_trade(pnl)


def get_trading_state() -> Dict:
    """Получить состояние торговли"""
    state = smart_analyzer.state
    return {
        'consecutive_losses': state.consecutive_losses,
        'daily_trades': state.daily_trades,
        'daily_pnl': state.daily_pnl,
        'is_paused': state.is_paused,
        'pause_until': state.pause_until.isoformat() if state.pause_until else None
    }


# ==================== SIGNAL STATISTICS ====================
_signal_stats = {
    'analyzed': 0,
    'accepted': 0,
    'rejected': 0,
    'bybit_opened': 0,
    'extreme_moves_detected': 0,
    'imbalance_trades': 0,
    'reasons': {
        'low_confidence': 0,
        'low_quality': 0,
        'bad_rr': 0,
        'bad_regime': 0,
        'no_setup': 0,
        'state_blocked': 0,
        'outside_hours': 0,
    }
}


def get_signal_stats() -> Dict:
    """Получить статистику сигналов"""
    return _signal_stats.copy()


def reset_signal_stats():
    """Сброс статистики"""
    global _signal_stats
    _signal_stats = {
        'analyzed': 0,
        'accepted': 0,
        'rejected': 0,
        'bybit_opened': 0,
        'extreme_moves_detected': 0,
        'imbalance_trades': 0,
        'reasons': {
            'low_confidence': 0,
            'low_quality': 0,
            'bad_rr': 0,
            'bad_regime': 0,
            'no_setup': 0,
            'state_blocked': 0,
            'outside_hours': 0,
        }
    }
    # Также сбрасываем состояние торговли
    smart_analyzer.state = TradingState()


def increment_bybit_opened():
    """Инкремент счётчика открытых позиций на Bybit"""
    _signal_stats['bybit_opened'] += 1


def increment_stat(key: str, reason: str = None):
    """Инкремент статистики"""
    if key in _signal_stats:
        _signal_stats[key] += 1
    if reason and reason in _signal_stats['reasons']:
        _signal_stats['reasons'][reason] += 1
