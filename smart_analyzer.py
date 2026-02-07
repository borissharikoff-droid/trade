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
import random
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# Импорты для улучшений
try:
    from liquidity_analyzer import liquidity_analyzer
    LIQUIDITY_ANALYSIS_ENABLED = True
except ImportError:
    LIQUIDITY_ANALYSIS_ENABLED = False
    logger.warning("[SMART] Liquidity analyzer not available")

# News Analyzer для торговли по новостям
try:
    from news_analyzer import (
        news_analyzer, enhance_setup_with_news, get_news_signals,
        get_market_sentiment, should_trade_now, detect_manipulations,
        get_news_trading_opportunities, COINGLASS_THRESHOLDS
    )
    NEWS_ANALYSIS_ENABLED = True
    logger.info("[SMART] News analyzer enabled")
except ImportError:
    NEWS_ANALYSIS_ENABLED = False
    logger.warning("[SMART] News analyzer not available")


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


@dataclass
class OrderBlock:
    """Order Block - зона институционального интереса"""
    price_high: float
    price_low: float
    type: str  # 'bullish', 'bearish'
    strength: float  # 0-1
    index: int
    mitigated: bool = False  # Был ли блок уже протестирован


@dataclass
class FairValueGap:
    """Fair Value Gap - гэп справедливой стоимости"""
    high: float
    low: float
    type: str  # 'bullish', 'bearish'
    index: int
    filled: bool = False  # Был ли гэп заполнен


@dataclass
class MTFAnalysis:
    """Multi-Timeframe Analysis результат"""
    trend_4h: str  # 'BULLISH', 'BEARISH', 'NEUTRAL'
    trend_1h: str
    trend_15m: str
    aligned: bool
    strength: int  # 0-3, сколько TF согласны


# ==================== TRADING STATE ====================
class TradingState:
    """Состояние торговли для защиты капитала v2.0 - ОПТИМИЗИРОВАНО для большего количества сделок"""
    
    def __init__(self):
        self.consecutive_losses = 0
        self.daily_trades = 0
        self.daily_pnl = 0.0
        self.daily_wins = 0
        self.last_trade_time: Optional[datetime] = None
        self.last_reset_date: Optional[str] = None
        self.is_paused = False
        self.pause_until: Optional[datetime] = None
        self.recent_trades: List[float] = []  # Последние 10 сделок для расчёта winrate
        
        # Настройки потока (без бана на убытки - торгуем всегда)
        self.MAX_DAILY_TRADES = 20
        self.MIN_TIME_BETWEEN_TRADES = 5     # минут между сделками
        
        # Адаптивные настройки
        self.ADAPTIVE_DAILY_TRADES = {
            'strong_trend': 25,    # В сильном тренде - больше сделок
            'trend': 20,           # В тренде - стандартно
            'ranging': 15,         # В рейндже - меньше
            'high_volatility': 10  # В волатильности - осторожно
        }
    
    def get_recent_winrate(self) -> float:
        """Получить winrate за последние 10 сделок"""
        if len(self.recent_trades) < 3:
            return 0.7  # Недостаточно данных - возвращаем 70%
        wins = sum(1 for pnl in self.recent_trades if pnl > 0)
        return wins / len(self.recent_trades)
    
    def reset_daily(self):
        """Сброс дневных счётчиков"""
        today = datetime.now(timezone.utc).date().isoformat()
        if self.last_reset_date != today:
            self.daily_trades = 0
            self.daily_pnl = 0.0
            self.daily_wins = 0
            self.last_reset_date = today
            logger.info("[STATE] Daily counters reset")
    
    def record_trade(self, pnl: float):
        """Записать результат сделки"""
        self.daily_trades += 1
        self.daily_pnl += pnl
        self.last_trade_time = datetime.now(timezone.utc)
        
        # Трекинг winrate
        self.recent_trades.append(pnl)
        if len(self.recent_trades) > 10:
            self.recent_trades.pop(0)
        
        if pnl > 0:
            self.daily_wins += 1
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
    
    def get_adaptive_max_trades(self, market_regime: str = 'trend') -> int:
        """Получить адаптивный лимит сделок на основе режима рынка"""
        regime_map = {
            'STRONG_UPTREND': 'strong_trend',
            'STRONG_DOWNTREND': 'strong_trend',
            'UPTREND': 'trend',
            'DOWNTREND': 'trend',
            'RANGING': 'ranging',
            'HIGH_VOLATILITY': 'high_volatility'
        }
        regime_key = regime_map.get(market_regime, 'trend')
        return self.ADAPTIVE_DAILY_TRADES.get(regime_key, self.MAX_DAILY_TRADES)
    
    def can_trade(self, balance: float, market_regime: str = None) -> Tuple[bool, str]:
        """Можно ли открывать новую сделку. Includes loss-streak cooldown & daily drawdown protection."""
        self.reset_daily()
        
        now = datetime.now(timezone.utc)
        
        # Проверка лимита сделок (адаптивный)
        max_trades = self.get_adaptive_max_trades(market_regime) if market_regime else self.MAX_DAILY_TRADES
        if self.daily_trades >= max_trades:
            return False, f"Лимит сделок в день ({self.daily_trades}/{max_trades})"
        
        # === LOSS STREAK COOLDOWN ===
        # After 3+ consecutive losses, add escalating cooldown to prevent tilt trading
        if self.consecutive_losses >= 3 and self.last_trade_time:
            cooldown_minutes = self.MIN_TIME_BETWEEN_TRADES + (self.consecutive_losses - 2) * 10  # +10 min per extra loss
            cooldown_minutes = min(cooldown_minutes, 60)  # Max 1 hour cooldown
            minutes_since_last = (now - self.last_trade_time).total_seconds() / 60
            if minutes_since_last < cooldown_minutes:
                remaining = int(cooldown_minutes - minutes_since_last)
                return False, f"Loss streak cooldown ({self.consecutive_losses} losses, {remaining} мин осталось)"
        
        # === DAILY DRAWDOWN PROTECTION ===
        # If daily PnL < -5% of balance, stop trading for the day
        if balance > 0 and self.daily_pnl < 0:
            drawdown_pct = abs(self.daily_pnl) / balance * 100
            if drawdown_pct >= 5.0:
                return False, f"Daily drawdown limit ({drawdown_pct:.1f}% > 5%)"
        
        # Проверка времени между сделками
        if self.last_trade_time:
            minutes_since_last = (now - self.last_trade_time).total_seconds() / 60
            if minutes_since_last < self.MIN_TIME_BETWEEN_TRADES:
                return False, f"Cooldown ({int(self.MIN_TIME_BETWEEN_TRADES - minutes_since_last)} мин)"
        
        return True, "OK"
    
    def get_position_size_multiplier(self, balance: float) -> float:
        """Get position size multiplier based on recent performance. Reduces size on drawdown/loss streaks."""
        mult = 1.0
        # Loss streak: reduce size
        if self.consecutive_losses >= 2:
            mult *= max(0.5, 1.0 - self.consecutive_losses * 0.1)  # -10% per loss, min 50%
        # Daily drawdown: reduce size
        if balance > 0 and self.daily_pnl < 0:
            dd_pct = abs(self.daily_pnl) / balance * 100
            if dd_pct >= 2.0:
                mult *= max(0.5, 1.0 - dd_pct * 0.1)  # Proportional reduction
        # Win streak: small bonus (max +20%)
        recent_wr = self.get_recent_winrate()
        if recent_wr > 0.7 and len(self.recent_trades) >= 5:
            mult *= min(1.2, 1.0 + (recent_wr - 0.7))
        return round(max(0.3, min(1.5, mult)), 2)


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
        
        # Кэш для momentum данных (заполняется в select_best_coins)
        self._momentum_cache: Dict[str, Dict] = {}
        
        # === АДАПТИВНЫЕ ПОРОГИ КАЧЕСТВА v3.0 (ОПТИМИЗИРОВАНО для большего количества сделок) ===
        # Базовые настройки (используются как fallback)
        self.MIN_QUALITY = SetupQuality.C  # Снижено с B до C
        self.MIN_RISK_REWARD = 1.0         # Снижено с 1.3 до 1.0
        self.MIN_CONFIDENCE = 0.45         # Снижено с 0.50 до 0.45
        
        # АДАПТИВНЫЕ ПОРОГИ по режиму рынка v3.1 (BALANCED: quality over quantity)
        # Key insight: 49% WR with 53 trades = thresholds too loose. Tighten to improve WR.
        # Формат: {режим: (min_quality, min_rr, min_confidence)}
        self.ADAPTIVE_THRESHOLDS = {
            # Сильный тренд - most permissive (trend gives edge)
            MarketRegime.STRONG_UPTREND: {
                'min_quality': SetupQuality.C,   # C-сетапы ОК в сильном тренде
                'min_rr': 1.2,                    # R/R 1:1.2 (raised from 0.8)
                'min_confidence': 0.50            # 50% уверенности (raised from 40%)
            },
            MarketRegime.STRONG_DOWNTREND: {
                'min_quality': SetupQuality.C,
                'min_rr': 1.2,
                'min_confidence': 0.50
            },
            # Обычный тренд - moderate
            MarketRegime.UPTREND: {
                'min_quality': SetupQuality.B,   # B-сетапы минимум (raised from C)
                'min_rr': 1.3,                    # R/R 1:1.3 (raised from 1.0)
                'min_confidence': 0.50            # 50% уверенности (raised from 45%)
            },
            MarketRegime.DOWNTREND: {
                'min_quality': SetupQuality.B,
                'min_rr': 1.3,
                'min_confidence': 0.50
            },
            # Рейндж - tighter (range trades are harder)
            MarketRegime.RANGING: {
                'min_quality': SetupQuality.B,   # B-сетапы минимум (raised from C)
                'min_rr': 1.5,                    # R/R 1:1.5 (raised from 1.0)
                'min_confidence': 0.55            # 55% уверенности (raised from 40%)
            },
            # Высокая волатильность - tight (dangerous)
            MarketRegime.HIGH_VOLATILITY: {
                'min_quality': SetupQuality.B,   # B-сетапы минимум (raised from C)
                'min_rr': 1.5,                    # R/R 1:1.5 (raised from 1.0)
                'min_confidence': 0.55            # 55% уверенности (raised from 40%)
            },
            # Неизвестный режим - cautious
            MarketRegime.UNKNOWN: {
                'min_quality': SetupQuality.B,   # B-сетапы минимум (raised from C)
                'min_rr': 1.3,
                'min_confidence': 0.50
            }
        }
        
        # Legacy: RR_THRESHOLDS для обратной совместимости
        self.RR_THRESHOLDS = {regime: thresholds['min_rr'] 
                             for regime, thresholds in self.ADAPTIVE_THRESHOLDS.items()}
        
        # Торговые сессии (UTC)
        self.LONDON_OPEN = 7
        self.LONDON_CLOSE = 16
        self.NY_OPEN = 13
        self.NY_CLOSE = 21
        
        logger.info("[SMART] Analyzer initialized with adaptive thresholds")
    
    def get_adaptive_thresholds(self, market_regime: MarketRegime) -> dict:
        """
        Получить адаптивные пороги для текущего режима рынка
        
        Returns:
            {
                'min_quality': SetupQuality,
                'min_rr': float,
                'min_confidence': float
            }
        """
        return self.ADAPTIVE_THRESHOLDS.get(market_regime, {
            'min_quality': self.MIN_QUALITY,
            'min_rr': self.MIN_RISK_REWARD,
            'min_confidence': self.MIN_CONFIDENCE
        })
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    # ==================== DATA FETCHING ====================
    
    async def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List:
        """Получить свечи с Binance (с retry и exponential backoff)"""
        binance_symbol = symbol.replace('/', '')
        url = f"https://api.binance.com/api/v3/klines?symbol={binance_symbol}&interval={interval}&limit={limit}"
        delay = 1.0
        for attempt in range(4):  # 1 initial + 3 retries
            try:
                session = await self._get_session()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception as e:
                if attempt == 3:
                    logger.warning(f"[KLINES] Error {symbol} after retries: {e}")
                    return []
                logger.debug(f"[KLINES] Attempt {attempt + 1} failed: {e}, retry in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
        return []
    
    async def get_price(self, symbol: str) -> float:
        """Текущая цена (с retry и exponential backoff)"""
        binance_symbol = symbol.replace('/', '')
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_symbol}"
        delay = 1.0
        for attempt in range(4):
            try:
                session = await self._get_session()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data['price'])
            except Exception as e:
                if attempt == 3:
                    logger.warning(f"[PRICE] Error {symbol} after retries: {e}")
                    return 0.0
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
        return 0.0
    
    # ==================== MULTI-TIMEFRAME ANALYSIS ====================
    
    async def analyze_mtf(self, symbol: str) -> MTFAnalysis:
        """
        Multi-Timeframe Analysis - анализ на нескольких таймфреймах
        
        4H - основной тренд (доминирующий)
        1H - подтверждение тренда
        15M - точка входа
        
        Сигнал только когда все TF согласны
        """
        # Получаем данные для каждого таймфрейма
        klines_4h = await self.get_klines(symbol, '4h', 50)
        klines_1h = await self.get_klines(symbol, '1h', 50)
        klines_15m = await self.get_klines(symbol, '15m', 50)
        
        def get_trend(klines) -> str:
            """Определить тренд по EMA и структуре"""
            if not klines or len(klines) < 20:
                return 'NEUTRAL'
            
            closes = [float(k[4]) for k in klines]
            
            # EMA 8 и EMA 21
            ema_8 = self.calculate_ema(closes, 8)
            ema_21 = self.calculate_ema(closes, 21)
            
            if not ema_8 or not ema_21:
                return 'NEUTRAL'
            
            current_price = closes[-1]
            ema_8_val = ema_8[-1]
            ema_21_val = ema_21[-1]
            
            # Bullish: цена > EMA8 > EMA21
            if current_price > ema_8_val > ema_21_val:
                # Проверяем силу тренда
                trend_strength = (current_price - ema_21_val) / ema_21_val * 100
                if trend_strength > 0.5:
                    return 'BULLISH'
            
            # Bearish: цена < EMA8 < EMA21
            if current_price < ema_8_val < ema_21_val:
                trend_strength = (ema_21_val - current_price) / ema_21_val * 100
                if trend_strength > 0.5:
                    return 'BEARISH'
            
            return 'NEUTRAL'
        
        trend_4h = get_trend(klines_4h)
        trend_1h = get_trend(klines_1h)
        trend_15m = get_trend(klines_15m)
        
        # Считаем согласованность
        trends = [trend_4h, trend_1h, trend_15m]
        bullish_count = trends.count('BULLISH')
        bearish_count = trends.count('BEARISH')
        
        # Все согласны?
        aligned = (bullish_count == 3) or (bearish_count == 3)
        
        # Сила - сколько TF в одном направлении
        strength = max(bullish_count, bearish_count)
        
        # Частичное выравнивание: 2 из 3 таймфреймов согласны
        partially_aligned = strength >= 2
        
        result = MTFAnalysis(
            trend_4h=trend_4h,
            trend_1h=trend_1h,
            trend_15m=trend_15m,
            aligned=aligned,
            strength=strength
        )
        
        # Добавляем атрибут частичного выравнивания
        result.partially_aligned = partially_aligned
        
        if aligned:
            direction = 'BULLISH' if bullish_count == 3 else 'BEARISH'
            logger.info(f"[MTF] {symbol}: All timeframes aligned {direction}")
        else:
            logger.info(f"[MTF] {symbol}: 4H={trend_4h}, 1H={trend_1h}, 15M={trend_15m} (not aligned)")
        
        return result
    
    # ==================== SMART MONEY CONCEPTS ====================
    
    def find_order_blocks(self, opens: List[float], highs: List[float], 
                          lows: List[float], closes: List[float],
                          min_impulse_percent: float = 0.5) -> List[OrderBlock]:
        """
        Найти Order Blocks - зоны институционального интереса
        
        Bullish OB: последняя медвежья свеча перед сильным импульсом вверх
        Bearish OB: последняя бычья свеча перед сильным импульсом вниз
        
        Args:
            min_impulse_percent: минимальный размер импульса в % для подтверждения OB
        """
        order_blocks = []
        n = len(closes)
        
        if n < 10:
            return order_blocks
        
        for i in range(3, n - 2):
            # Проверяем импульс после свечи i
            # Импульс = движение за следующие 2-3 свечи
            impulse_high = max(highs[i+1:min(i+4, n)])
            impulse_low = min(lows[i+1:min(i+4, n)])
            
            current_close = closes[i]
            current_open = opens[i]
            is_bearish_candle = current_close < current_open
            is_bullish_candle = current_close > current_open
            
            # BULLISH ORDER BLOCK
            # Медвежья свеча, после которой цена сильно выросла
            if is_bearish_candle:
                impulse_up = (impulse_high - highs[i]) / highs[i] * 100
                if impulse_up >= min_impulse_percent:
                    # Это Bullish OB
                    ob = OrderBlock(
                        price_high=highs[i],
                        price_low=lows[i],
                        type='bullish',
                        strength=min(1.0, impulse_up / 2),  # Нормализуем силу
                        index=i,
                        mitigated=False
                    )
                    # Проверяем, был ли OB уже протестирован
                    for j in range(i + 4, n):
                        if lows[j] <= ob.price_high:
                            ob.mitigated = True
                            break
                    order_blocks.append(ob)
            
            # BEARISH ORDER BLOCK
            # Бычья свеча, после которой цена сильно упала
            if is_bullish_candle:
                impulse_down = (lows[i] - impulse_low) / lows[i] * 100
                if impulse_down >= min_impulse_percent:
                    # Это Bearish OB
                    ob = OrderBlock(
                        price_high=highs[i],
                        price_low=lows[i],
                        type='bearish',
                        strength=min(1.0, impulse_down / 2),
                        index=i,
                        mitigated=False
                    )
                    # Проверяем, был ли OB уже протестирован
                    for j in range(i + 4, n):
                        if highs[j] >= ob.price_low:
                            ob.mitigated = True
                            break
                    order_blocks.append(ob)
        
        # Возвращаем только непротестированные OB (свежие)
        fresh_obs = [ob for ob in order_blocks if not ob.mitigated]
        logger.info(f"[SMC] Found {len(fresh_obs)} fresh Order Blocks (total: {len(order_blocks)})")
        
        return fresh_obs[-5:]  # Последние 5 свежих OB
    
    def find_fair_value_gaps(self, highs: List[float], lows: List[float],
                             min_gap_percent: float = 0.1) -> List[FairValueGap]:
        """
        Найти Fair Value Gaps (FVG) - гэпы справедливой стоимости
        
        Bullish FVG: low[i] > high[i-2] (гэп вверх)
        Bearish FVG: high[i] < low[i-2] (гэп вниз)
        
        Цена стремится заполнить эти гэпы
        """
        fvgs = []
        n = len(highs)
        
        if n < 5:
            return fvgs
        
        for i in range(2, n):
            # BULLISH FVG (гэп вверх)
            # Между high свечи i-2 и low свечи i есть пробел
            gap_up = lows[i] - highs[i-2]
            if gap_up > 0:
                gap_percent = gap_up / highs[i-2] * 100
                if gap_percent >= min_gap_percent:
                    fvg = FairValueGap(
                        high=lows[i],      # Верх гэпа = low текущей свечи
                        low=highs[i-2],    # Низ гэпа = high свечи i-2
                        type='bullish',
                        index=i,
                        filled=False
                    )
                    # Проверяем, был ли гэп заполнен
                    for j in range(i + 1, n):
                        if lows[j] <= fvg.low:
                            fvg.filled = True
                            break
                    fvgs.append(fvg)
            
            # BEARISH FVG (гэп вниз)
            gap_down = lows[i-2] - highs[i]
            if gap_down > 0:
                gap_percent = gap_down / lows[i-2] * 100
                if gap_percent >= min_gap_percent:
                    fvg = FairValueGap(
                        high=lows[i-2],    # Верх гэпа = low свечи i-2
                        low=highs[i],      # Низ гэпа = high текущей свечи
                        type='bearish',
                        index=i,
                        filled=False
                    )
                    # Проверяем, был ли гэп заполнен
                    for j in range(i + 1, n):
                        if highs[j] >= fvg.high:
                            fvg.filled = True
                            break
                    fvgs.append(fvg)
        
        # Возвращаем только незаполненные гэпы
        unfilled = [fvg for fvg in fvgs if not fvg.filled]
        logger.info(f"[SMC] Found {len(unfilled)} unfilled FVGs (total: {len(fvgs)})")
        
        return unfilled[-5:]  # Последние 5 незаполненных
    
    def detect_liquidity_sweep(self, highs: List[float], lows: List[float],
                               swings: List[SwingPoint], 
                               current_price: float) -> Optional[Dict]:
        """
        Обнаружить Liquidity Sweep - сбор ликвидности
        
        Sweep = пробой предыдущего swing high/low с быстрым возвратом
        Это сигнал разворота - "умные деньги" собрали стопы и развернули рынок
        
        Returns:
            {'type': 'bullish'/'bearish', 'swept_level': float, 'strength': float}
        """
        if len(swings) < 3 or len(highs) < 5:
            return None
        
        n = len(highs)
        
        # Ищем sweep за последние 5 свечей
        for i in range(max(0, n - 5), n):
            # BULLISH SWEEP (sweep low + возврат вверх)
            # Свеча пробила swing low, но закрылась выше
            for swing in swings:
                if swing.type in ['LL', 'HL', 'LOW']:
                    # Проверяем, был ли sweep этого уровня
                    if lows[i] < swing.price < current_price:
                        # Low свечи ниже swing, но текущая цена выше
                        # Это потенциальный bullish sweep
                        sweep_depth = (swing.price - lows[i]) / swing.price * 100
                        if sweep_depth > 0.1:  # Минимум 0.1% sweep
                            logger.info(f"[SMC] Bullish liquidity sweep detected at {swing.price:.2f}")
                            return {
                                'type': 'bullish',
                                'swept_level': swing.price,
                                'strength': min(1.0, sweep_depth * 2),
                                'reasoning': f"Sweep ликвидности на {swing.price:.2f}"
                            }
            
            # BEARISH SWEEP (sweep high + возврат вниз)
            for swing in swings:
                if swing.type in ['HH', 'LH', 'HIGH']:
                    if highs[i] > swing.price > current_price:
                        sweep_depth = (highs[i] - swing.price) / swing.price * 100
                        if sweep_depth > 0.1:
                            logger.info(f"[SMC] Bearish liquidity sweep detected at {swing.price:.2f}")
                            return {
                                'type': 'bearish',
                                'swept_level': swing.price,
                                'strength': min(1.0, sweep_depth * 2),
                                'reasoning': f"Sweep ликвидности на {swing.price:.2f}"
                            }
        
        return None
    
    def check_price_at_ob(self, current_price: float, 
                          order_blocks: List[OrderBlock],
                          tolerance: float = 0.003) -> Optional[OrderBlock]:
        """Проверить, находится ли цена у Order Block"""
        for ob in order_blocks:
            if ob.type == 'bullish':
                # Для bullish OB проверяем, что цена около зоны
                if ob.price_low * (1 - tolerance) <= current_price <= ob.price_high * (1 + tolerance):
                    return ob
            else:  # bearish
                if ob.price_low * (1 - tolerance) <= current_price <= ob.price_high * (1 + tolerance):
                    return ob
        return None
    
    def check_price_in_fvg(self, current_price: float,
                           fvgs: List[FairValueGap],
                           tolerance: float = 0.002) -> Optional[FairValueGap]:
        """Проверить, находится ли цена в Fair Value Gap"""
        for fvg in fvgs:
            if fvg.low * (1 - tolerance) <= current_price <= fvg.high * (1 + tolerance):
                return fvg
        return None
    
    # ==================== DIVERGENCE DETECTION ====================
    
    def detect_divergence(self, closes: List[float], 
                          highs: List[float], 
                          lows: List[float],
                          lookback: int = 14) -> Dict:
        """
        Обнаружить RSI дивергенции - сильный сигнал разворота
        
        Regular Bullish Divergence: цена делает LL, RSI делает HL
        Regular Bearish Divergence: цена делает HH, RSI делает LH
        
        Hidden Bullish Divergence: цена делает HL, RSI делает LL (продолжение тренда)
        Hidden Bearish Divergence: цена делает LH, RSI делает HH (продолжение тренда)
        """
        result = {
            'found': False,
            'type': None,  # 'regular_bullish', 'regular_bearish', 'hidden_bullish', 'hidden_bearish'
            'strength': 0,
            'reasoning': None
        }
        
        if len(closes) < lookback + 10:
            return result
        
        # Рассчитываем RSI
        rsi_values = []
        for i in range(lookback, len(closes)):
            rsi = self._calculate_rsi_at(closes[:i+1], lookback)
            rsi_values.append(rsi)
        
        if len(rsi_values) < 10:
            return result
        
        # Находим локальные минимумы и максимумы цены и RSI за последние 20 свечей
        price_window = closes[-20:]
        rsi_window = rsi_values[-20:]
        low_window = lows[-20:]
        high_window = highs[-20:]
        
        # Ищем два последних минимума цены
        price_lows = []
        rsi_at_lows = []
        for i in range(2, len(price_window) - 2):
            if low_window[i] < low_window[i-1] and low_window[i] < low_window[i-2] and \
               low_window[i] < low_window[i+1] and low_window[i] < low_window[i+2]:
                price_lows.append((i, low_window[i]))
                rsi_at_lows.append((i, rsi_window[i] if i < len(rsi_window) else 50))
        
        # Ищем два последних максимума цены
        price_highs = []
        rsi_at_highs = []
        for i in range(2, len(price_window) - 2):
            if high_window[i] > high_window[i-1] and high_window[i] > high_window[i-2] and \
               high_window[i] > high_window[i+1] and high_window[i] > high_window[i+2]:
                price_highs.append((i, high_window[i]))
                rsi_at_highs.append((i, rsi_window[i] if i < len(rsi_window) else 50))
        
        # REGULAR BULLISH DIVERGENCE
        # Цена: Lower Low, RSI: Higher Low
        if len(price_lows) >= 2 and len(rsi_at_lows) >= 2:
            prev_price_low = price_lows[-2][1]
            curr_price_low = price_lows[-1][1]
            prev_rsi_low = rsi_at_lows[-2][1]
            curr_rsi_low = rsi_at_lows[-1][1]
            
            if curr_price_low < prev_price_low and curr_rsi_low > prev_rsi_low:
                # Bullish divergence!
                strength = (curr_rsi_low - prev_rsi_low) / 10  # Нормализуем
                result = {
                    'found': True,
                    'type': 'regular_bullish',
                    'strength': min(1.0, strength),
                    'reasoning': f"Bullish дивергенция RSI (цена LL, RSI HL)"
                }
                logger.info(f"[DIVERGENCE] Regular Bullish: price LL, RSI HL")
                return result
        
        # REGULAR BEARISH DIVERGENCE
        # Цена: Higher High, RSI: Lower High
        if len(price_highs) >= 2 and len(rsi_at_highs) >= 2:
            prev_price_high = price_highs[-2][1]
            curr_price_high = price_highs[-1][1]
            prev_rsi_high = rsi_at_highs[-2][1]
            curr_rsi_high = rsi_at_highs[-1][1]
            
            if curr_price_high > prev_price_high and curr_rsi_high < prev_rsi_high:
                # Bearish divergence!
                strength = (prev_rsi_high - curr_rsi_high) / 10
                result = {
                    'found': True,
                    'type': 'regular_bearish',
                    'strength': min(1.0, strength),
                    'reasoning': f"Bearish дивергенция RSI (цена HH, RSI LH)"
                }
                logger.info(f"[DIVERGENCE] Regular Bearish: price HH, RSI LH")
                return result
        
        # HIDDEN BULLISH DIVERGENCE (продолжение тренда)
        # Цена: Higher Low, RSI: Lower Low
        if len(price_lows) >= 2 and len(rsi_at_lows) >= 2:
            prev_price_low = price_lows[-2][1]
            curr_price_low = price_lows[-1][1]
            prev_rsi_low = rsi_at_lows[-2][1]
            curr_rsi_low = rsi_at_lows[-1][1]
            
            if curr_price_low > prev_price_low and curr_rsi_low < prev_rsi_low:
                strength = (prev_rsi_low - curr_rsi_low) / 15
                result = {
                    'found': True,
                    'type': 'hidden_bullish',
                    'strength': min(0.8, strength),  # Скрытая дивергенция слабее
                    'reasoning': f"Hidden Bullish дивергенция (продолжение тренда)"
                }
                logger.info(f"[DIVERGENCE] Hidden Bullish: price HL, RSI LL")
                return result
        
        # HIDDEN BEARISH DIVERGENCE
        # Цена: Lower High, RSI: Higher High
        if len(price_highs) >= 2 and len(rsi_at_highs) >= 2:
            prev_price_high = price_highs[-2][1]
            curr_price_high = price_highs[-1][1]
            prev_rsi_high = rsi_at_highs[-2][1]
            curr_rsi_high = rsi_at_highs[-1][1]
            
            if curr_price_high < prev_price_high and curr_rsi_high > prev_rsi_high:
                strength = (curr_rsi_high - prev_rsi_high) / 15
                result = {
                    'found': True,
                    'type': 'hidden_bearish',
                    'strength': min(0.8, strength),
                    'reasoning': f"Hidden Bearish дивергенция (продолжение тренда)"
                }
                logger.info(f"[DIVERGENCE] Hidden Bearish: price LH, RSI HH")
                return result
        
        return result
    
    def _calculate_rsi_at(self, closes: List[float], period: int = 14) -> float:
        """Рассчитать RSI для заданного периода"""
        if len(closes) < period + 1:
            return 50
        
        gains = []
        losses = []
        
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return 50
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    # ==================== VOLUME SPREAD ANALYSIS ====================
    
    def analyze_vsa(self, opens: List[float], highs: List[float], 
                    lows: List[float], closes: List[float], 
                    volumes: List[float]) -> Dict:
        """
        Volume Spread Analysis - анализ объёма по свечам
        
        Ключевые паттерны:
        - No Demand: узкий спред, низкий объём на росте (медвежий)
        - No Supply: узкий спред, низкий объём на падении (бычий)
        - Stopping Volume: высокий объём с длинной нижней тенью (бычий)
        - Climax: экстремальный объём на экстремуме - разворот
        - Effort vs Result: большой объём, малое движение = разворот
        """
        result = {
            'signal': 'NEUTRAL',
            'pattern': None,
            'strength': 0,
            'reasoning': None
        }
        
        if len(closes) < 20 or len(volumes) < 20:
            return result
        
        # Средние значения за последние 20 свечей
        avg_volume = sum(volumes[-20:]) / 20
        avg_spread = sum(abs(closes[i] - opens[i]) for i in range(-20, 0)) / 20
        
        # Последняя свеча
        curr_open = opens[-1]
        curr_high = highs[-1]
        curr_low = lows[-1]
        curr_close = closes[-1]
        curr_volume = volumes[-1]
        
        curr_spread = abs(curr_close - curr_open)
        curr_range = curr_high - curr_low
        is_bullish = curr_close > curr_open
        is_bearish = curr_close < curr_open
        
        # Размеры теней
        upper_wick = curr_high - max(curr_open, curr_close)
        lower_wick = min(curr_open, curr_close) - curr_low
        
        # Относительные значения
        volume_ratio = curr_volume / avg_volume if avg_volume > 0 else 1
        spread_ratio = curr_spread / avg_spread if avg_spread > 0 else 1
        
        # NO DEMAND (медвежий сигнал)
        # Узкий спред + низкий объём на росте
        if is_bullish and spread_ratio < 0.5 and volume_ratio < 0.7:
            result = {
                'signal': 'SHORT',
                'pattern': 'no_demand',
                'strength': 0.6,
                'reasoning': "VSA: No Demand (слабый рост)"
            }
            logger.info(f"[VSA] No Demand detected")
            return result
        
        # NO SUPPLY (бычий сигнал)
        # Узкий спред + низкий объём на падении
        if is_bearish and spread_ratio < 0.5 and volume_ratio < 0.7:
            result = {
                'signal': 'LONG',
                'pattern': 'no_supply',
                'strength': 0.6,
                'reasoning': "VSA: No Supply (слабое падение)"
            }
            logger.info(f"[VSA] No Supply detected")
            return result
        
        # STOPPING VOLUME (бычий сигнал)
        # Высокий объём + длинная нижняя тень + закрытие в верхней половине
        if volume_ratio > 1.5 and lower_wick > curr_spread and \
           curr_close > (curr_high + curr_low) / 2:
            result = {
                'signal': 'LONG',
                'pattern': 'stopping_volume',
                'strength': 0.8,
                'reasoning': "VSA: Stopping Volume (покупатели)"
            }
            logger.info(f"[VSA] Stopping Volume detected")
            return result
        
        # CLIMAX VOLUME (разворот)
        # Экстремальный объём (> 2.5x среднего) часто означает разворот
        if volume_ratio > 2.5:
            if is_bullish:
                # Бычий climax = потенциальный разворот вниз
                result = {
                    'signal': 'SHORT',
                    'pattern': 'buying_climax',
                    'strength': 0.7,
                    'reasoning': "VSA: Buying Climax (истощение покупок)"
                }
                logger.info(f"[VSA] Buying Climax detected")
            else:
                # Медвежий climax = потенциальный разворот вверх
                result = {
                    'signal': 'LONG',
                    'pattern': 'selling_climax',
                    'strength': 0.7,
                    'reasoning': "VSA: Selling Climax (истощение продаж)"
                }
                logger.info(f"[VSA] Selling Climax detected")
            return result
        
        # EFFORT VS RESULT
        # Большой объём + маленькое движение = сопротивление
        if volume_ratio > 1.8 and spread_ratio < 0.4:
            if is_bullish:
                result = {
                    'signal': 'SHORT',
                    'pattern': 'effort_no_result_up',
                    'strength': 0.65,
                    'reasoning': "VSA: Effort>Result (сопротивление росту)"
                }
            else:
                result = {
                    'signal': 'LONG',
                    'pattern': 'effort_no_result_down',
                    'strength': 0.65,
                    'reasoning': "VSA: Effort>Result (поддержка)"
                }
            logger.info(f"[VSA] Effort vs Result detected")
            return result
        
        return result
    
    # ==================== SESSION ANALYSIS ====================
    
    def is_optimal_session(self) -> Tuple[bool, str, int]:
        """
        Проверить оптимальную торговую сессию
        
        London: 07:00-16:00 UTC (лучшая ликвидность для крипты)
        NY: 13:00-22:00 UTC (высокая волатильность)
        Overlap: 13:00-16:00 UTC (максимальная активность)
        Asian: 00:00-07:00 UTC (низкая волатильность, избегаем)
        
        Returns:
            (is_optimal, session_name, bonus_points)
        """
        now = datetime.now(timezone.utc)
        hour = now.hour
        
        # Overlap London + NY (лучшее время)
        if 13 <= hour < 16:
            return True, "London/NY Overlap", 15
        
        # NY Session
        if 13 <= hour < 21:
            return True, "NY Session", 10
        
        # London Session
        if 7 <= hour < 16:
            return True, "London Session", 10
        
        # Early London / Late NY (приемлемо)
        if 6 <= hour < 7 or 21 <= hour < 22:
            return True, "Session Edge", 5
        
        # Asian Session (избегаем для BTC/ETH)
        if 0 <= hour < 6:
            return False, "Asian Session", -10
        
        # Late night (избегаем)
        return False, "Off-hours", -15
    
    # ==================== CONFIRMATION CANDLE ====================
    
    async def check_confirmation_candle(self, symbol: str, direction: str, 
                                        entry_price: float, 
                                        timeout_minutes: int = 15) -> Dict:
        """
        Ждать подтверждающую свечу перед входом
        
        LONG: ждём закрытия свечи выше entry_price
        SHORT: ждём закрытия свечи ниже entry_price
        
        Это фильтрует ложные пробои
        """
        result = {
            'confirmed': False,
            'candle_close': None,
            'waited_minutes': 0,
            'reasoning': None
        }
        
        start_time = datetime.now()
        check_interval = 60  # Проверяем каждую минуту
        
        while True:
            elapsed = (datetime.now() - start_time).total_seconds() / 60
            
            if elapsed >= timeout_minutes:
                result['reasoning'] = f"⏰ Timeout {timeout_minutes}min без подтверждения"
                logger.info(f"[CONFIRM] Timeout waiting for confirmation candle")
                return result
            
            # Получаем последнюю закрытую свечу (15m)
            klines = await self.get_klines(symbol, '15m', 2)
            if not klines or len(klines) < 2:
                await asyncio.sleep(check_interval)
                continue
            
            # Предпоследняя свеча (последняя закрытая)
            last_closed = klines[-2]
            close_price = float(last_closed[4])
            
            result['candle_close'] = close_price
            result['waited_minutes'] = elapsed
            
            if direction == 'LONG' and close_price > entry_price:
                result['confirmed'] = True
                result['reasoning'] = f"✅ Подтверждение: свеча закрылась выше {entry_price:.2f}"
                logger.info(f"[CONFIRM] LONG confirmed: close {close_price:.2f} > entry {entry_price:.2f}")
                return result
            
            elif direction == 'SHORT' and close_price < entry_price:
                result['confirmed'] = True
                result['reasoning'] = f"✅ Подтверждение: свеча закрылась ниже {entry_price:.2f}"
                logger.info(f"[CONFIRM] SHORT confirmed: close {close_price:.2f} < entry {entry_price:.2f}")
                return result
            
            # Ждём следующую проверку
            await asyncio.sleep(check_interval)
        
        return result
    
    def get_quick_confirmation(self, closes: List[float], direction: str) -> Dict:
        """
        Быстрая проверка подтверждения на основе последних свечей
        (без ожидания, для синхронного использования)
        """
        if len(closes) < 3:
            return {'confirmed': False, 'strength': 0}
        
        # Проверяем последние 3 свечи
        last_3 = closes[-3:]
        
        if direction == 'LONG':
            # Подтверждение = последовательный рост
            if last_3[2] > last_3[1] > last_3[0]:
                return {
                    'confirmed': True, 
                    'strength': 0.8,
                    'reasoning': "3 растущие свечи подряд"
                }
            elif last_3[2] > last_3[1]:
                return {
                    'confirmed': True, 
                    'strength': 0.5,
                    'reasoning': "Последняя свеча растущая"
                }
        
        elif direction == 'SHORT':
            # Подтверждение = последовательное падение
            if last_3[2] < last_3[1] < last_3[0]:
                return {
                    'confirmed': True, 
                    'strength': 0.8,
                    'reasoning': "3 падающие свечи подряд"
                }
            elif last_3[2] < last_3[1]:
                return {
                    'confirmed': True, 
                    'strength': 0.5,
                    'reasoning': "Последняя свеча падающая"
                }
        
        return {'confirmed': False, 'strength': 0}
    
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
        
        # Высокая волатильность - не торгуем (повышен порог с 3% до 4%)
        if atr_percent > 4.0:
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
        
        # === ОПТИМИЗИРОВАНО: Более мягкие условия для определения тренда ===
        # Это позволит чаще находить сетапы
        
        # Сильный тренд: структура важнее чем price change
        if bullish_structure >= 5 and price_change_24h > 1.0:
            return MarketRegime.STRONG_UPTREND
        elif bullish_structure >= 4 and price_change_24h > 0.3:
            return MarketRegime.STRONG_UPTREND  # Добавлено: 4 свинга + небольшое движение = сильный тренд
        elif bullish_structure >= 3 and price_change_24h > 0.1:  # Снижено с 0.5% до 0.1%
            return MarketRegime.UPTREND
        elif bullish_structure >= 2 and price_change_24h > 0:  # Добавлено: 2 свинга + любой рост = тренд
            return MarketRegime.UPTREND
        elif bearish_structure >= 5 and price_change_24h < -1.0:
            return MarketRegime.STRONG_DOWNTREND
        elif bearish_structure >= 4 and price_change_24h < -0.3:
            return MarketRegime.STRONG_DOWNTREND
        elif bearish_structure >= 3 and price_change_24h < -0.1:  # Снижено с -0.5% до -0.1%
            return MarketRegime.DOWNTREND
        elif bearish_structure >= 2 and price_change_24h < 0:  # Добавлено: 2 свинга + любое падение = тренд
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
        
        # === ЭКСТРЕМАЛЬНОЕ ПАДЕНИЕ (перепроданность) - пороги сбалансированы ===
        if change_15m < -1.2 or change_10m < -0.9 or change_5m < -0.6:
            result['extreme'] = True
            result['type'] = 'OVERSOLD'
            result['signal'] = 'LONG'  # Покупаем на перепроданности
            
            # Сила сигнала
            strength = 0
            if change_15m < -2:
                strength += 3
                result['reasoning'].append(f"🔥 Обвал -{abs(change_15m):.1f}% за 15 мин")
            elif change_15m < -1.5:
                strength += 2
                result['reasoning'].append(f"📉 Сильное падение -{abs(change_15m):.1f}% за 15 мин")
            elif change_15m < -1.2:
                strength += 1
                result['reasoning'].append(f"📉 Падение -{abs(change_15m):.1f}% за 15 мин")
            
            if change_5m < -0.6:
                strength += 1
                result['reasoning'].append(f"⚡ Резкое движение -{abs(change_5m):.1f}% за 5 мин")
            
            # Объём подтверждает
            if volume_spike > 1.3:
                strength += 2
                result['reasoning'].append(f"📊 Всплеск объёма x{volume_spike:.1f}")
            elif volume_spike > 1.1:
                strength += 1
            
            result['strength'] = min(5, strength)
            
        # === ЭКСТРЕМАЛЬНЫЙ РОСТ (перекупленность) - пороги сбалансированы ===
        elif change_15m > 1.2 or change_10m > 0.9 or change_5m > 0.6:
            result['extreme'] = True
            result['type'] = 'OVERBOUGHT'
            result['signal'] = 'SHORT'  # Продаём на перекупленности
            
            # Сила сигнала
            strength = 0
            if change_15m > 2:
                strength += 3
                result['reasoning'].append(f"🚀 Памп +{change_15m:.1f}% за 15 мин")
            elif change_15m > 1.5:
                strength += 2
                result['reasoning'].append(f"📈 Сильный рост +{change_15m:.1f}% за 15 мин")
            elif change_15m > 1.2:
                strength += 1
                result['reasoning'].append(f"📈 Рост +{change_15m:.1f}% за 15 мин")
            
            if change_5m > 0.6:
                strength += 1
                result['reasoning'].append(f"⚡ Резкое движение +{change_5m:.1f}% за 5 мин")
            
            # Объём подтверждает
            if volume_spike > 1.3:
                strength += 2
                result['reasoning'].append(f"📊 Всплеск объёма x{volume_spike:.1f}")
            elif volume_spike > 1.1:
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
        
        # Экстремальные значения - сбалансированные пороги
        if k < 20:
            result['signal'] = 'LONG'
            result['extreme'] = True
            result['strength'] = min(3, int((20 - k) / 5) + 1)
        elif k > 80:
            result['signal'] = 'SHORT'
            result['extreme'] = True
            result['strength'] = min(3, int((k - 80) / 5) + 1)
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
                    
                    # Дисбаланс стакана - сбалансированные пороги
                    if imbalance > 25:  # Много покупателей
                        result['signal'] = 'LONG'
                        result['strength'] = min(3, int(imbalance / 12))
                        result['reasoning'] = f"📗 Стакан: покупатели {bid_ratio:.0%} (дисбаланс +{imbalance:.0f}%)"
                    elif imbalance < -25:  # Много продавцов
                        result['signal'] = 'SHORT'
                        result['strength'] = min(3, int(abs(imbalance) / 12))
                        result['reasoning'] = f"📕 Стакан: продавцы {1-bid_ratio:.0%} (дисбаланс {imbalance:.0f}%)"
                    elif imbalance > 15:
                        result['signal'] = 'LONG'
                        result['strength'] = 1
                        result['reasoning'] = f"📗 Стакан: больше покупателей ({imbalance:.0f}%)"
                    elif imbalance < -10:
                        result['signal'] = 'SHORT'
                        result['strength'] = 1
                        result['reasoning'] = f"📕 Стакан: больше продавцов ({imbalance:.0f}%)"
                    
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
        
        # Экстремальные значения - сниженные пороги
        if percent_b < 0.05:  # Ниже или у нижней полосы
            result['signal'] = 'LONG'
            result['extreme'] = True
            result['reasoning'] = f"📉 Цена НИЖЕ Bollinger ({percent_b:.0%})"
        elif percent_b > 0.95:  # Выше или у верхней полосы
            result['signal'] = 'SHORT'
            result['extreme'] = True
            result['reasoning'] = f"📈 Цена ВЫШЕ Bollinger ({percent_b:.0%})"
        elif percent_b < 0.2:
            result['signal'] = 'LONG'
            result['reasoning'] = f"📉 Цена у нижней Bollinger ({percent_b:.0%})"
        elif percent_b > 0.8:
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
                               has_divergence: bool = False,
                               # === NEW HIGH WINRATE PARAMETERS ===
                               mtf_aligned: bool = False,
                               mtf_strength: int = 0,
                               at_order_block: bool = False,
                               in_fvg_zone: bool = False,
                               liquidity_swept: bool = False,
                               vsa_confirms: bool = False,
                               session_bonus: int = 0,
                               confirmation_candle: bool = False) -> Tuple[SetupQuality, float]:
        """
        Оценка качества сетапа v2.0 - HIGH WINRATE EDITION
        
        Новые факторы для высокого винрейта:
        - MTF alignment (все TF согласны): +25
        - Order Block confluence: +15
        - Fair Value Gap zone: +10
        - Liquidity sweep: +20
        - RSI/MACD divergence: +15
        - VSA confirmation: +10
        - Optimal session: +10
        - Confirmation candle: +15
        
        A+ сетап (берём всегда): score >= 80
        A сетап: score >= 65
        B сетап: score >= 50
        C/D - пропускаем
        """
        score = 0
        max_score = 150  # Увеличен для новых факторов
        
        # === ДИСБАЛАНС БОНУС ===
        signal_count = bullish_signals if direction == "LONG" else bearish_signals
        opposite_count = bearish_signals if direction == "LONG" else bullish_signals
        net_imbalance = signal_count - opposite_count
        
        if net_imbalance >= 6:
            score += 20
            logger.info(f"[QUALITY] Strong imbalance bonus +20 (net={net_imbalance})")
        elif net_imbalance >= 4:
            score += 12
        elif net_imbalance >= 2:
            score += 6
        elif net_imbalance < 0:
            score -= 15
            logger.info(f"[QUALITY] Imbalance AGAINST us! penalty -15 (net={net_imbalance})")
        
        # === NEW: MULTI-TIMEFRAME ALIGNMENT (критически важно!) ===
        if mtf_aligned:
            score += 25
            logger.info(f"[QUALITY] MTF aligned bonus +25")
        elif mtf_strength >= 2:
            score += 10  # 2 из 3 TF согласны
        elif mtf_strength == 1:
            score -= 5  # Только 1 TF
        
        # === NEW: SMART MONEY CONCEPTS ===
        if liquidity_swept:
            score += 20  # Сильнейший сигнал разворота
            logger.info(f"[QUALITY] Liquidity sweep bonus +20")
        
        if at_order_block:
            score += 15
            logger.info(f"[QUALITY] Order Block bonus +15")
        
        if in_fvg_zone:
            score += 10
            logger.info(f"[QUALITY] FVG zone bonus +10")
        
        # === NEW: DIVERGENCE (сильный разворотный сигнал) ===
        if has_divergence:
            score += 15
            logger.info(f"[QUALITY] Divergence bonus +15")
        
        # === NEW: VSA CONFIRMATION ===
        if vsa_confirms:
            score += 10
            logger.info(f"[QUALITY] VSA confirms bonus +10")
        
        # === NEW: SESSION BONUS ===
        score += session_bonus
        if session_bonus > 0:
            logger.info(f"[QUALITY] Session bonus +{session_bonus}")
        elif session_bonus < 0:
            logger.info(f"[QUALITY] Session penalty {session_bonus}")
        
        # === NEW: CONFIRMATION CANDLE ===
        if confirmation_candle:
            score += 15
            logger.info(f"[QUALITY] Confirmation candle bonus +15")
        
        # Экстремальное движение бонус
        if has_extreme_move:
            score += 8
        
        # 1. Режим рынка (25 баллов)
        if direction == "LONG":
            if market_regime == MarketRegime.STRONG_UPTREND:
                score += 25
            elif market_regime == MarketRegime.UPTREND:
                score += 20
            elif market_regime == MarketRegime.RANGING:
                score += 8
            elif market_regime in [MarketRegime.DOWNTREND, MarketRegime.STRONG_DOWNTREND]:
                if net_imbalance >= 5 or liquidity_swept or has_divergence:
                    score -= 5  # Меньший штраф если есть разворотные сигналы
                else:
                    score -= 20
        else:  # SHORT
            if market_regime == MarketRegime.STRONG_DOWNTREND:
                score += 25
            elif market_regime == MarketRegime.DOWNTREND:
                score += 20
            elif market_regime == MarketRegime.RANGING:
                score += 8
            elif market_regime in [MarketRegime.UPTREND, MarketRegime.STRONG_UPTREND]:
                if net_imbalance >= 5 or liquidity_swept or has_divergence:
                    score -= 5
                else:
                    score -= 20
        
        # Высокая волатильность - не торгуем
        if market_regime == MarketRegime.HIGH_VOLATILITY:
            return SetupQuality.D, 0.0
        
        # 2. Ключевой уровень (15 баллов)
        if at_key_level:
            score += 15
        
        # 3. Свечной паттерн (15 баллов)
        if pattern_confirmation:
            score += 15
        
        # 4. Объём (10 баллов или -10 штраф за низкий объём)
        if volume_confirmation:
            score += 10
        else:
            score -= 10  # Штраф за низкий объём
            logger.info(f"[QUALITY] Low volume penalty -10")
        
        # 5. Моментум (10 баллов)
        if momentum_aligned:
            score += 10
        
        # 6. Risk/Reward бонус
        if risk_reward >= 3:
            score += 10
        elif risk_reward >= 2.5:
            score += 5
        elif risk_reward >= 2:
            score += 2
        elif risk_reward < 1.5:
            score -= 15  # Строже для низкого R/R
        
        # === WINRATE BOOST: Hard reject weak setups ===
        # If no volume AND no MTF alignment AND no key level -> too weak
        if not volume_confirmation and not mtf_aligned and not at_key_level:
            score -= 15
            logger.info(f"[QUALITY] Weak setup penalty -15 (no volume, MTF, level)")
        
        # If counter-trend without strong reversal signals -> penalty
        counter_trend = (direction == "LONG" and market_regime in [MarketRegime.DOWNTREND, MarketRegime.STRONG_DOWNTREND]) or \
                        (direction == "SHORT" and market_regime in [MarketRegime.UPTREND, MarketRegime.STRONG_UPTREND])
        if counter_trend and not liquidity_swept and not has_divergence:
            score -= 20
            logger.info(f"[QUALITY] Counter-trend without reversal signal penalty -20")
        
        # Определяем качество и confidence - УЛУЧШЕННАЯ формула
        # A_PLUS (80+) -> 78-95%
        # A (65-79) -> 68-77%
        # B (50-64) -> 58-67%
        # C (35-49) -> 45-57%
        # D (<35) -> 30-44%
        if score >= 80:
            quality = SetupQuality.A_PLUS
            confidence = min(0.95, 0.78 + (score - 80) * 0.005)
        elif score >= 65:
            quality = SetupQuality.A
            confidence = 0.68 + (score - 65) * 0.006
        elif score >= 50:
            quality = SetupQuality.B
            confidence = 0.58 + (score - 50) * 0.006
        elif score >= 35:
            quality = SetupQuality.C
            confidence = 0.45 + (score - 35) * 0.008
        else:
            quality = SetupQuality.D
            confidence = max(0.30, 0.30 + score * 0.004)
        
        logger.info(f"[QUALITY] Score={score}, Quality={quality.name}, Confidence={confidence:.0%}, MTF={mtf_aligned}, SMC={at_order_block or in_fvg_zone or liquidity_swept}")
        
        return quality, confidence
    
    def calculate_dynamic_levels(self, 
                                  entry: float,
                                  direction: str,
                                  atr: float,
                                  key_levels: List[KeyLevel],
                                  swings: List[SwingPoint],
                                  market_regime: MarketRegime = None) -> Dict:
        """
        Расчёт динамических TP/SL на основе структуры рынка
        
        SL: За ближайшим свингом + буфер (с ограничением макс. расстояния)
        TP1: До ближайшего уровня сопротивления (50% позиции)
        TP2: До следующего уровня (30% позиции)
        TP3: Трейлинг или далёкий уровень (20% позиции)
        
        В сильных трендах используем ATR-based уровни для лучшего R/R
        """
        
        # Буфер = 1.0 ATR (увеличен для меньшего количества выбитых стопов)
        buffer = atr * 1.0
        
        # В сильных трендах используем ATR-based уровни (гарантирует хороший R/R)
        is_strong_trend = market_regime in [MarketRegime.STRONG_UPTREND, MarketRegime.STRONG_DOWNTREND]
        
        if is_strong_trend:
            # ATR-based уровни для сильных трендов (расширены для меньшего количества выбитых стопов)
            if direction == "LONG":
                sl = entry - atr * 2.5      # SL: 2.5 ATR (расширен с 1.5)
                tp1 = entry + atr * 3.5     # TP1: 3.5 ATR (R/R = 1.4)
                tp2 = entry + atr * 5.0     # TP2: 5 ATR
                tp3 = entry + atr * 7.0     # TP3: 7 ATR
            else:  # SHORT
                sl = entry + atr * 2.5      # SL: 2.5 ATR (расширен с 1.5)
                tp1 = entry - atr * 3.5     # TP1: 3.5 ATR (R/R = 1.4)
                tp2 = entry - atr * 5.0     # TP2: 5 ATR
                tp3 = entry - atr * 7.0     # TP3: 7 ATR
            
            logger.info(f"[LEVELS] Using ATR-based levels for {market_regime.value}")
        else:
            # Стандартная логика на основе структуры
            if direction == "LONG":
                # SL: под последним swing low
                recent_lows = [s.price for s in swings if s.type in ['HL', 'LL', 'LOW']][-3:]
                if recent_lows:
                    sl = min(recent_lows) - buffer
                else:
                    sl = entry - atr * 2.0  # Fallback SL расширен с 1.5
                
                # TP: ближайшие уровни сопротивления
                resistances = sorted([l.price for l in key_levels 
                                      if l.type == 'resistance' and l.price > entry])
                
                if len(resistances) >= 3:
                    tp1 = resistances[0]
                    tp2 = resistances[1]
                    tp3 = resistances[2]
                elif len(resistances) >= 1:
                    tp1 = resistances[0]
                    tp2 = entry + atr * 5.0   # Увеличено с 2.5
                    tp3 = entry + atr * 7.0   # Увеличено с 4
                else:
                    tp1 = entry + atr * 3.5   # Увеличено с 1.5
                    tp2 = entry + atr * 5.0   # Увеличено с 2.5
                    tp3 = entry + atr * 7.0   # Увеличено с 4
                    
            else:  # SHORT
                # SL: над последним swing high
                recent_highs = [s.price for s in swings if s.type in ['HH', 'LH', 'HIGH']][-3:]
                if recent_highs:
                    sl = max(recent_highs) + buffer
                else:
                    sl = entry + atr * 2.0  # Fallback SL расширен с 1.5
                
                # TP: ближайшие уровни поддержки
                supports = sorted([l.price for l in key_levels 
                                  if l.type == 'support' and l.price < entry], reverse=True)
                
                if len(supports) >= 3:
                    tp1 = supports[0]
                    tp2 = supports[1]
                    tp3 = supports[2]
                elif len(supports) >= 1:
                    tp1 = supports[0]
                    tp2 = entry - atr * 5.0   # Увеличено с 2.5
                    tp3 = entry - atr * 7.0   # Увеличено с 4
                else:
                    tp1 = entry - atr * 3.5   # Увеличено с 1.5
                    tp2 = entry - atr * 5.0   # Увеличено с 2.5
                    tp3 = entry - atr * 7.0   # Увеличено с 4
            
            # Ограничение максимального расстояния SL (4.0 ATR) для не-трендовых режимов тоже
            max_sl_distance = atr * 4.0
            sl_distance = abs(entry - sl)
            if sl_distance > max_sl_distance:
                old_sl = sl
                if direction == "LONG":
                    sl = entry - max_sl_distance
                else:
                    sl = entry + max_sl_distance
                logger.info(f"[LEVELS] SL capped: {old_sl:.4f} -> {sl:.4f} (max {max_sl_distance:.4f})")
        
        # === ЗАЩИТА ОТ ОХОТЫ: Добавляем случайные отклонения ===
        # Небольшие отклонения делают уровни менее предсказуемыми для маркет-мейкеров
        sl_offset = random.uniform(-0.2, 0.2) * atr * 0.1
        tp1_offset = random.uniform(-0.15, 0.15) * atr * 0.1
        tp2_offset = random.uniform(-0.1, 0.1) * atr * 0.1
        # TP3 без отклонений (moonbag остаётся на месте)
        
        if direction == "LONG":
            sl = sl + sl_offset
            tp1 = tp1 + tp1_offset
            tp2 = tp2 + tp2_offset
        else:  # SHORT
            sl = sl - sl_offset
            tp1 = tp1 - tp1_offset
            tp2 = tp2 - tp2_offset
        
        logger.info(f"[LEVELS] Anti-hunt offsets applied: SL±{abs(sl_offset):.4f}, TP1±{abs(tp1_offset):.4f}")
        
        # Расчёт R/R
        risk = abs(entry - sl)
        reward = abs(tp1 - entry)
        risk_reward = reward / risk if risk > 0 else 0
        
        # === ГАРАНТИЯ МИНИМАЛЬНОГО R/R ===
        # Если R/R слишком низкий, пересчитываем TP на основе ATR
        MIN_GUARANTEED_RR = 1.2  # Минимум 1:1.2
        
        if risk_reward < MIN_GUARANTEED_RR and risk > 0:
            min_reward = risk * MIN_GUARANTEED_RR
            
            if direction == "LONG":
                new_tp1 = entry + min_reward
                # Проверяем что новый TP1 не дальше TP2
                if new_tp1 < tp2:
                    old_tp1 = tp1
                    tp1 = new_tp1
                    reward = min_reward
                    risk_reward = MIN_GUARANTEED_RR
                    logger.info(f"[LEVELS] TP1 adjusted for min R/R: {old_tp1:.4f} -> {tp1:.4f} (R/R: {risk_reward:.2f})")
            else:  # SHORT
                new_tp1 = entry - min_reward
                # Проверяем что новый TP1 не дальше TP2
                if new_tp1 > tp2:
                    old_tp1 = tp1
                    tp1 = new_tp1
                    reward = min_reward
                    risk_reward = MIN_GUARANTEED_RR
                    logger.info(f"[LEVELS] TP1 adjusted for min R/R: {old_tp1:.4f} -> {tp1:.4f} (R/R: {risk_reward:.2f})")
        
        # Финальная проверка - если R/R всё ещё плохой, используем ATR-based
        if risk_reward < 0.8:
            logger.warning(f"[LEVELS] R/R still low ({risk_reward:.2f}), using ATR fallback")
            if direction == "LONG":
                sl = entry - atr * 2.0
                tp1 = entry + atr * 2.5
                tp2 = entry + atr * 4.0
                tp3 = entry + atr * 6.0
            else:
                sl = entry + atr * 2.0
                tp1 = entry - atr * 2.5
                tp2 = entry - atr * 4.0
                tp3 = entry - atr * 6.0
            
            risk = abs(entry - sl)
            reward = abs(tp1 - entry)
            risk_reward = reward / risk if risk > 0 else 1.25
            logger.info(f"[LEVELS] ATR fallback applied: SL={sl:.4f}, TP1={tp1:.4f}, R/R={risk_reward:.2f}")
        
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
        
        # 2. Проверка торговой сессии - ОТКЛЮЧЕНО (крипта 24/7)
        # Логируем для информации, но не блокируем
        if not self._is_good_trading_time():
            logger.info("[SMART] ⚠️ Off-hours, но торговля разрешена")
        
        # 2.5. Проверка новостного времени - ТОЛЬКО ПРЕДУПРЕЖДЕНИЕ
        is_news, news_event = self._is_news_time()
        if is_news:
            logger.warning(f"[SMART] ⚠️ News event: {news_event} - но торговля разрешена")
        
        # 3. Загружаем данные
        klines_1h = await self.get_klines(symbol, '1h', 100)
        klines_4h = await self.get_klines(symbol, '4h', 50)
        klines_15m = await self.get_klines(symbol, '15m', 50)
        
        if not klines_1h or len(klines_1h) < 15:  # Снижено с 30 до 15 свечей для новых монет
            logger.warning(f"[SMART] Insufficient data for {symbol} (need 15, got {len(klines_1h) if klines_1h else 0})")
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
        
        # Логируем режим рынка, но НЕ блокируем вход (адаптивные пороги всё равно применятся)
        if market_regime in [MarketRegime.HIGH_VOLATILITY, MarketRegime.UNKNOWN]:
            logger.warning(f"[SMART] ⚠️ Volatile/Unknown regime - требования ужесточены, но вход разрешен")
        
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
        
        # === HIGH WINRATE METHODS ===
        
        # 9a. Multi-Timeframe Analysis
        mtf = await self.analyze_mtf(symbol)
        
        # 9b. Smart Money Concepts
        order_blocks = self.find_order_blocks(opens_1h, highs_1h, lows_1h, closes_1h)
        fvgs = self.find_fair_value_gaps(highs_1h, lows_1h)
        liquidity_sweep = self.detect_liquidity_sweep(highs_1h, lows_1h, swings, current_price)
        
        # Проверяем цену у SMC зон
        current_ob = self.check_price_at_ob(current_price, order_blocks)
        current_fvg = self.check_price_in_fvg(current_price, fvgs)
        
        # 9c. Divergence Detection
        divergence = self.detect_divergence(closes_1h, highs_1h, lows_1h)
        
        # 9d. Volume Spread Analysis
        vsa = self.analyze_vsa(opens_1h, highs_1h, lows_1h, closes_1h, volumes_1h)
        
        # 9e. Session Analysis
        is_optimal, session_name, session_bonus = self.is_optimal_session()
        
        # 9f. Quick Confirmation (без ожидания)
        # Будет использовано после определения направления
        
        # Логируем новые методы
        if mtf.aligned:
            logger.info(f"[SMART] ✅ MTF ALIGNED: {mtf.trend_4h}")
        if current_ob:
            logger.info(f"[SMART] 🎯 AT ORDER BLOCK: {current_ob.type}")
        if current_fvg:
            logger.info(f"[SMART] 📊 IN FVG ZONE: {current_fvg.type}")
        if liquidity_sweep:
            logger.info(f"[SMART] 💧 LIQUIDITY SWEEP: {liquidity_sweep['type']}")
        if divergence['found']:
            logger.info(f"[SMART] 📈 DIVERGENCE: {divergence['type']}")
        if vsa['pattern']:
            logger.info(f"[SMART] 📊 VSA: {vsa['pattern']} -> {vsa['signal']}")
        logger.info(f"[SMART] ⏰ Session: {session_name} (bonus={session_bonus})")
        
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
        
        # === СИСТЕМА ВЗВЕШЕННЫХ СИГНАЛОВ v2.0 ===
        # Веса для разных типов сигналов (от самых важных к менее важным)
        SIGNAL_WEIGHTS = {
            'liquidity_sweep': 5,    # Самый сильный сигнал
            'mtf_aligned': 4,        # MTF полностью согласованы
            'mtf_partial': 3,        # MTF частично согласованы
            'order_block': 3,        # Order Block confluence
            'divergence': 3,         # Дивергенция
            'fvg': 2,                # Fair Value Gap
            'vsa': 2,                # Volume Spread Analysis
            'extreme_move': 2,       # Экстремальное движение
            'stochastic': 2,         # Стохастик
            'bollinger': 2,          # Bollinger Bands
            'macd': 2,               # MACD crossover
            'funding': 1,            # Фандинг рейт
            'orderbook': 1,          # Дисбаланс стакана
            'oi_change': 1,          # Open Interest
        }
        
        bullish_signals = 0
        bearish_signals = 0
        
        # === HIGH WINRATE: SMC SIGNALS ===
        
        # 1. Liquidity sweep - САМЫЙ СИЛЬНЫЙ сигнал (вес 5)
        if liquidity_sweep:
            weight = SIGNAL_WEIGHTS['liquidity_sweep']
            if liquidity_sweep['type'] == 'bullish':
                bullish_signals += weight
                reasoning.append(f"[W{weight}] {liquidity_sweep['reasoning']}")
            elif liquidity_sweep['type'] == 'bearish':
                bearish_signals += weight
                reasoning.append(f"[W{weight}] {liquidity_sweep['reasoning']}")
        
        # 2. MTF alignment (вес 4 для полного, 3 для частичного)
        if mtf.aligned:
            weight = SIGNAL_WEIGHTS['mtf_aligned']
            if mtf.trend_4h == 'BULLISH':
                bullish_signals += weight
                reasoning.append(f"[W{weight}] MTF: все таймфреймы бычьи")
            elif mtf.trend_4h == 'BEARISH':
                bearish_signals += weight
                reasoning.append(f"[W{weight}] MTF: все таймфреймы медвежьи")
        elif hasattr(mtf, 'partially_aligned') and mtf.partially_aligned:
            weight = SIGNAL_WEIGHTS['mtf_partial']
            if mtf.trend_4h == 'BULLISH' or (mtf.trend_4h == 'NEUTRAL' and mtf.trend_1h == 'BULLISH'):
                bullish_signals += weight
                reasoning.append(f"[W{weight}] MTF: частичное выравнивание (бычье)")
            elif mtf.trend_4h == 'BEARISH' or (mtf.trend_4h == 'NEUTRAL' and mtf.trend_1h == 'BEARISH'):
                bearish_signals += weight
                reasoning.append(f"[W{weight}] MTF: частичное выравнивание (медвежье)")
        
        # 3. Order Block confluence (вес 3)
        if current_ob:
            weight = SIGNAL_WEIGHTS['order_block']
            if current_ob.type == 'bullish':
                bullish_signals += weight
                reasoning.append(f"[W{weight}] У Bullish Order Block ({current_ob.price_low:.2f}-{current_ob.price_high:.2f})")
            elif current_ob.type == 'bearish':
                bearish_signals += weight
                reasoning.append(f"[W{weight}] У Bearish Order Block ({current_ob.price_low:.2f}-{current_ob.price_high:.2f})")
        
        # 4. Divergence (вес 3)
        if divergence['found']:
            weight = SIGNAL_WEIGHTS['divergence']
            if divergence['type'] in ['regular_bullish', 'hidden_bullish']:
                bullish_signals += weight
                reasoning.append(f"[W{weight}] {divergence['reasoning']}")
            elif divergence['type'] in ['regular_bearish', 'hidden_bearish']:
                bearish_signals += weight
                reasoning.append(f"[W{weight}] {divergence['reasoning']}")
        
        # 5. Fair Value Gap zone (вес 2)
        if current_fvg:
            weight = SIGNAL_WEIGHTS['fvg']
            if current_fvg.type == 'bullish':
                bullish_signals += weight
                reasoning.append(f"[W{weight}] В Bullish FVG зоне")
            elif current_fvg.type == 'bearish':
                bearish_signals += weight
                reasoning.append(f"[W{weight}] В Bearish FVG зоне")
        
        # 6. VSA signals (вес 2)
        if vsa['signal'] == 'LONG':
            weight = SIGNAL_WEIGHTS['vsa']
            bullish_signals += weight
            reasoning.append(f"[W{weight}] {vsa['reasoning']}")
        elif vsa['signal'] == 'SHORT':
            weight = SIGNAL_WEIGHTS['vsa']
            bearish_signals += weight
            reasoning.append(f"[W{weight}] {vsa['reasoning']}")
        
        # === ЛОГИКА СИГНАЛА ===
        
        # === ОПТИМИЗИРОВАНО: Расширен tolerance для уровней с 0.5% до 1.5% ===
        # Это позволит чаще находить сетапы у ключевых уровней
        LEVEL_TOLERANCE = 0.015  # 1.5% (было 0.5%)
        LEVEL_TOLERANCE_CLOSE = 0.008  # 0.8% - близко к уровню (для бонуса)
        
        # Проверка на откат к уровню
        at_support = any(abs(current_price - l.price) / l.price < LEVEL_TOLERANCE 
                        for l in key_levels if l.type == 'support')
        at_resistance = any(abs(current_price - l.price) / l.price < LEVEL_TOLERANCE 
                           for l in key_levels if l.type == 'resistance')
        
        # Близко к уровню (для дополнительного бонуса)
        near_support = any(abs(current_price - l.price) / l.price < LEVEL_TOLERANCE_CLOSE 
                          for l in key_levels if l.type == 'support')
        near_resistance = any(abs(current_price - l.price) / l.price < LEVEL_TOLERANCE_CLOSE 
                             for l in key_levels if l.type == 'resistance')
        
        # Бычий паттерн на уровне (снижен порог с 0.7 до 0.5)
        bullish_pattern = any(p.type == 'bullish' and p.strength >= 0.5 for p in recent_patterns)
        bearish_pattern = any(p.type == 'bearish' and p.strength >= 0.5 for p in recent_patterns)
        
        # Моментум
        bullish_momentum = rsi > 40 and rsi < 70 and current_price > ema_20[-1]
        bearish_momentum = rsi < 60 and rsi > 30 and current_price < ema_20[-1]
        
        # Объём подтверждает
        volume_confirms = volume_data['ratio'] > 1.5 or volume_data['trend'] == 'INCREASING'  # Требование объёма повышено с 1.2
        
        # === ДОПОЛНИТЕЛЬНЫЕ СИГНАЛЫ (с весами) ===
        
        # 7. Экстремальное движение (вес 2 + бонус за силу)
        if extreme_move['extreme'] and extreme_move['strength'] >= 1:
            weight = SIGNAL_WEIGHTS['extreme_move']
            bonus = min(2, extreme_move['strength'])  # Бонус до +2 за силу
            if extreme_move['signal'] == 'LONG':
                bullish_signals += weight + bonus
                for r in extreme_move['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {r}")
            elif extreme_move['signal'] == 'SHORT':
                bearish_signals += weight + bonus
                for r in extreme_move['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {r}")
        
        # 8. Стохастик (вес 2)
        if stochastic['signal'] != 'NEUTRAL':
            weight = SIGNAL_WEIGHTS['stochastic']
            bonus = 1 if stochastic['extreme'] else 0
            if stochastic['signal'] == 'LONG':
                bullish_signals += weight + bonus
                msg = f"Стохастик перепродан" if stochastic['extreme'] else f"Стохастик низкий"
                reasoning.append(f"[W{weight}+{bonus}] {msg} (K={stochastic['k']:.0f})")
            elif stochastic['signal'] == 'SHORT':
                bearish_signals += weight + bonus
                msg = f"Стохастик перекуплен" if stochastic['extreme'] else f"Стохастик высокий"
                reasoning.append(f"[W{weight}+{bonus}] {msg} (K={stochastic['k']:.0f})")
        
        # 9. Bollinger Bands (вес 2)
        if bollinger['signal'] != 'NEUTRAL':
            weight = SIGNAL_WEIGHTS['bollinger']
            bonus = 1 if bollinger['extreme'] else 0
            if bollinger['signal'] == 'LONG':
                bullish_signals += weight + bonus
                if bollinger['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {bollinger['reasoning']}")
            elif bollinger['signal'] == 'SHORT':
                bearish_signals += weight + bonus
                if bollinger['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {bollinger['reasoning']}")
        
        # 10. MACD crossover (вес 2)
        if macd['crossover'] == 'BULLISH':
            weight = SIGNAL_WEIGHTS['macd']
            bullish_signals += weight
            reasoning.append(f"[W{weight}] MACD бычье пересечение")
        elif macd['crossover'] == 'BEARISH':
            weight = SIGNAL_WEIGHTS['macd']
            bearish_signals += weight
            reasoning.append(f"[W{weight}] MACD медвежье пересечение")
        
        # 11. Фандинг рейт (вес 1, но важен для контртренда)
        if funding['signal'] != 'NEUTRAL':
            weight = SIGNAL_WEIGHTS['funding']
            bonus = 1 if funding['extreme'] else 0
            if funding['signal'] == 'LONG':
                bullish_signals += weight + bonus
                if funding['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {funding['reasoning']}")
            elif funding['signal'] == 'SHORT':
                bearish_signals += weight + bonus
                if funding['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {funding['reasoning']}")
        
        # 12. Дисбаланс стакана (вес 1)
        if orderbook['strength'] >= 1:
            weight = SIGNAL_WEIGHTS['orderbook']
            bonus = min(2, orderbook['strength'] - 1)  # Бонус за сильный дисбаланс
            if orderbook['signal'] == 'LONG':
                bullish_signals += weight + bonus
                if orderbook['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {orderbook['reasoning']}")
            elif orderbook['signal'] == 'SHORT':
                bearish_signals += weight + bonus
                if orderbook['reasoning']:
                    reasoning.append(f"[W{weight}+{bonus}] {orderbook['reasoning']}")
        
        # 13. Open Interest (вес 1)
        if oi_change['reasoning']:
            weight = SIGNAL_WEIGHTS['oi_change']
            if oi_change['trend'] == 'INCREASING':
                # Рост OI усиливает текущий тренд
                if bullish_signals > bearish_signals:
                    bullish_signals += weight
                    reasoning.append(f"[W{weight}] {oi_change['reasoning']} (усиливает LONG)")
                else:
                    bearish_signals += weight
                    reasoning.append(f"[W{weight}] {oi_change['reasoning']} (усиливает SHORT)")
        
        # === 14. MOMENTUM BONUS (TOP MOVERS) ===
        # Если монета в топ гейнерах/лузерах - добавляем бонус
        momentum_info = self._momentum_cache.get(symbol)
        if momentum_info:
            momentum_type = momentum_info.get('type')
            momentum_change = momentum_info.get('change', 0)
            
            # Вес зависит от силы движения
            momentum_weight = min(4, int(abs(momentum_change) / 2))  # 2% = 1, 4% = 2, 6% = 3, 8%+ = 4
            
            if momentum_type == 'gainer':
                # Топ гейнер - усиливаем LONG сигналы (momentum продолжение)
                bullish_signals += momentum_weight
                reasoning.insert(0, f"🚀 TOP GAINER +{momentum_change:.1f}%")
                logger.info(f"[MOMENTUM] {symbol} is TOP GAINER (+{momentum_change:.1f}%), LONG bonus +{momentum_weight}")
                
            elif momentum_type == 'loser':
                # Топ лузер - два варианта:
                # 1. Если уже перепродан (RSI < 35) - возможен отскок (LONG)
                # 2. Если ещё падает - продолжение (SHORT)
                if rsi < 35:
                    bullish_signals += momentum_weight
                    reasoning.insert(0, f"📉 TOP LOSER {momentum_change:.1f}% + RSI перепродан → отскок")
                    logger.info(f"[MOMENTUM] {symbol} is TOP LOSER but oversold RSI={rsi:.0f}, LONG bounce +{momentum_weight}")
                else:
                    bearish_signals += momentum_weight
                    reasoning.insert(0, f"📉 TOP LOSER {momentum_change:.1f}%")
                    logger.info(f"[MOMENTUM] {symbol} is TOP LOSER ({momentum_change:.1f}%), SHORT momentum +{momentum_weight}")
                    
            elif momentum_type == 'hot':
                # Hot монета - усиливаем текущее направление
                if momentum_change > 0:
                    bullish_signals += momentum_weight
                    reasoning.insert(0, f"🔥 HOT +{momentum_change:.1f}%")
                else:
                    bearish_signals += momentum_weight
                    reasoning.insert(0, f"🔥 HOT {momentum_change:.1f}%")
                logger.info(f"[MOMENTUM] {symbol} is HOT ({momentum_change:+.1f}%), bonus +{momentum_weight}")
        
        # Логируем взвешенные сигналы
        logger.info(f"[SMART] Weighted Signals: Bullish={bullish_signals}, Bearish={bearish_signals}")
        
        # === КЛАССИЧЕСКАЯ ЛОГИКА (по тренду) ===
        
        # === LONG SETUP === (менее строгие условия)
        if market_regime in [MarketRegime.STRONG_UPTREND, MarketRegime.UPTREND]:
            # Восходящий тренд - вход без строгих условий уровня
            # Разрешить если цена выше EMA20 или RSI < 55 (было ema_50 и rsi < 50)
            if at_support or current_price > ema_20[-1] or rsi < 55:
                direction = "LONG"
                signal_type = SignalType.PULLBACK
                reasoning.insert(0, "Восходящий тренд")
                if at_support:
                    reasoning.insert(1, "У поддержки")
                    bullish_signals += 2
                else:
                    reasoning.insert(1, "По тренду")
                bullish_signals += 3
                
                if bullish_pattern:
                    bullish_signals += 2
                    reasoning.append(f"Паттерн: {[p.name for p in recent_patterns if p.type == 'bullish']}")
        
        elif market_regime == MarketRegime.RANGING:
            # === ОПТИМИЗИРОВАНО: Упрощённые условия для RANGING ===
            # Теперь достаточно ОДНОГО из условий: уровень ИЛИ RSI ИЛИ паттерн
            if at_support or rsi < 45 or bullish_pattern:
                direction = "LONG"
                signal_type = SignalType.TREND_REVERSAL
                reasoning.insert(0, "Рейндж: покупка")
                if at_support:
                    reasoning.insert(1, "У поддержки")
                    bullish_signals += 2
                    if near_support:
                        bullish_signals += 1  # Бонус за близость
                if rsi < 45:
                    reasoning.append(f"RSI перепродан ({rsi:.0f})")
                    bullish_signals += 1
                if bullish_pattern:
                    reasoning.append("Бычий паттерн")
                    bullish_signals += 1
        
        # === SHORT SETUP === (менее строгие условия)
        if direction is None:  # Если ещё не определили направление
            if market_regime in [MarketRegime.STRONG_DOWNTREND, MarketRegime.DOWNTREND]:
                # Нисходящий тренд - вход без строгих условий уровня
                # Разрешить если цена ниже EMA20 или RSI > 45 (было ema_50 и rsi > 50)
                if at_resistance or current_price < ema_20[-1] or rsi > 45:
                    direction = "SHORT"
                    signal_type = SignalType.PULLBACK
                    reasoning.insert(0, "Нисходящий тренд")
                    if at_resistance:
                        reasoning.insert(1, "У сопротивления")
                        bearish_signals += 2
                    else:
                        reasoning.insert(1, "По тренду")
                    bearish_signals += 3
                    
                    if bearish_pattern:
                        bearish_signals += 2
                        reasoning.append(f"Паттерн: {[p.name for p in recent_patterns if p.type == 'bearish']}")
            
            elif market_regime == MarketRegime.RANGING:
                # === ОПТИМИЗИРОВАНО: Упрощённые условия для RANGING ===
                # Теперь достаточно ОДНОГО из условий
                if at_resistance or rsi > 55 or bearish_pattern:
                    direction = "SHORT"
                    signal_type = SignalType.TREND_REVERSAL
                    reasoning.insert(0, "Рейндж: продажа")
                    if at_resistance:
                        reasoning.insert(1, "У сопротивления")
                        bearish_signals += 2
                        if near_resistance:
                            bearish_signals += 1  # Бонус за близость
                    if rsi > 55:
                        reasoning.append(f"RSI перекуплен ({rsi:.0f})")
                        bearish_signals += 1
                    if bearish_pattern:
                        reasoning.append("Медвежий паттерн")
                        bearish_signals += 1
        
        # === ДИСБАЛАНС-ЛОГИКА: Если нет сигнала по тренду, но есть сильный дисбаланс ===
        # Raised from 2 to 4 signals: only trade imbalance on STRONG conviction
        if direction is None and (bullish_signals >= 4 or bearish_signals >= 4):
            # Дисбаланс может создать сигнал - but only with multiple confluences
            if bullish_signals >= 4 and bullish_signals > bearish_signals + 1:
                direction = "LONG"
                signal_type = SignalType.TREND_REVERSAL
                reasoning.insert(0, "🔥 ДИСБАЛАНС: Сильная перепроданность")
                logger.info(f"[SMART] IMBALANCE LONG: {bullish_signals} vs {bearish_signals}")
            elif bearish_signals >= 4 and bearish_signals > bullish_signals + 1:
                direction = "SHORT"
                signal_type = SignalType.TREND_REVERSAL
                reasoning.insert(0, "🔥 ДИСБАЛАНС: Сильная перекупленность")
                logger.info(f"[SMART] IMBALANCE SHORT: {bearish_signals} vs {bullish_signals}")
        
        # FALLBACK УДАЛЁН - генерировал слабые сигналы с 5% winrate
        # Лучше пропустить сигнал, чем взять плохой
        
        # Нет сигнала
        if direction is None:
            logger.info("[SMART] No valid setup found")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['no_setup'] += 1
            return None
        
        # 10. Расчёт уровней (с учётом режима рынка для ATR-based уровней в трендах)
        levels = self.calculate_dynamic_levels(
            entry=current_price,
            direction=direction,
            atr=atr,
            key_levels=key_levels,
            swings=swings,
            market_regime=market_regime
        )
        
        # === ПРОВЕРКА ЛИКВИДНОСТИ: Избегаем зон охоты на стопы ===
        # ОСЛАБЛЕНО: Теперь только предупреждаем, но не блокируем вход
        if LIQUIDITY_ANALYSIS_ENABLED:
            liquidity_zones = liquidity_analyzer.find_liquidity_zones(klines_1h, direction, symbol)
            # Снижено: минимум 0.15% или 0.3 ATR
            min_distance_pct = max(0.15, (atr / current_price) * 30)  # 0.3 ATR в процентах
            should_avoid, reason = liquidity_analyzer.should_avoid_entry(
                current_price, liquidity_zones, atr, min_distance_percent=min_distance_pct
            )
            
            if should_avoid:
                # Теперь только логируем как warning, но НЕ блокируем вход
                logger.warning(f"[SMART] ⚠️ {symbol}: близко к ликвидности - {reason} (вход разрешен)")
                _signal_stats['reasons']['liquidity_zone'] = _signal_stats['reasons'].get('liquidity_zone', 0) + 1
                # return None  # ЗАКОММЕНТИРОВАНО - не блокируем вход
            
            # Проверка order flow на манипуляции
            order_flow = liquidity_analyzer.analyze_order_flow(klines_1h)
            if order_flow.get('manipulation_risk', False):
                logger.warning(f"[SMART] ⚠️ Manipulation risk detected: {order_flow.get('reason', '')}")
                warnings.append(f"⚠️ Риск манипуляции: {order_flow.get('reason', '')}")
        
        # === АДАПТИВНЫЕ ПОРОГИ по режиму рынка ===
        adaptive = self.get_adaptive_thresholds(market_regime)
        min_rr = adaptive['min_rr']
        min_quality = adaptive['min_quality']
        min_confidence = adaptive['min_confidence']
        
        logger.info(f"[SMART] Adaptive thresholds for {market_regime.value}: Quality>={min_quality.name}, R/R>={min_rr}, Conf>={min_confidence:.0%}")
        
        # Проверка минимального R/R
        if levels['risk_reward'] < min_rr:
            logger.info(f"[SMART] Skip: R/R too low ({levels['risk_reward']:.2f} < {min_rr})")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['bad_rr'] += 1
            return None
        
        # 11. Quick confirmation check
        confirmation = self.get_quick_confirmation(closes_1h, direction)
        
        # 12. Оценка качества с HIGH WINRATE параметрами
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
            has_divergence=divergence['found'],
            # === NEW HIGH WINRATE PARAMETERS ===
            mtf_aligned=mtf.aligned,
            mtf_strength=mtf.strength,
            at_order_block=current_ob is not None,
            in_fvg_zone=current_fvg is not None,
            liquidity_swept=liquidity_sweep is not None,
            vsa_confirms=(vsa['signal'] == direction),
            session_bonus=session_bonus,
            confirmation_candle=confirmation.get('confirmed', False)
        )
        
        # Фильтр по АДАПТИВНОМУ качеству (A_PLUS=5, A=4, B=3, C=2, D=1)
        if quality.value < min_quality.value:
            logger.info(f"[SMART] Skip: Quality too low ({quality.name} < {min_quality.name})")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['low_quality'] += 1
            return None
        
        # Фильтр по АДАПТИВНОЙ уверенности
        if confidence < min_confidence:
            logger.info(f"[SMART] Skip: Confidence too low ({confidence:.0%} < {min_confidence:.0%})")
            _signal_stats['rejected'] += 1
            _signal_stats['reasons']['low_confidence'] += 1
            return None
        
        # 13. Добавляем предупреждения
        if volume_data['ratio'] < 0.8:
            warnings.append("⚠️ Низкий объём")
        
        if market_regime == MarketRegime.RANGING:
            warnings.append("⚠️ Рейндж - выше риск ложного пробоя")
        
        if atr_percent > 2:
            warnings.append(f"⚠️ Высокая волатильность ({atr_percent:.1f}%)")
        
        # Новые предупреждения для HIGH WINRATE
        if not mtf.aligned:
            warnings.append(f"⚠️ MTF не согласованы (4H={mtf.trend_4h}, 1H={mtf.trend_1h})")
        
        if session_bonus < 0:
            warnings.append(f"⚠️ Неоптимальная сессия ({session_name})")
        
        if not confirmation.get('confirmed', False):
            warnings.append("⚠️ Нет подтверждающей свечи")
        
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
        
        # Увеличиваем valid_setups - сетап прошёл все фильтры
        _signal_stats['valid_setups'] += 1
        
        # НЕ увеличиваем accepted здесь - это будет сделано только когда сигнал реально отправлен или открыт через автотрейд
        
        # Отслеживаем дисбаланс-сделки
        if extreme_move['extreme']:
            _signal_stats['extreme_moves_detected'] += 1
        if signal_type == SignalType.TREND_REVERSAL and (bullish_signals >= 4 or bearish_signals >= 4):
            _signal_stats['imbalance_trades'] += 1
        
        # === УЛУЧШЕНИЕ СЕТАПА НОВОСТЯМИ ===
        if NEWS_ANALYSIS_ENABLED:
            try:
                coin = symbol.replace('USDT', '').replace('/USDT', '')
                setup = await enhance_setup_with_news(setup, coin)
                logger.info(f"[SMART] Setup enhanced with news data for {coin}")
            except Exception as e:
                logger.warning(f"[SMART] News enhancement failed: {e}")
        
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
    
    def _is_news_time(self) -> tuple:
        """
        Проверка на время важных экономических новостей.
        Избегаем торговли за 30 минут до и после важных событий.
        
        Returns:
            (is_news, event_name) - флаг и название события
        """
        now = datetime.now(timezone.utc)
        hour = now.hour
        weekday = now.weekday()  # 0=Monday, 6=Sunday
        day = now.day
        
        # === FOMC MEETINGS ===
        # FOMC обычно в среду в 18:00-19:00 UTC (раз в 6 недель)
        # Упрощённо: избегаем каждую среду 17:30-20:00 UTC
        if weekday == 2 and 17 <= hour <= 19:  # Wednesday
            logger.info(f"[NEWS] Possible FOMC window (Wed {hour}:00 UTC)")
            return True, "FOMC"
        
        # === NFP (Non-Farm Payrolls) ===
        # Первая пятница месяца в 12:30 UTC
        if weekday == 4 and day <= 7 and 12 <= hour <= 14:  # First Friday
            logger.info(f"[NEWS] NFP window (1st Friday {hour}:00 UTC)")
            return True, "NFP"
        
        # === CPI (Consumer Price Index) ===
        # Обычно ~10-15 число месяца в 12:30 UTC
        if 10 <= day <= 15 and 12 <= hour <= 14:
            logger.info(f"[NEWS] Possible CPI window (day {day}, {hour}:00 UTC)")
            return True, "CPI"
        
        return False, None
    
    # ==================== COIN SELECTION ====================
    
    async def get_top_movers(self, top_n: int = 10) -> Dict[str, List[Dict]]:
        """
        Получить топ монеты по движению с Bybit
        
        Returns:
            {
                'gainers': [{'symbol': 'BTC/USDT', 'change': 5.2, 'turnover': 1000000000, 'price': 95000}, ...],
                'losers': [{'symbol': 'ETH/USDT', 'change': -4.1, ...}, ...],
                'volume': [{'symbol': 'SOL/USDT', 'turnover': 500000000, ...}, ...],
                'hot': [...]  # Комбинация: высокий объём + высокое движение
            }
        """
        result = {
            'gainers': [],
            'losers': [],
            'volume': [],
            'hot': []
        }
        
        try:
            session = await self._get_session()
            url = "https://api.bybit.com/v5/market/tickers?category=linear"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"[TOP_MOVERS] Bybit API error: {resp.status}")
                    return result
                data = await resp.json()
            
            if data.get('retCode') != 0:
                logger.warning(f"[TOP_MOVERS] Bybit retCode: {data.get('retCode')}")
                return result
            
            tickers = data.get('result', {}).get('list', [])
            
            all_coins = []
            
            for ticker in tickers:
                symbol = ticker.get('symbol', '')
                
                if not symbol.endswith('USDT'):
                    continue
                
                # Пропускаем стейблы
                skip = ['USDC', 'BUSD', 'TUSD', 'DAI', 'FDUSD', 'USDP', 'GUSD']
                if any(s in symbol for s in skip):
                    continue
                
                try:
                    turnover = float(ticker.get('turnover24h', '0'))
                    price_change = float(ticker.get('price24hPcnt', '0')) * 100  # В процентах
                    last_price = float(ticker.get('lastPrice', '0'))
                    volume = float(ticker.get('volume24h', '0'))
                    
                    # Минимум $5M оборота для ликвидности
                    if turnover < 5_000_000:
                        continue
                    
                    base = symbol.replace('USDT', '')
                    our_symbol = f"{base}/USDT"
                    
                    coin_info = {
                        'symbol': our_symbol,
                        'base': base,
                        'change': price_change,
                        'turnover': turnover,
                        'volume': volume,
                        'price': last_price,
                        'abs_change': abs(price_change),
                        # Hot score = движение * ликвидность
                        'hot_score': abs(price_change) * (turnover / 100_000_000)
                    }
                    
                    all_coins.append(coin_info)
                    
                except (ValueError, TypeError):
                    continue
            
            # Сортируем по разным критериям
            
            # Топ гейнеры (по росту)
            gainers = sorted([c for c in all_coins if c['change'] > 1.0], 
                           key=lambda x: x['change'], reverse=True)
            result['gainers'] = gainers[:top_n]
            
            # Топ лузеры (по падению)
            losers = sorted([c for c in all_coins if c['change'] < -1.0], 
                          key=lambda x: x['change'])
            result['losers'] = losers[:top_n]
            
            # Топ по объёму
            by_volume = sorted(all_coins, key=lambda x: x['turnover'], reverse=True)
            result['volume'] = by_volume[:top_n]
            
            # Hot - комбинация движения и объёма (самые активные)
            by_hot = sorted([c for c in all_coins if c['abs_change'] > 2.0], 
                          key=lambda x: x['hot_score'], reverse=True)
            result['hot'] = by_hot[:top_n]
            
            logger.info(f"[TOP_MOVERS] Gainers: {len(result['gainers'])}, Losers: {len(result['losers'])}, Hot: {len(result['hot'])}")
            
            if result['gainers']:
                top3_gainers = [f"{c['base']}(+{c['change']:.1f}%)" for c in result['gainers'][:3]]
                logger.info(f"[TOP_MOVERS] 🚀 Top Gainers: {', '.join(top3_gainers)}")
            
            if result['losers']:
                top3_losers = [f"{c['base']}({c['change']:.1f}%)" for c in result['losers'][:3]]
                logger.info(f"[TOP_MOVERS] 📉 Top Losers: {', '.join(top3_losers)}")
            
            if result['hot']:
                top3_hot = [f"{c['base']}({c['change']:+.1f}%)" for c in result['hot'][:3]]
                logger.info(f"[TOP_MOVERS] 🔥 Hot: {', '.join(top3_hot)}")
            
        except Exception as e:
            logger.error(f"[TOP_MOVERS] Error: {e}")
        
        return result
    
    # Категории монет для диверсификации v2.0 - РАСШИРЕННЫЙ СПИСОК
    COIN_CATEGORIES = {
        'major': ['BTC', 'ETH', 'BNB', 'XRP'],  # Топ-4 по капитализации
        'layer1': [
            'SOL', 'AVAX', 'NEAR', 'APT', 'SUI', 'SEI', 'TON', 'INJ', 'ATOM', 'DOT', 'ADA',
            'TRX', 'LINK', 'FTM', 'ALGO', 'HBAR', 'VET', 'EOS', 'XLM', 'ICP', 'FIL',
            'EGLD', 'FLOW', 'KAVA', 'ROSE', 'ZIL', 'ONE', 'QTUM', 'IOTA', 'XTZ', 'NEO'
        ],
        'layer2': [
            'ARB', 'OP', 'STRK', 'ZK', 'MATIC', 'MANTA', 'METIS', 'IMX',
            'LRC', 'CELR', 'BOBA', 'SKL', 'CTSI'
        ],
        'memes': [
            # Классические мемы
            'PEPE', 'DOGE', 'SHIB', 'FLOKI', 'BONK', 'WIF', 'MEME', 'TURBO', 'NEIRO', 'POPCAT',
            'BABYDOGE', 'ELON', 'SATS', 'ORDI', 'RATS', '1000PEPE', 'COQ', 'MYRO', 'TOSHI',
            # Новые хайповые мемы 2025-2026
            'FARTCOIN', 'PNUT', 'VINE', 'TRUMP', 'PENGU', 'SWARMS', 'ELIZA', 'ANIME',
            'AI16Z', 'BRETT', 'MOG', 'GOAT', 'PORK', 'LADYS', 'WOJAK', 'PEPE2', 'BOBO',
            'MOCHI', 'PONKE', 'SLERF', 'BOME', 'CAT', 'DOG', 'PEOPLE', 'LUNC'
        ],
        'defi': [
            'UNI', 'AAVE', 'MKR', 'CRV', 'LDO', 'PENDLE', 'GMX', 'DYDX', 'SNX', 'COMP',
            'SUSHI', '1INCH', 'BAL', 'YFI', 'CAKE', 'RSR', 'ALPHA', 'SPELL', 'LQTY', 'RPL'
        ],
        'ai': [
            'FET', 'RNDR', 'TAO', 'WLD', 'ARKM', 'AGIX', 'OCEAN', 'GRT',
            'PRIME', 'AI', 'NMR', 'CTXC', 'DKA', 'PHB', 'MDT', 'NFP', 'ALI'
        ],
        'gaming': [
            'GALA', 'AXS', 'SAND', 'MANA', 'PIXEL', 'SUPER', 'MAGIC',
            'ENJ', 'ILV', 'ALICE', 'YGG', 'PRIME', 'GMT', 'GST', 'GODS', 'PYR', 'VOXEL', 'HIGH'
        ],
        'infrastructure': [
            'LINK', 'API3', 'BAND', 'TRB', 'UMA', 'REQ', 'RLC', 'STORJ', 'AR', 'MASK',
            'SSV', 'ANKR', 'GLM', 'NKN', 'COTI', 'CTSI', 'OGN', 'SYN'
        ],
        'new': [
            # Новые листинги 2025-2026 (активно добавлять новые!)
            'JUP', 'ENA', 'W', 'ETHFI', 'AEVO', 'PORTAL', 'DYM', 'ALT', 'PYTH',
            'TIA', 'STRK', 'MANTA', 'PIXEL', 'ACE', 'XAI', 'NFP', 'AI', 'SLERF', 'BOME',
            # Свежие хайповые листинги
            'HYPE', 'MOVE', 'ME', 'USUAL', 'VANA', 'PENGU', 'BIO', 'COOKIE', 'AIXBT',
            'CGV', 'SONIC', 'PLUME', 'KAITO', 'ONDO', 'EIGEN', 'ZRO', 'LISTA', 'NOT',
            'DOGS', 'CATI', 'HMSTR', 'BANANA', 'RENDER', 'JTO', 'TNSR', 'KMNO', 'PARCL'
        ],
        'exchange': [
            'BNB', 'OKB', 'CRO', 'KCS', 'GT', 'HT', 'MX', 'FTT', 'LEO'
        ]
    }
    
    async def select_best_coins(self, top_n: int = 5) -> List[str]:
        """
        Выбор лучших монет для торговли v4.0 - TOP MOVERS FIRST
        
        НОВАЯ СТРАТЕГИЯ:
        1. ПРИОРИТЕТ: Топ гейнеры и лузеры с Bybit (там momentum!)
        2. Затем: Hot монеты (высокий объём + движение)
        3. Fallback: Категорийный отбор
        
        ЭТО ДАЁТ:
        - Больше сделок (фокус на активных монетах)
        - Лучший momentum (торгуем то что движется)
        - Выше вероятность продолжения движения
        """
        
        # === ШАГА 1: Получаем топ movers ===
        top_movers = await self.get_top_movers(top_n=15)
        
        priority_coins = []
        seen = set()
        
        # Добавляем топ гейнеры (momentum LONG)
        for coin in top_movers.get('gainers', [])[:7]:
            if coin['symbol'] not in seen:
                priority_coins.append(coin['symbol'])
                seen.add(coin['symbol'])
                # Сохраняем momentum info для анализа
                self._momentum_cache[coin['symbol']] = {
                    'type': 'gainer',
                    'change': coin['change'],
                    'turnover': coin['turnover']
                }
        
        # Добавляем топ лузеры (потенциальный отскок или momentum SHORT)
        for coin in top_movers.get('losers', [])[:7]:
            if coin['symbol'] not in seen:
                priority_coins.append(coin['symbol'])
                seen.add(coin['symbol'])
                self._momentum_cache[coin['symbol']] = {
                    'type': 'loser',
                    'change': coin['change'],
                    'turnover': coin['turnover']
                }
        
        # Добавляем hot монеты
        for coin in top_movers.get('hot', [])[:5]:
            if coin['symbol'] not in seen:
                priority_coins.append(coin['symbol'])
                seen.add(coin['symbol'])
                self._momentum_cache[coin['symbol']] = {
                    'type': 'hot',
                    'change': coin['change'],
                    'turnover': coin['turnover']
                }
        
        logger.info(f"[SELECT] Priority coins from top movers: {len(priority_coins)}")
        
        # Если достаточно приоритетных монет - возвращаем их
        if len(priority_coins) >= top_n:
            logger.info(f"[SELECT] Using {top_n} top movers: {priority_coins[:top_n]}")
            return priority_coins[:top_n]
        
        # === ШАГ 2: Дополняем из общего списка ===
        try:
            session = await self._get_session()
            url = "https://api.bybit.com/v5/market/tickers?category=linear"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return priority_coins if priority_coins else self._default_coins()
                data = await resp.json()
            
            if data.get('retCode') != 0:
                return priority_coins if priority_coins else self._default_coins()
            
            tickers = data.get('result', {}).get('list', [])
            
            # Словарь для хранения данных по монетам
            coin_data_map = {}
            
            # Категории по ликвидности
            high_liquidity = []      # >$100M
            medium_liquidity = []    # $50-100M
            low_liquidity = []       # $20-50M
            
            # Категории по типу монеты
            category_coins = {cat: [] for cat in self.COIN_CATEGORIES.keys()}
            
            for ticker in tickers:
                symbol = ticker.get('symbol', '')
                
                if not symbol.endswith('USDT'):
                    continue
                
                # Фильтруем стейблы
                skip = ['USDC', 'BUSD', 'TUSD', 'DAI', 'FDUSD', 'USDP', 'GUSD']
                if any(s in symbol for s in skip):
                    continue
                
                try:
                    turnover = float(ticker.get('turnover24h', '0'))
                    price_change = abs(float(ticker.get('price24hPcnt', '0'))) * 100
                    last_price = float(ticker.get('lastPrice', '0'))
                    
                    # АГРЕССИВНЫЕ фильтры: 0.1-20% волатильность, $5M+ оборот (больше хайпа!)
                    if price_change < 0.1 or price_change > 20:
                        continue
                    if turnover < 5_000_000:  # Снижено с $10M до $5M для ловли новых монет
                        continue
                    
                    base = symbol.replace('USDT', '')
                    our_symbol = f"{base}/USDT"
                    
                    # Улучшенный скоринг
                    if 2 <= price_change <= 5:
                        vol_score = 1.0  # Идеальная волатильность
                    elif 1 <= price_change < 2 or 5 < price_change <= 8:
                        vol_score = 0.85
                    elif 0.5 <= price_change < 1 or 8 < price_change <= 10:
                        vol_score = 0.7
                    else:
                        vol_score = 0.5
                    
                    liquidity_bonus = min(1.5, turnover / 100_000_000)
                    score = vol_score * liquidity_bonus
                    
                    coin_info = {
                        'symbol': our_symbol,
                        'base': base,
                        'score': score,
                        'turnover': turnover,
                        'change': price_change,
                        'price': last_price
                    }
                    
                    coin_data_map[base] = coin_info
                    
                    # Распределяем по ликвидности
                    if turnover >= 100_000_000:
                        high_liquidity.append(coin_info)
                    elif turnover >= 50_000_000:
                        medium_liquidity.append(coin_info)
                    else:
                        low_liquidity.append(coin_info)
                    
                    # Распределяем по категориям
                    for cat_name, cat_coins in self.COIN_CATEGORIES.items():
                        if base in cat_coins:
                            category_coins[cat_name].append(coin_info)
                            break
                    
                except (ValueError, TypeError):
                    continue
            
            # Сортируем по скору
            high_liquidity.sort(key=lambda x: x['score'], reverse=True)
            medium_liquidity.sort(key=lambda x: x['score'], reverse=True)
            low_liquidity.sort(key=lambda x: x['score'], reverse=True)
            
            for cat in category_coins:
                category_coins[cat].sort(key=lambda x: x['score'], reverse=True)
            
            # === КОМБИНИРОВАННЫЙ ОТБОР: TOP MOVERS + КАТЕГОРИИ ===
            result = []
            used_bases = set()
            
            # 1. ПРИОРИТЕТ: Добавляем монеты из top movers (уже отобраны выше)
            for symbol in priority_coins:
                base = symbol.replace('/USDT', '')
                if base not in used_bases:
                    result.append(symbol)
                    used_bases.add(base)
            
            logger.info(f"[COINS] Added {len(result)} priority coins from top movers")
            
            # 2. Добавляем BTC и ETH если ещё не включены
            for major in ['BTC', 'ETH']:
                if major in coin_data_map and major not in used_bases:
                    result.append(f"{major}/USDT")
                    used_bases.add(major)
            
            # 2. По 2-5 монет из каждой категории (кроме major) - БОЛЬШЕ МЕМОВ И ХАЙПА!
            coins_per_category = {
                'layer1': 2,   # 2 Layer1 (меньше скучных монет)
                'layer2': 1,   # 1 Layer2
                'memes': 5,    # 5 мемов! (больше хайпа и волатильности)
                'defi': 2,     # 2 DeFi
                'ai': 3,       # 3 AI (хайповая тема)
                'gaming': 2,   # 2 Gaming
                'new': 5       # 5 новых листингов! (ловим хайп на старте)
            }
            
            for cat_name, count in coins_per_category.items():
                cat_list = category_coins.get(cat_name, [])
                added = 0
                for coin in cat_list:
                    if coin['base'] not in used_bases and added < count:
                        result.append(coin['symbol'])
                        used_bases.add(coin['base'])
                        added += 1
            
            # 3. Добавляем высоколиквидные монеты, которые ещё не включены
            for coin in high_liquidity:
                if len(result) >= top_n:
                    break
                if coin['base'] not in used_bases:
                    result.append(coin['symbol'])
                    used_bases.add(coin['base'])
            
            # 4. Добавляем среднеликвидные с высокой волатильностью
            volatile_medium = [c for c in medium_liquidity if c['change'] >= 3.0]
            volatile_medium.sort(key=lambda x: x['change'], reverse=True)
            for coin in volatile_medium:
                if len(result) >= top_n:
                    break
                if coin['base'] not in used_bases:
                    result.append(coin['symbol'])
                    used_bases.add(coin['base'])
            
            # 5. Добавляем низколиквидные с экстремальными движениями
            extreme_low = [c for c in low_liquidity if c['change'] >= 4.0]
            extreme_low.sort(key=lambda x: x['change'], reverse=True)
            for coin in extreme_low[:5]:
                if len(result) >= top_n:
                    break
                if coin['base'] not in used_bases:
                    result.append(coin['symbol'])
                    used_bases.add(coin['base'])
            
            # Логируем распределение по категориям
            cat_counts = {cat: len([c for c in result if c.replace('/USDT', '') in coins]) 
                         for cat, coins in self.COIN_CATEGORIES.items()}
            logger.info(f"[COINS] Category distribution: {cat_counts}")
            logger.info(f"[COINS] Total selected: {len(result[:top_n])} | High: {len(high_liquidity)}, Med: {len(medium_liquidity)}, Low: {len(low_liquidity)}")
            logger.info(f"[COINS] Top coins: {result[:min(10, top_n)]}")
            
            return result[:top_n]
            
        except Exception as e:
            logger.error(f"[COINS] Error: {e}")
            return self._default_coins()
    
    def _default_coins(self) -> List[str]:
        """Расширенный дефолтный список с диверсификацией"""
        return [
            # Major
            'BTC/USDT', 'ETH/USDT',
            # Layer1
            'SOL/USDT', 'AVAX/USDT', 'NEAR/USDT',
            # Layer2
            'ARB/USDT', 'OP/USDT',
            # Memes
            'DOGE/USDT', 'PEPE/USDT',
            # DeFi
            'LINK/USDT', 'UNI/USDT',
            # AI
            'FET/USDT', 'RNDR/USDT'
        ]


# ==================== GLOBAL INSTANCE ====================
smart_analyzer = SmartAnalyzer()


async def find_best_setup(balance: float = 0, use_whale_data: bool = True, use_news_data: bool = True) -> Optional[TradeSetup]:
    """
    Найти лучший торговый сетап
    
    Возвращает только качественные сетапы (A+, A, B, C)
    Может учитывать данные китов с Hyperliquid и новостей
    """
    
    # === ПРОВЕРКА НОВОСТЕЙ НА МАНИПУЛЯЦИИ ===
    if use_news_data and NEWS_ANALYSIS_ENABLED:
        try:
            manipulations = await detect_manipulations()
            if manipulations:
                for m in manipulations:
                    logger.warning(f"[NEWS] ⚠️ {m['type']}: {m['description']}")
            
            # Получаем рыночный сентимент
            sentiment = await get_market_sentiment()
            if sentiment.get('last_update'):
                logger.info(f"[NEWS] Market sentiment: {sentiment['score']:.0f} ({sentiment['trend']})")
        except Exception as e:
            logger.warning(f"[NEWS] Sentiment check error: {e}")
    
    # === ПРОВЕРКА NEWS-BASED OPPORTUNITIES ===
    news_opportunities = []
    if use_news_data and NEWS_ANALYSIS_ENABLED:
        try:
            news_opportunities = await get_news_trading_opportunities()
            if news_opportunities:
                logger.info(f"[NEWS] Found {len(news_opportunities)} news-based opportunities")
                for opp in news_opportunities[:3]:
                    logger.info(f"[NEWS] 📰 {opp['direction']} {opp['coins']} ({opp['confidence']:.0%}): {opp['reasoning'][0]}")
        except Exception as e:
            logger.warning(f"[NEWS] Opportunities check error: {e}")
    
    # Выбираем монеты - УВЕЛИЧЕНО до 40 для максимального количества возможностей
    coins = await smart_analyzer.select_best_coins(top_n=40)
    
    # Добавляем монеты из news opportunities в начало списка
    if news_opportunities:
        news_coins = []
        for opp in news_opportunities:
            for coin in opp['coins']:
                symbol = f"{coin}USDT"
                if symbol not in news_coins and symbol not in coins[:10]:
                    news_coins.append(symbol)
        if news_coins:
            logger.info(f"[NEWS] Adding news-based coins: {news_coins[:5]}")
            coins = news_coins[:5] + coins  # Приоритизируем монеты из новостей
    
    # Получаем данные китов если доступно
    whale_signals = {}
    if use_whale_data:
        try:
            from whale_tracker import get_combined_whale_analysis
            for coin in coins[:10]:  # Топ-10 монет проверяем на китов (увеличено с 5)
                ticker = coin.split('/')[0]
                analysis = await get_combined_whale_analysis(ticker)
                if analysis.get('confidence', 0) > 0.5:
                    whale_signals[coin] = analysis
                    logger.info(f"[WHALE] {coin}: {analysis.get('direction')} ({analysis.get('confidence'):.0%})")
        except ImportError:
            pass  # Whale tracker не установлен
        except Exception as e:
            logger.warning(f"[WHALE] Error getting whale data: {e}")
    
    # News signals для монет
    news_signals = {}
    if news_opportunities:
        for opp in news_opportunities:
            for coin in opp['coins']:
                symbol = f"{coin}USDT"
                if symbol not in news_signals or opp['confidence'] > news_signals[symbol]['confidence']:
                    news_signals[symbol] = opp
    
    # === COINGLASS DATA (Funding, Liquidations, L/S Ratio) ===
    coinglass_signals = {}
    if use_news_data and NEWS_ANALYSIS_ENABLED:
        try:
            coinglass_data = await news_analyzer.get_coinglass_signals()
            
            # Обрабатываем экстремальные funding rates
            if coinglass_data.get('funding', {}).get('extreme_long'):
                for item in coinglass_data['funding']['extreme_long']:
                    symbol = f"{item['symbol']}/USDT"
                    coinglass_signals[symbol] = {
                        'direction': 'SHORT',
                        'reason': f"🔴 Extreme funding {item['rate']:.3%}",
                        'confidence': 0.65
                    }
                    logger.info(f"[COINGLASS] {symbol}: Extreme long funding -> SHORT signal")
            
            if coinglass_data.get('funding', {}).get('extreme_short'):
                for item in coinglass_data['funding']['extreme_short']:
                    symbol = f"{item['symbol']}/USDT"
                    coinglass_signals[symbol] = {
                        'direction': 'LONG',
                        'reason': f"🟢 Negative funding {item['rate']:.3%}",
                        'confidence': 0.65
                    }
                    logger.info(f"[COINGLASS] {symbol}: Extreme short funding -> LONG signal")
            
            # Liquidation signal
            liq_signal = coinglass_data.get('liquidations', {}).get('signal')
            if liq_signal:
                liq_total = coinglass_data['liquidations'].get('total_24h', 0)
                logger.info(f"[COINGLASS] Liquidation signal: {liq_signal} (${liq_total/1e6:.1f}M total)")
                # Применяем ко всем мажорам
                for major in ['BTC/USDT', 'ETH/USDT']:
                    if major not in coinglass_signals:
                        coinglass_signals[major] = {
                            'direction': liq_signal,
                            'reason': f"💥 Liquidations ${liq_total/1e6:.1f}M",
                            'confidence': 0.60
                        }
            
            # Long/Short ratio signals
            for ls_symbol, ls_data in coinglass_data.get('long_short', {}).items():
                if isinstance(ls_data, dict) and ls_data.get('signal'):
                    symbol = f"{ls_symbol}/USDT"
                    coinglass_signals[symbol] = {
                        'direction': ls_data['signal'],
                        'reason': f"📊 L/S Ratio {ls_data.get('long_ratio', 50):.1f}%",
                        'confidence': 0.60
                    }
                    logger.info(f"[COINGLASS] {symbol}: L/S Ratio signal -> {ls_data['signal']}")
                    
        except Exception as e:
            logger.warning(f"[COINGLASS] Error getting signals: {e}")
    
    best_setup: Optional[TradeSetup] = None
    
    for symbol in coins:
        try:
            setup = await smart_analyzer.analyze(symbol, balance)
            
            if setup is not None:
                # Если есть сигнал китов - бустим confidence
                if symbol in whale_signals:
                    whale = whale_signals[symbol]
                    if whale.get('direction') == setup.direction:
                        # Киты подтверждают направление
                        setup.confidence = min(0.95, setup.confidence + 0.1)
                        setup.reasoning.insert(0, f"🐋 Киты в {setup.direction} ({whale.get('confidence'):.0%})")
                        logger.info(f"[SMART] {symbol}: Whale confirmation +10% confidence")
                    elif whale.get('direction') and whale.get('direction') != setup.direction:
                        # Киты против - снижаем confidence
                        setup.confidence = max(0.3, setup.confidence - 0.15)
                        setup.warnings.insert(0, f"⚠️ Киты против: {whale.get('direction')}")
                        logger.info(f"[SMART] {symbol}: Whale disagreement -15% confidence")
                
                # === БУСТ ОТ НОВОСТЕЙ ===
                if symbol in news_signals:
                    news = news_signals[symbol]
                    if news['direction'] == setup.direction:
                        # Новости подтверждают направление
                        boost = min(0.15, news['confidence'] * 0.2)
                        setup.confidence = min(0.95, setup.confidence + boost)
                        setup.reasoning.insert(0, f"📰 Новости: {news['reasoning'][0][:50]}")
                        logger.info(f"[SMART] {symbol}: News confirmation +{boost:.0%} confidence")
                    elif news['direction'] and news['direction'] != setup.direction:
                        # Новости против - снижаем confidence
                        setup.confidence = max(0.3, setup.confidence - 0.1)
                        setup.warnings.insert(0, f"⚠️ Новости против: {news['direction']}")
                        logger.info(f"[SMART] {symbol}: News disagreement -10% confidence")
                
                # === БУСТ ОТ COINGLASS (Funding, Liquidations, L/S Ratio) ===
                if symbol in coinglass_signals:
                    cg = coinglass_signals[symbol]
                    if cg['direction'] == setup.direction:
                        # Coinglass подтверждает направление
                        boost = cg.get('confidence', 0.6) * 0.15
                        setup.confidence = min(0.95, setup.confidence + boost)
                        setup.reasoning.insert(0, f"📊 Coinglass: {cg['reason']}")
                        logger.info(f"[SMART] {symbol}: Coinglass confirmation +{boost:.0%} confidence")
                    elif cg['direction'] and cg['direction'] != setup.direction:
                        # Coinglass против - снижаем confidence
                        setup.confidence = max(0.3, setup.confidence - 0.12)
                        setup.warnings.insert(0, f"⚠️ Coinglass против: {cg['reason']}")
                        logger.info(f"[SMART] {symbol}: Coinglass disagreement -12% confidence")
                
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
    'valid_setups': 0,  # Прошли фильтры, но не обязательно отправлены
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
        'liquidity_zone': 0,  # Добавляем liquidity_zone в словарь причин
    }
}

def increment_accepted():
    """Увеличить счетчик принятых сигналов (только когда сигнал реально отправлен)"""
    _signal_stats['accepted'] += 1


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
        'valid_setups': 0,
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
            'liquidity_zone': 0,  # Добавляем liquidity_zone в словарь причин
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
