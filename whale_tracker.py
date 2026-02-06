"""
Whale Tracker - Отслеживание китов на Hyperliquid
Анализирует крупные позиции, ликвидации и активность топ-трейдеров
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# ==================== КОНФИГ ====================
HYPERLIQUID_API = "https://api.hyperliquid.xyz"
WHALE_THRESHOLD_USD = 100000  # $100k+ = кит
MEGA_WHALE_THRESHOLD = 500000  # $500k+ = мега-кит
UPDATE_INTERVAL = 60  # секунд


class WhaleSignal(Enum):
    """Тип сигнала от кита"""
    MEGA_LONG = "MEGA_LONG"      # Мега-кит открыл лонг
    MEGA_SHORT = "MEGA_SHORT"    # Мега-кит открыл шорт
    WHALE_LONG = "WHALE_LONG"    # Кит открыл лонг
    WHALE_SHORT = "WHALE_SHORT"  # Кит открыл шорт
    LIQUIDATION_LONG = "LIQ_LONG"   # Массовые ликвидации лонгов
    LIQUIDATION_SHORT = "LIQ_SHORT" # Массовые ликвидации шортов
    NONE = "NONE"


@dataclass
class WhalePosition:
    """Позиция кита"""
    address: str
    coin: str
    size: float  # В монетах
    notional: float  # В USD
    side: str  # "long" или "short"
    entry_price: float
    unrealized_pnl: float
    leverage: float
    timestamp: datetime


@dataclass
class WhaleAlert:
    """Алерт о действии кита"""
    signal: WhaleSignal
    coin: str
    direction: str  # "LONG" или "SHORT"
    size_usd: float
    whale_count: int  # Сколько китов
    confidence: float  # 0-1
    reasoning: List[str]
    timestamp: datetime


class WhaleTracker:
    """Отслеживание китов на Hyperliquid"""
    
    def __init__(self):
        self.whale_positions: Dict[str, List[WhalePosition]] = {}  # {coin: [positions]}
        self.recent_liquidations: Dict[str, List[Dict]] = {}  # {coin: [liquidations]}
        self.top_traders: Dict[str, Dict] = {}  # {address: stats}
        self.last_update: datetime = None
        
        # Кэш для уменьшения запросов
        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = 30  # секунд
    
    async def _request(self, endpoint: str, payload: dict = None) -> Optional[Dict]:
        """Запрос к Hyperliquid API (с retry и exponential backoff)"""
        delay = 1.0
        for attempt in range(4):
            try:
                async with aiohttp.ClientSession() as session:
                    if payload:
                        async with session.post(
                            f"{HYPERLIQUID_API}{endpoint}",
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            if resp.status == 200:
                                return await resp.json()
                    else:
                        async with session.get(
                            f"{HYPERLIQUID_API}{endpoint}",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            if resp.status == 200:
                                return await resp.json()
            except Exception as e:
                if attempt == 3:
                    logger.warning(f"[WHALE] API error after retries: {e}")
                    return None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
        return None
    
    async def get_all_mids(self) -> Dict[str, float]:
        """Получить текущие цены всех монет"""
        data = await self._request("/info", {"type": "allMids"})
        if data:
            return {k: float(v) for k, v in data.items()}
        return {}
    
    async def get_meta_and_asset_ctxs(self) -> Tuple[Dict, List]:
        """Получить метаданные и контекст активов"""
        data = await self._request("/info", {"type": "metaAndAssetCtxs"})
        if data and len(data) >= 2:
            return data[0], data[1]
        return {}, []
    
    async def get_user_state(self, address: str) -> Optional[Dict]:
        """Получить состояние пользователя"""
        return await self._request("/info", {
            "type": "clearinghouseState",
            "user": address
        })
    
    async def get_leaderboard(self, window: str = "day") -> List[Dict]:
        """
        Получить лидерборд трейдеров
        window: "day", "week", "month", "allTime"
        """
        data = await self._request("/info", {
            "type": "leaderboard",
            "window": window
        })
        return data if data else []
    
    async def get_funding_history(self, coin: str, limit: int = 100) -> List[Dict]:
        """Получить историю фандинга"""
        data = await self._request("/info", {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
        })
        return data[:limit] if data else []
    
    async def get_open_orders(self, address: str) -> List[Dict]:
        """Получить открытые ордера пользователя"""
        data = await self._request("/info", {
            "type": "openOrders",
            "user": address
        })
        return data if data else []
    
    async def scan_whale_positions(self, coin: str) -> List[WhalePosition]:
        """
        Сканировать позиции китов для монеты
        Использует лидерборд для поиска крупных трейдеров
        """
        whales = []
        
        try:
            # Получаем топ трейдеров
            leaderboard = await self.get_leaderboard("week")
            
            if not leaderboard:
                return whales
            
            # Проверяем позиции топ-50 трейдеров
            for trader in leaderboard[:50]:
                address = trader.get("ethAddress", "")
                if not address:
                    continue
                
                state = await self.get_user_state(address)
                if not state:
                    continue
                
                positions = state.get("assetPositions", [])
                
                for pos in positions:
                    pos_data = pos.get("position", {})
                    pos_coin = pos_data.get("coin", "")
                    
                    if pos_coin.upper() != coin.upper():
                        continue
                    
                    size = abs(float(pos_data.get("szi", 0)))
                    entry_price = float(pos_data.get("entryPx", 0))
                    notional = size * entry_price
                    
                    if notional < WHALE_THRESHOLD_USD:
                        continue
                    
                    side = "long" if float(pos_data.get("szi", 0)) > 0 else "short"
                    
                    whale = WhalePosition(
                        address=address[:10] + "...",
                        coin=pos_coin,
                        size=size,
                        notional=notional,
                        side=side,
                        entry_price=entry_price,
                        unrealized_pnl=float(pos_data.get("unrealizedPnl", 0)),
                        leverage=float(pos_data.get("leverage", {}).get("value", 1)),
                        timestamp=datetime.now()
                    )
                    whales.append(whale)
                
                # Небольшая задержка чтобы не спамить API
                await asyncio.sleep(0.1)
            
            # Сортируем по размеру
            whales.sort(key=lambda x: x.notional, reverse=True)
            
            # Кэшируем
            self.whale_positions[coin] = whales
            
            logger.info(f"[WHALE] {coin}: найдено {len(whales)} китов")
            
        except Exception as e:
            logger.error(f"[WHALE] Scan error: {e}")
        
        return whales
    
    async def analyze_whale_sentiment(self, coin: str) -> WhaleAlert:
        """
        Анализировать настроение китов для монеты
        Возвращает сигнал на основе позиций китов
        """
        whales = self.whale_positions.get(coin, [])
        
        if not whales:
            whales = await self.scan_whale_positions(coin)
        
        if not whales:
            return WhaleAlert(
                signal=WhaleSignal.NONE,
                coin=coin,
                direction="",
                size_usd=0,
                whale_count=0,
                confidence=0,
                reasoning=["Нет данных о китах"],
                timestamp=datetime.now()
            )
        
        # Считаем баланс лонг/шорт
        long_volume = sum(w.notional for w in whales if w.side == "long")
        short_volume = sum(w.notional for w in whales if w.side == "short")
        long_count = sum(1 for w in whales if w.side == "long")
        short_count = sum(1 for w in whales if w.side == "short")
        
        total_volume = long_volume + short_volume
        
        if total_volume == 0:
            return WhaleAlert(
                signal=WhaleSignal.NONE,
                coin=coin,
                direction="",
                size_usd=0,
                whale_count=len(whales),
                confidence=0,
                reasoning=["Киты в нейтрале"],
                timestamp=datetime.now()
            )
        
        # Рассчитываем дисбаланс
        long_ratio = long_volume / total_volume
        short_ratio = short_volume / total_volume
        
        reasoning = []
        reasoning.append(f"Китов: {len(whales)} (${total_volume/1000:.0f}K)")
        reasoning.append(f"Long: {long_count} (${long_volume/1000:.0f}K)")
        reasoning.append(f"Short: {short_count} (${short_volume/1000:.0f}K)")
        
        # Определяем сигнал
        signal = WhaleSignal.NONE
        direction = ""
        confidence = 0
        
        # Мега-киты (>$500K)
        mega_longs = [w for w in whales if w.side == "long" and w.notional >= MEGA_WHALE_THRESHOLD]
        mega_shorts = [w for w in whales if w.side == "short" and w.notional >= MEGA_WHALE_THRESHOLD]
        
        if mega_longs and not mega_shorts:
            signal = WhaleSignal.MEGA_LONG
            direction = "LONG"
            confidence = min(0.9, 0.6 + len(mega_longs) * 0.1)
            reasoning.insert(0, f"{len(mega_longs)} МЕГА-КИТ в LONG")
        elif mega_shorts and not mega_longs:
            signal = WhaleSignal.MEGA_SHORT
            direction = "SHORT"
            confidence = min(0.9, 0.6 + len(mega_shorts) * 0.1)
            reasoning.insert(0, f"{len(mega_shorts)} МЕГА-КИТ в SHORT")
        elif long_ratio > 0.65:
            signal = WhaleSignal.WHALE_LONG
            direction = "LONG"
            confidence = min(0.8, long_ratio)
            reasoning.insert(0, f"Киты в LONG ({long_ratio:.0%})")
        elif short_ratio > 0.65:
            signal = WhaleSignal.WHALE_SHORT
            direction = "SHORT"
            confidence = min(0.8, short_ratio)
            reasoning.insert(0, f"Киты в SHORT ({short_ratio:.0%})")
        
        return WhaleAlert(
            signal=signal,
            coin=coin,
            direction=direction,
            size_usd=total_volume,
            whale_count=len(whales),
            confidence=confidence,
            reasoning=reasoning,
            timestamp=datetime.now()
        )
    
    async def get_funding_signal(self, coin: str) -> Dict:
        """
        Анализировать фандинг для сигнала
        Высокий положительный = много лонгов (контр-сигнал SHORT)
        Высокий отрицательный = много шортов (контр-сигнал LONG)
        """
        result = {
            'signal': None,
            'funding_rate': 0,
            'reasoning': '',
            'strength': 0
        }
        
        try:
            history = await self.get_funding_history(coin, 10)
            
            if not history:
                return result
            
            # Средний фандинг за последние периоды
            rates = [float(h.get('fundingRate', 0)) for h in history]
            avg_rate = sum(rates) / len(rates) if rates else 0
            current_rate = rates[0] if rates else 0
            
            # Аннуализированный %
            annual_rate = current_rate * 3 * 365 * 100  # 3 периода в день, 365 дней
            
            result['funding_rate'] = current_rate
            
            # Экстремальный фандинг = контр-сигнал
            if annual_rate > 50:  # >50% годовых = слишком много лонгов
                result['signal'] = 'SHORT'
                result['strength'] = min(3, int(annual_rate / 30))
                result['reasoning'] = f"Фандинг +{annual_rate:.0f}% годовых - перекос в лонг"
            elif annual_rate < -50:  # <-50% годовых = слишком много шортов
                result['signal'] = 'LONG'
                result['strength'] = min(3, int(abs(annual_rate) / 30))
                result['reasoning'] = f"Фандинг {annual_rate:.0f}% годовых - перекос в шорт"
            elif annual_rate > 20:
                result['signal'] = 'SHORT'
                result['strength'] = 1
                result['reasoning'] = f"Фандинг +{annual_rate:.0f}% - больше лонгов"
            elif annual_rate < -20:
                result['signal'] = 'LONG'
                result['strength'] = 1
                result['reasoning'] = f"Фандинг {annual_rate:.0f}% - больше шортов"
                
        except Exception as e:
            logger.warning(f"[WHALE] Funding error: {e}")
        
        return result
    
    async def get_open_interest_change(self, coin: str) -> Dict:
        """
        Анализировать изменение открытого интереса
        Резкий рост OI + цена вверх = сильный тренд
        Резкий рост OI + цена вниз = скоро разворот
        """
        result = {
            'signal': None,
            'oi_change': 0,
            'reasoning': '',
            'strength': 0
        }
        
        try:
            meta, asset_ctxs = await self.get_meta_and_asset_ctxs()
            
            if not asset_ctxs:
                return result
            
            # Ищем нужную монету
            for ctx in asset_ctxs:
                if ctx.get('coin', '').upper() == coin.upper():
                    oi = float(ctx.get('openInterest', 0))
                    
                    # К сожалению, HL API не даёт историю OI напрямую
                    # Но мы можем отслеживать изменения между вызовами
                    
                    result['oi_change'] = oi
                    result['reasoning'] = f"Open Interest: ${oi/1000:.0f}K"
                    break
                    
        except Exception as e:
            logger.warning(f"[WHALE] OI error: {e}")
        
        return result


# Глобальный экземпляр
whale_tracker = WhaleTracker()


# ==================== API ФУНКЦИИ ====================

async def get_whale_signal(coin: str) -> WhaleAlert:
    """Получить сигнал от китов для монеты"""
    return await whale_tracker.analyze_whale_sentiment(coin)


async def get_funding_signal(coin: str) -> Dict:
    """Получить сигнал по фандингу"""
    return await whale_tracker.get_funding_signal(coin)


async def get_combined_whale_analysis(coin: str) -> Dict:
    """
    Комбинированный анализ: киты + фандинг
    Возвращает сводный сигнал
    """
    whale_alert = await get_whale_signal(coin)
    funding = await get_funding_signal(coin)
    
    result = {
        'signal': None,
        'direction': '',
        'confidence': 0,
        'reasoning': [],
        'whale_data': whale_alert,
        'funding_data': funding
    }
    
    # Объединяем сигналы
    signals = []
    
    if whale_alert.signal != WhaleSignal.NONE:
        signals.append({
            'direction': whale_alert.direction,
            'weight': whale_alert.confidence * 2,  # Киты важнее
            'source': 'whale'
        })
        result['reasoning'].extend(whale_alert.reasoning[:2])
    
    if funding.get('signal'):
        signals.append({
            'direction': funding['signal'],
            'weight': funding['strength'] * 0.3,
            'source': 'funding'
        })
        result['reasoning'].append(funding['reasoning'])
    
    if not signals:
        return result
    
    # Считаем взвешенный сигнал
    long_weight = sum(s['weight'] for s in signals if s['direction'] == 'LONG')
    short_weight = sum(s['weight'] for s in signals if s['direction'] == 'SHORT')
    
    if long_weight > short_weight and long_weight > 0.5:
        result['signal'] = 'LONG'
        result['direction'] = 'LONG'
        result['confidence'] = min(0.9, long_weight / (long_weight + short_weight + 0.1))
    elif short_weight > long_weight and short_weight > 0.5:
        result['signal'] = 'SHORT'
        result['direction'] = 'SHORT'
        result['confidence'] = min(0.9, short_weight / (long_weight + short_weight + 0.1))
    
    return result
