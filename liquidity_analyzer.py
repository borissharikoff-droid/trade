"""
Liquidity Analyzer - Анализ зон ликвидности и защита от охоты на стопы
Детекция зон, где маркет-мейкеры собирают стоп-лоссы
"""

import logging
from typing import Dict, List, Optional, Tuple
import numpy as np

logger = logging.getLogger(__name__)


class LiquidityZone:
    """Зона ликвидности"""
    def __init__(self, price: float, strength: float, zone_type: str, touches: int = 0):
        self.price = price
        self.strength = strength  # 0-1
        self.zone_type = zone_type  # 'support', 'resistance', 'liquidity_pool'
        self.touches = touches


class LiquidityAnalyzer:
    """Анализатор зон ликвидности"""
    
    def __init__(self):
        self.detected_zones: Dict[str, List[LiquidityZone]] = {}  # {symbol: [zones]}
    
    def find_liquidity_zones(self, klines: List, direction: str, symbol: str = "") -> List[LiquidityZone]:
        """
        Найти зоны ликвидности на графике
        
        Args:
            klines: Список свечей
            direction: 'LONG' или 'SHORT'
            symbol: Символ для кэширования
        
        Returns:
            Список зон ликвидности
        """
        if not klines or len(klines) < 20:
            return []
        
        zones = []
        
        # Парсим данные
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        
        # 1. Ищем swing points (локальные экстремумы)
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(klines) - 2):
            # Swing High
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
               highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append({
                    'price': highs[i],
                    'index': i,
                    'volume': volumes[i]
                })
            
            # Swing Low
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
               lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append({
                    'price': lows[i],
                    'index': i,
                    'volume': volumes[i]
                })
        
        # 2. Для LONG: ищем liquidity pools ниже swing lows
        if direction == "LONG":
            for swing in swing_lows[-5:]:  # Последние 5 swing lows
                # Зона ликвидности = 0.1-0.3% ниже swing low
                zone_price = swing['price'] * 0.998  # 0.2% ниже
                
                # Сила зоны зависит от:
                # - Количества касаний уровня
                # - Объёма на уровне
                touches = self._count_touches(lows, zone_price, tolerance=0.002)
                volume_factor = min(1.0, swing['volume'] / np.mean(volumes) if volumes else 1.0)
                
                strength = min(1.0, (touches * 0.2) + (volume_factor * 0.3))
                
                zones.append(LiquidityZone(
                    price=zone_price,
                    strength=strength,
                    zone_type='liquidity_pool',
                    touches=touches
                ))
        
        # 3. Для SHORT: ищем liquidity pools выше swing highs
        else:  # SHORT
            for swing in swing_highs[-5:]:  # Последние 5 swing highs
                # Зона ликвидности = 0.1-0.3% выше swing high
                zone_price = swing['price'] * 1.002  # 0.2% выше
                
                touches = self._count_touches(highs, zone_price, tolerance=0.002)
                volume_factor = min(1.0, swing['volume'] / np.mean(volumes) if volumes else 1.0)
                
                strength = min(1.0, (touches * 0.2) + (volume_factor * 0.3))
                
                zones.append(LiquidityZone(
                    price=zone_price,
                    strength=strength,
                    zone_type='liquidity_pool',
                    touches=touches
                ))
        
        # 4. Ищем скопления round numbers (психологические уровни)
        round_zones = self._find_round_number_zones(closes, direction)
        zones.extend(round_zones)
        
        # Кэшируем для символа
        if symbol:
            self.detected_zones[symbol] = zones
        
        logger.info(f"[LIQUIDITY] Found {len(zones)} liquidity zones for {direction}")
        
        return zones
    
    def _count_touches(self, prices: List[float], target_price: float, tolerance: float = 0.002) -> int:
        """Посчитать количество касаний уровня"""
        count = 0
        for price in prices:
            if abs(price - target_price) / target_price <= tolerance:
                count += 1
        return count
    
    def _find_round_number_zones(self, closes: List[float], direction: str) -> List[LiquidityZone]:
        """Найти зоны на круглых числах (психологические уровни)"""
        if not closes:
            return []
        
        current_price = closes[-1]
        zones = []
        
        # Определяем порядок величины
        if current_price >= 1000:
            round_step = 100  # BTC: 50000, 51000, 52000
        elif current_price >= 100:
            round_step = 10   # ETH: 3000, 3100, 3200
        elif current_price >= 10:
            round_step = 1    # SOL: 100, 101, 102
        else:
            round_step = 0.1  # Малые монеты
        
        # Ищем ближайшие круглые числа
        if direction == "LONG":
            # Ищем круглые числа ниже текущей цены
            below_round = (int(current_price / round_step) - 1) * round_step
            zone_price = below_round * 0.998  # Немного ниже круглого числа
            
            zones.append(LiquidityZone(
                price=zone_price,
                strength=0.5,
                zone_type='round_number',
                touches=0
            ))
        else:  # SHORT
            # Ищем круглые числа выше текущей цены
            above_round = (int(current_price / round_step) + 1) * round_step
            zone_price = above_round * 1.002  # Немного выше круглого числа
            
            zones.append(LiquidityZone(
                price=zone_price,
                strength=0.5,
                zone_type='round_number',
                touches=0
            ))
        
        return zones
    
    def should_avoid_entry(self, entry: float, liquidity_zones: List[LiquidityZone], 
                          atr: float, min_distance_percent: float = 0.5) -> Tuple[bool, str]:
        """
        Проверить, не слишком ли близко вход к зоне ликвидности
        
        Args:
            entry: Цена входа
            liquidity_zones: Список зон ликвидности
            atr: ATR для расчёта расстояния
            min_distance_percent: Минимальное расстояние в % от entry
        
        Returns:
            (should_avoid, reason)
        """
        if not liquidity_zones:
            return False, "OK"
        
        # Минимальное расстояние = max(0.5% от entry, 1 ATR)
        min_distance = max(entry * (min_distance_percent / 100), atr)
        
        for zone in liquidity_zones:
            distance = abs(entry - zone.price)
            
            # Если вход слишком близко к зоне ликвидности
            if distance < min_distance:
                return True, f"Entry too close to liquidity zone at {zone.price:.4f} (distance: {distance/entry*100:.2f}%)"
        
        return False, "OK"
    
    def analyze_order_flow(self, klines: List) -> Dict:
        """
        Анализ потока ордеров для детекции манипуляций
        
        Ищем паттерны "сбора стопов":
        1. Резкий пробой уровня
        2. Быстрый откат
        3. Низкий объём на пробое
        """
        if not klines or len(klines) < 10:
            return {'manipulation_risk': False, 'reason': 'Insufficient data'}
        
        recent = klines[-10:]
        volumes = [float(k[5]) for k in recent]
        closes = [float(k[4]) for k in recent]
        highs = [float(k[2]) for k in recent]
        lows = [float(k[3]) for k in recent]
        
        # 1. Проверка на резкий пробой с низким объёмом
        if len(recent) >= 5:
            price_change = abs(closes[-1] - closes[-5]) / closes[-5] * 100
            volume_avg = sum(volumes[-5:-1]) / 4 if len(volumes) >= 5 else np.mean(volumes[:-1])
            volume_last = volumes[-1]
            
            # Паттерн: большой пробой + низкий объём = возможная охота
            if price_change > 1.0 and volume_last < volume_avg * 0.7:
                return {
                    'manipulation_risk': True,
                    'reason': f'Low volume breakout ({price_change:.2f}% move, volume {volume_last/volume_avg:.2f}x avg)',
                    'confidence': 0.7
                }
        
        # 2. Проверка на "wick rejection" (свечи с длинными тенями)
        if len(recent) >= 3:
            last_candle = recent[-1]
            high = float(last_candle[2])
            low = float(last_candle[3])
            open_price = float(last_candle[1])
            close = float(last_candle[4])
            
            body = abs(close - open_price)
            upper_wick = high - max(open_price, close)
            lower_wick = min(open_price, close) - low
            
            # Длинная тень = возможный отбой от уровня (сбор стопов)
            if upper_wick > body * 2 or lower_wick > body * 2:
                return {
                    'manipulation_risk': True,
                    'reason': 'Long wick rejection - possible stop hunt',
                    'confidence': 0.6
                }
        
        return {'manipulation_risk': False, 'reason': 'Normal order flow'}


# Глобальный экземпляр
liquidity_analyzer = LiquidityAnalyzer()
