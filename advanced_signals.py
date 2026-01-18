"""
Advanced Trading Signals - Продвинутые торговые техники
Дополнительные индикаторы и стратегии помимо BTC/ETH
"""

import asyncio
import aiohttp
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


# ==================== АЛЬТКОИНЫ ДЛЯ ТОРГОВЛИ ====================

# Категории монет с разными характеристиками
COIN_CATEGORIES = {
    # Layer 1 - основа, высокая ликвидность
    'layer1': ['SOL', 'AVAX', 'NEAR', 'APT', 'SUI', 'SEI', 'TON', 'INJ'],
    
    # Layer 2 - быстрый рост при хайпе ETH
    'layer2': ['ARB', 'OP', 'STRK', 'ZK', 'MATIC', 'MANTA'],
    
    # Мемы - высокая волатильность, быстрые движения
    'memes': ['PEPE', 'DOGE', 'SHIB', 'FLOKI', 'BONK', 'WIF', 'MEME', 'TURBO', 
              'NEIRO', 'POPCAT', 'MOG', 'BRETT', 'BOME'],
    
    # DeFi - двигаются с TVL и хайпом
    'defi': ['UNI', 'AAVE', 'MKR', 'CRV', 'LDO', 'PENDLE', 'GMX', 'DYDX'],
    
    # AI - хайповая тема
    'ai': ['FET', 'RNDR', 'TAO', 'WLD', 'ARKM', 'AGIX'],
    
    # Gaming - волатильные, следуют за новостями
    'gaming': ['IMX', 'GALA', 'AXS', 'SAND', 'MANA', 'PIXEL'],
    
    # Новые листинги - высокий риск/награда
    'new': ['JUP', 'STRK', 'ZK', 'ENA', 'W', 'ETHFI']
}

# Все торгуемые монеты
ALL_TRADEABLE = []
for coins in COIN_CATEGORIES.values():
    ALL_TRADEABLE.extend(coins)
ALL_TRADEABLE = list(set(ALL_TRADEABLE))  # Уникальные


# ==================== КОРРЕЛЯЦИОННЫЙ АНАЛИЗ ====================

class CorrelationAnalyzer:
    """
    Анализ корреляций между монетами
    Используется для:
    1. Хеджирования (торговля парами)
    2. Определения лидеров/отстающих
    3. Избежания одинаковых позиций
    """
    
    def __init__(self):
        self.correlation_matrix: Dict[str, Dict[str, float]] = {}
        self.price_history: Dict[str, List[float]] = {}
        self.last_update: datetime = None
    
    async def update_correlations(self, coins: List[str], period: int = 100):
        """Обновить матрицу корреляций"""
        # Собираем историю цен
        for coin in coins:
            prices = await self._get_price_history(coin, period)
            if prices:
                self.price_history[coin] = prices
        
        # Рассчитываем корреляции
        for coin1 in coins:
            if coin1 not in self.price_history:
                continue
            
            self.correlation_matrix[coin1] = {}
            
            for coin2 in coins:
                if coin2 not in self.price_history:
                    continue
                
                if coin1 == coin2:
                    self.correlation_matrix[coin1][coin2] = 1.0
                else:
                    corr = self._calculate_correlation(
                        self.price_history[coin1],
                        self.price_history[coin2]
                    )
                    self.correlation_matrix[coin1][coin2] = corr
        
        self.last_update = datetime.now()
    
    def _calculate_correlation(self, prices1: List[float], prices2: List[float]) -> float:
        """Рассчитать корреляцию между двумя рядами"""
        try:
            min_len = min(len(prices1), len(prices2))
            if min_len < 10:
                return 0
            
            p1 = np.array(prices1[:min_len])
            p2 = np.array(prices2[:min_len])
            
            # Возвраты
            returns1 = np.diff(p1) / p1[:-1]
            returns2 = np.diff(p2) / p2[:-1]
            
            # Корреляция
            corr = np.corrcoef(returns1, returns2)[0, 1]
            return float(corr) if not np.isnan(corr) else 0
        except:
            return 0
    
    async def _get_price_history(self, coin: str, period: int) -> List[float]:
        """Получить историю цен с Binance"""
        try:
            symbol = f"{coin}USDT"
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit={period}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return [float(k[4]) for k in data]  # Close prices
        except:
            pass
        return []
    
    def get_correlated_coins(self, coin: str, threshold: float = 0.7) -> List[Tuple[str, float]]:
        """Получить монеты с высокой корреляцией"""
        if coin not in self.correlation_matrix:
            return []
        
        correlated = []
        for other_coin, corr in self.correlation_matrix[coin].items():
            if other_coin != coin and abs(corr) >= threshold:
                correlated.append((other_coin, corr))
        
        return sorted(correlated, key=lambda x: abs(x[1]), reverse=True)
    
    def find_divergence(self, coin1: str, coin2: str) -> Optional[Dict]:
        """
        Найти дивергенцию между коррелирующими монетами
        Если обычно двигаются вместе, но сейчас разошлись - возможность для арбитража
        """
        if coin1 not in self.price_history or coin2 not in self.price_history:
            return None
        
        corr = self.correlation_matrix.get(coin1, {}).get(coin2, 0)
        
        if abs(corr) < 0.6:
            return None  # Недостаточно коррелированы
        
        # Последние изменения
        p1 = self.price_history[coin1]
        p2 = self.price_history[coin2]
        
        if len(p1) < 24 or len(p2) < 24:
            return None
        
        # Изменение за 24 часа
        change1 = (p1[-1] - p1[-24]) / p1[-24] * 100
        change2 = (p2[-1] - p2[-24]) / p2[-24] * 100
        
        diff = abs(change1 - change2)
        
        if diff > 3:  # Разница более 3%
            # Определяем отстающую монету
            if change1 > change2:
                laggard = coin2
                leader = coin1
                expected_direction = "LONG"  # Отстающий должен догнать
            else:
                laggard = coin1
                leader = coin2
                expected_direction = "LONG"
            
            return {
                'signal': True,
                'laggard': laggard,
                'leader': leader,
                'direction': expected_direction,
                'divergence': diff,
                'correlation': corr,
                'reasoning': f"📊 Дивергенция: {leader} +{max(change1,change2):.1f}%, {laggard} отстаёт на {diff:.1f}%"
            }
        
        return None


# ==================== МЕЖРЫНОЧНЫЙ АНАЛИЗ ====================

class CrossMarketAnalyzer:
    """
    Анализ связей между рынками
    - BTC доминация
    - ETH/BTC ratio
    - Альт-сезон индикатор
    - Fear & Greed
    """
    
    def __init__(self):
        self.btc_dominance: float = 0
        self.eth_btc_ratio: float = 0
        self.altseason_index: float = 0
        self.fear_greed: int = 50
    
    async def update_metrics(self):
        """Обновить все метрики"""
        await asyncio.gather(
            self._update_btc_dominance(),
            self._update_eth_btc_ratio(),
            self._update_fear_greed()
        )
        self._calculate_altseason()
    
    async def _update_btc_dominance(self):
        """Получить BTC доминацию"""
        try:
            url = "https://api.coingecko.com/api/v3/global"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.btc_dominance = data['data']['market_cap_percentage']['btc']
        except Exception as e:
            logger.warning(f"[CROSS] BTC dominance error: {e}")
    
    async def _update_eth_btc_ratio(self):
        """Получить ETH/BTC соотношение"""
        try:
            url = "https://api.binance.com/api/v3/ticker/price?symbol=ETHBTC"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.eth_btc_ratio = float(data['price'])
        except Exception as e:
            logger.warning(f"[CROSS] ETH/BTC error: {e}")
    
    async def _update_fear_greed(self):
        """Получить Fear & Greed Index"""
        try:
            url = "https://api.alternative.me/fng/?limit=1"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.fear_greed = int(data['data'][0]['value'])
        except Exception as e:
            logger.warning(f"[CROSS] Fear/Greed error: {e}")
    
    def _calculate_altseason(self):
        """
        Рассчитать индекс альт-сезона
        - BTC.D падает + альты растут = альт-сезон
        """
        # Упрощённый индекс
        # BTC.D < 45% = сильный альт-сезон
        # BTC.D > 55% = BTC сезон
        
        if self.btc_dominance < 42:
            self.altseason_index = 100  # Полный альт-сезон
        elif self.btc_dominance < 45:
            self.altseason_index = 80
        elif self.btc_dominance < 50:
            self.altseason_index = 50
        elif self.btc_dominance < 55:
            self.altseason_index = 30
        else:
            self.altseason_index = 10  # BTC сезон
    
    def get_market_bias(self) -> Dict:
        """
        Получить рыночный уклон
        Что лучше торговать сейчас
        """
        result = {
            'btc_dominance': self.btc_dominance,
            'eth_btc': self.eth_btc_ratio,
            'altseason': self.altseason_index,
            'fear_greed': self.fear_greed,
            'recommendation': [],
            'best_category': 'layer1'
        }
        
        # Рекомендации
        if self.fear_greed < 25:
            result['recommendation'].append("😱 Страх - хорошо для покупок")
        elif self.fear_greed > 75:
            result['recommendation'].append("🤑 Жадность - осторожно с лонгами")
        
        if self.altseason_index > 70:
            result['recommendation'].append("🚀 Альт-сезон - торгуй альты")
            result['best_category'] = 'memes'  # Мемы лучше всего в альт-сезон
        elif self.altseason_index < 30:
            result['recommendation'].append("₿ BTC сезон - фокус на BTC/ETH")
            result['best_category'] = 'layer1'
        
        if self.eth_btc_ratio > 0.055:
            result['recommendation'].append("📈 ETH сильный - L2 могут расти")
        elif self.eth_btc_ratio < 0.045:
            result['recommendation'].append("📉 ETH слабый - избегай L2")
        
        return result


# ==================== МЕМКОИН СКАНЕР ====================

class MemeCoinScanner:
    """
    Специальный сканер для мемкоинов
    Быстрые движения, высокий риск/награда
    """
    
    def __init__(self):
        self.meme_coins = COIN_CATEGORIES['memes']
        self.momentum_cache: Dict[str, Dict] = {}
    
    async def scan_momentum(self) -> List[Dict]:
        """
        Сканировать моментум по всем мемам
        Ищем:
        1. Резкий рост объёма
        2. Пробой локального максимума
        3. Сильный RSI но не перекупленность
        """
        results = []
        
        for coin in self.meme_coins:
            try:
                data = await self._get_coin_data(coin)
                if not data:
                    continue
                
                # Анализ
                signal = self._analyze_meme(coin, data)
                if signal:
                    results.append(signal)
                    
            except Exception as e:
                logger.warning(f"[MEME] {coin} error: {e}")
        
        # Сортируем по силе сигнала
        results.sort(key=lambda x: x.get('strength', 0), reverse=True)
        
        return results[:5]  # Топ-5
    
    async def _get_coin_data(self, coin: str) -> Optional[Dict]:
        """Получить данные монеты"""
        try:
            symbol = f"{coin}USDT"
            
            async with aiohttp.ClientSession() as session:
                # Свечи
                url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=15m&limit=50"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        return None
                    klines = await resp.json()
                
                # 24h данные
                url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        return None
                    ticker = await resp.json()
            
            closes = [float(k[4]) for k in klines]
            volumes = [float(k[5]) for k in klines]
            highs = [float(k[2]) for k in klines]
            
            return {
                'closes': closes,
                'volumes': volumes,
                'highs': highs,
                'price_change_24h': float(ticker['priceChangePercent']),
                'volume_24h': float(ticker['quoteVolume']),
                'high_24h': float(ticker['highPrice']),
                'low_24h': float(ticker['lowPrice'])
            }
            
        except:
            return None
    
    def _analyze_meme(self, coin: str, data: Dict) -> Optional[Dict]:
        """Анализ мемкоина"""
        closes = data['closes']
        volumes = data['volumes']
        
        if len(closes) < 20:
            return None
        
        current_price = closes[-1]
        
        # RSI
        rsi = self._calculate_rsi(closes)
        
        # Объём vs средний
        avg_volume = sum(volumes[-20:-1]) / 19
        current_volume = volumes[-1]
        volume_spike = current_volume / avg_volume if avg_volume > 0 else 1
        
        # Изменение цены
        change_1h = (closes[-1] - closes[-4]) / closes[-4] * 100 if closes[-4] > 0 else 0
        change_4h = (closes[-1] - closes[-16]) / closes[-16] * 100 if closes[-16] > 0 else 0
        
        # Близость к 24h хаю
        high_24h = data['high_24h']
        distance_to_high = (high_24h - current_price) / current_price * 100
        
        # Сигнал
        signal = None
        strength = 0
        reasoning = []
        
        # LONG условия для мемов
        if (volume_spike > 2 and  # Объём x2+
            change_1h > 1 and  # Рост 1%+ за час
            rsi > 50 and rsi < 75 and  # Сила но не перекупленность
            distance_to_high < 5):  # Близко к хаю
            
            signal = "LONG"
            strength = min(5, int(volume_spike))
            reasoning.append(f"🔥 Объём x{volume_spike:.1f}")
            reasoning.append(f"📈 +{change_1h:.1f}% за час")
            reasoning.append(f"💪 RSI={rsi:.0f}")
            if distance_to_high < 2:
                reasoning.append("🎯 Пробой хая!")
                strength += 1
        
        # SHORT условия (мемы часто дампятся резко)
        elif (change_1h < -3 and  # Падение 3%+ за час
              rsi < 40 and
              volume_spike > 1.5):
            
            signal = "SHORT"
            strength = min(4, int(abs(change_1h)))
            reasoning.append(f"📉 {change_1h:.1f}% за час")
            reasoning.append(f"😰 RSI={rsi:.0f}")
        
        if not signal:
            return None
        
        return {
            'coin': coin,
            'signal': signal,
            'strength': strength,
            'price': current_price,
            'change_1h': change_1h,
            'change_4h': change_4h,
            'volume_spike': volume_spike,
            'rsi': rsi,
            'reasoning': reasoning
        }
    
    def _calculate_rsi(self, closes: List[float], period: int = 14) -> float:
        """Рассчитать RSI"""
        if len(closes) < period + 1:
            return 50
        
        deltas = np.diff(closes[-period-1:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return float(rsi)


# ==================== ГЛОБАЛЬНЫЕ ЭКЗЕМПЛЯРЫ ====================

correlation_analyzer = CorrelationAnalyzer()
cross_market = CrossMarketAnalyzer()
meme_scanner = MemeCoinScanner()


# ==================== API ФУНКЦИИ ====================

async def get_best_coins_to_trade() -> List[Dict]:
    """
    Получить лучшие монеты для торговли прямо сейчас
    Учитывает: рыночный контекст, моментум, корреляции
    """
    # Обновляем данные
    await cross_market.update_metrics()
    
    market_bias = cross_market.get_market_bias()
    
    results = []
    
    # Сканируем мемы если альт-сезон
    if market_bias['altseason'] > 50:
        meme_signals = await meme_scanner.scan_momentum()
        for sig in meme_signals:
            sig['category'] = 'meme'
            sig['priority'] = sig['strength'] * 1.2  # Бонус в альт-сезон
            results.append(sig)
    
    # Добавляем L1/L2 всегда
    for category in ['layer1', 'layer2']:
        for coin in COIN_CATEGORIES[category][:5]:
            # Тут можно добавить анализ каждой монеты
            results.append({
                'coin': coin,
                'category': category,
                'priority': 1
            })
    
    return results


async def get_meme_opportunities() -> List[Dict]:
    """Получить возможности в мемкоинах"""
    return await meme_scanner.scan_momentum()


async def get_market_context() -> Dict:
    """Получить общий контекст рынка"""
    await cross_market.update_metrics()
    return cross_market.get_market_bias()


async def find_correlation_trades() -> List[Dict]:
    """Найти сделки на основе корреляций/дивергенций"""
    # Обновляем корреляции для основных монет
    coins = COIN_CATEGORIES['layer1'] + COIN_CATEGORIES['defi'][:5]
    await correlation_analyzer.update_correlations(coins)
    
    trades = []
    
    # Ищем дивергенции
    for i, coin1 in enumerate(coins):
        for coin2 in coins[i+1:]:
            div = correlation_analyzer.find_divergence(coin1, coin2)
            if div and div.get('signal'):
                trades.append(div)
    
    return trades
