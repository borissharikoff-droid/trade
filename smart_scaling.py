"""
Smart Scaling In - Умное добавление к позициям на основе комплексного анализа
Не просто по расстоянию, а с проверкой всех факторов успеха
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


def analyze_scaling_opportunity(
    pos: Dict,
    current_price: float,
    klines: List,
    key_levels: List,
    swings: List,
    market_regime: str,
    rsi: float,
    volume_data: Dict,
    mtf_aligned: bool,
    order_blocks: List,
    fvgs: List,
    divergence: Dict
) -> Dict:
    """
    Комплексный анализ возможности добавления к позиции
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        klines: Свечи для анализа
        key_levels: Ключевые уровни поддержки/сопротивления
        swings: Swing points
        market_regime: Режим рынка
        rsi: Текущий RSI
        volume_data: Данные об объёме
        mtf_aligned: MTF alignment
        order_blocks: Order blocks
        fvgs: Fair Value Gaps
        divergence: Данные о дивергенции
    
    Returns:
        {
            'should_scale': bool,
            'confidence': float (0-1),
            'reasoning': List[str],
            'size_multiplier': float (0.3-0.5)
        }
    """
    direction = pos['direction']
    entry = pos['entry']
    
    # Рассчитываем PnL%
    if direction == "LONG":
        pnl_percent = (current_price - entry) / entry * 100
    else:
        pnl_percent = (entry - current_price) / entry * 100
    
    # Базовые проверки
    if pnl_percent < -2.0 or pnl_percent > 0.5:
        return {
            'should_scale': False,
            'confidence': 0.0,
            'reasoning': [f"PnL вне диапазона для scaling ({pnl_percent:.2f}%)"],
            'size_multiplier': 0.0
        }
    
    score = 0.0
    max_score = 0.0
    reasoning = []
    
    # 1. Проверка ключевого уровня (20 баллов)
    max_score += 20
    at_key_level = False
    if direction == "LONG":
        # Ищем поддержку ниже текущей цены
        supports = [l for l in key_levels if l.type == 'support' and l.price < current_price]
        if supports:
            closest_support = max(supports, key=lambda x: x.price)
            distance_to_support = abs(current_price - closest_support.price) / current_price * 100
            if distance_to_support < 0.3:  # В пределах 0.3% от поддержки
                score += 20
                at_key_level = True
                reasoning.append(f"У ключевой поддержки ({closest_support.price:.4f})")
    else:  # SHORT
        # Ищем сопротивление выше текущей цены
        resistances = [l for l in key_levels if l.type == 'resistance' and l.price > current_price]
        if resistances:
            closest_resistance = min(resistances, key=lambda x: x.price)
            distance_to_resistance = abs(current_price - closest_resistance.price) / current_price * 100
            if distance_to_resistance < 0.3:
                score += 20
                at_key_level = True
                reasoning.append(f"У ключевого сопротивления ({closest_resistance.price:.4f})")
    
    # 2. Order Block или FVG зона (15 баллов)
    max_score += 15
    at_smc_zone = False
    if direction == "LONG":
        # Ищем bullish order block или FVG
        for ob in order_blocks:
            if ob.type == 'bullish' and ob.price_low <= current_price <= ob.price_high:
                score += 15
                at_smc_zone = True
                reasoning.append(f"В Bullish Order Block зоне")
                break
        
        if not at_smc_zone:
            for fvg in fvgs:
                if fvg.type == 'bullish' and fvg.low <= current_price <= fvg.high:
                    score += 10
                    at_smc_zone = True
                    reasoning.append(f"В Bullish FVG зоне")
                    break
    else:  # SHORT
        for ob in order_blocks:
            if ob.type == 'bearish' and ob.price_low <= current_price <= ob.price_high:
                score += 15
                at_smc_zone = True
                reasoning.append(f"В Bearish Order Block зоне")
                break
        
        if not at_smc_zone:
            for fvg in fvgs:
                if fvg.type == 'bearish' and fvg.low <= current_price <= fvg.high:
                    score += 10
                    at_smc_zone = True
                    reasoning.append(f"В Bearish FVG зоне")
                    break
    
    # 3. MTF Alignment (15 баллов)
    max_score += 15
    if mtf_aligned:
        score += 15
        reasoning.append("MTF alignment подтверждает направление")
    else:
        score += 5  # Частичные баллы если не полностью выровнено
        reasoning.append("MTF частично выровнены")
    
    # 4. Тренд в нашу сторону (15 баллов)
    max_score += 15
    if direction == "LONG":
        if market_regime in ['STRONG_UPTREND', 'UPTREND']:
            score += 15
            reasoning.append("Восходящий тренд")
        elif market_regime == 'RANGING':
            score += 8
            reasoning.append("Рейндж - умеренный сигнал")
        else:
            score -= 10
            reasoning.append("⚠️ Нисходящий тренд - против нас")
    else:  # SHORT
        if market_regime in ['STRONG_DOWNTREND', 'DOWNTREND']:
            score += 15
            reasoning.append("Нисходящий тренд")
        elif market_regime == 'RANGING':
            score += 8
            reasoning.append("Рейндж - умеренный сигнал")
        else:
            score -= 10
            reasoning.append("⚠️ Восходящий тренд - против нас")
    
    # 5. RSI не в экстремуме противоположного направления (10 баллов)
    max_score += 10
    if direction == "LONG":
        if rsi < 70:  # Не перекуплен
            if rsi < 40:  # Перепродан - хороший сигнал
                score += 10
                reasoning.append(f"RSI перепродан ({rsi:.0f}) - хороший вход")
            else:
                score += 5
                reasoning.append(f"RSI нормальный ({rsi:.0f})")
        else:
            score -= 5
            reasoning.append(f"⚠️ RSI перекуплен ({rsi:.0f})")
    else:  # SHORT
        if rsi > 30:  # Не перепродан
            if rsi > 60:  # Перекуплен - хороший сигнал
                score += 10
                reasoning.append(f"RSI перекуплен ({rsi:.0f}) - хороший вход")
            else:
                score += 5
                reasoning.append(f"RSI нормальный ({rsi:.0f})")
        else:
            score -= 5
            reasoning.append(f"⚠️ RSI перепродан ({rsi:.0f})")
    
    # 6. Объём подтверждает (10 баллов)
    max_score += 10
    volume_ratio = volume_data.get('ratio', 1.0)
    if volume_ratio > 1.2:  # Объём выше среднего
        score += 10
        reasoning.append(f"Объём увеличен ({volume_ratio:.2f}x)")
    elif volume_ratio > 0.8:
        score += 5
        reasoning.append(f"Объём нормальный ({volume_ratio:.2f}x)")
    else:
        score -= 5
        reasoning.append(f"⚠️ Низкий объём ({volume_ratio:.2f}x)")
    
    # 7. Нет признаков разворота (15 баллов)
    max_score += 15
    reversal_risk = False
    
    # Проверка дивергенции против нас
    if divergence.get('found', False):
        div_type = divergence.get('type', '')
        if direction == "LONG" and 'bearish' in div_type:
            score -= 15
            reversal_risk = True
            reasoning.append("⚠️ Bearish дивергенция - признак разворота")
        elif direction == "SHORT" and 'bullish' in div_type:
            score -= 15
            reversal_risk = True
            reasoning.append("⚠️ Bullish дивергенция - признак разворота")
    
    if not reversal_risk:
        score += 15
        reasoning.append("Нет признаков разворота")
    
    # Нормализуем score
    confidence = max(0.0, min(1.0, score / max_score)) if max_score > 0 else 0.0
    
    # Определяем размер добавления на основе confidence
    if confidence >= 0.7:
        size_multiplier = 0.5  # 50% от первоначальной позиции
    elif confidence >= 0.5:
        size_multiplier = 0.3  # 30% от первоначальной позиции
    else:
        size_multiplier = 0.0  # Не добавляем
    
    should_scale = confidence >= 0.5 and not reversal_risk
    
    return {
        'should_scale': should_scale,
        'confidence': confidence,
        'reasoning': reasoning,
        'size_multiplier': size_multiplier,
        'score': score,
        'max_score': max_score
    }


def calculate_scale_in_size(
    pos: Dict,
    scaling_opportunity: Dict,
    user_balance: float,
    min_size: float = 10.0
) -> float:
    """
    Рассчитать размер добавления к позиции
    
    Args:
        pos: Данные позиции
        scaling_opportunity: Результат analyze_scaling_opportunity
        user_balance: Баланс пользователя
        min_size: Минимальный размер
    
    Returns:
        Размер в USD для добавления
    """
    if not scaling_opportunity['should_scale']:
        return 0.0
    
    original_amount = pos.get('original_amount', pos['amount'])
    multiplier = scaling_opportunity['size_multiplier']
    
    # Размер = процент от первоначальной позиции
    scale_size = original_amount * multiplier
    
    # Ограничения
    # Не более 20% баланса на одно добавление
    max_size = user_balance * 0.2
    scale_size = min(scale_size, max_size)
    
    # Минимальный размер
    scale_size = max(scale_size, min_size)
    
    # Не больше чем осталось баланса
    scale_size = min(scale_size, user_balance * 0.9)
    
    logger.info(f"[SCALE] Calculated scale size: ${scale_size:.2f} (multiplier: {multiplier:.0%}, confidence: {scaling_opportunity['confidence']:.0%})")
    
    return scale_size


def should_scale_in_smart(
    pos: Dict,
    current_price: float,
    analysis_data: Dict
) -> Tuple[bool, Dict]:
    """
    Упрощённая проверка возможности scaling in
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        analysis_data: Данные анализа (ключевые уровни, RSI, объём и т.д.)
    
    Returns:
        (should_scale, opportunity_data)
    """
    opportunity = analyze_scaling_opportunity(
        pos=pos,
        current_price=current_price,
        klines=analysis_data.get('klines', []),
        key_levels=analysis_data.get('key_levels', []),
        swings=analysis_data.get('swings', []),
        market_regime=analysis_data.get('market_regime', 'UNKNOWN'),
        rsi=analysis_data.get('rsi', 50),
        volume_data=analysis_data.get('volume_data', {}),
        mtf_aligned=analysis_data.get('mtf_aligned', False),
        order_blocks=analysis_data.get('order_blocks', []),
        fvgs=analysis_data.get('fvgs', []),
        divergence=analysis_data.get('divergence', {})
    )
    
    return opportunity['should_scale'], opportunity
