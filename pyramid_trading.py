"""
Pyramid Trading - Добавление к прибыльным позициям при сильном тренде
Увеличиваем размер при подтверждении движения
"""

import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def analyze_pyramid_opportunity(
    pos: Dict,
    current_price: float,
    market_regime: str,
    mtf_aligned: bool,
    volume_data: Dict,
    atr: float,
    order_blocks: List,
    exhaustion_signals: List
) -> Dict:
    """
    Анализ возможности pyramid trading (добавление к прибыльной позиции)
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        market_regime: Режим рынка
        mtf_aligned: MTF alignment
        volume_data: Данные об объёме
        atr: ATR
        order_blocks: Order blocks
        exhaustion_signals: Признаки истощения
    
    Returns:
        {
            'should_pyramid': bool,
            'confidence': float (0-1),
            'reasoning': List[str],
            'size_multiplier': float (0.3-0.5),
            'new_sl': float
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
    if pnl_percent < 2.0:
        return {
            'should_pyramid': False,
            'confidence': 0.0,
            'reasoning': [f"PnL недостаточен для pyramid ({pnl_percent:.2f}% < 2%)"],
            'size_multiplier': 0.0,
            'new_sl': None
        }
    
    score = 0.0
    max_score = 0.0
    reasoning = []
    
    # 1. Сильный тренд (25 баллов)
    max_score += 25
    is_strong_trend = False
    if direction == "LONG":
        if market_regime == 'STRONG_UPTREND':
            score += 25
            is_strong_trend = True
            reasoning.append("Сильный восходящий тренд")
        elif market_regime == 'UPTREND':
            score += 15
            reasoning.append("Восходящий тренд")
        else:
            score -= 10
            reasoning.append("⚠️ Не сильный тренд")
    else:  # SHORT
        if market_regime == 'STRONG_DOWNTREND':
            score += 25
            is_strong_trend = True
            reasoning.append("Сильный нисходящий тренд")
        elif market_regime == 'DOWNTREND':
            score += 15
            reasoning.append("Нисходящий тренд")
        else:
            score -= 10
            reasoning.append("⚠️ Не сильный тренд")
    
    # 2. MTF Alignment (20 баллов)
    max_score += 20
    if mtf_aligned:
        score += 20
        reasoning.append("MTF полностью выровнены")
    else:
        score += 5
        reasoning.append("MTF частично выровнены")
    
    # 3. Объём растёт (20 баллов)
    max_score += 20
    volume_ratio = volume_data.get('ratio', 1.0)
    volume_trend = volume_data.get('trend', 'neutral')
    
    if volume_ratio > 1.5 and volume_trend == 'increasing':
        score += 20
        reasoning.append(f"Объём сильно растёт ({volume_ratio:.2f}x, тренд вверх)")
    elif volume_ratio > 1.2:
        score += 12
        reasoning.append(f"Объём увеличен ({volume_ratio:.2f}x)")
    elif volume_ratio > 0.8:
        score += 5
        reasoning.append(f"Объём нормальный ({volume_ratio:.2f}x)")
    else:
        score -= 10
        reasoning.append(f"⚠️ Низкий объём ({volume_ratio:.2f}x)")
    
    # 4. Нет признаков истощения (25 баллов)
    max_score += 25
    has_exhaustion = len(exhaustion_signals) > 0
    
    if has_exhaustion:
        score -= 25
        reasoning.append(f"⚠️ Признаки истощения: {', '.join(exhaustion_signals)}")
    else:
        score += 25
        reasoning.append("Нет признаков истощения")
    
    # 5. Скорость движения (10 баллов)
    max_score += 10
    # Рассчитываем скорость движения за последние свечи
    if len(pos.get('price_history', [])) >= 5:
        recent_prices = pos['price_history'][-5:]
        if direction == "LONG":
            price_change = (recent_prices[-1] - recent_prices[0]) / recent_prices[0] * 100
        else:
            price_change = (recent_prices[0] - recent_prices[-1]) / recent_prices[0] * 100
        
        # Нормализуем по ATR
        speed_atr = price_change / (atr / current_price * 100) if atr > 0 else 0
        
        if speed_atr > 2.0:  # Очень быстрое движение
            score += 10
            reasoning.append(f"Быстрое движение ({price_change:.2f}% за 5 свечей)")
        elif speed_atr > 1.0:
            score += 5
            reasoning.append(f"Умеренное движение ({price_change:.2f}%)")
        else:
            score += 2
            reasoning.append(f"Медленное движение ({price_change:.2f}%)")
    else:
        score += 5  # Недостаточно данных
    
    # Нормализуем score
    confidence = max(0.0, min(1.0, score / max_score)) if max_score > 0 else 0.0
    
    # Определяем размер pyramid
    if confidence >= 0.75 and is_strong_trend and not has_exhaustion:
        size_multiplier = 0.5  # 50% от первоначальной позиции
    elif confidence >= 0.6:
        size_multiplier = 0.3  # 30% от первоначальной позиции
    else:
        size_multiplier = 0.0
    
    # Рассчитываем новый SL для pyramid позиции
    # SL = предыдущий TP уровень (TP1)
    new_sl = None
    if direction == "LONG":
        tp1 = pos.get('tp1', pos.get('tp', current_price * 1.02))
        new_sl = tp1 * 0.998  # Немного ниже TP1
    else:
        tp1 = pos.get('tp1', pos.get('tp', current_price * 0.98))
        new_sl = tp1 * 1.002  # Немного выше TP1
    
    should_pyramid = confidence >= 0.6 and not has_exhaustion
    
    return {
        'should_pyramid': should_pyramid,
        'confidence': confidence,
        'reasoning': reasoning,
        'size_multiplier': size_multiplier,
        'new_sl': new_sl,
        'score': score,
        'max_score': max_score
    }


def calculate_pyramid_size(
    pos: Dict,
    pyramid_opportunity: Dict,
    user_balance: float,
    min_size: float = 10.0
) -> float:
    """
    Рассчитать размер pyramid позиции
    
    Args:
        pos: Данные позиции
        pyramid_opportunity: Результат analyze_pyramid_opportunity
        user_balance: Баланс пользователя
        min_size: Минимальный размер
    
    Returns:
        Размер в USD для pyramid позиции
    """
    if not pyramid_opportunity['should_pyramid']:
        return 0.0
    
    original_amount = pos.get('original_amount', pos['amount'])
    multiplier = pyramid_opportunity['size_multiplier']
    
    # Размер = процент от первоначальной позиции
    pyramid_size = original_amount * multiplier
    
    # Ограничения
    # Не более 25% баланса на pyramid
    max_size = user_balance * 0.25
    pyramid_size = min(pyramid_size, max_size)
    
    # Минимальный размер
    pyramid_size = max(pyramid_size, min_size)
    
    # Не больше чем осталось баланса
    pyramid_size = min(pyramid_size, user_balance * 0.9)
    
    logger.info(f"[PYRAMID] Calculated pyramid size: ${pyramid_size:.2f} (multiplier: {multiplier:.0%}, confidence: {pyramid_opportunity['confidence']:.0%})")
    
    return pyramid_size


def should_pyramid(
    pos: Dict,
    current_price: float,
    analysis_data: Dict
) -> Tuple[bool, Dict]:
    """
    Упрощённая проверка возможности pyramid
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        analysis_data: Данные анализа
    
    Returns:
        (should_pyramid, opportunity_data)
    """
    opportunity = analyze_pyramid_opportunity(
        pos=pos,
        current_price=current_price,
        market_regime=analysis_data.get('market_regime', 'UNKNOWN'),
        mtf_aligned=analysis_data.get('mtf_aligned', False),
        volume_data=analysis_data.get('volume_data', {}),
        atr=analysis_data.get('atr', 0),
        order_blocks=analysis_data.get('order_blocks', []),
        exhaustion_signals=analysis_data.get('exhaustion_signals', [])
    )
    
    return opportunity['should_pyramid'], opportunity
