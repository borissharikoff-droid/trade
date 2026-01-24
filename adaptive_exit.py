"""
Adaptive Exit Strategies - Адаптивные стратегии выхода
Ранний выход, динамическое изменение TP, детекция разворота
"""

import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def detect_reversal_signals(
    pos: Dict,
    current_price: float,
    divergence: Dict,
    exhaustion_patterns: List,
    volume_data: Dict,
    key_levels: List,
    direction: str
) -> Dict:
    """
    Детекция признаков разворота против позиции
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        divergence: Данные о дивергенции
        exhaustion_patterns: Паттерны истощения
        volume_data: Данные об объёме
        key_levels: Ключевые уровни
        direction: Направление позиции
    
    Returns:
        {
            'reversal_risk': bool,
            'confidence': float (0-1),
            'signals': List[str],
            'recommended_action': str ('close_50', 'close_100', 'none')
        }
    """
    signals = []
    score = 0.0
    max_score = 0.0
    
    # 1. Дивергенция против нас (30 баллов)
    max_score += 30
    if divergence.get('found', False):
        div_type = divergence.get('type', '')
        if direction == "LONG" and 'bearish' in div_type:
            score += 30
            signals.append(f"Bearish дивергенция ({div_type})")
        elif direction == "SHORT" and 'bullish' in div_type:
            score += 30
            signals.append(f"Bullish дивергенция ({div_type})")
    
    # 2. Exhaustion patterns (25 баллов)
    max_score += 25
    if exhaustion_patterns:
        score += 25
        signals.append(f"Паттерны истощения: {', '.join(exhaustion_patterns)}")
    
    # 3. Резкое снижение объёма (20 баллов)
    max_score += 20
    volume_ratio = volume_data.get('ratio', 1.0)
    volume_trend = volume_data.get('trend', 'neutral')
    
    if volume_ratio < 0.5 and volume_trend == 'decreasing':
        score += 20
        signals.append(f"Резкое снижение объёма ({volume_ratio:.2f}x)")
    elif volume_ratio < 0.7:
        score += 10
        signals.append(f"Низкий объём ({volume_ratio:.2f}x)")
    
    # 4. Пробой ключевого уровня против нас (25 баллов)
    max_score += 25
    if direction == "LONG":
        # Ищем пробой поддержки
        supports = [l for l in key_levels if l.type == 'support' and l.price < current_price]
        if supports:
            closest_support = max(supports, key=lambda x: x.price)
            if current_price < closest_support.price * 0.998:  # Пробой на 0.2%
                score += 25
                signals.append(f"Пробой поддержки {closest_support.price:.4f}")
    else:  # SHORT
        # Ищем пробой сопротивления
        resistances = [l for l in key_levels if l.type == 'resistance' and l.price > current_price]
        if resistances:
            closest_resistance = min(resistances, key=lambda x: x.price)
            if current_price > closest_resistance.price * 1.002:  # Пробой на 0.2%
                score += 25
                signals.append(f"Пробой сопротивления {closest_resistance.price:.4f}")
    
    # Нормализуем
    confidence = max(0.0, min(1.0, score / max_score)) if max_score > 0 else 0.0
    
    # Определяем действие
    if confidence >= 0.7:
        recommended_action = 'close_100'  # Закрыть полностью
    elif confidence >= 0.5:
        recommended_action = 'close_50'   # Закрыть 50%
    else:
        recommended_action = 'none'
    
    reversal_risk = confidence >= 0.5
    
    return {
        'reversal_risk': reversal_risk,
        'confidence': confidence,
        'signals': signals,
        'recommended_action': recommended_action,
        'score': score
    }


def adjust_tp_dynamically(
    pos: Dict,
    current_price: float,
    original_tp1: float,
    original_tp2: float,
    original_tp3: float,
    atr: float,
    volume_data: Dict,
    market_regime: str,
    movement_speed: float
) -> Dict:
    """
    Динамическое изменение TP на основе силы движения
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        original_tp1, tp2, tp3: Исходные TP уровни
        atr: ATR
        volume_data: Данные об объёме
        market_regime: Режим рынка
        movement_speed: Скорость движения (в ATR единицах)
    
    Returns:
        {
            'tp1': float,
            'tp2': float,
            'tp3': float,
            'adjusted': bool,
            'reasoning': str
        }
    """
    direction = pos['direction']
    entry = pos['entry']
    
    # Рассчитываем прогресс к TP1
    if direction == "LONG":
        progress_to_tp1 = (current_price - entry) / (original_tp1 - entry) if original_tp1 > entry else 0
    else:
        progress_to_tp1 = (entry - current_price) / (entry - original_tp1) if original_tp1 < entry else 0
    
    # Если движение очень сильное и быстрое
    volume_ratio = volume_data.get('ratio', 1.0)
    is_strong_trend = market_regime in ['STRONG_UPTREND', 'STRONG_DOWNTREND']
    
    # Факторы для увеличения TP
    speed_factor = min(2.0, movement_speed / 1.0)  # До 2x если движение очень быстрое
    volume_factor = min(1.5, volume_ratio / 1.5)  # До 1.5x если объём высокий
    trend_factor = 1.2 if is_strong_trend else 1.0
    
    adjustment_multiplier = min(1.5, (speed_factor * 0.4 + volume_factor * 0.3 + trend_factor * 0.3))
    
    # Если движение слабое
    if movement_speed < 0.5 and volume_ratio < 0.8:
        adjustment_multiplier = 0.9  # Немного уменьшаем TP
        reasoning = "Слабое движение - уменьшаем TP"
    elif adjustment_multiplier > 1.1:
        reasoning = f"Сильное движение - увеличиваем TP ({adjustment_multiplier:.2f}x)"
    else:
        adjustment_multiplier = 1.0
        reasoning = "Движение нормальное - TP без изменений"
    
    # Применяем изменения
    if direction == "LONG":
        distance_tp1 = original_tp1 - entry
        distance_tp2 = original_tp2 - entry
        distance_tp3 = original_tp3 - entry
        
        new_tp1 = entry + distance_tp1 * adjustment_multiplier
        new_tp2 = entry + distance_tp2 * adjustment_multiplier
        new_tp3 = entry + distance_tp3 * adjustment_multiplier
    else:
        distance_tp1 = entry - original_tp1
        distance_tp2 = entry - original_tp2
        distance_tp3 = entry - original_tp3
        
        new_tp1 = entry - distance_tp1 * adjustment_multiplier
        new_tp2 = entry - distance_tp2 * adjustment_multiplier
        new_tp3 = entry - distance_tp3 * adjustment_multiplier
    
    adjusted = abs(adjustment_multiplier - 1.0) > 0.05
    
    return {
        'tp1': new_tp1,
        'tp2': new_tp2,
        'tp3': new_tp3,
        'adjusted': adjusted,
        'reasoning': reasoning,
        'multiplier': adjustment_multiplier
    }


def should_exit_early(
    pos: Dict,
    current_price: float,
    reversal_signals: Dict,
    pnl_percent: float
) -> Tuple[bool, str, float]:
    """
    Проверка необходимости раннего выхода v2.0
    
    УЛУЧШЕНО:
    - Более агрессивные пороги для защиты прибыли
    - Добавлен вариант close_30 для частичного выхода
    - Улучшенная логика при разных уровнях PnL
    
    Args:
        pos: Данные позиции
        current_price: Текущая цена
        reversal_signals: Результат detect_reversal_signals
        pnl_percent: Текущий PnL в процентах
    
    Returns:
        (should_exit, action, close_percent)
        action: 'close_30', 'close_50', 'close_100', 'none'
        close_percent: 0.3, 0.5, 1.0, 0.0
    """
    if not reversal_signals['reversal_risk']:
        return False, 'none', 0.0
    
    action = reversal_signals['recommended_action']
    confidence = reversal_signals['confidence']
    
    # === УЛУЧШЕННАЯ ЛОГИКА ВЫХОДА ===
    
    # Если позиция в хорошем плюсе (>2%) - более агрессивный выход
    if pnl_percent > 2.0:
        if confidence >= 0.50:  # Снижен порог с 0.6
            return True, action, 1.0 if action == 'close_100' else 0.5
    
    # Если позиция в небольшом плюсе (0-2%)
    elif pnl_percent > 0:
        if confidence >= 0.55:  # Снижен с 0.6
            # Частичный выход для защиты прибыли
            if confidence >= 0.70:
                return True, 'close_100', 1.0  # Закрыть всё
            elif confidence >= 0.60:
                return True, 'close_50', 0.5   # Закрыть 50%
            else:
                return True, 'close_30', 0.3   # Закрыть 30%
    
    # Если в небольшом минусе (>-1%) - даём шанс
    elif pnl_percent > -1.0:
        if confidence >= 0.70:  # Снижен с 0.75
            return True, 'close_50', 0.5  # Закрыть 50%
    
    # Если в минусе (<-1%) - более консервативный
    else:
        if confidence >= 0.75:
            return True, action, 1.0 if action == 'close_100' else 0.5
    
    return False, 'none', 0.0


def get_adaptive_tp_multiplier(
    movement_speed: float,
    volume_ratio: float,
    market_regime: str
) -> float:
    """
    Получить адаптивный множитель для TP на основе силы движения
    
    Args:
        movement_speed: Скорость движения (в ATR за час)
        volume_ratio: Отношение текущего объёма к среднему
        market_regime: Режим рынка (STRONG_UPTREND, UPTREND, etc.)
    
    Returns:
        Множитель для TP (0.9 - 1.5)
    """
    multiplier = 1.0
    
    # Скорость движения
    if movement_speed > 2.0:  # Очень быстрое движение
        multiplier += 0.20  # +20% к TP
    elif movement_speed > 1.0:  # Быстрое движение
        multiplier += 0.10  # +10% к TP
    elif movement_speed < 0.3:  # Очень медленное
        multiplier -= 0.10  # -10% к TP
    
    # Объём
    if volume_ratio > 2.0:  # Высокий объём
        multiplier += 0.10  # +10% к TP
    elif volume_ratio > 1.5:
        multiplier += 0.05  # +5% к TP
    elif volume_ratio < 0.5:  # Низкий объём
        multiplier -= 0.10  # -10% к TP
    
    # Режим рынка
    if market_regime in ['STRONG_UPTREND', 'STRONG_DOWNTREND']:
        multiplier += 0.15  # В сильном тренде увеличиваем TP
    elif market_regime == 'HIGH_VOLATILITY':
        multiplier -= 0.15  # В волатильности уменьшаем TP
    
    # Ограничиваем множитель
    return max(0.85, min(1.5, multiplier))
