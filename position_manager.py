"""
Advanced Position Management - Улучшенное управление позициями
Volatility-based sizing, correlation checks, adaptive management
"""

import logging
from typing import Dict, List, Optional, Tuple
from advanced_signals import correlation_analyzer

logger = logging.getLogger(__name__)


def calculate_volatility_based_size(balance: float, atr: float, entry: float, 
                                   risk_percent: float = 1.0,
                                   max_position_percent: float = 15.0) -> float:
    """
    Расчёт размера позиции на основе волатильности
    
    Args:
        balance: Баланс пользователя
        atr: Average True Range
        entry: Цена входа
        risk_percent: Процент риска от баланса (по умолчанию 1%)
        max_position_percent: Максимальный размер позиции от баланса (по умолчанию 15%)
    
    Returns:
        Размер позиции в USD
    """
    # Базовый риск: 1% от баланса
    risk_amount = balance * (risk_percent / 100)
    
    # Расстояние до SL = 1.5 ATR (стандартное)
    sl_distance = atr * 1.5
    
    # Размер позиции = риск / расстояние до SL
    position_size = risk_amount / sl_distance * entry
    
    # Ограничение: не более 15% баланса
    max_size = balance * (max_position_percent / 100)
    position_size = min(position_size, max_size)
    
    # В высоковолатильных условиях уменьшаем размер
    volatility_ratio = atr / entry
    if volatility_ratio > 0.03:  # >3% волатильность
        position_size *= 0.7  # Уменьшаем на 30%
        logger.info(f"[SIZE] High volatility ({volatility_ratio:.2%}), reducing size by 30%")
    elif volatility_ratio > 0.02:  # >2% волатильность
        position_size *= 0.85  # Уменьшаем на 15%
        logger.info(f"[SIZE] Medium volatility ({volatility_ratio:.2%}), reducing size by 15%")
    
    # Минимальный размер позиции
    min_size = 10.0  # $10 минимум
    position_size = max(position_size, min_size)
    
    logger.info(f"[SIZE] Calculated position size: ${position_size:.2f} (risk: {risk_percent}%, volatility: {volatility_ratio:.2%})")
    
    return position_size


def check_correlation_risk(user_positions: List[Dict], new_symbol: str, 
                          new_direction: str, user_balance: float,
                          correlation_threshold: float = 0.7,
                          max_exposure_percent: float = 30.0) -> Tuple[bool, str]:
    """
    Проверка риска корреляции перед открытием новой позиции
    
    Args:
        user_positions: Существующие позиции пользователя
        new_symbol: Новый символ (например, "BTC/USDT")
        new_direction: Направление новой позиции ("LONG" или "SHORT")
        user_balance: Баланс пользователя
        correlation_threshold: Порог корреляции (0.7 = 70%)
        max_exposure_percent: Максимальная экспозиция в одном направлении (%)
    
    Returns:
        (is_safe, reason)
    """
    if not user_positions:
        return True, "OK"
    
    # Извлекаем базовый символ (BTC из BTC/USDT)
    new_base = new_symbol.split('/')[0] if '/' in new_symbol else new_symbol.replace('USDT', '')
    
    # Проверяем корреляцию с существующими позициями
    correlated_positions = []
    total_exposure_same_direction = 0.0
    
    for pos in user_positions:
        existing_symbol = pos['symbol']
        existing_base = existing_symbol.split('/')[0] if '/' in existing_symbol else existing_symbol.replace('USDT', '')
        existing_direction = pos['direction']
        existing_amount = pos.get('amount', 0)
        
        # Если одинаковое направление - считаем экспозицию
        if existing_direction == new_direction:
            total_exposure_same_direction += existing_amount
        
        # Получаем корреляцию (если доступна)
        try:
            if hasattr(correlation_analyzer, 'correlation_matrix'):
                corr = correlation_analyzer.correlation_matrix.get(existing_base, {}).get(new_base, 0)
                
                # Если высокая корреляция
                if abs(corr) > correlation_threshold:
                    correlated_positions.append({
                        'symbol': existing_symbol,
                        'correlation': corr,
                        'direction': existing_direction,
                        'amount': existing_amount
                    })
        except Exception as e:
            logger.warning(f"[CORR] Error checking correlation: {e}")
    
    # Проверка 1: Высокая корреляция + одинаковое направление
    high_corr_same_dir = [p for p in correlated_positions if p['direction'] == new_direction]
    if high_corr_same_dir:
        total_correlated = sum(p['amount'] for p in high_corr_same_dir)
        if total_correlated > user_balance * 0.2:  # >20% баланса в коррелированных
            return False, f"High correlation risk: {len(high_corr_same_dir)} correlated positions in same direction (${total_correlated:.0f})"
    
    # Проверка 2: Общая экспозиция в одном направлении
    if total_exposure_same_direction > user_balance * (max_exposure_percent / 100):
        return False, f"Max exposure exceeded: ${total_exposure_same_direction:.0f} ({total_exposure_same_direction/user_balance*100:.0f}%) in {new_direction}"
    
    # Проверка 3: Противоположные коррелированные позиции (хедж)
    high_corr_opposite = [p for p in correlated_positions 
                         if p['direction'] != new_direction and abs(p['correlation']) > 0.8]
    if high_corr_opposite:
        # Это может быть хедж - разрешаем, но предупреждаем
        logger.info(f"[CORR] Opening opposite position to {len(high_corr_opposite)} correlated positions (hedge)")
    
    return True, "OK"


def calculate_partial_close_amount(entry: float, current: float, tp1: float, 
                                  direction: str, total_amount: float,
                                  close_percent: float = 0.30,  # УВЕЛИЧЕНО с 25% до 30%
                                  progress_threshold: float = 0.5) -> float:
    """
    Рассчитать размер частичного закрытия на полпути к TP v2.0
    
    УЛУЧШЕНО:
    - Увеличен процент закрытия с 25% до 30% для лучшей защиты прибыли
    - Расширен диапазон срабатывания
    
    Args:
        entry: Цена входа
        current: Текущая цена
        tp1: Первый Take Profit
        direction: Направление позиции
        total_amount: Общий размер позиции
        close_percent: Процент позиции для закрытия (30% по умолчанию - УВЕЛИЧЕНО)
        progress_threshold: Порог прогресса к TP (50% по умолчанию)
    
    Returns:
        Размер для частичного закрытия (0 если не нужно закрывать)
    """
    if direction == "LONG":
        total_distance = tp1 - entry
        current_distance = current - entry
    else:  # SHORT
        total_distance = entry - tp1
        current_distance = entry - current
    
    if total_distance <= 0:
        return 0.0
    
    progress = current_distance / total_distance
    
    # Закрываем на 40-60% пути к TP (расширен диапазон с 45-55%)
    if progress_threshold - 0.10 <= progress <= progress_threshold + 0.10:
        close_amount = total_amount * close_percent
        logger.info(f"[PARTIAL] Closing {close_percent:.0%} at {progress:.0%} progress to TP1")
        return close_amount
    
    return 0.0


def should_scale_in(entry: float, current: float, direction: str, 
                   atr: float, scale_distance: float = 0.5) -> bool:
    """
    Проверить, стоит ли добавлять к позиции (scaling in)
    
    Условия:
    - Позиция в небольшом минусе (<1%)
    - Цена откатилась к ключевому уровню
    - Не добавляем к убыточной позиции (>1.5%)
    
    Args:
        entry: Цена входа
        current: Текущая цена
        direction: Направление позиции
        atr: ATR
        scale_distance: Расстояние для добавления (в ATR)
    
    Returns:
        True если можно добавлять
    """
    if direction == "LONG":
        pnl_percent = (current - entry) / entry * 100
        # Можно добавлять если:
        # 1. В небольшом минусе (<1%)
        # 2. Или в небольшом плюсе (<0.5%)
        if -1.0 <= pnl_percent <= 0.5:
            # Проверяем расстояние от входа
            distance = abs(current - entry) / entry
            if distance <= atr * scale_distance / entry:
                return True
    else:  # SHORT
        pnl_percent = (entry - current) / entry * 100
        if -1.0 <= pnl_percent <= 0.5:
            distance = abs(entry - current) / entry
            if distance <= atr * scale_distance / entry:
                return True
    
    return False
