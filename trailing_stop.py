"""
Trailing Stop Manager - Защита прибыли и минимизация убытков
Адаптивные трейлинг-стопы на основе ATR и структуры рынка
"""

import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class TrailingStop:
    """Трейлинг-стоп для одной позиции - enhanced with step-based logic"""
    
    def __init__(self, pos_id: int, entry: float, direction: str, atr: float, initial_sl: float,
                 tp1: float = None, tp2: float = None, tp3: float = None):
        self.pos_id = pos_id
        self.entry = entry
        self.direction = direction
        self.atr = atr
        self.stop_loss = initial_sl
        self.breakeven_triggered = False
        self.trailing_active = False
        self.highest_price = entry if direction == "LONG" else None
        self.lowest_price = entry if direction == "SHORT" else None
        self.last_update = datetime.now()
        # Step targets for step-based trailing
        self.tp1 = tp1
        self.tp2 = tp2
        self.tp3 = tp3
        self.tp1_reached = False
        self.tp2_reached = False
        
    def update(self, current_price: float) -> Optional[float]:
        """
        Обновить трейлинг-стоп
        
        Returns:
            Новый уровень стоп-лосса или None если не изменился
        """
        old_sl = self.stop_loss
        
        # Рассчитываем PnL%
        if self.direction == "LONG":
            pnl_percent = (current_price - self.entry) / self.entry * 100
            if self.highest_price is None or current_price > self.highest_price:
                self.highest_price = current_price
        else:  # SHORT
            pnl_percent = (self.entry - current_price) / self.entry * 100
            if self.lowest_price is None or current_price < self.lowest_price:
                self.lowest_price = current_price
        
        # === STEP-BASED TRAILING (primary - if TP levels provided) ===
        if self.tp1 and self.tp2:
            # Check TP2 reached
            tp2_reached_now = (self.direction == "LONG" and current_price >= self.tp2) or \
                              (self.direction == "SHORT" and current_price <= self.tp2)
            if tp2_reached_now and not self.tp2_reached:
                self.tp2_reached = True
                self.tp1_reached = True
                self.breakeven_triggered = True
                self.trailing_active = True
                # Move SL to TP1 level (lock substantial profit)
                self.stop_loss = self.tp1
                logger.info(f"[TRAIL] Position {self.pos_id}: TP2 hit! SL moved to TP1 ({self.tp1:.4f})")
                return self.stop_loss
            
            # Check TP1 reached
            tp1_reached_now = (self.direction == "LONG" and current_price >= self.tp1) or \
                              (self.direction == "SHORT" and current_price <= self.tp1)
            if tp1_reached_now and not self.tp1_reached:
                self.tp1_reached = True
                self.breakeven_triggered = True
                self.trailing_active = True
                # Move SL to breakeven + small profit (lock entry)
                buffer = self.atr * 0.2
                if self.direction == "LONG":
                    self.stop_loss = max(self.stop_loss, self.entry + buffer)
                else:
                    self.stop_loss = min(self.stop_loss, self.entry - buffer)
                logger.info(f"[TRAIL] Position {self.pos_id}: TP1 hit! SL moved to breakeven+ ({self.stop_loss:.4f})")
                return self.stop_loss
        
        # 1. Breakeven при +0.8% прибыли (tightened from 1% to lock profit faster)
        if pnl_percent >= 0.8 and not self.breakeven_triggered:
            self.stop_loss = self.entry
            self.breakeven_triggered = True
            logger.info(f"[TRAIL] Position {self.pos_id}: Breakeven activated at {current_price:.4f}")
            return self.stop_loss
        
        # 2. Трейлинг при +1% прибыли
        if pnl_percent >= 1.0:
            self.trailing_active = True
            trailing_distance = self.atr * 0.8  # 0.8 ATR от текущей цены
            
            if self.direction == "LONG":
                new_sl = current_price - trailing_distance
                if new_sl > self.stop_loss:
                    self.stop_loss = new_sl
                    logger.info(f"[TRAIL] Position {self.pos_id}: SL moved to {new_sl:.4f} (trailing)")
                    return self.stop_loss
            else:  # SHORT
                new_sl = current_price + trailing_distance
                if new_sl < self.stop_loss:
                    self.stop_loss = new_sl
                    logger.info(f"[TRAIL] Position {self.pos_id}: SL moved to {new_sl:.4f} (trailing)")
                    return self.stop_loss
        
        # 3. Агрессивный трейлинг при +2% прибыли
        if pnl_percent >= 2.0:
            trailing_distance = self.atr * 0.6  # Ближе к цене
            
            if self.direction == "LONG":
                new_sl = current_price - trailing_distance
                if new_sl > self.stop_loss:
                    self.stop_loss = new_sl
                    return self.stop_loss
            else:
                new_sl = current_price + trailing_distance
                if new_sl < self.stop_loss:
                    self.stop_loss = new_sl
                    return self.stop_loss
        
        # 4. Ultra-tight трейлинг при +3% (lock most of the gain)
        if pnl_percent >= 3.0:
            trailing_distance = self.atr * 0.4  # Very tight
            
            if self.direction == "LONG":
                new_sl = current_price - trailing_distance
                if new_sl > self.stop_loss:
                    self.stop_loss = new_sl
                    return self.stop_loss
            else:
                new_sl = current_price + trailing_distance
                if new_sl < self.stop_loss:
                    self.stop_loss = new_sl
                    return self.stop_loss
        
        # 5. Защита от разворота: если цена откатилась от максимума
        if self.direction == "LONG" and self.highest_price:
            pullback = (self.highest_price - current_price) / self.highest_price * 100
            if pullback > 1.0 and self.trailing_active:
                pass  # SL stays
        
        if self.stop_loss != old_sl:
            return self.stop_loss
        
        return None


class TrailingStopManager:
    """Менеджер трейлинг-стопов для всех открытых позиций"""
    
    def __init__(self):
        self.active_trailing: Dict[int, TrailingStop] = {}
    
    def add_position(self, pos_id: int, entry: float, direction: str, atr: float, initial_sl: float,
                     tp1: float = None, tp2: float = None, tp3: float = None):
        """Добавить позицию для трейлинга"""
        self.active_trailing[pos_id] = TrailingStop(pos_id, entry, direction, atr, initial_sl, tp1=tp1, tp2=tp2, tp3=tp3)
        logger.info(f"[TRAIL] Added position {pos_id} for trailing: entry={entry:.4f}, SL={initial_sl:.4f}, TPs={tp1}/{tp2}/{tp3}")
    
    def update_position(self, pos_id: int, current_price: float) -> Optional[float]:
        """
        Обновить трейлинг-стоп для позиции
        
        Returns:
            Новый уровень SL или None
        """
        if pos_id not in self.active_trailing:
            return None
        
        trail = self.active_trailing[pos_id]
        new_sl = trail.update(current_price)
        
        return new_sl
    
    def remove_position(self, pos_id: int):
        """Удалить позицию из трейлинга"""
        if pos_id in self.active_trailing:
            del self.active_trailing[pos_id]
            logger.info(f"[TRAIL] Removed position {pos_id}")
    
    def get_stop_loss(self, pos_id: int) -> Optional[float]:
        """Получить текущий уровень SL"""
        if pos_id in self.active_trailing:
            return self.active_trailing[pos_id].stop_loss
        return None
    
    def is_breakeven(self, pos_id: int) -> bool:
        """Проверить, активирован ли breakeven"""
        if pos_id in self.active_trailing:
            return self.active_trailing[pos_id].breakeven_triggered
        return False


# Глобальный экземпляр
trailing_manager = TrailingStopManager()
