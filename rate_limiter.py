"""
Persistent Rate Limiting System with Database Storage
Supports progressive penalties and spam detection alerts
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from functools import wraps
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# Database functions will be injected to avoid circular imports
_run_sql = None
_USE_POSTGRES = False
_ADMIN_IDS = []

def configure_rate_limiter(run_sql_func, use_postgres: bool, admin_ids: list):
    """Configure rate limiter with database functions"""
    global _run_sql, _USE_POSTGRES, _ADMIN_IDS
    _run_sql = run_sql_func
    _USE_POSTGRES = use_postgres
    _ADMIN_IDS = admin_ids


class RateLimiter:
    """Persistent rate limiter with database storage"""
    
    def __init__(self):
        self.in_memory_cache: Dict[int, Dict] = {}  # Temporary cache
        self.cache_ttl = 5  # seconds
        
    def _init_table(self):
        """Initialize rate_limits table if not exists"""
        if not _run_sql:
            logger.warning("[RATE_LIMITER] Database functions not configured yet")
            return
        
        if _USE_POSTGRES:
            _run_sql("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    user_id BIGINT PRIMARY KEY,
                    count INTEGER DEFAULT 1,
                    reset_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    penalty_level INTEGER DEFAULT 0,
                    last_alert_time TIMESTAMP,
                    total_violations INTEGER DEFAULT 0
                )
            """)
        else:
            _run_sql("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    user_id INTEGER PRIMARY KEY,
                    count INTEGER DEFAULT 1,
                    reset_time TEXT DEFAULT CURRENT_TIMESTAMP,
                    penalty_level INTEGER DEFAULT 0,
                    last_alert_time TEXT,
                    total_violations INTEGER DEFAULT 0
                )
            """)
        
        # Create spam_logs table
        if _USE_POSTGRES:
            _run_sql("""
                CREATE TABLE IF NOT EXISTS spam_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    violation_type TEXT,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            _run_sql("""
                CREATE TABLE IF NOT EXISTS spam_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    violation_type TEXT,
                    details TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        logger.info("[RATE_LIMITER] Tables initialized")
    
    def _get_user_limit(self, user_id: int) -> Dict:
        """Get user rate limit from database"""
        if not _run_sql:
            # Fallback to in-memory only
            if user_id not in self.in_memory_cache:
                self.in_memory_cache[user_id] = {
                    'count': 1,
                    'reset_time': datetime.now(),
                    'penalty_level': 0,
                    'total_violations': 0,
                    '_cache_time': time.time()
                }
            return self.in_memory_cache[user_id]
        
        # Check in-memory cache first
        if user_id in self.in_memory_cache:
            cached = self.in_memory_cache[user_id]
            if (time.time() - cached.get('_cache_time', 0)) < self.cache_ttl:
                return cached
        
        # Query database
        row = _run_sql(
            "SELECT count, reset_time, penalty_level, total_violations FROM rate_limits WHERE user_id = ?",
            (user_id,),
            fetch="one"
        )
        
        if not row:
            # New user - initialize
            now = datetime.now()
            if _USE_POSTGRES:
                reset_time = now
            else:
                reset_time = now.isoformat()
            
            _run_sql(
                "INSERT INTO rate_limits (user_id, count, reset_time, penalty_level, total_violations) VALUES (?, ?, ?, 0, 0)",
                (user_id, 1, reset_time)
            )
            
            result = {
                'count': 1,
                'reset_time': now,
                'penalty_level': 0,
                'total_violations': 0
            }
        else:
            # Parse reset_time
            reset_time = row['reset_time']
            if isinstance(reset_time, str):
                reset_time = datetime.fromisoformat(reset_time.replace('Z', '+00:00'))
            elif not isinstance(reset_time, datetime):
                reset_time = datetime.now()
            
            result = {
                'count': int(row['count'] or 1),
                'reset_time': reset_time,
                'penalty_level': int(row['penalty_level'] or 0),
                'total_violations': int(row['total_violations'] or 0)
            }
        
        # Cache it
        result['_cache_time'] = time.time()
        self.in_memory_cache[user_id] = result
        
        return result
    
    def _update_user_limit(self, user_id: int, count: int, reset_time: datetime, 
                           penalty_level: int = None, increment_violations: bool = False):
        """Update user rate limit in database"""
        if not _run_sql:
            return
        
        if _USE_POSTGRES:
            reset_time_str = reset_time
        else:
            reset_time_str = reset_time.isoformat()
        
        if penalty_level is not None:
            if increment_violations:
                _run_sql(
                    """UPDATE rate_limits 
                       SET count = ?, reset_time = ?, penalty_level = ?, total_violations = total_violations + 1
                       WHERE user_id = ?""",
                    (count, reset_time_str, penalty_level, user_id)
                )
            else:
                _run_sql(
                    "UPDATE rate_limits SET count = ?, reset_time = ?, penalty_level = ? WHERE user_id = ?",
                    (count, reset_time_str, penalty_level, user_id)
                )
        else:
            _run_sql(
                "UPDATE rate_limits SET count = ?, reset_time = ? WHERE user_id = ?",
                (count, reset_time_str, user_id)
            )
        
        # Invalidate cache
        self.in_memory_cache.pop(user_id, None)
    
    def _log_spam(self, user_id: int, violation_type: str, details: str):
        """Log spam violation"""
        if _run_sql:
            _run_sql(
                "INSERT INTO spam_logs (user_id, violation_type, details) VALUES (?, ?, ?)",
                (user_id, violation_type, details)
            )
        logger.warning(f"[SPAM] User {user_id}: {violation_type} - {details}")
    
    def _send_spam_alert(self, user_id: int, context: ContextTypes.DEFAULT_TYPE, 
                        penalty_level: int, violation_count: int):
        """Send alert to admins about spam"""
        try:
            # Check if we should alert (max once per 5 minutes per user)
            last_alert = run_sql(
                "SELECT last_alert_time FROM rate_limits WHERE user_id = ?",
                (user_id,),
                fetch="one"
            )
            
            should_alert = True
            if last_alert and last_alert.get('last_alert_time'):
                last_time = last_alert['last_alert_time']
                if isinstance(last_time, str):
                    last_time = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                
                if (datetime.now() - last_time).total_seconds() < 300:  # 5 minutes
                    should_alert = False
            
            if should_alert and ADMIN_IDS:
                alert_text = f"⚠️ <b>SPAM DETECTED</b>\n\n"
                alert_text += f"User ID: <code>{user_id}</code>\n"
                alert_text += f"Penalty Level: {penalty_level}\n"
                alert_text += f"Total Violations: {violation_count}\n"
                alert_text += f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                
                for admin_id in ADMIN_IDS:
                    try:
                        context.bot.send_message(admin_id, alert_text, parse_mode="HTML")
                    except Exception as e:
                        logger.error(f"[SPAM_ALERT] Failed to send to admin {admin_id}: {e}")
                
                # Update last_alert_time
                if _run_sql:
                    now = datetime.now()
                    if _USE_POSTGRES:
                        now_str = now
                    else:
                        now_str = now.isoformat()
                    _run_sql(
                        "UPDATE rate_limits SET last_alert_time = ? WHERE user_id = ?",
                        (now_str, user_id)
                    )
        except Exception as e:
            logger.error(f"[SPAM_ALERT] Error: {e}")
    
    def check_rate_limit(self, user_id: int, max_requests: int = 30, 
                        window_seconds: int = 60, context: ContextTypes.DEFAULT_TYPE = None) -> Tuple[bool, Optional[str]]:
        """
        Check if user exceeded rate limit
        
        Returns:
            (is_blocked, reason)
        """
        # Admins bypass rate limiting
        if user_id in _ADMIN_IDS:
            return False, None
        
        now = datetime.now()
        user_limit = self._get_user_limit(user_id)
        
        # Check if window expired
        reset_time = user_limit['reset_time']
        if isinstance(reset_time, str):
            reset_time = datetime.fromisoformat(reset_time.replace('Z', '+00:00'))
        
        time_since_reset = (now - reset_time).total_seconds()
        
        # Reset if window expired
        if time_since_reset >= window_seconds:
            self._update_user_limit(user_id, 1, now, penalty_level=0)
            return False, None
        
        # Apply penalty multiplier
        penalty_level = user_limit['penalty_level']
        effective_max = max_requests
        
        # Progressive penalties
        if penalty_level == 1:
            effective_max = max_requests // 2  # 50% reduction
        elif penalty_level == 2:
            effective_max = max_requests // 4  # 75% reduction
        elif penalty_level >= 3:
            effective_max = max_requests // 10  # 90% reduction
        
        current_count = user_limit['count']
        
        # Check limit
        if current_count >= effective_max:
            # Increment violations
            self._update_user_limit(
                user_id, 
                current_count, 
                reset_time, 
                penalty_level=min(penalty_level + 1, 5),  # Max penalty level 5
                increment_violations=True
            )
            
            # Log spam
            self._log_spam(
                user_id,
                "RATE_LIMIT_EXCEEDED",
                f"Count: {current_count}/{effective_max}, Penalty: {penalty_level}"
            )
            
            # Send alert
            if context:
                self._send_spam_alert(user_id, context, penalty_level + 1, user_limit['total_violations'] + 1)
            
            # Calculate cooldown
            remaining = window_seconds - time_since_reset
            reason = f"⏳ Слишком много запросов. Подождите {int(remaining)} сек."
            
            return True, reason
        
        # Increment count
        self._update_user_limit(user_id, current_count + 1, reset_time, penalty_level)
        
        return False, None
    
    def reset_user_limit(self, user_id: int):
        """Reset user's rate limit (for manual admin action)"""
        now = datetime.now()
        self._update_user_limit(user_id, 1, now, penalty_level=0)
        logger.info(f"[RATE_LIMITER] Reset limit for user {user_id}")
    
    def get_user_stats(self, user_id: int) -> Dict:
        """Get rate limiting stats for user"""
        user_limit = self._get_user_limit(user_id)
        return {
            'penalty_level': user_limit['penalty_level'],
            'total_violations': user_limit['total_violations'],
            'current_count': user_limit['count']
        }


# Global instance
rate_limiter = RateLimiter()


def rate_limit(max_requests: int = 30, window_seconds: int = 60, 
               action_type: str = "command"):
    """
    Decorator for rate limiting handlers
    
    Args:
        max_requests: Maximum requests per window
        window_seconds: Time window in seconds
        action_type: Type of action (for logging)
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            
            # Check rate limit
            is_blocked, reason = rate_limiter.check_rate_limit(
                user_id, max_requests, window_seconds, context
            )
            
            if is_blocked:
                # Try to answer callback query if exists
                if update.callback_query:
                    try:
                        await update.callback_query.answer(reason, show_alert=True)
                    except:
                        pass
                
                # Try to send message
                try:
                    if update.message:
                        await update.message.reply_text(reason)
                    elif update.callback_query:
                        await context.bot.send_message(user_id, reason)
                except:
                    pass
                
                return
            
            # Execute handler
            return await func(update, context, *args, **kwargs)
        
        return wrapper
    return decorator


def init_rate_limiter():
    """Initialize rate limiter tables"""
    if _run_sql:
        rate_limiter._init_table()
