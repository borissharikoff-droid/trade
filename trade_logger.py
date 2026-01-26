"""
Comprehensive Trading Logger - Database-backed logging system
Stores all trading activity, signals, errors, and system events for analysis
"""

import logging
import json
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    """Log severity levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogCategory(Enum):
    """Categories of log entries"""
    SIGNAL = "SIGNAL"           # Trading signal generated/analyzed
    TRADE_OPEN = "TRADE_OPEN"   # Position opened
    TRADE_CLOSE = "TRADE_CLOSE" # Position closed
    TRADE_UPDATE = "TRADE_UPDATE" # Position modified (scaling, partial close)
    BALANCE = "BALANCE"         # Balance changes
    BYBIT_SYNC = "BYBIT_SYNC"   # Bybit synchronization
    BYBIT_API = "BYBIT_API"     # Bybit API calls
    ERROR = "ERROR"             # Errors and exceptions
    SYSTEM = "SYSTEM"           # System events (startup, shutdown, etc)
    ANALYSIS = "ANALYSIS"       # Market analysis results
    OPTIMIZATION = "OPTIMIZATION" # Auto-optimizer decisions
    USER_ACTION = "USER_ACTION" # User interactions
    PERFORMANCE = "PERFORMANCE" # Performance metrics


class TradeLogger:
    """
    Comprehensive trading logger with database persistence
    
    Stores all trading activity for later analysis and optimization
    """
    
    def __init__(self, db_run_sql_func=None, use_postgres: bool = False):
        """
        Initialize the trade logger
        
        Args:
            db_run_sql_func: Function to execute SQL queries (from bot.py)
            use_postgres: Whether using PostgreSQL (vs SQLite)
        """
        self._run_sql = db_run_sql_func
        self._use_postgres = use_postgres
        self._buffer: List[Dict] = []  # Buffer for batch inserts
        self._buffer_size = 50  # Flush after this many entries
        self._initialized = False
        
        # In-memory stats for quick access
        self._session_stats = defaultdict(int)
        self._last_flush = datetime.now(timezone.utc)
    
    def set_db_func(self, run_sql_func, use_postgres: bool = False):
        """Set the database function after initialization"""
        self._run_sql = run_sql_func
        self._use_postgres = use_postgres
    
    def init_tables(self):
        """Initialize logging tables in database"""
        if not self._run_sql:
            logger.warning("[TRADE_LOGGER] Cannot init tables - no DB function")
            return False
        
        try:
            if self._use_postgres:
                # PostgreSQL tables
                self._run_sql("""
                    CREATE TABLE IF NOT EXISTS trading_logs (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        category TEXT NOT NULL,
                        level TEXT DEFAULT 'INFO',
                        user_id BIGINT,
                        symbol TEXT,
                        direction TEXT,
                        message TEXT,
                        data JSONB,
                        error_traceback TEXT,
                        session_id TEXT
                    )
                """)
                
                # Indexes for efficient querying
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON trading_logs(timestamp)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_category ON trading_logs(category)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_user ON trading_logs(user_id)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_symbol ON trading_logs(symbol)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_level ON trading_logs(level)")
                
                # Aggregated stats table for quick queries
                self._run_sql("""
                    CREATE TABLE IF NOT EXISTS trading_stats (
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL,
                        symbol TEXT,
                        direction TEXT,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        total_pnl REAL DEFAULT 0,
                        avg_pnl REAL DEFAULT 0,
                        max_win REAL DEFAULT 0,
                        max_loss REAL DEFAULT 0,
                        avg_holding_time_minutes REAL DEFAULT 0,
                        signals_generated INTEGER DEFAULT 0,
                        signals_accepted INTEGER DEFAULT 0,
                        bybit_synced INTEGER DEFAULT 0,
                        errors_count INTEGER DEFAULT 0,
                        UNIQUE(date, symbol, direction)
                    )
                """)
                
                # Optimization history
                self._run_sql("""
                    CREATE TABLE IF NOT EXISTS optimization_history (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        parameter_name TEXT NOT NULL,
                        old_value TEXT,
                        new_value TEXT,
                        reason TEXT,
                        expected_improvement REAL,
                        actual_improvement REAL,
                        applied BOOLEAN DEFAULT FALSE
                    )
                """)
                
            else:
                # SQLite tables
                self._run_sql("""
                    CREATE TABLE IF NOT EXISTS trading_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                        category TEXT NOT NULL,
                        level TEXT DEFAULT 'INFO',
                        user_id INTEGER,
                        symbol TEXT,
                        direction TEXT,
                        message TEXT,
                        data TEXT,
                        error_traceback TEXT,
                        session_id TEXT
                    )
                """)
                
                # Indexes
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON trading_logs(timestamp)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_category ON trading_logs(category)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_user ON trading_logs(user_id)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_symbol ON trading_logs(symbol)")
                self._run_sql("CREATE INDEX IF NOT EXISTS idx_logs_level ON trading_logs(level)")
                
                # Stats table
                self._run_sql("""
                    CREATE TABLE IF NOT EXISTS trading_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        symbol TEXT,
                        direction TEXT,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        total_pnl REAL DEFAULT 0,
                        avg_pnl REAL DEFAULT 0,
                        max_win REAL DEFAULT 0,
                        max_loss REAL DEFAULT 0,
                        avg_holding_time_minutes REAL DEFAULT 0,
                        signals_generated INTEGER DEFAULT 0,
                        signals_accepted INTEGER DEFAULT 0,
                        bybit_synced INTEGER DEFAULT 0,
                        errors_count INTEGER DEFAULT 0,
                        UNIQUE(date, symbol, direction)
                    )
                """)
                
                # Optimization history
                self._run_sql("""
                    CREATE TABLE IF NOT EXISTS optimization_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                        parameter_name TEXT NOT NULL,
                        old_value TEXT,
                        new_value TEXT,
                        reason TEXT,
                        expected_improvement REAL,
                        actual_improvement REAL,
                        applied INTEGER DEFAULT 0
                    )
                """)
            
            self._initialized = True
            logger.info("[TRADE_LOGGER] Database tables initialized")
            return True
            
        except Exception as e:
            logger.error(f"[TRADE_LOGGER] Failed to init tables: {e}")
            return False
    
    def _serialize_data(self, data: Any) -> str:
        """Serialize data to JSON string"""
        if data is None:
            return None
        try:
            return json.dumps(data, default=str, ensure_ascii=False)
        except:
            return str(data)
    
    def _add_to_buffer(self, entry: Dict):
        """Add entry to buffer and flush if needed"""
        self._buffer.append(entry)
        self._session_stats[entry.get('category', 'UNKNOWN')] += 1
        
        # Flush buffer if it's full or enough time has passed
        if len(self._buffer) >= self._buffer_size:
            self.flush()
        elif (datetime.now(timezone.utc) - self._last_flush).seconds > 30:
            self.flush()
    
    def flush(self):
        """Flush buffer to database"""
        if not self._buffer or not self._run_sql:
            return
        
        try:
            for entry in self._buffer:
                self._run_sql(
                    """INSERT INTO trading_logs 
                    (timestamp, category, level, user_id, symbol, direction, message, data, error_traceback, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        entry.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        entry.get('category', 'SYSTEM'),
                        entry.get('level', 'INFO'),
                        entry.get('user_id'),
                        entry.get('symbol'),
                        entry.get('direction'),
                        entry.get('message'),
                        entry.get('data'),
                        entry.get('error_traceback'),
                        entry.get('session_id')
                    )
                )
            
            self._buffer.clear()
            self._last_flush = datetime.now(timezone.utc)
            
        except Exception as e:
            logger.error(f"[TRADE_LOGGER] Flush error: {e}")
    
    def log(self, category: LogCategory, message: str, 
            level: LogLevel = LogLevel.INFO,
            user_id: int = None, symbol: str = None, direction: str = None,
            data: Dict = None, error: Exception = None, session_id: str = None):
        """
        Log an entry to the database
        
        Args:
            category: Type of log entry
            message: Human-readable message
            level: Severity level
            user_id: Related user ID (if applicable)
            symbol: Trading symbol (if applicable)
            direction: Trade direction LONG/SHORT (if applicable)
            data: Additional structured data
            error: Exception object (if logging an error)
            session_id: Session identifier for grouping related logs
        """
        entry = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'category': category.value if isinstance(category, LogCategory) else str(category),
            'level': level.value if isinstance(level, LogLevel) else str(level),
            'user_id': user_id,
            'symbol': symbol,
            'direction': direction,
            'message': message,
            'data': self._serialize_data(data),
            'error_traceback': traceback.format_exc() if error else None,
            'session_id': session_id
        }
        
        self._add_to_buffer(entry)
        
        # Also log to standard logger
        log_level_map = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL
        }
        std_level = log_level_map.get(level, logging.INFO)
        logger.log(std_level, f"[{category.value if isinstance(category, LogCategory) else category}] {message}")
    
    # === Convenience methods for common log types ===
    
    def log_signal(self, symbol: str, direction: str, quality: str, 
                   entry: float, sl: float, tp: float, confidence: float,
                   accepted: bool, reason: str = None, user_id: int = None):
        """Log a trading signal"""
        self.log(
            LogCategory.SIGNAL,
            f"Signal {symbol} {direction} quality={quality} conf={confidence:.0%} {'ACCEPTED' if accepted else 'REJECTED'}",
            level=LogLevel.INFO,
            user_id=user_id,
            symbol=symbol,
            direction=direction,
            data={
                'quality': quality,
                'entry': entry,
                'stop_loss': sl,
                'take_profit': tp,
                'confidence': confidence,
                'accepted': accepted,
                'rejection_reason': reason
            }
        )
    
    def log_trade_open(self, user_id: int, symbol: str, direction: str,
                       amount: float, entry: float, sl: float, tp: float,
                       bybit_qty: float = 0, position_id: int = None):
        """Log a trade opening"""
        self.log(
            LogCategory.TRADE_OPEN,
            f"OPEN {symbol} {direction} ${amount:.2f} @ {entry:.4f}",
            level=LogLevel.INFO,
            user_id=user_id,
            symbol=symbol,
            direction=direction,
            data={
                'position_id': position_id,
                'amount': amount,
                'entry_price': entry,
                'stop_loss': sl,
                'take_profit': tp,
                'bybit_qty': bybit_qty
            }
        )
    
    def log_trade_close(self, user_id: int, symbol: str, direction: str,
                        amount: float, entry: float, exit_price: float, 
                        pnl: float, reason: str, position_id: int = None,
                        holding_minutes: float = None):
        """Log a trade closing"""
        pnl_sign = "+" if pnl >= 0 else ""
        self.log(
            LogCategory.TRADE_CLOSE,
            f"CLOSE {symbol} {direction} PnL={pnl_sign}${pnl:.2f} reason={reason}",
            level=LogLevel.INFO,
            user_id=user_id,
            symbol=symbol,
            direction=direction,
            data={
                'position_id': position_id,
                'amount': amount,
                'entry_price': entry,
                'exit_price': exit_price,
                'pnl': pnl,
                'pnl_percent': (pnl / amount * 100) if amount > 0 else 0,
                'reason': reason,
                'is_winner': pnl > 0,
                'holding_minutes': holding_minutes
            }
        )
    
    def log_balance_change(self, user_id: int, old_balance: float, 
                           new_balance: float, reason: str):
        """Log a balance change"""
        delta = new_balance - old_balance
        self.log(
            LogCategory.BALANCE,
            f"Balance {'+' if delta >= 0 else ''}{delta:.2f} ({reason}): ${old_balance:.2f} -> ${new_balance:.2f}",
            level=LogLevel.INFO,
            user_id=user_id,
            data={
                'old_balance': old_balance,
                'new_balance': new_balance,
                'delta': delta,
                'reason': reason
            }
        )
    
    def log_bybit_sync(self, symbol: str, action: str, details: Dict = None, 
                       user_id: int = None, success: bool = True):
        """Log Bybit synchronization event"""
        self.log(
            LogCategory.BYBIT_SYNC,
            f"Bybit sync {symbol}: {action} {'OK' if success else 'FAILED'}",
            level=LogLevel.INFO if success else LogLevel.WARNING,
            user_id=user_id,
            symbol=symbol,
            data=details
        )
    
    def log_error(self, message: str, error: Exception = None, 
                  user_id: int = None, symbol: str = None, context: Dict = None):
        """Log an error"""
        self.log(
            LogCategory.ERROR,
            message,
            level=LogLevel.ERROR,
            user_id=user_id,
            symbol=symbol,
            data=context,
            error=error
        )
    
    def log_system(self, message: str, level: LogLevel = LogLevel.INFO, 
                   data: Dict = None):
        """Log a system event"""
        self.log(
            LogCategory.SYSTEM,
            message,
            level=level,
            data=data
        )
    
    def log_analysis(self, symbol: str, analysis_type: str, result: Dict,
                     recommendation: str = None):
        """Log market analysis result"""
        self.log(
            LogCategory.ANALYSIS,
            f"Analysis {symbol}: {analysis_type} -> {recommendation or 'N/A'}",
            level=LogLevel.INFO,
            symbol=symbol,
            data={
                'analysis_type': analysis_type,
                'result': result,
                'recommendation': recommendation
            }
        )
    
    def log_optimization(self, parameter: str, old_value: Any, new_value: Any,
                         reason: str, expected_improvement: float = None):
        """Log an optimization decision"""
        self.log(
            LogCategory.OPTIMIZATION,
            f"Optimize {parameter}: {old_value} -> {new_value} ({reason})",
            level=LogLevel.INFO,
            data={
                'parameter': parameter,
                'old_value': str(old_value),
                'new_value': str(new_value),
                'reason': reason,
                'expected_improvement': expected_improvement
            }
        )
        
        # Also save to optimization history
        if self._run_sql:
            try:
                self._run_sql(
                    """INSERT INTO optimization_history 
                    (parameter_name, old_value, new_value, reason, expected_improvement)
                    VALUES (?, ?, ?, ?, ?)""",
                    (parameter, str(old_value), str(new_value), reason, expected_improvement)
                )
            except Exception as e:
                logger.error(f"[TRADE_LOGGER] Failed to save optimization: {e}")
    
    # === Query methods for analysis ===
    
    def get_recent_logs(self, category: LogCategory = None, 
                        hours: int = 24, limit: int = 1000) -> List[Dict]:
        """Get recent log entries"""
        if not self._run_sql:
            return []
        
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            
            if category:
                return self._run_sql(
                    """SELECT * FROM trading_logs 
                    WHERE timestamp > ? AND category = ?
                    ORDER BY timestamp DESC LIMIT ?""",
                    (cutoff, category.value, limit),
                    fetch="all"
                ) or []
            else:
                return self._run_sql(
                    """SELECT * FROM trading_logs 
                    WHERE timestamp > ?
                    ORDER BY timestamp DESC LIMIT ?""",
                    (cutoff, limit),
                    fetch="all"
                ) or []
        except Exception as e:
            logger.error(f"[TRADE_LOGGER] Query error: {e}")
            return []
    
    def get_trade_stats(self, days: int = 7, symbol: str = None) -> Dict:
        """Get aggregated trading statistics"""
        if not self._run_sql:
            return {}
        
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            
            # Get trade close logs
            if symbol:
                trades = self._run_sql(
                    """SELECT data FROM trading_logs 
                    WHERE timestamp > ? AND category = ? AND symbol = ?""",
                    (cutoff, LogCategory.TRADE_CLOSE.value, symbol),
                    fetch="all"
                ) or []
            else:
                trades = self._run_sql(
                    """SELECT data FROM trading_logs 
                    WHERE timestamp > ? AND category = ?""",
                    (cutoff, LogCategory.TRADE_CLOSE.value),
                    fetch="all"
                ) or []
            
            if not trades:
                return {'total_trades': 0}
            
            # Parse and aggregate
            total_pnl = 0
            winners = 0
            losers = 0
            pnls = []
            
            for trade in trades:
                try:
                    data = json.loads(trade['data']) if isinstance(trade['data'], str) else trade['data']
                    pnl = data.get('pnl', 0)
                    total_pnl += pnl
                    pnls.append(pnl)
                    if pnl > 0:
                        winners += 1
                    elif pnl < 0:
                        losers += 1
                except:
                    pass
            
            total_trades = len(pnls)
            
            return {
                'total_trades': total_trades,
                'winning_trades': winners,
                'losing_trades': losers,
                'break_even': total_trades - winners - losers,
                'win_rate': winners / total_trades if total_trades > 0 else 0,
                'total_pnl': total_pnl,
                'avg_pnl': total_pnl / total_trades if total_trades > 0 else 0,
                'max_win': max(pnls) if pnls else 0,
                'max_loss': min(pnls) if pnls else 0,
                'profit_factor': abs(sum(p for p in pnls if p > 0) / sum(p for p in pnls if p < 0)) if any(p < 0 for p in pnls) else float('inf')
            }
            
        except Exception as e:
            logger.error(f"[TRADE_LOGGER] Stats error: {e}")
            return {'error': str(e)}
    
    def get_error_summary(self, hours: int = 24) -> Dict:
        """Get error summary for monitoring"""
        if not self._run_sql:
            return {}
        
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            
            errors = self._run_sql(
                """SELECT message, COUNT(*) as count FROM trading_logs 
                WHERE timestamp > ? AND category = ?
                GROUP BY message ORDER BY count DESC LIMIT 20""",
                (cutoff, LogCategory.ERROR.value),
                fetch="all"
            ) or []
            
            total_errors = sum(e['count'] for e in errors)
            
            return {
                'total_errors': total_errors,
                'unique_errors': len(errors),
                'top_errors': [{'message': e['message'], 'count': e['count']} for e in errors[:10]]
            }
            
        except Exception as e:
            logger.error(f"[TRADE_LOGGER] Error summary query failed: {e}")
            return {'error': str(e)}
    
    def get_session_stats(self) -> Dict:
        """Get current session statistics"""
        return dict(self._session_stats)
    
    def cleanup_old_logs(self, days: int = 30):
        """Remove logs older than specified days"""
        if not self._run_sql:
            return
        
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            self._run_sql(
                "DELETE FROM trading_logs WHERE timestamp < ?",
                (cutoff,)
            )
            logger.info(f"[TRADE_LOGGER] Cleaned up logs older than {days} days")
        except Exception as e:
            logger.error(f"[TRADE_LOGGER] Cleanup error: {e}")


# Global instance
trade_logger = TradeLogger()


def init_trade_logger(run_sql_func, use_postgres: bool = False):
    """Initialize the global trade logger"""
    trade_logger.set_db_func(run_sql_func, use_postgres)
    trade_logger.init_tables()
    trade_logger.log_system("Trade logger initialized", data={'postgres': use_postgres})
    return trade_logger
