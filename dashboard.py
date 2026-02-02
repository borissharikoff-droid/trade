"""
Trading Bot Dashboard
Minimalist web interface for monitoring trading statistics
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, render_template, request, Response
from threading import Thread
from collections import deque
import json

logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__, template_folder='templates')
app.config['JSON_SORT_KEYS'] = False

# Will be set from bot.py
_run_sql = None
_use_postgres = False

# Signal stats getter (will be set from bot.py)
_get_signal_stats = None

# News analyzer instance (will be set from bot.py)
_news_analyzer = None

# Moscow timezone (UTC+3)
MOSCOW_TZ = timezone(timedelta(hours=3))

# In-memory log buffer - increased to 50000 for full export capability
_log_buffer = deque(maxlen=50000)

# Separate buffer for errors and critical issues (always visible on top)
_error_buffer = deque(maxlen=500)


def to_moscow_time(dt=None):
    """Convert datetime to Moscow time string"""
    if dt is None:
        dt = datetime.now(MOSCOW_TZ)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(MOSCOW_TZ)
    else:
        dt = dt.astimezone(MOSCOW_TZ)
    return dt.strftime('%Y-%m-%d %H:%M:%S MSK')


class DashboardLogHandler(logging.Handler):
    """Custom log handler to capture logs for dashboard"""
    def emit(self, record):
        try:
            # Use Moscow time
            moscow_now = datetime.now(MOSCOW_TZ)
            log_entry = {
                'timestamp': moscow_now.strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp_iso': moscow_now.isoformat(),
                'level': record.levelname,
                'message': self.format(record)
            }
            _log_buffer.append(log_entry)
            
            # Also add to error buffer if it's WARNING, ERROR, or CRITICAL
            if record.levelno >= logging.WARNING:
                _error_buffer.append(log_entry)
        except Exception:
            pass


def init_dashboard(run_sql_func, use_postgres: bool = False, get_signal_stats_func=None, news_analyzer=None):
    """Initialize dashboard with database function from bot.py"""
    global _run_sql, _use_postgres, _get_signal_stats, _news_analyzer
    _run_sql = run_sql_func
    _use_postgres = use_postgres
    _get_signal_stats = get_signal_stats_func
    _news_analyzer = news_analyzer
    
    # Add log handler to capture bot logs
    root_logger = logging.getLogger()
    handler = DashboardLogHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    handler.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    
    logger.info("[DASHBOARD] Initialized with database connection")
    if news_analyzer:
        logger.info("[DASHBOARD] News analyzer connected")


def get_open_positions_count() -> int:
    """Get count of all open positions"""
    if not _run_sql:
        return 0
    try:
        result = _run_sql("SELECT COUNT(*) as count FROM positions", fetch="one")
        return int(result['count']) if result else 0
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting positions count: {e}")
        return 0


def get_positions_stats() -> dict:
    """Get detailed statistics about open positions across all users"""
    if not _run_sql:
        return {
            'total_positions': 0,
            'users_with_positions': 0,
            'total_value': 0,
            'total_unrealized_pnl': 0,
            'by_direction': {'LONG': 0, 'SHORT': 0}
        }
    try:
        # Total positions and users with positions
        stats = _run_sql("""
            SELECT 
                COUNT(*) as total_positions,
                COUNT(DISTINCT user_id) as users_with_positions,
                COALESCE(SUM(amount), 0) as total_value,
                COALESCE(SUM(pnl), 0) as total_unrealized_pnl
            FROM positions
        """, fetch="one")
        
        # By direction
        by_dir = _run_sql("""
            SELECT direction, COUNT(*) as count
            FROM positions
            GROUP BY direction
        """, fetch="all")
        
        direction_counts = {'LONG': 0, 'SHORT': 0}
        for row in (by_dir or []):
            d = row.get('direction', '').upper()
            if d in direction_counts:
                direction_counts[d] = int(row.get('count', 0))
        
        return {
            'total_positions': int(stats.get('total_positions', 0) or 0) if stats else 0,
            'users_with_positions': int(stats.get('users_with_positions', 0) or 0) if stats else 0,
            'total_value': round(float(stats.get('total_value', 0) or 0), 2) if stats else 0,
            'total_unrealized_pnl': round(float(stats.get('total_unrealized_pnl', 0) or 0), 2) if stats else 0,
            'by_direction': direction_counts
        }
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting positions stats: {e}")
        return {
            'total_positions': 0,
            'users_with_positions': 0,
            'total_value': 0,
            'total_unrealized_pnl': 0,
            'by_direction': {'LONG': 0, 'SHORT': 0}
        }


def get_open_positions_details() -> list:
    """Get details of all open positions"""
    if not _run_sql:
        return []
    try:
        positions = _run_sql("""
            SELECT symbol, direction, entry, current, pnl, amount, opened_at, user_id, is_auto, bybit_qty
            FROM positions
            ORDER BY ABS(pnl) DESC, opened_at DESC
            LIMIT 50
        """, fetch="all")
        
        # Format positions for API response
        result = []
        for pos in (positions or []):
            result.append({
                'symbol': pos.get('symbol', ''),
                'direction': pos.get('direction', ''),
                'entry': float(pos.get('entry', 0) or 0),
                'current': float(pos.get('current', 0) or 0),
                'pnl': round(float(pos.get('pnl', 0) or 0), 2),
                'amount': round(float(pos.get('amount', 0) or 0), 2),
                'opened_at': pos.get('opened_at'),
                'user_id': pos.get('user_id'),
                'is_auto': bool(pos.get('is_auto')),
                'on_bybit': float(pos.get('bybit_qty', 0) or 0) > 0
            })
        return result
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting positions details: {e}")
        return []


def get_history_stats() -> dict:
    """Get aggregated statistics from history"""
    default_result = {
        'total': 0,
        'profitable': 0,
        'losing': 0,
        'win_rate': 0,
        'total_pnl': 0,
        'win_reasons': {},
        'loss_reasons': {}
    }
    
    if not _run_sql:
        logger.warning("[DASHBOARD] get_history_stats: _run_sql is None")
        return default_result
    
    try:
        # Get overall stats
        stats = _run_sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as profitable,
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losing,
                SUM(pnl) as total_pnl
            FROM history
        """, fetch="one")
        
        logger.debug(f"[DASHBOARD] History stats raw: {stats}")
        
        if not stats:
            logger.warning("[DASHBOARD] get_history_stats: No stats returned")
            return default_result
        
        total = int(stats.get('total') or 0)
        profitable = int(stats.get('profitable') or 0)
        losing = int(stats.get('losing') or 0)
        total_pnl = float(stats.get('total_pnl') or 0)
        win_rate = round(profitable / total * 100, 2) if total > 0 else 0
        
        logger.debug(f"[DASHBOARD] History: total={total}, profitable={profitable}, losing={losing}")
        
        # Get win reasons breakdown
        win_reasons_rows = _run_sql("""
            SELECT reason, COUNT(*) as count
            FROM history
            WHERE pnl > 0
            GROUP BY reason
            ORDER BY count DESC
        """, fetch="all")
        win_reasons = {row['reason']: int(row['count']) for row in (win_reasons_rows or []) if row.get('reason')}
        
        # Get loss reasons breakdown
        loss_reasons_rows = _run_sql("""
            SELECT reason, COUNT(*) as count
            FROM history
            WHERE pnl <= 0
            GROUP BY reason
            ORDER BY count DESC
        """, fetch="all")
        loss_reasons = {row['reason']: int(row['count']) for row in (loss_reasons_rows or []) if row.get('reason')}
        
        return {
            'total': total,
            'profitable': profitable,
            'losing': losing,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'win_reasons': win_reasons,
            'loss_reasons': loss_reasons
        }
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting history stats: {e}")
        return {
            'total': 0,
            'profitable': 0,
            'losing': 0,
            'win_rate': 0,
            'total_pnl': 0,
            'win_reasons': {},
            'loss_reasons': {}
        }


def get_recent_trades(limit: int = 20) -> list:
    """Get recent closed trades with reasons"""
    if not _run_sql:
        return []
    try:
        # Try with is_auto first, fallback without it
        try:
            trades = _run_sql("""
                SELECT symbol, direction, entry, exit_price, pnl, reason, amount, closed_at, is_auto
                FROM history
                ORDER BY closed_at DESC
                LIMIT ?
            """, (limit,), fetch="all")
        except Exception:
            # is_auto column might not exist
            trades = _run_sql("""
                SELECT symbol, direction, entry, exit_price, pnl, reason, amount, closed_at
                FROM history
                ORDER BY closed_at DESC
                LIMIT ?
            """, (limit,), fetch="all")
        
        # Format trades for API response
        result = []
        for trade in (trades or []):
            result.append({
                'symbol': trade.get('symbol', ''),
                'direction': trade.get('direction', ''),
                'entry': float(trade.get('entry', 0) or 0),
                'exit_price': float(trade.get('exit_price', 0) or 0),
                'pnl': round(float(trade.get('pnl', 0) or 0), 2),
                'reason': trade.get('reason', ''),
                'amount': round(float(trade.get('amount', 0) or 0), 2),
                'closed_at': trade.get('closed_at'),
                'is_auto': bool(trade.get('is_auto', False))
            })
        return result
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting recent trades: {e}")
        return []


def get_extended_stats() -> dict:
    """Get extended statistics for dashboard"""
    if not _run_sql:
        return {}
    
    try:
        # Best and worst trades
        best_trade = _run_sql("""
            SELECT symbol, direction, pnl, reason, closed_at
            FROM history WHERE pnl IS NOT NULL
            ORDER BY pnl DESC LIMIT 1
        """, fetch="one")
        
        worst_trade = _run_sql("""
            SELECT symbol, direction, pnl, reason, closed_at
            FROM history WHERE pnl IS NOT NULL
            ORDER BY pnl ASC LIMIT 1
        """, fetch="one")
        
        # Average PnL
        avg_stats = _run_sql("""
            SELECT 
                AVG(pnl) as avg_pnl,
                AVG(CASE WHEN pnl > 0 THEN pnl END) as avg_win,
                AVG(CASE WHEN pnl <= 0 THEN pnl END) as avg_loss,
                SUM(amount) as total_volume
            FROM history
        """, fetch="one")
        
        # Today's stats (PostgreSQL и SQLite совместимый синтаксис)
        if _use_postgres:
            today_stats = _run_sql("""
                SELECT 
                    COUNT(*) as today_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as today_wins,
                    SUM(pnl) as today_pnl
                FROM history
                WHERE DATE(closed_at) = CURRENT_DATE
            """, fetch="one")
        else:
            today_stats = _run_sql("""
                SELECT 
                    COUNT(*) as today_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as today_wins,
                    SUM(pnl) as today_pnl
                FROM history
                WHERE DATE(closed_at) = DATE('now')
            """, fetch="one")
        
        # Streak calculation
        streak_rows = _run_sql("""
            SELECT pnl FROM history 
            ORDER BY closed_at DESC LIMIT 20
        """, fetch="all")
        
        current_streak = 0
        streak_type = None
        if streak_rows:
            first_pnl = float(streak_rows[0].get('pnl', 0) or 0)
            streak_type = 'win' if first_pnl > 0 else 'loss'
            for row in streak_rows:
                pnl = float(row.get('pnl', 0) or 0)
                if (streak_type == 'win' and pnl > 0) or (streak_type == 'loss' and pnl <= 0):
                    current_streak += 1
                else:
                    break
        
        # Users stats
        users_stats = _run_sql("""
            SELECT 
                COUNT(DISTINCT user_id) as total_users,
                SUM(balance) as total_balance
            FROM users
        """, fetch="one")
        
        # Positions by symbol
        positions_by_symbol = _run_sql("""
            SELECT symbol, COUNT(*) as count, SUM(amount) as total_amount
            FROM positions
            GROUP BY symbol
            ORDER BY count DESC
            LIMIT 5
        """, fetch="all")
        
        return {
            'best_trade': {
                'symbol': best_trade.get('symbol', '-') if best_trade else '-',
                'pnl': float(best_trade.get('pnl', 0) or 0) if best_trade else 0,
                'direction': best_trade.get('direction', '-') if best_trade else '-'
            } if best_trade else None,
            'worst_trade': {
                'symbol': worst_trade.get('symbol', '-') if worst_trade else '-',
                'pnl': float(worst_trade.get('pnl', 0) or 0) if worst_trade else 0,
                'direction': worst_trade.get('direction', '-') if worst_trade else '-'
            } if worst_trade else None,
            'avg_pnl': round(float(avg_stats.get('avg_pnl', 0) or 0), 2) if avg_stats else 0,
            'avg_win': round(float(avg_stats.get('avg_win', 0) or 0), 2) if avg_stats else 0,
            'avg_loss': round(float(avg_stats.get('avg_loss', 0) or 0), 2) if avg_stats else 0,
            'total_volume': round(float(avg_stats.get('total_volume', 0) or 0), 2) if avg_stats else 0,
            'today_trades': int(today_stats.get('today_trades', 0) or 0) if today_stats else 0,
            'today_wins': int(today_stats.get('today_wins', 0) or 0) if today_stats else 0,
            'today_pnl': round(float(today_stats.get('today_pnl', 0) or 0), 2) if today_stats else 0,
            'current_streak': current_streak,
            'streak_type': streak_type,
            'total_users': int(users_stats.get('total_users', 0) or 0) if users_stats else 0,
            'total_balance': round(float(users_stats.get('total_balance', 0) or 0), 2) if users_stats else 0,
            'positions_by_symbol': positions_by_symbol or []
        }
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting extended stats: {e}")
        return {}


def get_logs(limit: int = 100, level: str = None) -> list:
    """Get recent logs from buffer"""
    logs = list(_log_buffer)
    logs.reverse()  # Newest first
    
    if level:
        logs = [l for l in logs if l['level'] == level.upper()]
    
    return logs[:limit]


def get_db_logs(limit: int = 50) -> list:
    """Get logs from trading_logs table"""
    if not _run_sql:
        return []
    try:
        logs = _run_sql("""
            SELECT timestamp, category, level, message, symbol, direction
            FROM trading_logs
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,), fetch="all")
        return logs or []
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting DB logs: {e}")
        return []


def get_all_users_stats() -> list:
    """Get all users with their trading statistics"""
    if not _run_sql:
        return []
    try:
        # Get all users with basic info
        users = _run_sql("""
            SELECT 
                u.user_id,
                u.balance,
                u.total_deposit,
                u.total_profit,
                u.trading,
                u.auto_trade,
                u.auto_trade_today,
                u.auto_trade_max_daily
            FROM users u
            ORDER BY u.balance DESC
        """, fetch="all")
        
        if not users:
            return []
        
        result = []
        for user in users:
            user_id = user['user_id']
            
            # Get user's trading stats from history
            stats = _run_sql("""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
                    COALESCE(SUM(pnl), 0) as total_pnl
                FROM history
                WHERE user_id = ?
            """, (user_id,), fetch="one")
            
            # Get open positions count
            positions = _run_sql("""
                SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total_amount
                FROM positions
                WHERE user_id = ?
            """, (user_id,), fetch="one")
            
            total_trades = int(stats['total_trades'] or 0) if stats else 0
            wins = int(stats['wins'] or 0) if stats else 0
            total_pnl = float(stats['total_pnl'] or 0) if stats else 0
            winrate = round(wins / total_trades * 100, 1) if total_trades > 0 else 0
            
            result.append({
                'user_id': user_id,
                'balance': round(float(user['balance'] or 0), 2),
                'total_deposit': round(float(user['total_deposit'] or 0), 2),
                'total_profit': round(float(user['total_profit'] or 0), 2),
                'trading': bool(user['trading']),
                'auto_trade': bool(user['auto_trade']),
                'auto_trade_today': int(user['auto_trade_today'] or 0),
                'auto_trade_max_daily': int(user['auto_trade_max_daily'] or 10),
                'total_trades': total_trades,
                'wins': wins,
                'losses': int(stats['losses'] or 0) if stats else 0,
                'winrate': winrate,
                'total_pnl': round(total_pnl, 2),
                'open_positions': int(positions['count'] or 0) if positions else 0,
                'positions_amount': round(float(positions['total_amount'] or 0), 2) if positions else 0
            })
        
        return result
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting users stats: {e}")
        return []


# API Routes
@app.route('/api/stats')
def api_stats():
    """Main statistics endpoint"""
    open_count = get_open_positions_count()
    history_stats = get_history_stats()
    positions_stats = get_positions_stats()
    
    # Get signal stats if available
    signal_stats = {}
    if _get_signal_stats:
        signal_stats = _get_signal_stats()
    
    return jsonify({
        'open_deals': open_count,
        'total_closed': history_stats['total'],
        'profitable': history_stats['profitable'],
        'losing': history_stats['losing'],
        'win_rate': history_stats['win_rate'],
        'total_pnl': round(history_stats['total_pnl'], 2),
        'win_reasons': history_stats['win_reasons'],
        'loss_reasons': history_stats['loss_reasons'],
        'analyzed': signal_stats.get('analyzed', 0),
        'rejected': signal_stats.get('rejected', 0),
        'valid_setups': signal_stats.get('valid_setups', 0),
        'accepted': signal_stats.get('accepted', 0),
        'bybit_opened': signal_stats.get('bybit_opened', 0),
        # New positions stats
        'positions_total': positions_stats['total_positions'],
        'positions_users': positions_stats['users_with_positions'],
        'positions_value': positions_stats['total_value'],
        'positions_unrealized_pnl': positions_stats['total_unrealized_pnl'],
        'positions_long': positions_stats['by_direction']['LONG'],
        'positions_short': positions_stats['by_direction']['SHORT'],
        'timestamp': to_moscow_time()
    })


@app.route('/api/positions')
def api_positions():
    """Open positions endpoint"""
    positions = get_open_positions_details()
    return jsonify({
        'count': len(positions),
        'positions': positions,
        'timestamp': to_moscow_time()
    })


@app.route('/api/trades')
def api_trades():
    """Recent trades endpoint"""
    trades = get_recent_trades(30)
    return jsonify({
        'count': len(trades),
        'trades': trades,
        'timestamp': to_moscow_time()
    })


@app.route('/api/extended')
def api_extended():
    """Extended statistics endpoint"""
    stats = get_extended_stats()
    return jsonify({
        **stats,
        'timestamp': to_moscow_time()
    })


@app.route('/api/logs')
def api_logs():
    """Logs endpoint"""
    limit = request.args.get('limit', 100, type=int)
    level = request.args.get('level', None)
    
    # Get in-memory logs
    memory_logs = get_logs(limit, level)
    
    # Get DB logs
    db_logs = get_db_logs(limit)
    
    return jsonify({
        'memory_logs': memory_logs,
        'db_logs': db_logs,
        'total_logs_in_buffer': len(_log_buffer),
        'timestamp': to_moscow_time()
    })


@app.route('/api/alerts')
def api_alerts():
    """Get recent errors and warnings for display at top of dashboard"""
    limit = request.args.get('limit', 50, type=int)
    hours = request.args.get('hours', 6, type=int)  # Default 6 hours
    
    # Get errors from buffer
    errors = list(_error_buffer)
    errors.reverse()  # Newest first
    
    # Filter by time if specified
    cutoff = datetime.now(MOSCOW_TZ) - timedelta(hours=hours)
    recent_errors = []
    
    for error in errors:
        try:
            # Parse timestamp
            ts_str = error.get('timestamp_iso') or error.get('timestamp')
            if ts_str:
                if 'T' in str(ts_str):
                    ts = datetime.fromisoformat(str(ts_str).replace('Z', '+00:00'))
                else:
                    ts = datetime.strptime(str(ts_str), '%Y-%m-%d %H:%M:%S')
                    ts = ts.replace(tzinfo=MOSCOW_TZ)
                
                if ts.replace(tzinfo=None) > cutoff.replace(tzinfo=None):
                    recent_errors.append(error)
        except Exception:
            recent_errors.append(error)  # Include if we can't parse
    
    # Count by level
    error_count = sum(1 for e in recent_errors if e.get('level') == 'ERROR')
    warning_count = sum(1 for e in recent_errors if e.get('level') == 'WARNING')
    critical_count = sum(1 for e in recent_errors if e.get('level') == 'CRITICAL')
    
    # Get unique error patterns (group similar errors)
    error_patterns = {}
    for error in recent_errors[:100]:  # Analyze first 100
        msg = error.get('message', '')
        # Extract key part of message for grouping
        if '[' in msg:
            key = msg.split(']')[0] + ']' if ']' in msg else msg[:50]
        else:
            key = msg[:50]
        
        if key not in error_patterns:
            error_patterns[key] = {
                'pattern': key,
                'count': 0,
                'level': error.get('level'),
                'last_seen': error.get('timestamp'),
                'sample': msg[:200]
            }
        error_patterns[key]['count'] += 1
    
    # Sort patterns by count
    top_patterns = sorted(error_patterns.values(), key=lambda x: x['count'], reverse=True)[:10]
    
    return jsonify({
        'alerts': recent_errors[:limit],
        'counts': {
            'critical': critical_count,
            'error': error_count,
            'warning': warning_count,
            'total': len(recent_errors)
        },
        'top_patterns': top_patterns,
        'hours': hours,
        'timestamp': to_moscow_time()
    })


@app.route('/api/signal_stats')
def api_signal_stats():
    """Signal generation statistics endpoint"""
    if _get_signal_stats:
        stats = _get_signal_stats()
    else:
        stats = {
            'analyzed': 0,
            'accepted': 0,
            'rejected': 0,
            'reasons': {}
        }
    
    return jsonify({
        **stats,
        'timestamp': to_moscow_time()
    })


@app.route('/api/winrate_breakdown')
def api_winrate_breakdown():
    """Get breakdown of users/trades by winrate thresholds"""
    if not _run_sql:
        return jsonify({'error': 'Database not connected', 'thresholds': {
            '60': {'users': 0, 'trades': 0},
            '70': {'users': 0, 'trades': 0},
            '80': {'users': 0, 'trades': 0},
            '90': {'users': 0, 'trades': 0},
            '95': {'users': 0, 'trades': 0}
        }})
    
    try:
        # Get users with winrate stats (минимум 1 сделка)
        users_wr = _run_sql("""
            SELECT 
                h.user_id,
                COUNT(*) as total_trades,
                SUM(CASE WHEN h.pnl > 0 THEN 1 ELSE 0 END) as wins
            FROM history h
            GROUP BY h.user_id
            HAVING COUNT(*) >= 1
        """, fetch="all")
        
        thresholds = {
            '60': {'users': 0, 'trades': 0},
            '70': {'users': 0, 'trades': 0},
            '80': {'users': 0, 'trades': 0},
            '90': {'users': 0, 'trades': 0},
            '95': {'users': 0, 'trades': 0}
        }
        
        logger.debug(f"[DASHBOARD] Winrate breakdown: found {len(users_wr or [])} users with trades")
        
        for user in (users_wr or []):
            total = int(user['total_trades'] or 0)
            wins = int(user['wins'] or 0)
            wr = (wins / total * 100) if total > 0 else 0
            
            if wr >= 60:
                thresholds['60']['users'] += 1
                thresholds['60']['trades'] += total
            if wr >= 70:
                thresholds['70']['users'] += 1
                thresholds['70']['trades'] += total
            if wr >= 80:
                thresholds['80']['users'] += 1
                thresholds['80']['trades'] += total
            if wr >= 90:
                thresholds['90']['users'] += 1
                thresholds['90']['trades'] += total
            if wr >= 95:
                thresholds['95']['users'] += 1
                thresholds['95']['trades'] += total
        
        return jsonify({
            'thresholds': thresholds,
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting winrate breakdown: {e}", exc_info=True)
        return jsonify({'error': str(e), 'thresholds': {
            '60': {'users': 0, 'trades': 0},
            '70': {'users': 0, 'trades': 0},
            '80': {'users': 0, 'trades': 0},
            '90': {'users': 0, 'trades': 0},
            '95': {'users': 0, 'trades': 0}
        }})


@app.route('/api/export_logs')
def api_export_logs():
    """Export logs for a specified period"""
    # Period: hours, days, months, or all
    hours = request.args.get('hours', type=int)
    days = request.args.get('days', type=int)
    months = request.args.get('months', type=int)
    export_all = request.args.get('all', type=int)
    
    if not _run_sql:
        return jsonify({'error': 'Database not connected'})
    
    # Calculate the cutoff time
    now = datetime.now()
    if export_all:
        cutoff = datetime(2020, 1, 1)  # Very old date to get all logs
        period_str = "all"
    elif hours:
        cutoff = now - timedelta(hours=hours)
        period_str = f"{hours}h"
    elif days:
        cutoff = now - timedelta(days=days)
        period_str = f"{days}d"
    elif months:
        cutoff = now - timedelta(days=months * 30)
        period_str = f"{months}m"
    else:
        cutoff = now - timedelta(hours=24)  # Default 24h
        period_str = "24h"
    
    try:
        # Get logs from database
        if _use_postgres:
            db_logs = _run_sql("""
                SELECT timestamp, category, level, user_id, message, symbol, direction, data
                FROM trading_logs
                WHERE timestamp > %s
                ORDER BY timestamp DESC
            """.replace('%s', '?'), (cutoff.isoformat(),), fetch="all")
        else:
            db_logs = _run_sql("""
                SELECT timestamp, category, level, user_id, message, symbol, direction, data
                FROM trading_logs
                WHERE timestamp > ?
                ORDER BY timestamp DESC
            """, (cutoff.isoformat(),), fetch="all")
        
        # Get in-memory logs
        memory_logs = list(_log_buffer)
        memory_logs.reverse()
        
        # Filter memory logs by time (or include all if export_all)
        filtered_memory = []
        for log in memory_logs:
            try:
                log_time = datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M:%S')
                if log_time > cutoff:
                    filtered_memory.append(log)
            except:
                # Include logs with unparseable timestamps if exporting all
                if export_all:
                    filtered_memory.append(log)
        
        # Combine and format for export
        export_data = {
            'period': period_str,
            'exported_at': to_moscow_time(),
            'cutoff': cutoff.isoformat(),
            'db_logs_count': len(db_logs or []),
            'memory_logs_count': len(filtered_memory),
            'db_logs': db_logs or [],
            'memory_logs': filtered_memory
        }
        
        # Return as downloadable JSON
        response = Response(
            json.dumps(export_data, indent=2, ensure_ascii=False, default=str),
            mimetype='application/json',
            headers={
                'Content-Disposition': f'attachment; filename=logs_export_{period_str}_{now.strftime("%Y%m%d_%H%M%S")}.json'
            }
        )
        return response
        
    except Exception as e:
        logger.error(f"[DASHBOARD] Error exporting logs: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/users')
def api_users():
    """Users statistics endpoint"""
    users = get_all_users_stats()
    
    # Calculate totals
    total_balance = sum(u['balance'] for u in users)
    total_deposits = sum(u['total_deposit'] for u in users)
    active_traders = sum(1 for u in users if u['trading'])
    auto_traders = sum(1 for u in users if u['auto_trade'])
    
    return jsonify({
        'count': len(users),
        'users': users,
        'totals': {
            'total_balance': round(total_balance, 2),
            'total_deposits': round(total_deposits, 2),
            'active_traders': active_traders,
            'auto_traders': auto_traders
        },
        'timestamp': to_moscow_time()
    })


@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': to_moscow_time()})


@app.route('/api/debug')
def api_debug():
    """Debug endpoint to check database state"""
    if not _run_sql:
        return jsonify({'error': 'Database not connected'})
    
    try:
        # Count history records
        history_count = _run_sql("SELECT COUNT(*) as cnt FROM history", fetch="one")
        
        # Count positions
        positions_count = _run_sql("SELECT COUNT(*) as cnt FROM positions", fetch="one")
        
        # Count users
        users_count = _run_sql("SELECT COUNT(*) as cnt FROM users", fetch="one")
        
        # Sample history
        sample_history = _run_sql("""
            SELECT id, user_id, symbol, pnl, reason, closed_at 
            FROM history 
            ORDER BY closed_at DESC 
            LIMIT 5
        """, fetch="all")
        
        # Stats check
        stats_check = _run_sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as profitable,
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losing,
                SUM(pnl) as total_pnl
            FROM history
        """, fetch="one")
        
        return jsonify({
            'history_count': history_count.get('cnt', 0) if history_count else 0,
            'positions_count': positions_count.get('cnt', 0) if positions_count else 0,
            'users_count': users_count.get('cnt', 0) if users_count else 0,
            'sample_history': sample_history or [],
            'stats_check': stats_check,
            'use_postgres': _use_postgres,
            'signal_stats_available': _get_signal_stats is not None,
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Debug endpoint error: {e}", exc_info=True)
        return jsonify({'error': str(e)})


@app.route('/api/user/<int:user_id>')
def api_user_info(user_id):
    """Get detailed info about a specific user for diagnostics"""
    if not _run_sql:
        return jsonify({'error': 'Database not connected'})
    
    try:
        # User info
        user = _run_sql("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch="one")
        
        # Open positions
        positions = _run_sql("SELECT * FROM positions WHERE user_id = ? ORDER BY opened_at DESC", (user_id,), fetch="all")
        
        # Trade history
        history = _run_sql("SELECT * FROM history WHERE user_id = ? ORDER BY closed_at DESC LIMIT 50", (user_id,), fetch="all")
        
        # Stats
        stats = _run_sql("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
                SUM(pnl) as total_pnl,
                SUM(amount) as total_volume
            FROM history WHERE user_id = ?
        """, (user_id,), fetch="one")
        
        # Recent logs for this user
        user_logs = _run_sql("""
            SELECT timestamp, category, level, message, symbol, direction, data
            FROM trading_logs
            WHERE user_id = ?
            ORDER BY timestamp DESC
            LIMIT 100
        """, (user_id,), fetch="all")
        
        return jsonify({
            'user': user,
            'balance': float(user.get('balance', 0)) if user else 0,
            'total_deposit': float(user.get('total_deposit', 0)) if user else 0,
            'total_profit': float(user.get('total_profit', 0)) if user else 0,
            'open_positions': positions or [],
            'open_positions_count': len(positions) if positions else 0,
            'history': history or [],
            'history_count': len(history) if history else 0,
            'stats': stats,
            'user_logs': user_logs or [],
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting user info: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/audit')
def api_audit():
    """Get audit/balance change logs"""
    if not _run_sql:
        return jsonify({'error': 'Database not connected'})
    
    try:
        # Get balance-related logs
        logs = _run_sql("""
            SELECT timestamp, category, level, user_id, message, data
            FROM trading_logs
            WHERE category IN ('BALANCE', 'TRADE_OPEN', 'TRADE_CLOSE', 'DEPOSIT', 'WITHDRAWAL')
            ORDER BY timestamp DESC
            LIMIT 200
        """, fetch="all")
        
        return jsonify({
            'logs': logs or [],
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/news')
def api_news():
    """Get recent news being analyzed by the bot"""
    if not _news_analyzer:
        return jsonify({
            'news': [],
            'sentiment': {'score': 0, 'trend': 'NEUTRAL'},
            'error': 'News analyzer not connected',
            'timestamp': to_moscow_time()
        })
    
    try:
        # Get recent news events from analyzer cache
        recent_events = list(_news_analyzer.recent_events)[-30:]  # Last 30 events
        recent_events.reverse()  # Newest first
        
        news_list = []
        for event in recent_events:
            news_list.append({
                'id': event.id,
                'source': event.source,
                'title': event.title[:150] + '...' if len(event.title) > 150 else event.title,
                'sentiment': event.sentiment.name,
                'sentiment_value': event.sentiment.value,
                'impact': event.impact.name,
                'impact_value': event.impact.value,
                'category': event.category.value,
                'coins': event.affected_coins[:5],  # Top 5 coins
                'keywords': event.keywords_found[:5],  # Top 5 keywords
                'confidence': round(event.confidence * 100),
                'timestamp': event.timestamp.isoformat() if event.timestamp else None,
                'url': event.url
            })
        
        # Get market sentiment
        sentiment = _news_analyzer.get_market_sentiment()
        
        return jsonify({
            'news': news_list,
            'count': len(news_list),
            'sentiment': {
                'score': round(sentiment.get('score', 0), 1),
                'trend': sentiment.get('trend', 'NEUTRAL'),
                'last_update': sentiment.get('last_update').isoformat() if sentiment.get('last_update') else None
            },
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting news: {e}")
        return jsonify({
            'news': [],
            'sentiment': {'score': 0, 'trend': 'NEUTRAL'},
            'error': str(e),
            'timestamp': to_moscow_time()
        })


def run_dashboard(port: int = 5000, host: str = '0.0.0.0'):
    """Run Flask server (called in thread)"""
    # Disable Flask's default logging for cleaner output
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.WARNING)
    
    logger.info(f"[DASHBOARD] Starting on http://{host}:{port}")
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)


def start_dashboard_thread(port: int = None):
    """Start dashboard in a background thread"""
    dashboard_port = port or int(os.getenv('DASHBOARD_PORT', 5000))
    
    thread = Thread(target=run_dashboard, args=(dashboard_port,), daemon=True)
    thread.start()
    logger.info(f"[DASHBOARD] Started in background thread on port {dashboard_port}")
    return thread
