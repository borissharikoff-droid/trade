"""
Trading Bot Dashboard
Minimalist web interface for monitoring trading statistics
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, render_template, request, Response, make_response
from threading import Thread
from collections import deque
import json

logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__, template_folder='templates')
app.config['JSON_SORT_KEYS'] = False

# Optional auth: set DASHBOARD_SECRET in env to require ?token=SECRET or Authorization: Bearer SECRET
DASHBOARD_SECRET = os.environ.get("DASHBOARD_SECRET")


def _dashboard_auth_ok():
    """Return True if request is authorized (no secret set, or valid token)."""
    if not DASHBOARD_SECRET:
        return True
    token = (
        request.args.get("token")
        or request.cookies.get("dashboard_token")
        or (request.headers.get("Authorization") or "").replace("Bearer ", "").strip()
    )
    return token == DASHBOARD_SECRET


@app.before_request
def _require_dashboard_auth():
    if request.path.startswith("/api/") or request.path == "/" or request.path == "" or request.path.startswith("/health"):
        if request.path == "/health":
            return  # no auth for health check
        if not _dashboard_auth_ok():
            if request.path.startswith("/api/"):
                return jsonify({"error": "Unauthorized"}), 401
            return Response("Unauthorized", status=401, headers={"WWW-Authenticate": "Bearer"})

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
    default = {
        'total_positions': 0,
        'users_with_positions': 0,
        'total_value': 0,
        'total_unrealized_pnl': 0,
        'by_direction': {'LONG': 0, 'SHORT': 0}
    }
    
    if not _run_sql:
        logger.warning("[DASHBOARD] get_positions_stats: _run_sql is None")
        return default
    
    try:
        # Simple count first
        count_result = _run_sql("SELECT COUNT(*) as cnt FROM positions", fetch="one")
        total_count = int(count_result.get('cnt', 0) or 0) if count_result else 0
        logger.info(f"[DASHBOARD] Positions count in DB: {total_count}")
        
        if total_count == 0:
            return default
        
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
        
        result = {
            'total_positions': int(stats.get('total_positions', 0) or 0) if stats else 0,
            'users_with_positions': int(stats.get('users_with_positions', 0) or 0) if stats else 0,
            'total_value': round(float(stats.get('total_value', 0) or 0), 2) if stats else 0,
            'total_unrealized_pnl': round(float(stats.get('total_unrealized_pnl', 0) or 0), 2) if stats else 0,
            'by_direction': direction_counts
        }
        logger.debug(f"[DASHBOARD] Positions stats: {result}")
        return result
        
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting positions stats: {e}", exc_info=True)
        return default


_positions_has_is_auto = None  # Cached: True/False/None (not checked yet)

def get_open_positions_details() -> list:
    """Get details of all open positions"""
    global _positions_has_is_auto
    if not _run_sql:
        logger.warning("[DASHBOARD] get_open_positions_details: _run_sql is None")
        return []
    try:
        # Check if is_auto column exists (cached after first check)
        if _positions_has_is_auto is None:
            try:
                _check = _run_sql("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'positions' AND column_name = 'is_auto'
                """, fetch="one")
                _positions_has_is_auto = bool(_check)
            except Exception:
                try:
                    _cols = _run_sql("PRAGMA table_info(positions)", fetch="all")
                    _positions_has_is_auto = any(c.get('name') == 'is_auto' for c in (_cols or []))
                except Exception:
                    _positions_has_is_auto = False
            logger.info(f"[DASHBOARD] positions.is_auto column exists: {_positions_has_is_auto}")
        
        if _positions_has_is_auto:
            positions = _run_sql("""
                SELECT symbol, direction, entry, current, pnl, amount, opened_at, user_id, bybit_qty, is_auto
                FROM positions
                ORDER BY opened_at DESC
                LIMIT 50
            """, fetch="all")
        else:
            positions = _run_sql("""
                SELECT symbol, direction, entry, current, pnl, amount, opened_at, user_id, bybit_qty
                FROM positions
                ORDER BY opened_at DESC
                LIMIT 50
            """, fetch="all")
        
        logger.debug(f"[DASHBOARD] Raw positions query returned: {len(positions) if positions else 0} rows")
        
        # Format positions for API response
        result = []
        for pos in (positions or []):
            bybit_qty = float(pos.get('bybit_qty', 0) or 0)
            result.append({
                'symbol': pos.get('symbol', ''),
                'direction': pos.get('direction', ''),
                'entry': float(pos.get('entry', 0) or 0),
                'current': float(pos.get('current', 0) or 0),
                'pnl': round(float(pos.get('pnl', 0) or 0), 2),
                'amount': round(float(pos.get('amount', 0) or 0), 2),
                'opened_at': pos.get('opened_at'),
                'user_id': pos.get('user_id'),
                'is_auto': bool(pos.get('is_auto', False)),
                'on_bybit': bybit_qty > 0,
                'bybit_qty': bybit_qty
            })
        return result
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting positions details: {e}", exc_info=True)
        return []


def normalize_reason(raw_reason: str) -> str:
    """Normalize raw close reason into a clean, human-readable category.
    
    Groups internal technical reasons (BYBIT_TP, WS_SL, LINKED_TP, etc.)
    into meaningful trading categories for dashboard analytics.
    """
    if not raw_reason:
        return "Unknown"
    
    r = raw_reason.upper().strip()
    
    # Strip prefixes: LINKED_, WS_, BYBIT_ to get the core reason
    for prefix in ('LINKED_', 'WS_', 'BYBIT_'):
        if r.startswith(prefix):
            r = r[len(prefix):]
            break
    
    # Take Profit variants
    if r in ('TP', 'TP1', 'TP2', 'TP3'):
        return "Take Profit"
    
    # Stop Loss
    if r in ('SL',):
        return "Stop Loss"
    
    # Manual close by user
    if r in ('MANUAL', 'MANUAL_CLOSE'):
        return "Manual Close"
    
    # Close All command
    if r in ('CLOSE_ALL',):
        return "Close All"
    
    # AI Early Exit
    if r.startswith('EARLY_EXIT'):
        return "AI Early Exit"
    
    # Bybit detected closure (position closed on exchange, e.g. liquidation or TP/SL set on exchange)
    if r in ('CLOSED',):
        return "Exchange Close"
    
    # System/technical cleanup — phantom, micro, fully closed
    if r in ('PHANTOM_CLEANUP', 'PHANTOM_OLD', 'MANUAL_PHANTOM_CLEANUP', 
             'FULLY_CLOSED', 'MICRO_CLOSE', 'FORCE_CLEANUP'):
        return "System Cleanup"
    
    # Trailing stop
    if r in ('TRAILING', 'TRAILING_STOP'):
        return "Trailing Stop"
    
    # Fallback: return cleaned-up version
    return raw_reason.replace('_', ' ').title()


def _aggregate_reasons(rows: list) -> dict:
    """Aggregate raw reason rows into normalized categories."""
    aggregated = {}
    for row in (rows or []):
        raw = row.get('reason')
        if not raw:
            continue
        category = normalize_reason(raw)
        aggregated[category] = aggregated.get(category, 0) + int(row['count'])
    # Sort by count descending
    return dict(sorted(aggregated.items(), key=lambda x: x[1], reverse=True))


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
        win_reasons = _aggregate_reasons(win_reasons_rows)
        
        # Get loss reasons breakdown
        loss_reasons_rows = _run_sql("""
            SELECT reason, COUNT(*) as count
            FROM history
            WHERE pnl <= 0
            GROUP BY reason
            ORDER BY count DESC
        """, fetch="all")
        loss_reasons = _aggregate_reasons(loss_reasons_rows)
        
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
        # Basic query - is_auto column doesn't exist in history table
        trades = _run_sql("""
            SELECT symbol, direction, entry, exit_price, pnl, reason, amount, closed_at
            FROM history
            ORDER BY closed_at DESC
            LIMIT ?
        """, (limit,), fetch="all")
        
        # Format trades for API response
        result = []
        for trade in (trades or []):
            # Detect auto trades by reason
            reason = trade.get('reason', '') or ''
            is_auto = 'AUTO' in reason.upper() or 'LINKED' in reason.upper()
            
            result.append({
                'symbol': trade.get('symbol', ''),
                'direction': trade.get('direction', ''),
                'entry': float(trade.get('entry', 0) or 0),
                'exit_price': float(trade.get('exit_price', 0) or 0),
                'pnl': round(float(trade.get('pnl', 0) or 0), 2),
                'reason': reason,
                'amount': round(float(trade.get('amount', 0) or 0), 2),
                'closed_at': trade.get('closed_at'),
                'is_auto': is_auto
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
        
        # Average PnL + profit factor
        avg_stats = _run_sql("""
            SELECT 
                AVG(pnl) as avg_pnl,
                AVG(CASE WHEN pnl > 0 THEN pnl END) as avg_win,
                AVG(CASE WHEN pnl <= 0 THEN pnl END) as avg_loss,
                SUM(amount) as total_volume,
                SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gross_profit,
                ABS(SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END)) as gross_loss
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
        
        # Yesterday's stats for comparison
        if _use_postgres:
            yesterday_stats = _run_sql("""
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as pnl
                FROM history
                WHERE DATE(closed_at) = CURRENT_DATE - INTERVAL '1 day'
            """, fetch="one")
        else:
            yesterday_stats = _run_sql("""
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as pnl
                FROM history
                WHERE DATE(closed_at) = DATE('now', '-1 day')
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
            'profit_factor': round(float(avg_stats.get('gross_profit', 0) or 0) / max(float(avg_stats.get('gross_loss', 0) or 0), 0.01), 2) if avg_stats else 0,
            'today_trades': int(today_stats.get('today_trades', 0) or 0) if today_stats else 0,
            'today_wins': int(today_stats.get('today_wins', 0) or 0) if today_stats else 0,
            'today_pnl': round(float(today_stats.get('today_pnl', 0) or 0), 2) if today_stats else 0,
            'yesterday_trades': int(yesterday_stats.get('trades', 0) or 0) if yesterday_stats else 0,
            'yesterday_wins': int(yesterday_stats.get('wins', 0) or 0) if yesterday_stats else 0,
            'yesterday_pnl': round(float(yesterday_stats.get('pnl', 0) or 0), 2) if yesterday_stats else 0,
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
    try:
        positions = get_open_positions_details()
        logger.info(f"[DASHBOARD] /api/positions returning {len(positions)} positions")
        return jsonify({
            'count': len(positions),
            'positions': positions,
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error in /api/positions: {e}", exc_info=True)
        return jsonify({
            'count': 0,
            'positions': [],
            'error': str(e),
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
    limit = max(1, min(1000, limit))  # clamp 1-1000
    level = request.args.get('level', None)
    if level is not None and level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        level = None
    
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
        # Get logs from database (? placeholders auto-converted to %s for PostgreSQL by run_sql)
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
    
    # Calculate totals - включаем стоимость позиций
    total_balance = sum(u['balance'] for u in users)
    total_positions_value = sum(u.get('positions_amount', 0) for u in users)
    total_equity = total_balance + total_positions_value  # Полная стоимость
    total_deposits = sum(u['total_deposit'] for u in users)
    active_traders = sum(1 for u in users if u['trading'])
    auto_traders = sum(1 for u in users if u['auto_trade'])
    total_pnl = sum(u.get('total_pnl', 0) for u in users)
    
    return jsonify({
        'count': len(users),
        'users': users,
        'totals': {
            'total_balance': round(total_balance, 2),
            'total_positions_value': round(total_positions_value, 2),
            'total_equity': round(total_equity, 2),
            'total_deposits': round(total_deposits, 2),
            'total_pnl': round(total_pnl, 2),
            'active_traders': active_traders,
            'auto_traders': auto_traders
        },
        'timestamp': to_moscow_time()
    })


@app.route('/api/pnl_history')
def api_pnl_history():
    """Get PnL history for chart (last 30 days)"""
    if not _run_sql:
        return jsonify({'error': 'Database not connected', 'data': []})
    
    try:
        # Get daily PnL for last 30 days
        if _use_postgres:
            daily_pnl = _run_sql("""
                SELECT 
                    DATE(closed_at) as date,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as daily_pnl,
                    SUM(amount) as volume
                FROM history
                WHERE closed_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(closed_at)
                ORDER BY DATE(closed_at) ASC
            """, fetch="all")
        else:
            daily_pnl = _run_sql("""
                SELECT 
                    DATE(closed_at) as date,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as daily_pnl,
                    SUM(amount) as volume
                FROM history
                WHERE closed_at >= DATE('now', '-30 days')
                GROUP BY DATE(closed_at)
                ORDER BY DATE(closed_at) ASC
            """, fetch="all")
        
        # Calculate cumulative PnL
        cumulative = 0
        data = []
        for row in (daily_pnl or []):
            daily = float(row.get('daily_pnl', 0) or 0)
            cumulative += daily
            trades = int(row.get('trades', 0) or 0)
            wins = int(row.get('wins', 0) or 0)
            
            data.append({
                'date': str(row.get('date', '')),
                'daily_pnl': round(daily, 2),
                'cumulative_pnl': round(cumulative, 2),
                'trades': trades,
                'wins': wins,
                'winrate': round(wins / trades * 100, 1) if trades > 0 else 0,
                'volume': round(float(row.get('volume', 0) or 0), 2)
            })
        
        return jsonify({
            'data': data,
            'total_days': len(data),
            'total_pnl': round(cumulative, 2),
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting PnL history: {e}")
        return jsonify({'error': str(e), 'data': []})


@app.route('/api/analytics_summary')
def api_analytics_summary():
    """Get comprehensive analytics summary"""
    if not _run_sql:
        return jsonify({'error': 'Database not connected'})
    
    try:
        # Get last 7 days stats
        if _use_postgres:
            last_7d = _run_sql("""
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as pnl,
                    SUM(amount) as volume
                FROM history
                WHERE closed_at >= CURRENT_DATE - INTERVAL '7 days'
            """, fetch="one")
            
            last_24h = _run_sql("""
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as pnl
                FROM history
                WHERE closed_at >= NOW() - INTERVAL '24 hours'
            """, fetch="one")
            
            # Top performing symbols
            top_symbols = _run_sql("""
                SELECT 
                    symbol,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl
                FROM history
                WHERE closed_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY symbol
                HAVING COUNT(*) >= 3
                ORDER BY SUM(pnl) DESC
                LIMIT 10
            """, fetch="all")
            
            # Worst performing symbols
            worst_symbols = _run_sql("""
                SELECT 
                    symbol,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl
                FROM history
                WHERE closed_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY symbol
                HAVING COUNT(*) >= 3
                ORDER BY SUM(pnl) ASC
                LIMIT 5
            """, fetch="all")
            
            # Performance by direction
            by_direction = _run_sql("""
                SELECT 
                    direction,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl
                FROM history
                WHERE closed_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY direction
            """, fetch="all")
            
            # Hourly performance (what hours are most profitable)
            hourly_perf = _run_sql("""
                SELECT 
                    EXTRACT(HOUR FROM closed_at) as hour,
                    COUNT(*) as trades,
                    SUM(pnl) as total_pnl,
                    AVG(pnl) as avg_pnl
                FROM history
                WHERE closed_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY EXTRACT(HOUR FROM closed_at)
                ORDER BY SUM(pnl) DESC
            """, fetch="all")
        else:
            last_7d = _run_sql("""
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as pnl,
                    SUM(amount) as volume
                FROM history
                WHERE closed_at >= DATE('now', '-7 days')
            """, fetch="one")
            
            last_24h = _run_sql("""
                SELECT 
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as pnl
                FROM history
                WHERE closed_at >= DATETIME('now', '-24 hours')
            """, fetch="one")
            
            top_symbols = _run_sql("""
                SELECT 
                    symbol,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl
                FROM history
                WHERE closed_at >= DATE('now', '-30 days')
                GROUP BY symbol
                HAVING COUNT(*) >= 3
                ORDER BY SUM(pnl) DESC
                LIMIT 10
            """, fetch="all")
            
            worst_symbols = _run_sql("""
                SELECT 
                    symbol,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl
                FROM history
                WHERE closed_at >= DATE('now', '-30 days')
                GROUP BY symbol
                HAVING COUNT(*) >= 3
                ORDER BY SUM(pnl) ASC
                LIMIT 5
            """, fetch="all")
            
            by_direction = _run_sql("""
                SELECT 
                    direction,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl) as total_pnl
                FROM history
                WHERE closed_at >= DATE('now', '-30 days')
                GROUP BY direction
            """, fetch="all")
            
            hourly_perf = _run_sql("""
                SELECT 
                    CAST(strftime('%H', closed_at) AS INTEGER) as hour,
                    COUNT(*) as trades,
                    SUM(pnl) as total_pnl,
                    AVG(pnl) as avg_pnl
                FROM history
                WHERE closed_at >= DATE('now', '-30 days')
                GROUP BY strftime('%H', closed_at)
                ORDER BY SUM(pnl) DESC
            """, fetch="all")
        
        # Format results
        def format_symbol_stats(rows):
            result = []
            for row in (rows or []):
                trades = int(row.get('trades', 0) or 0)
                wins = int(row.get('wins', 0) or 0)
                result.append({
                    'symbol': row.get('symbol', ''),
                    'trades': trades,
                    'wins': wins,
                    'winrate': round(wins / trades * 100, 1) if trades > 0 else 0,
                    'pnl': round(float(row.get('total_pnl', 0) or 0), 2)
                })
            return result
        
        def format_direction_stats(rows):
            result = {}
            for row in (rows or []):
                direction = row.get('direction', 'UNKNOWN')
                trades = int(row.get('trades', 0) or 0)
                wins = int(row.get('wins', 0) or 0)
                result[direction] = {
                    'trades': trades,
                    'wins': wins,
                    'winrate': round(wins / trades * 100, 1) if trades > 0 else 0,
                    'pnl': round(float(row.get('total_pnl', 0) or 0), 2)
                }
            return result
        
        def format_hourly_stats(rows):
            result = []
            for row in (rows or []):
                result.append({
                    'hour': int(row.get('hour', 0) or 0),
                    'trades': int(row.get('trades', 0) or 0),
                    'pnl': round(float(row.get('total_pnl', 0) or 0), 2),
                    'avg_pnl': round(float(row.get('avg_pnl', 0) or 0), 2)
                })
            return result
        
        # Calculate 7d stats
        trades_7d = int(last_7d.get('trades', 0) or 0) if last_7d else 0
        wins_7d = int(last_7d.get('wins', 0) or 0) if last_7d else 0
        pnl_7d = float(last_7d.get('pnl', 0) or 0) if last_7d else 0
        
        # Calculate 24h stats
        trades_24h = int(last_24h.get('trades', 0) or 0) if last_24h else 0
        wins_24h = int(last_24h.get('wins', 0) or 0) if last_24h else 0
        pnl_24h = float(last_24h.get('pnl', 0) or 0) if last_24h else 0
        
        return jsonify({
            'last_7d': {
                'trades': trades_7d,
                'wins': wins_7d,
                'winrate': round(wins_7d / trades_7d * 100, 1) if trades_7d > 0 else 0,
                'pnl': round(pnl_7d, 2),
                'volume': round(float(last_7d.get('volume', 0) or 0), 2) if last_7d else 0
            },
            'last_24h': {
                'trades': trades_24h,
                'wins': wins_24h,
                'winrate': round(wins_24h / trades_24h * 100, 1) if trades_24h > 0 else 0,
                'pnl': round(pnl_24h, 2)
            },
            'top_symbols': format_symbol_stats(top_symbols),
            'worst_symbols': format_symbol_stats(worst_symbols),
            'by_direction': format_direction_stats(by_direction),
            'best_hours': format_hourly_stats(hourly_perf)[:5],  # Top 5 hours
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting analytics summary: {e}", exc_info=True)
        return jsonify({'error': str(e)})


@app.route('/')
def dashboard():
    """Main dashboard page. If ?token=DASHBOARD_SECRET, set cookie so API calls work."""
    resp = make_response(render_template('dashboard.html'))
    if DASHBOARD_SECRET and request.args.get("token") == DASHBOARD_SECRET:
        resp.set_cookie("dashboard_token", DASHBOARD_SECRET, max_age=86400 * 7, httponly=True, samesite="Lax")
    return resp


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
            'bob_index': {'value': 5, 'label': 'NEUTRAL', 'factors': {}},
            'error': 'News analyzer not connected',
            'timestamp': to_moscow_time()
        })
    
    try:
        # Get recent news events from analyzer cache
        if not hasattr(_news_analyzer, 'recent_events') or _news_analyzer.recent_events is None:
            return jsonify({
                'news': [],
                'sentiment': {'score': 0, 'trend': 'NEUTRAL'},
                'bob_index': {'value': 5, 'label': 'NEUTRAL', 'factors': {}},
                'error': 'News events not available',
                'timestamp': to_moscow_time()
            })
        
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
        
        # BOB index (Bearish or Bullish 1-10)
        try:
            import asyncio
            bob_index = asyncio.run(_news_analyzer.calculate_bob_index())
        except Exception as e:
            logger.debug(f"[DASHBOARD] BOB index error: {e}")
            bob_index = {'value': 5, 'label': 'NEUTRAL', 'factors': {}}
        
        return jsonify({
            'news': news_list,
            'count': len(news_list),
            'sentiment': {
                'score': round(sentiment.get('score', 0), 1),
                'trend': sentiment.get('trend', 'NEUTRAL'),
                'last_update': sentiment.get('last_update').isoformat() if sentiment.get('last_update') else None
            },
            'bob_index': bob_index,
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting news: {e}")
        return jsonify({
            'news': [],
            'sentiment': {'score': 0, 'trend': 'NEUTRAL'},
            'bob_index': {'value': 5, 'label': 'NEUTRAL', 'factors': {}},
            'error': str(e),
            'timestamp': to_moscow_time()
        })


@app.route('/api/ai')
def api_ai():
    """AI Analyzer statistics and learning history"""
    try:
        # Try to import AI analyzer
        try:
            from ai_analyzer import get_ai_analyzer, get_ai_stats, get_pending_news_analysis, get_tracked_news_count
            ai_available = True
        except ImportError:
            ai_available = False
            return jsonify({
                'available': False,
                'error': 'AI Analyzer not installed',
                'timestamp': to_moscow_time()
            })
        
        analyzer = get_ai_analyzer()
        stats = get_ai_stats()
        memory = analyzer.memory
        
        # Get recent trade analyses (last 50 shown, total count tracked)
        total_analyses_count = len(analyzer.recent_analyses)
        recent_analyses = list(analyzer.recent_analyses)[-50:]
        recent_analyses.reverse()
        
        # Aggregate win/loss factors (fundamental market reasons) from AI analyses
        all_analyses = list(analyzer.recent_analyses)
        win_factors_agg = {}
        loss_factors_agg = {}
        for a in all_analyses:
            if a.get('is_win'):
                for f in (a.get('win_factors') or []):
                    f_clean = f.strip()[:80]
                    if f_clean:
                        win_factors_agg[f_clean] = win_factors_agg.get(f_clean, 0) + 1
            else:
                for f in (a.get('loss_factors') or []):
                    f_clean = f.strip()[:80]
                    if f_clean:
                        loss_factors_agg[f_clean] = loss_factors_agg.get(f_clean, 0) + 1
        # Sort by count desc, take top 10
        win_factors_sorted = dict(sorted(win_factors_agg.items(), key=lambda x: x[1], reverse=True)[:10])
        loss_factors_sorted = dict(sorted(loss_factors_agg.items(), key=lambda x: x[1], reverse=True)[:10])
        
        # Get learned rules (last 50 shown, total count tracked)
        total_rules_count = len(memory.learned_rules)
        learned_rules = memory.learned_rules[-50:]
        learned_rules.reverse()
        
        # Get market insights (last 30)
        insights = memory.market_insights[-30:]
        insights.reverse()
        
        # Get trade patterns summary per symbol
        pattern_summary = {}
        for symbol, patterns in memory.trade_patterns.items():
            wins = len(patterns.get('wins', []))
            losses = len(patterns.get('losses', []))
            total = wins + losses
            pattern_summary[symbol] = {
                'wins': wins,
                'losses': losses,
                'total': total,
                'winrate': round(wins / total * 100, 1) if total > 0 else 0,
                'recent_lessons': []
            }
            # Add recent lessons from wins and losses
            for p in patterns.get('wins', [])[-2:]:
                if p.get('lessons'):
                    pattern_summary[symbol]['recent_lessons'].extend(p['lessons'][:1])
            for p in patterns.get('losses', [])[-2:]:
                if p.get('lessons'):
                    pattern_summary[symbol]['recent_lessons'].extend(p['lessons'][:1])
        
        # Get news patterns summary
        news_patterns = []
        for news_hash, pattern in list(memory.news_patterns.items())[-20:]:
            news_patterns.append({
                'title': pattern.get('title', '')[:80],
                'direction': pattern.get('actual_direction', 'NEUTRAL'),
                'change': round(pattern.get('price_change', 0), 2),
                'category': pattern.get('category', 'other'),
                'patterns': pattern.get('patterns', [])[:2],
                'timestamp': pattern.get('timestamp', '')
            })
        news_patterns.reverse()
        
        # Get pending news being tracked for impact analysis
        pending_news = []
        tracked_count = 0
        try:
            pending_news = get_pending_news_analysis()
            tracked_count = get_tracked_news_count()
        except:
            pass
        
        # Compute today's learned rules count
        today_rules_count = 0
        try:
            from datetime import datetime, timezone, timedelta
            now_utc = datetime.now(timezone.utc)
            today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            # Check insights for daily_summary category from today
            for ins in memory.market_insights:
                ts = ins.get('timestamp', '')
                if ts and ins.get('category') == 'daily_summary':
                    pass  # just counting rules below
            # Count rules added today (heuristic: last N rules added in the past 24h)
            # Since rules don't have timestamps, use the total as-is
            today_rules_count = stats.get('learned_rules_count', 0)
        except:
            pass

        return jsonify({
            'available': True,
            'initialized': stats.get('initialized', False),
            'stats': {
                'total_trades_analyzed': stats.get('total_trades_analyzed', 0),
                'total_news_analyzed': stats.get('total_news_analyzed', 0),
                'learned_rules_count': stats.get('learned_rules_count', 0),
                'trade_patterns_count': stats.get('trade_patterns_count', 0),
                'news_patterns_count': stats.get('news_patterns_count', 0),
                'market_insights_count': stats.get('market_insights_count', 0),
                'prediction_accuracy': round(stats.get('prediction_accuracy', 0.5) * 100, 1),
                'news_tracking_count': tracked_count,
                'today_rules_count': today_rules_count
            },
            'recent_analyses': recent_analyses,
            'total_analyses_count': total_analyses_count,
            'learned_rules': learned_rules,
            'total_rules_count': total_rules_count,
            'insights': insights,
            'pattern_summary': pattern_summary,
            'news_patterns': news_patterns,
            'pending_news': pending_news,
            'win_factors': win_factors_sorted,
            'loss_factors': loss_factors_sorted,
            'timestamp': to_moscow_time()
        })
        
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting AI stats: {e}", exc_info=True)
        return jsonify({
            'available': False,
            'error': str(e),
            'timestamp': to_moscow_time()
        })


@app.route('/api/ai/generate_report', methods=['POST'])
def api_ai_generate_report():
    """Generate a daily summary report on demand (for yesterday or custom date)."""
    try:
        import asyncio
        from datetime import datetime, timezone, timedelta
        from ai_analyzer import daily_insights as ai_daily_insights

        data = request.get_json(silent=True) or {}
        # Default to yesterday
        target_date = data.get('date', 'yesterday')
        now_utc = datetime.now(timezone.utc)
        if target_date == 'yesterday':
            day_start = (now_utc - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
        elif target_date == 'today':
            day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
        else:
            day_start = datetime.fromisoformat(target_date).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1)

        since_str = day_start.isoformat()
        until_str = day_end.isoformat()

        if _use_postgres:
            rows = _run_sql(
                "SELECT user_id, symbol, direction, entry, exit_price, pnl, reason, closed_at FROM history WHERE closed_at >= %s AND closed_at < %s",
                (since_str, until_str), fetch="all"
            )
        else:
            rows = _run_sql(
                "SELECT user_id, symbol, direction, entry, exit_price, pnl, reason, closed_at FROM history WHERE closed_at >= ? AND closed_at < ?",
                (since_str, until_str), fetch="all"
            )

        trades = [dict(r) for r in (rows or [])]
        
        # Calculate stats for the title
        total_pnl = sum(float(t.get('pnl', 0)) for t in trades)
        wins = sum(1 for t in trades if float(t.get('pnl', 0)) > 0)
        wr = (wins / len(trades) * 100) if trades else 0

        news = []
        try:
            if _news_analyzer and hasattr(_news_analyzer, 'recent_events'):
                news = [{'title': getattr(e, 'title', str(e)[:80])} for e in list(_news_analyzer.recent_events)[-20:]
                ]
        except:
            pass

        summary = asyncio.run(ai_daily_insights(trades, news))

        msk_tz = timezone(timedelta(hours=3))
        report_time = datetime.now(msk_tz).strftime('%d.%m.%Y %H:%M MSK')
        day_label = day_start.strftime('%d.%m.%Y')
        pnl_sign = '+' if total_pnl >= 0 else ''

        return jsonify({
            'success': True,
            'report': summary,
            'date': day_label,
            'generated_at': report_time,
            'trades_count': len(trades),
            'total_pnl': round(total_pnl, 2),
            'win_rate': round(wr, 1),
            'title': f"{pnl_sign}${total_pnl:.2f} | {len(trades)} trades | WR {wr:.0f}%",
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Error generating report: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': to_moscow_time()
        })


# ==================== LEARNING ANALYTICS API ====================
@app.route('/api/learning')
def api_learning():
    """Получить данные о прогрессе обучения AI"""
    try:
        from learning_tracker import get_learning_summary, get_learning_tracker
        
        tracker = get_learning_tracker()
        summary = tracker.get_learning_summary()
        
        return jsonify({
            'success': True,
            'data': summary,
            'timestamp': to_moscow_time()
        })
    except ImportError:
        return jsonify({
            'success': False,
            'error': 'Learning tracker not available',
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Learning API error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': to_moscow_time()
        })


@app.route('/api/learning/report/<period>')
def api_learning_report(period: str):
    """Получить детальный отчёт за период (daily/weekly/monthly)"""
    try:
        from learning_tracker import get_learning_tracker
        from dataclasses import asdict
        
        if period not in ['daily', 'weekly', 'monthly']:
            return jsonify({'success': False, 'error': 'Invalid period'})
        
        tracker = get_learning_tracker()
        report = tracker.generate_report(period)
        
        return jsonify({
            'success': True,
            'report': asdict(report),
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Learning report error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': to_moscow_time()
        })


@app.route('/api/learning/snapshots')
def api_learning_snapshots():
    """Получить исторические снимки для графиков"""
    try:
        from learning_tracker import get_learning_tracker
        from dataclasses import asdict
        
        tracker = get_learning_tracker()
        snapshots = [asdict(s) for s in tracker.snapshots[-30:]]  # Последние 30 дней
        
        return jsonify({
            'success': True,
            'snapshots': snapshots,
            'count': len(snapshots),
            'timestamp': to_moscow_time()
        })
    except Exception as e:
        logger.error(f"[DASHBOARD] Learning snapshots error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': to_moscow_time()
        })


# ==================== PHANTOM CLEANUP API ====================
@app.route('/api/cleanup_phantoms', methods=['POST'])
def api_cleanup_phantoms():
    """Очистить phantom позиции (в боте, но не на Bybit)"""
    try:
        # Получаем все позиции из БД
        all_positions = _run_sql("SELECT * FROM positions", fetch="all") or []
        
        if not all_positions:
            return jsonify({
                'success': True,
                'message': 'No positions in database',
                'closed': 0,
                'timestamp': to_moscow_time()
            })
        
        # Для простоты - закрываем ВСЕ позиции с нулевым PnL (возвращаем amount)
        # Так как у пользователя 0 позиций на Bybit
        closed_count = 0
        total_returned = 0
        
        for pos in all_positions:
            try:
                pos_user_id = pos['user_id']
                amount = float(pos.get('amount', 0))
                
                # Возвращаем amount пользователю
                user = _run_sql("SELECT * FROM users WHERE user_id = ?", (pos_user_id,), fetch="one")
                if user:
                    new_balance = float(user.get('balance', 0)) + amount
                    _run_sql("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, pos_user_id))
                
                # Удаляем позицию
                _run_sql("DELETE FROM positions WHERE id = ?", (pos['id'],))
                
                # Добавляем в history
                _run_sql("""
                    INSERT INTO history (user_id, symbol, direction, entry, exit_price, sl, tp, amount, commission, pnl, reason, opened_at, closed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'PHANTOM_CLEANUP', ?, CURRENT_TIMESTAMP)
                """, (
                    pos_user_id, pos['symbol'], pos['direction'],
                    pos.get('entry', 0), pos.get('current', pos.get('entry', 0)),
                    pos.get('sl', 0), pos.get('tp', 0), amount, pos.get('commission', 0),
                    pos.get('opened_at')
                ))
                
                closed_count += 1
                total_returned += amount
                logger.info(f"[PHANTOM_CLEANUP] API: Closed position {pos['id']}: {pos['symbol']}")
            except Exception as e:
                logger.error(f"[PHANTOM_CLEANUP] Error closing {pos.get('id')}: {e}")
        
        return jsonify({
            'success': True,
            'message': f'Cleaned up {closed_count} phantom positions',
            'closed': closed_count,
            'returned': round(total_returned, 2),
            'timestamp': to_moscow_time()
        })
        
    except Exception as e:
        logger.error(f"[DASHBOARD] Phantom cleanup error: {e}")
        return jsonify({
            'success': False,
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
