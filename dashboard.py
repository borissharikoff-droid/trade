"""
Trading Bot Dashboard
Minimalist web interface for monitoring trading statistics
"""

import os
import logging
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
from threading import Thread
from collections import deque

logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__, template_folder='templates')
app.config['JSON_SORT_KEYS'] = False

# Will be set from bot.py
_run_sql = None
_use_postgres = False

# In-memory log buffer (last 500 logs)
_log_buffer = deque(maxlen=500)


class DashboardLogHandler(logging.Handler):
    """Custom log handler to capture logs for dashboard"""
    def emit(self, record):
        try:
            log_entry = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'level': record.levelname,
                'message': self.format(record)
            }
            _log_buffer.append(log_entry)
        except Exception:
            pass


def init_dashboard(run_sql_func, use_postgres: bool = False):
    """Initialize dashboard with database function from bot.py"""
    global _run_sql, _use_postgres
    _run_sql = run_sql_func
    _use_postgres = use_postgres
    
    # Add log handler to capture bot logs
    root_logger = logging.getLogger()
    handler = DashboardLogHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    handler.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    
    logger.info("[DASHBOARD] Initialized with database connection")


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


def get_open_positions_details() -> list:
    """Get details of all open positions"""
    if not _run_sql:
        return []
    try:
        positions = _run_sql("""
            SELECT symbol, direction, entry, current, pnl, amount, opened_at
            FROM positions
            ORDER BY opened_at DESC
            LIMIT 50
        """, fetch="all")
        return positions or []
    except Exception as e:
        logger.error(f"[DASHBOARD] Error getting positions details: {e}")
        return []


def get_history_stats() -> dict:
    """Get aggregated statistics from history"""
    if not _run_sql:
        return {
            'total': 0,
            'profitable': 0,
            'losing': 0,
            'win_rate': 0,
            'total_pnl': 0,
            'win_reasons': {},
            'loss_reasons': {}
        }
    
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
        
        total = int(stats['total'] or 0) if stats else 0
        profitable = int(stats['profitable'] or 0) if stats else 0
        losing = int(stats['losing'] or 0) if stats else 0
        total_pnl = float(stats['total_pnl'] or 0) if stats else 0
        win_rate = round(profitable / total * 100, 2) if total > 0 else 0
        
        # Get win reasons breakdown
        win_reasons_rows = _run_sql("""
            SELECT reason, COUNT(*) as count
            FROM history
            WHERE pnl > 0
            GROUP BY reason
            ORDER BY count DESC
        """, fetch="all")
        win_reasons = {row['reason']: row['count'] for row in (win_reasons_rows or [])}
        
        # Get loss reasons breakdown
        loss_reasons_rows = _run_sql("""
            SELECT reason, COUNT(*) as count
            FROM history
            WHERE pnl <= 0
            GROUP BY reason
            ORDER BY count DESC
        """, fetch="all")
        loss_reasons = {row['reason']: row['count'] for row in (loss_reasons_rows or [])}
        
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
        trades = _run_sql("""
            SELECT symbol, direction, entry, exit_price, pnl, reason, amount, closed_at
            FROM history
            ORDER BY closed_at DESC
            LIMIT ?
        """, (limit,), fetch="all")
        return trades or []
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
        
        # Today's stats
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
    
    return jsonify({
        'open_deals': open_count,
        'total_closed': history_stats['total'],
        'profitable': history_stats['profitable'],
        'losing': history_stats['losing'],
        'win_rate': history_stats['win_rate'],
        'total_pnl': round(history_stats['total_pnl'], 2),
        'win_reasons': history_stats['win_reasons'],
        'loss_reasons': history_stats['loss_reasons'],
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/positions')
def api_positions():
    """Open positions endpoint"""
    positions = get_open_positions_details()
    return jsonify({
        'count': len(positions),
        'positions': positions,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/trades')
def api_trades():
    """Recent trades endpoint"""
    trades = get_recent_trades(30)
    return jsonify({
        'count': len(trades),
        'trades': trades,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/extended')
def api_extended():
    """Extended statistics endpoint"""
    stats = get_extended_stats()
    return jsonify({
        **stats,
        'timestamp': datetime.now().isoformat()
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
        'timestamp': datetime.now().isoformat()
    })


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
        'timestamp': datetime.now().isoformat()
    })


@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


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
            'timestamp': datetime.now().isoformat()
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
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)})


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
