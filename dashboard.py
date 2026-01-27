"""
Trading Bot Dashboard
Minimalist web interface for monitoring trading statistics
"""

import os
import logging
from datetime import datetime
from flask import Flask, jsonify, render_template
from threading import Thread

logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__, template_folder='templates')
app.config['JSON_SORT_KEYS'] = False

# Will be set from bot.py
_run_sql = None
_use_postgres = False


def init_dashboard(run_sql_func, use_postgres: bool = False):
    """Initialize dashboard with database function from bot.py"""
    global _run_sql, _use_postgres
    _run_sql = run_sql_func
    _use_postgres = use_postgres
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


@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


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
