"""
Monitoring and Health Checks
Provides metrics collection and health check endpoints
"""

import logging
import time
from typing import Dict, List
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and stores metrics"""
    
    def __init__(self):
        self.metrics: Dict[str, any] = defaultdict(int)
        self.timestamps: Dict[str, float] = {}
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.last_reset = time.time()
    
    def increment(self, metric: str, value: int = 1):
        """Increment a counter metric"""
        self.metrics[metric] += value
    
    def set_gauge(self, metric: str, value: float):
        """Set a gauge metric"""
        self.metrics[metric] = value
        self.timestamps[metric] = time.time()
    
    def record_error(self, error_type: str):
        """Record an error"""
        self.error_counts[error_type] += 1
        self.increment("errors_total")
    
    def get_metrics(self) -> Dict:
        """Get all metrics"""
        return {
            'counters': dict(self.metrics),
            'errors': dict(self.error_counts),
            'uptime': time.time() - self.last_reset
        }
    
    def reset(self):
        """Reset metrics"""
        self.metrics.clear()
        self.error_counts.clear()
        self.last_reset = time.time()


# Global metrics collector
metrics = MetricsCollector()


def record_handler_call(handler_name: str):
    """Record a handler call"""
    metrics.increment(f"handlers_{handler_name}")
    metrics.increment("handlers_total")


def record_api_call(service: str, success: bool):
    """Record an API call"""
    metrics.increment(f"api_calls_{service}")
    if success:
        metrics.increment(f"api_success_{service}")
    else:
        metrics.increment(f"api_errors_{service}")
        metrics.record_error(f"api_{service}")


def record_database_operation(operation: str, duration: float):
    """Record database operation"""
    metrics.increment(f"db_operations_{operation}")
    metrics.set_gauge(f"db_duration_{operation}", duration)


def record_position_update(user_count: int, position_count: int, duration: float):
    """Record position update metrics"""
    metrics.set_gauge("positions_users", user_count)
    metrics.set_gauge("positions_count", position_count)
    metrics.set_gauge("positions_update_duration", duration)


async def health_check_internal() -> Dict:
    """Internal health check"""
    from bot import run_sql, get_pooled_connection, return_pooled_connection
    from connection_pool import _connection_pool
    
    health = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'checks': {}
    }
    
    # Database check
    try:
        conn = get_pooled_connection()
        c = conn.cursor()
        c.execute("SELECT 1")
        c.fetchone()
        return_pooled_connection(conn)
        health['checks']['database'] = 'ok'
    except Exception as e:
        health['checks']['database'] = f'error: {e}'
        health['status'] = 'degraded'
    
    # Connection pool check
    if _connection_pool:
        pool_stats = _connection_pool.get_pool_stats()
        health['checks']['connection_pool'] = pool_stats
    else:
        health['checks']['connection_pool'] = 'not_initialized'
        health['status'] = 'degraded'
    
    # Metrics
    health['metrics'] = metrics.get_metrics()
    
    return health
