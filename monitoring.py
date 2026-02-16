"""
Monitoring and Health Checks
Provides metrics collection and health check endpoints
"""

import logging
import time
import threading
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and stores metrics (thread-safe)"""
    
    def __init__(self):
        self.metrics: Dict[str, any] = defaultdict(int)
        self.timestamps: Dict[str, float] = {}
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.last_reset = time.time()
        self._lock = threading.Lock()
    
    def increment(self, metric: str, value: int = 1):
        """Increment a counter metric"""
        with self._lock:
            self.metrics[metric] += value
    
    def set_gauge(self, metric: str, value: float):
        """Set a gauge metric"""
        with self._lock:
            self.metrics[metric] = value
            self.timestamps[metric] = time.time()

    def get_gauge(self, metric: str, default: float = 0.0) -> float:
        with self._lock:
            try:
                return float(self.metrics.get(metric, default))
            except Exception:
                return float(default)
    
    def record_error(self, error_type: str):
        """Record an error"""
        with self._lock:
            self.error_counts[error_type] += 1
            self.metrics["errors_total"] += 1
    
    def get_metrics(self) -> Dict:
        """Get all metrics"""
        with self._lock:
            return {
                'counters': dict(self.metrics),
                'errors': dict(self.error_counts),
                'uptime': time.time() - self.last_reset
            }
    
    def reset(self):
        """Reset metrics"""
        with self._lock:
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


def record_latency(service: str, duration_seconds: float):
    """Record latency snapshots and rough rolling percentiles."""
    d = max(0.0, float(duration_seconds or 0.0))
    metrics.set_gauge(f"latency_last_{service}", d)
    # Lightweight EWMA approximations for p50/p95 tracking without large buffers.
    prev_p50 = metrics.get_gauge(f"latency_p50_{service}", d)
    prev_p95 = metrics.get_gauge(f"latency_p95_{service}", d)
    metrics.set_gauge(f"latency_p50_{service}", prev_p50 * 0.8 + d * 0.2)
    metrics.set_gauge(f"latency_p95_{service}", max(prev_p95 * 0.9, d))


def record_retry(service: str, success: bool):
    metrics.increment(f"retry_total_{service}")
    if success:
        metrics.increment(f"retry_success_{service}")
    else:
        metrics.increment(f"retry_failed_{service}")


def set_circuit_state(service: str, state: str, failure_count: int = 0):
    mapping = {"closed": 0, "half_open": 1, "open": 2}
    normalized = str(state or "").strip().lower()
    metrics.set_gauge(f"circuit_state_{service}", mapping.get(normalized, -1))
    metrics.set_gauge(f"circuit_failures_{service}", float(max(0, int(failure_count or 0))))


def set_ws_health(connected: bool, reconnect_delay: float = 0.0, disconnects: Optional[int] = None):
    metrics.set_gauge("ws_connected", 1.0 if connected else 0.0)
    metrics.set_gauge("ws_reconnect_delay", float(reconnect_delay or 0.0))
    if disconnects is not None:
        metrics.set_gauge("ws_disconnects", float(max(0, int(disconnects))))


def set_rate_limit_usage(service: str, usage_ratio: float):
    metrics.set_gauge(f"rate_limit_usage_{service}", max(0.0, min(1.0, float(usage_ratio or 0.0))))


def set_stuck_positions_count(count: int):
    metrics.set_gauge("stuck_positions_count", float(max(0, int(count or 0))))


def get_execution_health() -> Dict:
    data = metrics.get_metrics()
    counters = data.get("counters", {})
    return {
        "ws_connected": bool(counters.get("ws_connected", 0)),
        "ws_reconnect_delay": counters.get("ws_reconnect_delay", 0),
        "stuck_positions_count": int(counters.get("stuck_positions_count", 0) or 0),
        "errors_total": int(counters.get("errors_total", 0) or 0),
        "retries_total": int(sum(v for k, v in counters.items() if str(k).startswith("retry_total_"))),
    }


def get_circuit_breaker_snapshot() -> Dict:
    data = metrics.get_metrics().get("counters", {})
    snapshot = {}
    for service in ("binance", "bybit", "database"):
        state_num = int(data.get(f"circuit_state_{service}", -1))
        state_map = {-1: "unknown", 0: "closed", 1: "half_open", 2: "open"}
        snapshot[service] = {
            "state": state_map.get(state_num, "unknown"),
            "failure_count": int(data.get(f"circuit_failures_{service}", 0) or 0),
        }
    return snapshot


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
