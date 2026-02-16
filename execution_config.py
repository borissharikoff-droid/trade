"""
Centralized execution and rollout configuration.
"""

from __future__ import annotations

import os


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


# Timeouts/retries
BYBIT_SESSION_TOTAL_TIMEOUT = _env_int("BYBIT_SESSION_TOTAL_TIMEOUT", 30)
BYBIT_SESSION_CONNECT_TIMEOUT = _env_int("BYBIT_SESSION_CONNECT_TIMEOUT", 10)
BYBIT_REQUEST_TIMEOUT = _env_int("BYBIT_REQUEST_TIMEOUT", 10)
BYBIT_MAX_RETRIES = _env_int("BYBIT_MAX_RETRIES", 3)
BYBIT_RETRY_DELAY = _env_float("BYBIT_RETRY_DELAY", 1.0)

WS_PING_INTERVAL_SECONDS = _env_int("WS_PING_INTERVAL_SECONDS", 20)
WS_MAX_RECONNECT_DELAY_SECONDS = _env_int("WS_MAX_RECONNECT_DELAY_SECONDS", 30)

# Feature flags (phased rollout)
FEATURE_EXECUTION_WATCHDOGS = _env_bool("FEATURE_EXECUTION_WATCHDOGS", False)
FEATURE_AUTO_ORPHAN_CLEANUP = _env_bool("FEATURE_AUTO_ORPHAN_CLEANUP", False)
FEATURE_DEAD_LETTER_QUEUE = _env_bool("FEATURE_DEAD_LETTER_QUEUE", False)
FEATURE_ADMIN_ALERTS = _env_bool("FEATURE_ADMIN_ALERTS", True)
FEATURE_LEARNING_PROMOTION = _env_bool("FEATURE_LEARNING_PROMOTION", False)
FEATURE_OPS_DASHBOARD = _env_bool("FEATURE_OPS_DASHBOARD", True)

# Rollout mode: dry_run | limited | full
ROLLOUT_MODE = os.getenv("ROLLOUT_MODE", "dry_run").strip().lower() or "dry_run"

# Reliability thresholds
STUCK_PENDING_SECONDS = _env_int("STUCK_PENDING_SECONDS", 300)
STUCK_OPENING_SECONDS = _env_int("STUCK_OPENING_SECONDS", 300)
STUCK_CLOSING_SECONDS = _env_int("STUCK_CLOSING_SECONDS", 300)
ORPHAN_GRACE_SECONDS = _env_int("ORPHAN_GRACE_SECONDS", 600)

# Strategy/risk defaults (balanced profile)
MAX_DIRECTIONAL_EXPOSURE_PCT = _env_float("MAX_DIRECTIONAL_EXPOSURE_PCT", 40.0)
MAX_CORRELATED_EXPOSURE_PCT = _env_float("MAX_CORRELATED_EXPOSURE_PCT", 30.0)
MAX_EQUITY_DRAWDOWN_PCT = _env_float("MAX_EQUITY_DRAWDOWN_PCT", 10.0)
EXECUTION_COST_BUFFER_PCT = _env_float("EXECUTION_COST_BUFFER_PCT", 0.25)

# Learning gate thresholds
LEARNING_MIN_TRADES = _env_int("LEARNING_MIN_TRADES", 30)
LEARNING_MIN_PROFIT_FACTOR = _env_float("LEARNING_MIN_PROFIT_FACTOR", 1.2)
LEARNING_MAX_DRAWDOWN_PCT = _env_float("LEARNING_MAX_DRAWDOWN_PCT", 10.0)
LEARNING_MIN_WINRATE_PCT = _env_float("LEARNING_MIN_WINRATE_PCT", 50.0)
