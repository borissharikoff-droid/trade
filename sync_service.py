"""
Sync policy helpers for Bybit/phantom logic.
"""


def should_consider_phantom_cleanup(
    *,
    hedging_enabled: bool,
    has_bybit_qty: bool,
    position_age_minutes: float,
    min_age_minutes: float = 10.0,
) -> bool:
    """
    Decide whether a position is eligible for phantom cleanup policy.
    Local (non-Bybit) positions must never be cleaned as phantom.
    """
    if not hedging_enabled:
        return False
    if not has_bybit_qty:
        return False
    return float(position_age_minutes or 0) >= float(min_age_minutes or 10.0)

