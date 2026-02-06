"""
Unit tests for PnL calculation logic (LONG/SHORT, commission).
Used by close position and balance updates.
"""
import pytest

LEVERAGE = 10


def pnl_long(entry: float, exit_price: float, amount: float, commission: float = 0) -> float:
    """PnL for LONG: (exit - entry) / entry * amount * leverage - commission"""
    if entry <= 0:
        return 0.0
    pnl_pct = (exit_price - entry) / entry
    return amount * LEVERAGE * pnl_pct - commission


def pnl_short(entry: float, exit_price: float, amount: float, commission: float = 0) -> float:
    """PnL for SHORT: (entry - exit) / entry * amount * leverage - commission"""
    if entry <= 0:
        return 0.0
    pnl_pct = (entry - exit_price) / entry
    return amount * LEVERAGE * pnl_pct - commission


def test_pnl_long_profit():
    """LONG: price up -> profit"""
    # entry 100, exit 110, amount 100, 10x -> 100 * 10 * 0.1 = 100
    assert pnl_long(100.0, 110.0, 100.0, 0) == pytest.approx(100.0)


def test_pnl_long_loss():
    """LONG: price down -> loss"""
    assert pnl_long(100.0, 90.0, 100.0, 0) == pytest.approx(-100.0)


def test_pnl_short_profit():
    """SHORT: price down -> profit"""
    assert pnl_short(100.0, 90.0, 100.0, 0) == pytest.approx(100.0)


def test_pnl_short_loss():
    """SHORT: price up -> loss"""
    assert pnl_short(100.0, 110.0, 100.0, 0) == pytest.approx(-100.0)


def test_pnl_commission_subtracted():
    """Commission is subtracted from PnL"""
    assert pnl_long(100.0, 110.0, 100.0, 2.0) == pytest.approx(98.0)
    assert pnl_short(100.0, 90.0, 100.0, 1.5) == pytest.approx(98.5)


def test_returned_amount():
    """Returned = amount + pnl (user gets margin back + PnL)"""
    amount = 50.0
    pnl = pnl_long(100.0, 110.0, amount, 0)  # 50 * 10 * 0.1 = 50
    returned = amount + pnl
    assert returned == pytest.approx(100.0)
