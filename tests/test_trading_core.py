import pytest

from trading_core import calculate_trade_pnl, combine_realized_pnl, classify_pnl_reason
from position_service import calculate_close_outcome


def test_calculate_trade_pnl_long_short():
    long_pnl = calculate_trade_pnl(
        entry_price=100.0,
        exit_price=110.0,
        direction="LONG",
        amount=100.0,
        leverage=10,
        commission=2.0,
    )
    short_pnl = calculate_trade_pnl(
        entry_price=100.0,
        exit_price=90.0,
        direction="SHORT",
        amount=100.0,
        leverage=10,
        commission=2.0,
    )
    assert long_pnl == pytest.approx(98.0)
    assert short_pnl == pytest.approx(98.0)


def test_combine_realized_pnl():
    assert combine_realized_pnl(12.5, 3.5) == pytest.approx(16.0)
    assert combine_realized_pnl(-2.0, None) == pytest.approx(-2.0)


def test_classify_pnl_reason_fallback():
    assert classify_pnl_reason("", 10.0) == "TP"
    assert classify_pnl_reason("", -1.0) == "SL"
    assert classify_pnl_reason("manual", 5.0) == "MANUAL"


def test_position_service_close_outcome_uses_realized_pnl():
    pos = {
        "entry": 100.0,
        "direction": "LONG",
        "amount": 50.0,
        "commission": 1.0,
        "realized_pnl": 4.0,
    }
    outcome = calculate_close_outcome(pos, close_price=110.0, leverage=10)
    assert outcome["pnl"] == pytest.approx(49.0)
    assert outcome["returned"] == pytest.approx(99.0)
    assert outcome["total_pnl_for_history"] == pytest.approx(53.0)

