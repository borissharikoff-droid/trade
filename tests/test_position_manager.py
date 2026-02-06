"""
Unit tests for position_manager: validation and correlation check.
"""
import pytest
from position_manager import (
    check_correlation_risk,
    calculate_volatility_based_size,
    _validate_sizing_inputs,
)


def test_validate_sizing_inputs_raises_on_invalid():
    with pytest.raises(ValueError):
        _validate_sizing_inputs(-1, 1.0, 100.0)
    with pytest.raises(ValueError):
        _validate_sizing_inputs(100.0, -0.1, 100.0)
    with pytest.raises(ValueError):
        _validate_sizing_inputs(100.0, 1.0, 0)


def test_validate_sizing_inputs_ok():
    _validate_sizing_inputs(100.0, 1.0, 100.0)


def test_check_correlation_risk_invalid_balance():
    safe, reason = check_correlation_risk([], "BTC/USDT", "LONG", -1)
    assert safe is False
    assert "balance" in reason.lower()


def test_check_correlation_risk_invalid_direction():
    safe, reason = check_correlation_risk([], "BTC/USDT", "INVALID", 100.0)
    assert safe is False


def test_check_correlation_risk_empty_positions_safe():
    safe, reason = check_correlation_risk([], "BTC/USDT", "LONG", 1000.0)
    assert safe is True
    assert reason == "OK"


def test_calculate_volatility_based_size_invalid_entry():
    with pytest.raises(ValueError):
        calculate_volatility_based_size(1000.0, 2.0, 0)
