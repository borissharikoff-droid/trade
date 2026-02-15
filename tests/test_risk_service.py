from risk_service import validate_position_open, validate_daily_limit


def test_validate_position_open_accepts_valid_trade():
    ok, reason = validate_position_open(
        balance=100.0,
        bet_amount=20.0,
        min_balance_reserve=10.0,
        min_bet=5.0,
    )
    assert ok is True
    assert reason == "ok"


def test_validate_position_open_rejects_low_bet():
    ok, reason = validate_position_open(
        balance=100.0,
        bet_amount=1.0,
        min_balance_reserve=10.0,
        min_bet=5.0,
    )
    assert ok is False
    assert reason == "bet_below_minimum"


def test_validate_daily_limit():
    ok1, reason1 = validate_daily_limit(3, 5)
    ok2, reason2 = validate_daily_limit(5, 5)
    assert (ok1, reason1) == (True, "ok")
    assert (ok2, reason2) == (False, "daily_limit_reached")

