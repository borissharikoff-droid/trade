import risk_guard
from strategy_validation import validate_candidate_metrics


def test_validate_candidate_metrics_pass():
    passed, checks = validate_candidate_metrics(
        {
            "total_trades": 120,
            "win_rate": 61.0,
            "profit_factor": 1.6,
            "max_drawdown_pct": 7.5,
        }
    )
    assert passed is True
    assert checks["passed"] is True
    assert checks["min_trades_ok"] is True


def test_validate_candidate_metrics_fail():
    passed, checks = validate_candidate_metrics(
        {
            "total_trades": 5,
            "win_rate": 30.0,
            "profit_factor": 0.8,
            "max_drawdown_pct": 35.0,
        }
    )
    assert passed is False
    assert checks["passed"] is False
    assert checks["min_trades_ok"] is False


def test_check_total_directional_exposure_limit(monkeypatch):
    def fake_run_sql(query, params=None, fetch=None):
        if "FROM users" in query:
            return {"user_id": 1, "balance": 1000.0, "total_deposit": 1000.0}
        if "SUM(amount)" in query:
            return {"exposure": 500.0}
        return {}

    monkeypatch.setattr(risk_guard, "run_sql", fake_run_sql)
    ok, reason = risk_guard.check_total_directional_exposure(
        1, direction="LONG", new_amount=200.0, max_exposure_percent=60.0
    )
    assert ok is False
    assert "directional_exposure_limit_hit" in reason


def test_check_portfolio_drawdown_hit(monkeypatch):
    def fake_run_sql(query, params=None, fetch=None):
        if "FROM users" in query:
            return {"user_id": 2, "balance": 800.0, "total_deposit": 1000.0}
        return {}

    monkeypatch.setattr(risk_guard, "run_sql", fake_run_sql)
    ok, reason = risk_guard.check_portfolio_drawdown(2, max_drawdown_percent=10.0)
    assert ok is False
    assert "max_drawdown_hit" in reason
