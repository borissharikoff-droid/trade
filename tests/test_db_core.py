"""
Unit tests for db_core: _sql_prepare (placeholder conversion, SQL injection safety).
"""
import pytest
from db_core import _sql_prepare


def test_sql_prepare_returns_same_params():
    """Params are returned unchanged (no string interpolation)"""
    q, p = _sql_prepare("SELECT * FROM users WHERE id = ?", (123,))
    assert p == (123,)


def test_sql_prepare_params_must_be_tuple_or_list():
    """Params must be tuple or list, not string (injection protection)"""
    with pytest.raises(TypeError):
        _sql_prepare("SELECT * FROM users WHERE id = ?", "1; DROP TABLE users--")


def test_sql_prepare_params_list_ok():
    """List is accepted as params"""
    q, p = _sql_prepare("SELECT * FROM users WHERE id = ?", [42])
    assert p == [42]
