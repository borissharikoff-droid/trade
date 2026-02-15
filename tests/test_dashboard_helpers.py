from dashboard import _parse_datetime_arg, _paginate_list


def test_parse_datetime_arg_supports_date_and_iso():
    assert _parse_datetime_arg("2026-02-10") is not None
    assert _parse_datetime_arg("2026-02-10T12:30:00Z") is not None
    assert _parse_datetime_arg("bad-date") is None


def test_paginate_list():
    data = list(range(25))
    p1 = _paginate_list(data, page=1, limit=10)
    p3 = _paginate_list(data, page=3, limit=10)
    assert p1["items"] == list(range(10))
    assert p1["total"] == 25
    assert p1["pages"] == 3
    assert p3["items"] == list(range(20, 25))

