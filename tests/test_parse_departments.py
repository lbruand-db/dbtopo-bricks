from dbtopo.cli import _parse_departments
from dbtopo.config import ALL_DEPARTMENTS


def test_parse_single():
    assert _parse_departments("001") == ["001"]


def test_parse_multiple():
    assert _parse_departments("001,075,092") == ["001", "075", "092"]


def test_parse_all():
    result = _parse_departments("all")
    assert result == ALL_DEPARTMENTS
    assert len(result) == 96  # 001-095 minus 020, plus 02A + 02B


def test_parse_all_case_insensitive():
    assert _parse_departments("ALL") == ALL_DEPARTMENTS
    assert _parse_departments(" All ") == ALL_DEPARTMENTS


def test_parse_strips_whitespace():
    assert _parse_departments(" 001 , 075 ") == ["001", "075"]
