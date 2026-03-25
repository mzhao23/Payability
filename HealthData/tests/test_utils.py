from health_risk.utils import clamp, normalize_key, pct_to_ratio, safe_float


def test_pct_to_ratio():
    assert pct_to_ratio(50.0) == 0.5
    assert pct_to_ratio(None) is None


def test_safe_float_decimal_string():
    assert safe_float("1.5") == 1.5


def test_normalize_key():
    assert normalize_key("  AbC  ") == "abc"
    assert normalize_key("") is None


def test_clamp():
    assert clamp(5.0, 0.0, 10.0) == 5.0
    assert clamp(-1.0, 0.0, 10.0) == 0.0
