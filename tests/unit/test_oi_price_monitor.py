from unitrade.scanner.oi_price_monitor import classify_oi_price, pct_change, _floor_to_5m_end


def test_pct_change_handles_zero_old():
    assert pct_change(10.0, 0.0) is None
    assert pct_change(10.0, -1.0) is None


def test_pct_change_basic():
    assert pct_change(110.0, 100.0) == 0.10


def test_classify_oi_price_quadrants():
    assert classify_oi_price(0.1, 0.1).endswith("Long Build")
    assert classify_oi_price(0.1, -0.1).endswith("Short Build")
    assert classify_oi_price(-0.1, 0.1).endswith("Short Cover")
    assert classify_oi_price(-0.1, -0.1).endswith("Long Unwind")


def test_floor_to_5m_end():
    # 00:02:30 -> last closed is 00:00:00
    assert _floor_to_5m_end(150) == 0  # 150s since epoch -> boundary 0ms
    # Exactly on boundary should return previous bucket
    assert _floor_to_5m_end(300) == 0  # 5m -> previous closes at 0ms

