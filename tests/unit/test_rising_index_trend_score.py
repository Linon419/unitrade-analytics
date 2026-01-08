from unitrade.scanner.signal_detector.rising_index import RisingIndex, RisingIndexConfig


def test_trend_score_uptrend_high_when_linear():
    cfg = RisingIndexConfig(trend_angle_cap_deg=0.6, trend_dev_cap_pct=3.0)
    idx = RisingIndex(cfg)

    closes = [100.0 * (1.0 + 0.01 * i) for i in range(50)]
    score = idx._calc_trend_score(closes)

    assert score is not None
    assert score > 0.9


def test_trend_score_downtrend_zeroish():
    cfg = RisingIndexConfig(trend_angle_cap_deg=0.6, trend_dev_cap_pct=3.0)
    idx = RisingIndex(cfg)

    closes = [100.0 * (1.0 - 0.005 * i) for i in range(50)]
    score = idx._calc_trend_score(closes)

    assert score is not None
    assert score == 0.0


def test_trend_score_penalizes_large_deviation():
    cfg = RisingIndexConfig(trend_angle_cap_deg=0.6, trend_dev_cap_pct=1.0)
    idx = RisingIndex(cfg)

    # Mostly linear uptrend, but with an exaggerated last close to create large baseline deviation.
    closes = [100.0 * (1.0 + 0.01 * i) for i in range(49)] + [250.0]
    score = idx._calc_trend_score(closes)

    assert score is not None
    assert score < 0.2

