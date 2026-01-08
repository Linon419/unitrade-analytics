from unitrade.scanner.signal_detector.redis_state import RedisStateManager


import pytest


@pytest.mark.asyncio
async def test_first_ranked_in_memory_fallback_is_stable():
    mgr = RedisStateManager(redis_url="redis://localhost:0")
    # Not connected -> in-memory fallback
    assert mgr._connected is False

    ts1, price1 = 100.0, 1.23
    ts2, price2 = 200.0, 4.56

    first1 = await mgr.get_or_set_first_ranked("TESTUSDT", ts1, price1, prefix="anomaly")
    first2 = await mgr.get_or_set_first_ranked("TESTUSDT", ts2, price2, prefix="anomaly")

    assert first1 == (ts1, price1)
    assert first2 == (ts1, price1)
