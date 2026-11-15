import asyncio
import time
from dataclasses import replace
from decimal import Decimal as D
from unittest.mock import AsyncMock

import pytest

from auto_hedge.config import Account, Config, Route
from auto_hedge.control import Control, FileLock
from auto_hedge.engine import Engine
from auto_hedge.exchange import Exchange, Market, rows
from auto_hedge.store import Store


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def setup(tmp_path):
    source = Account("https://api.rh.lighter.xyz", 466324, 11, 5, "RH_KEY")
    target = Account("https://mainnet.zklighter.elliot.ai", 304, 22, 4, "CORE_KEY")
    cfg = Config(source, target, (Route("BTC", "BTC", D("0.01"), D("0.03")),), tmp_path)
    control = Control(tmp_path)
    control.heartbeat(True)
    control.resume("test", 35)
    store = Store(tmp_path, {"test": True})
    s, t = Exchange(source), Exchange(target)
    s.source_store = store
    s.find_source_order = AsyncMock(return_value={"type": "limit"})
    t.best_price = AsyncMock(return_value=D("100000"))
    t.submit = AsyncMock()
    t.terminal = AsyncMock(side_effect=lambda cid, m, requested, timeout: requested)
    engine = Engine(cfg, control, store, s, t, True)
    engine.source_markets = {"BTC": Market("BTC", 7, 5, 1, D("0.0001"), D("10"), D(1))}
    engine.target_markets = {"BTC": Market("BTC", 99, 5, 1, D("0.0001"), D("10"), D(1))}
    yield engine, control, store
    store.close()


def fill(tid=1, size="0.001", sell=False, **extra):
    return {"market_id": 7, "trade_id": tid, "type": "trade", "size": size,
            "ask_account_id": 11 if sell else 99, "bid_account_id": 99 if sell else 11,
            "ask_id": 1, "bid_id": 2, "ask_client_id": 3, "bid_client_id": 4,
            "timestamp": int(time.time() * 1000) + 1, **extra}


@pytest.mark.parametrize("sell,expected", [(False, D("-0.001")), (True, D("0.001"))])
@pytest.mark.parametrize("order_type", ["limit", "market"])
def test_fill_direction_partial_and_duplicate(setup, sell, expected, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    async def scenario():
        await e.ingest(fill(sell=sell))
        await e.ingest(fill(sell=sell))
        await e.ingest(fill(2, "0.002", sell=sell))
        assert s.pending("BTC", 1) == expected * 3
        await e.hedge()
        assert s.pending("BTC", 1) == 0
        assert e.target.submit.call_count == 1
        assert e.target.submit.call_args.args[0].index == 99
        assert e.target.submit.call_args.args[2] == expected * 3
    run(scenario())


@pytest.mark.parametrize("order_type", ["limit", "market"])
def test_partial_ioc_only_retries_actual_remainder(setup, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    e.target.terminal.side_effect = [D("-0.0004"), D("-0.0006")]
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        assert [x.args[2] for x in e.target.submit.call_args_list] == [D("-0.001"), D("-0.0006")]
        assert s.pending("BTC", 1) == 0
    run(scenario())


@pytest.mark.parametrize("order_type", ["limit", "market"])
def test_ambiguous_submission_is_never_resent(setup, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    e.target.submit.side_effect = TimeoutError()
    async def scenario():
        await e.ingest(fill())
        with pytest.raises(TimeoutError):
            await e.hedge()
        with pytest.raises(RuntimeError, match="unresolved"):
            await e.hedge()
        assert e.target.submit.call_count == 1
        assert len(s.uncertain()) == 1
        # Restart/recovery queries the same client ID and records actual filled size.
        await e.reconcile()
        assert not s.uncertain()
        assert s.pending("BTC", 1) == 0
    run(scenario())


@pytest.mark.parametrize("order_type", ["limit", "market"])
def test_pause_between_quote_and_send_wins(setup, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    async def quote(*args):
        c.pause("liquidating")
        return D("100000")
    e.target.best_price.side_effect = quote
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        e.target.submit.assert_not_called()
        assert s.pending("BTC", 1) == D("-0.001")
    run(scenario())


@pytest.mark.parametrize("order_type", ["limit", "market"])
def test_old_epoch_not_rehedged_after_risk_flatten(setup, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    async def scenario():
        await e.ingest(fill())
        c.pause("flatten")
        await e.ingest(fill(2))
        c.resume("both actual positions reviewed", 35)
        await e.ingest(fill(3, timestamp=1))
        await e.hedge()
        assert s.pending("BTC", 2) == 0
        e.target.submit.assert_not_called()
    run(scenario())


def test_reduce_only_market_fills_and_self_trades_do_not_trigger(setup):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": "market", "reduce_only": True}
    async def scenario():
        await e.ingest(fill())
        await e.ingest(fill(2, ask_account_id=11))
        await e.hedge()
        e.target.submit.assert_not_called()
        assert s.pending("BTC", 1) == 0
        assert e.source.find_source_order.call_count == 1
    run(scenario())


@pytest.mark.parametrize("reduce_only", [None, "false", 0, 1])
def test_market_order_with_unknown_reduce_only_flag_cannot_trigger(setup, reduce_only):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": "market", "reduce_only": reduce_only}
    async def scenario():
        with pytest.raises(RuntimeError, match="reduce-only"):
            await e.ingest(fill())
        await e.hedge()
        e.target.submit.assert_not_called()
        assert s.pending("BTC", 1) == 0
    run(scenario())


@pytest.mark.parametrize("order_type", ["stop-loss", "stop-loss-limit", "take-profit", "take-profit-limit", "twap", "twap-sub"])
def test_conditional_and_twap_fills_do_not_trigger(setup, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        e.target.submit.assert_not_called()
        assert s.pending("BTC", 1) == 0
    run(scenario())


def test_existing_reduce_only_limit_behavior_is_preserved(setup):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": "limit", "reduce_only": True}
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        assert e.target.submit.call_args.args[2] == D("-0.001")
        assert s.pending("BTC", 1) == 0
    run(scenario())


def test_minimum_dust_accumulates_exactly(setup):
    e, c, s = setup
    async def scenario():
        await e.ingest(fill(size="0.00004"))
        await e.hedge()
        e.target.submit.assert_not_called()
        await e.ingest(fill(2, "0.00006"))
        await e.hedge()
        assert s.pending("BTC", 1) == 0
        assert e.target.submit.call_args.args[2] == D("-0.0001")
    run(scenario())


def test_expired_dust_and_exposure_cap_stop(setup):
    e, c, s = setup
    async def scenario():
        await e.ingest(fill(size="0.00004"))
        e.dust_since[("BTC", 1)] = time.monotonic() - 60
        with pytest.raises(RuntimeError, match="below venue minimum"):
            await e.hedge()
        await e.ingest(fill(2, "0.04"))
        with pytest.raises(RuntimeError, match="max_unhedged"):
            await e.hedge()
    run(scenario())


def test_heartbeat_required_and_stale_is_not_ready(setup):
    e, c, s = setup
    c.heartbeat(False)
    assert not c.ready(35)
    with pytest.raises(RuntimeError, match="heartbeat"):
        c.resume("test", 35)
    c.heartbeat(True)
    c.resume("test", 35)
    with c.connect() as db:
        db.execute("UPDATE control SET heartbeat=?", (time.time() - 100,))
    assert not c.ready(35)


def test_decimal_precision_and_inward_price_rounding():
    m = Market("BTC", 1, 5, 1, D("0.0001"), D(10), D(1))
    assert m.units(D("0.000019999999999")) == 1
    assert m.cap(D("100.01"), D(30), True) == 998
    assert m.cap(D("100.01"), D(30), False) == 1003
    assert rows({"1": [{"a": 1}], "2": []}) == [{"a": 1}]
    with pytest.raises(ValueError):
        rows("invalid")


def test_process_lock_and_store_identity(tmp_path):
    a, b = FileLock(tmp_path / "lock"), FileLock(tmp_path / "lock")
    try:
        assert a.acquire()
        assert not b.acquire()
        a.close()
        assert b.acquire()
    finally:
        b.close()
    s = Store(tmp_path, {"live": False})
    s.close()
    with pytest.raises(RuntimeError, match="identity"):
        Store(tmp_path, {"live": True})


def test_canceled_ioc_with_fill_is_not_treated_as_zero(setup):
    e, c, s = setup
    t = Exchange(e.cfg.target)
    t.order_cache["123"] = {"status": "canceled-not-enough-liquidity", "market_index": 99,
                            "is_ask": True, "filled_base_amount": "0.0003"}
    assert run(t.terminal(123, e.target_markets["BTC"], D("-0.001"), 1)) == D("-0.0003")


def test_dry_run_never_submits(setup):
    e, c, s = setup
    e.live = False
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        e.target.submit.assert_not_called()
        assert s.pending("BTC", 1) == 0
    run(scenario())


def test_automatic_markets_follow_actual_symbol_and_size_without_caps(setup):
    e, c, s = setup
    e.cfg = replace(e.cfg, routes=(), auto_markets=True)
    e.source.markets = AsyncMock(return_value={
        **e.source_markets, "ETH": Market("ETH", 8, 4, 2, D("0.001"), D(10), D(1))})
    e.target.markets = AsyncMock(return_value={
        **e.target_markets, "ETH": Market("ETH", 100, 4, 2, D("0.001"), D(10), D(1))})
    async def scenario():
        await e.start()
        await e.ingest(fill(size="1.2345", market_id=8))
        await e.hedge()
        call = e.target.submit.call_args.args
        assert call[0].symbol == "ETH"
        assert call[0].index == 100
        assert call[2] == D("-1.2345")
        assert s.pending("ETH", 1) == 0
    run(scenario())


@pytest.mark.parametrize("order_type", ["limit", "market"])
def test_automatic_mode_never_silently_skips_unmatched_source_fill(setup, order_type):
    e, c, s = setup
    e.source.find_source_order.return_value = {"type": order_type, "reduce_only": False}
    e.cfg = replace(e.cfg, routes=(), auto_markets=True)
    e.source.markets = AsyncMock(return_value={
        **e.source_markets, "RH_ONLY": Market("RH_ONLY", 8, 4, 2, D("0.001"), D(10), D(1))})
    e.target.markets = AsyncMock(return_value=e.target_markets)
    async def scenario():
        await e.start()
        with pytest.raises(RuntimeError, match="no equivalent"):
            await e.ingest(fill(market_id=8))
        e.target.submit.assert_not_called()
    run(scenario())


def test_automatic_mode_ignores_pre_session_fills_in_expired_markets(setup):
    e, c, s = setup
    e.cfg = replace(e.cfg, auto_markets=True)
    e.source.markets = AsyncMock()
    run(e.ingest(fill(market_id=999, timestamp=1)))
    e.source.markets.assert_not_called()
    e.target.submit.assert_not_called()


def test_success_result_message_contains_confirmed_size_and_is_not_duplicated(setup):
    e, c, s = setup
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        await e.ingest(fill())
        await e.hedge()
        messages = s.pending_notifications()
        assert len(messages) == 1
        assert "对冲完成" in messages[0]["title"]
        assert "方向：卖出" in messages[0]["body"]
        assert "实际成交：0.001 BTC" in messages[0]["body"]
        assert "剩余待对冲：0 BTC" in messages[0]["body"]
    run(scenario())


def test_partial_result_message_is_not_reported_as_fully_hedged(setup):
    e, c, s = setup
    e.target.terminal.side_effect = [D("-0.0004"), D("-0.0006")]
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        messages = s.pending_notifications()
        assert len(messages) == 2
        assert "部分成交" in messages[0]["title"]
        assert "对冲完成" not in messages[0]["title"]
        assert "实际成交：0.0004 BTC" in messages[0]["body"]
        assert "剩余待对冲：卖出 0.0006 BTC" in messages[0]["body"]
        assert "对冲完成" in messages[1]["title"]
        assert "实际成交：0.0006 BTC" in messages[1]["body"]
    run(scenario())


def test_zero_fill_notifies_failure_and_never_success(setup):
    e, c, s = setup
    e.target.terminal.side_effect = [D(0)]
    async def scenario():
        await e.ingest(fill())
        with pytest.raises(RuntimeError, match="filled zero"):
            await e.hedge()
        messages = s.pending_notifications()
        assert len(messages) == 1
        assert "失败" in messages[0]["title"]
        assert "零成交" in messages[0]["title"]
        assert "实际成交：0 BTC" in messages[0]["body"]
        assert not s.uncertain()
    run(scenario())


def test_timeout_alert_and_later_confirmation_use_same_order_id(setup):
    e, c, s = setup
    e.target.submit.side_effect = TimeoutError()
    async def scenario():
        await e.ingest(fill())
        with pytest.raises(TimeoutError):
            await e.hedge()
        messages = s.pending_notifications()
        assert len(messages) == 1
        assert "未确认" in messages[0]["title"]
        assert "可能已经成交" in messages[0]["body"]
        cid = s.uncertain()[0]["id"]
        assert str(cid) in messages[0]["body"]
        await e.reconcile()
        await e.reconcile()
        messages = s.pending_notifications()
        assert len(messages) == 1
        assert "查单确认" in messages[0]["title"]
        assert "对冲完成" in messages[0]["title"]
        assert str(cid) in messages[0]["body"]
        assert e.target.submit.call_count == 1
    run(scenario())


def test_paper_result_is_clearly_marked_as_simulated(setup):
    e, c, s = setup
    e.live = False
    async def scenario():
        await e.ingest(fill())
        await e.hedge()
        messages = s.pending_notifications()
        assert "DRY-RUN" in messages[0]["title"]
        assert "无真实订单" in messages[0]["title"]
        e.target.submit.assert_not_called()
    run(scenario())
