import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from auto_hedge.config import Account, Config, MarginExecutionConfig, MonitorConfig, NotificationConfig
from auto_hedge.control import Control, execution_lock
from auto_hedge.exchange import Exchange
from auto_hedge.margin.account import MarginAccount
from auto_hedge.margin.guard import MarginGuard
from auto_hedge.margin.models import AccountSnapshot, GuardState, Position, RiskLevel
from auto_hedge.margin.risk import Decision, evaluate
from auto_hedge.service import Service
from auto_hedge.store import Store


def config(tmp_path, live=True):
    return Config(
        Account("https://api.rh.lighter.xyz", 466324, 11, 5, "RH_KEY"),
        Account("https://mainnet.zklighter.elliot.ai", 304, 22, 4, "CORE_KEY"),
        (), tmp_path, enabled=live, auto_markets=True,
        margin_execution=MarginExecutionConfig(verify_delay_seconds=0.001),
        notifications=NotificationConfig(live_status_enabled=False),
    )


def snapshot(name="lighter-core", buffer="500", isolated=False, size="0.01", mark="100", liq="50"):
    positions = () if size == "0" else (
        Position(name, 1, "BTC", D(size), D(mark), D(liq), 5, isolated),)
    return AccountSnapshot(name, positions, D(1000), D(500), D(500) + D(buffer), D(500), time.time())


@pytest.fixture
def guard(tmp_path):
    cfg = config(tmp_path)
    control = Control(tmp_path)
    control.heartbeat(True)
    control.resume("test positions reviewed", 35)
    store = Store(tmp_path, {"test": True})
    source, target = Exchange(cfg.source), Exchange(cfg.target)
    guard = MarginGuard(cfg, control, store, source, target, True)
    yield guard
    store.close()


@pytest.mark.parametrize("buffer,expected", [
    ("50", RiskLevel.EMERGENCY), ("90", RiskLevel.CRITICAL),
    ("125", RiskLevel.WARNING), ("160", RiskLevel.HEALTHY),
])
def test_original_cross_margin_thresholds(buffer, expected):
    decision = evaluate((snapshot(buffer=buffer), snapshot("rh", buffer="5000")), GuardState(), MonitorConfig())
    assert decision.level == expected
    assert decision.should_flatten == (expected == RiskLevel.EMERGENCY)


def test_accounts_and_isolated_domains_are_not_added_together():
    decision = evaluate((snapshot(buffer="50"), snapshot("rh", buffer="500000")), GuardState(), MonitorConfig())
    assert decision.level == RiskLevel.EMERGENCY
    # This isolated position has a huge USD cushion, but is only 2% from liquidation.
    position = snapshot("rh", isolated=True, size="100000", mark="100", liq="98")
    decision = evaluate((position,), GuardState(), MonitorConfig())
    assert decision.level == RiskLevel.EMERGENCY
    assert decision.binding_distance_pct == D("2")


def test_recovery_hysteresis_and_unknown_margin_data():
    state = GuardState()
    assert evaluate((snapshot(buffer="125"),), state, MonitorConfig()).level == RiskLevel.WARNING
    assert evaluate((snapshot(buffer="160"),), state, MonitorConfig()).level == RiskLevel.WARNING
    assert evaluate((snapshot(buffer="180"),), state, MonitorConfig()).level == RiskLevel.HEALTHY
    assert evaluate((snapshot(isolated=True, liq="0"),), state, MonitorConfig()).level == RiskLevel.UNKNOWN
    stale = replace(snapshot(), observed_at_s=time.time() - 100)
    assert evaluate((stale,), state, MonitorConfig()).level == RiskLevel.UNKNOWN
    assert evaluate((snapshot(size="0"),), state, MonitorConfig()).level == RiskLevel.HEALTHY


def test_successful_monitor_read_supplies_internal_heartbeat_even_in_paper(guard):
    guard.live = False
    guard.control.heartbeat(False)
    guard.read_all = AsyncMock(return_value=(snapshot(), snapshot("rh")))
    asyncio.run(guard.run_once())
    assert guard.control.snapshot()["healthy"]
    # Good data never silently unpauses a session.
    assert guard.control.snapshot()["paused"]
    status = json.loads(guard.store.get("margin_status"))
    assert status["risk"] == "healthy"
    assert len(status["accounts"]) == 2
    assert status["mode"] == "DRY-RUN"


def test_emergency_disables_hedging_before_notifications_or_flatten(guard):
    guard.read_all = AsyncMock(return_value=(snapshot(buffer="50"), snapshot("rh")))
    async def flatten(*args):
        assert guard.control.snapshot()["paused"]
        assert not guard.control.snapshot()["healthy"]
    guard.flatten_both = AsyncMock(side_effect=flatten)
    guard.notifier.send = AsyncMock()
    asyncio.run(guard.run_once())
    guard.flatten_both.assert_awaited_once()
    # Notification delivery happens in a different coroutine, after risk decisions.
    guard.notifier.send.assert_not_called()


def test_emergency_waits_for_inflight_hedge_then_refreshes_and_flattens(guard):
    fresh = (snapshot(size="0.02"), snapshot("rh", size="-0.02"))
    flat = (snapshot(size="0"), snapshot("rh", size="0"))
    guard.read_all = AsyncMock(side_effect=[fresh, flat])
    for c in guard.clients:
        c.cancel_all = AsyncMock()
        c.flatten_once = AsyncMock(return_value=["close"])
    decision = Decision(RiskLevel.EMERGENCY, D(50), True, "danger")
    async def scenario():
        async with execution_lock(guard.control.directory):
            task = asyncio.create_task(guard.flatten_both((snapshot(), snapshot("rh")), decision))
            await asyncio.sleep(0.05)
            assert guard.control.snapshot()["paused"]
            for c in guard.clients:
                c.cancel_all.assert_not_called()
                c.flatten_once.assert_not_called()
        await asyncio.wait_for(task, 1)
        for c, s in zip(guard.clients, fresh):
            c.cancel_all.assert_awaited_once()
            c.flatten_once.assert_awaited_once_with(s)
        assert guard.control.snapshot()["paused"]
    asyncio.run(scenario())


def test_emergency_retries_real_residual_positions(guard):
    opened = (snapshot(), snapshot("rh"))
    flat = (snapshot(size="0"), snapshot("rh", size="0"))
    guard.read_all = AsyncMock(side_effect=[opened, opened, flat])
    for c in guard.clients:
        c.cancel_all = AsyncMock()
        c.flatten_once = AsyncMock(return_value=["close"])
    decision = Decision(RiskLevel.EMERGENCY, D(50), True, "danger")
    asyncio.run(guard.flatten_both(opened, decision))
    assert all(c.flatten_once.call_count == 2 for c in guard.clients)
    assert guard.control.snapshot()["paused"]


def test_cancel_failure_does_not_prevent_emergency_closes(guard):
    opened = (snapshot(), snapshot("rh"))
    flat = (snapshot(size="0"), snapshot("rh", size="0"))
    guard.read_all = AsyncMock(side_effect=[opened, flat])
    for c in guard.clients:
        c.cancel_all = AsyncMock(side_effect=RuntimeError("cancel rejected"))
        c.flatten_once = AsyncMock(return_value=["close"])
    asyncio.run(guard.flatten_both(opened, Decision(RiskLevel.EMERGENCY, D(50), True, "danger")))
    assert all(c.flatten_once.call_count == 1 for c in guard.clients)


def test_failed_verification_can_retry_on_next_monitor_iteration(guard):
    opened = (snapshot(), snapshot("rh"))
    guard.read_all = AsyncMock(side_effect=[opened, RuntimeError("read failed")])
    for c in guard.clients:
        c.cancel_all = AsyncMock()
        c.flatten_once = AsyncMock(return_value=["close"])
    with pytest.raises(RuntimeError, match="read failed"):
        asyncio.run(guard.flatten_both(opened, Decision(RiskLevel.EMERGENCY, D(50), True, "danger")))
    assert not guard.state.flatten_started
    assert guard.control.snapshot()["paused"]


def test_risk_closes_use_shared_signer_managed_nonces_and_unique_ids(guard):
    client = guard.clients[0]
    ok = (None, SimpleNamespace(code=200), None)
    signer = SimpleNamespace(CANCEL_ALL_TIF_IMMEDIATE=0,
                             cancel_all_orders=AsyncMock(return_value=ok),
                             create_market_order_limited_slippage=AsyncMock(return_value=ok))
    client.exchange.signer = signer
    async def scenario():
        prior = guard.store.new_attempt("BTC", 1, D("-0.01"))
        await client.cancel_all()
        await client.flatten_once(snapshot())
        await client.flatten_once(snapshot(size="-0.01"))
        signer.cancel_all_orders.assert_awaited_once_with(0, 0)
        calls = signer.create_market_order_limited_slippage.call_args_list
        assert all(c.kwargs["reduce_only"] is True for c in calls)
        assert all("api_key_index" not in c.kwargs for c in calls)
        assert calls[0].kwargs["base_amount"] == 1000
        assert calls[0].kwargs["is_ask"] is True
        assert calls[1].kwargs["is_ask"] is False
        assert prior < calls[0].kwargs["client_order_index"] < calls[1].kwargs["client_order_index"] < 2**48
        # Reduce-only emergency closes never affect the normal hedge-fill ledger.
        assert len(guard.store.uncertain()) == 1
    asyncio.run(scenario())


def test_paper_risk_execution_never_sends_orders(guard):
    client = guard.clients[0]
    client.live = False
    client.exchange.signer = SimpleNamespace(cancel_all_orders=AsyncMock(), create_market_order_limited_slippage=AsyncMock())
    async def scenario():
        await client.cancel_all()
        actions = await client.flatten_once(snapshot())
        assert actions and "reduce-only" in actions[0]
        client.exchange.signer.cancel_all_orders.assert_not_called()
        client.exchange.signer.create_market_order_limited_slippage.assert_not_called()
    asyncio.run(scenario())


def test_paper_emergency_is_reported_once_until_risk_recovers(guard):
    guard.live = False
    for client in guard.clients:
        client.live = False
    guard.read_all = AsyncMock(return_value=(snapshot(buffer="50"), snapshot("rh")))
    async def scenario():
        await guard.run_once()
        await guard.run_once()
        notices = []
        while not guard.notifications.empty():
            notices.append(guard.notifications.get_nowait()[0])
        assert notices.count("DRY-RUN 全平清单") == 1
        assert guard.state.flatten_started
        guard.read_all.return_value = (snapshot(size="0"), snapshot("rh", size="0"))
        await guard.run_once()
        assert not guard.state.flatten_started
    asyncio.run(scenario())


def account_payload():
    return {"accounts": [{"index": 22, "positions": [{
        "market_id": 1, "position": "0.01", "sign": -1, "margin_mode": 1,
        "liquidation_price": "110", "symbol": "BTC",
    }], "collateral": "1000", "available_balance": "500",
        "cross_asset_value": "1000", "cross_maintenance_margin_requirement": "500"}]}


def test_margin_snapshot_parses_all_positions_using_existing_http_client(guard):
    client = guard.clients[0]
    client.exchange.get = AsyncMock(side_effect=[
        account_payload(), {"order_book_details": [{"market_id": 1, "symbol": "BTC", "mark_price": "100", "size_decimals": 5}]}])
    result = asyncio.run(client.snapshot())
    assert result.positions[0].signed_size == D("-0.01")
    assert result.positions[0].isolated
    assert result.positions[0].liquidation_distance_pct == D(10)


def test_margin_snapshot_missing_positions_does_not_look_flat(guard):
    client = guard.clients[0]
    data = account_payload()
    del data["accounts"][0]["positions"]
    client.exchange.get = AsyncMock(side_effect=[data, {"order_book_details": []}])
    with pytest.raises(RuntimeError, match="missing positions"):
        asyncio.run(client.snapshot())


def test_single_service_reuses_two_exchange_instances(tmp_path):
    cfg = config(tmp_path)
    service = Service(cfg, True)
    try:
        assert service.margin.clients[0].exchange is service.target
        assert service.margin.clients[1].exchange is service.source
        assert service.margin.store is service.engine.store
        assert service.margin.control is service.engine.control
    finally:
        service.store.close()


def test_hedge_failure_does_not_stop_margin_monitoring(tmp_path):
    service = Service(replace(config(tmp_path), monitor=MonitorConfig(poll_seconds=0.1)), True)
    service.start = AsyncMock()
    service.engine.start = AsyncMock()
    service.margin.notifier.send = AsyncMock()
    service.worker = AsyncMock(side_effect=RuntimeError("hedge order failed"))
    async def idle(*args):
        await asyncio.Event().wait()
    service.stream = idle
    service.backfill_loop = idle
    service.watchdog = idle
    observed = asyncio.Event()
    reads = 0
    async def read():
        nonlocal reads
        reads += 1
        if reads >= 2:
            observed.set()
        return (snapshot(), snapshot("rh"))
    service.margin.read_all = read
    async def scenario():
        task = asyncio.create_task(service.run())
        try:
            await asyncio.wait_for(observed.wait(), 2)
            assert not task.done()
            assert reads >= 2
            assert service.control.snapshot()["paused"]
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    asyncio.run(scenario())


def test_monitor_read_failure_pauses_and_keeps_polling(guard):
    guard.cfg = replace(guard.cfg, monitor=MonitorConfig(poll_seconds=0.1))
    recovered = asyncio.Event()
    reads = 0
    async def read():
        nonlocal reads
        reads += 1
        if reads == 1:
            raise RuntimeError("margin endpoint unavailable")
        recovered.set()
        return (snapshot(), snapshot("rh"))
    guard.read_all = read
    async def scenario():
        task = asyncio.create_task(guard.run_forever())
        try:
            await asyncio.wait_for(recovered.wait(), 1)
            assert guard.control.snapshot()["paused"]
            assert guard.control.snapshot()["healthy"]
            assert reads >= 2
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    asyncio.run(scenario())


def test_unknown_margin_data_never_authorizes_flatten_or_hedging(guard):
    guard.read_all = AsyncMock(return_value=(snapshot(isolated=True, liq="0"), snapshot("rh")))
    guard.flatten_both = AsyncMock()
    decision = asyncio.run(guard.run_once())
    assert decision.level == RiskLevel.UNKNOWN
    assert not guard.control.ready(35)
    guard.flatten_both.assert_not_called()


def test_telegram_status_retains_one_message_and_shows_both_accounts(guard, monkeypatch):
    publisher = guard.live_status
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    calls = []
    def call(token, method, payload):
        calls.append((method, payload))
        return {"ok": True, "result": {"message_id": 123}}
    publisher._call = call
    decision = evaluate((snapshot(), snapshot("robinhood-lighter")), GuardState(), MonitorConfig())
    body = guard.describe(decision, (snapshot(), snapshot("robinhood-lighter"))) + "; hedging=暂停"
    text = publisher.render(body)
    assert "lighter-core" in text and "robinhood-lighter" in text
    assert "北京时间" in text and "对冲=暂停" in text
    publisher.push(text)
    publisher.push(text + "\nupdated")
    assert [m for m, _ in calls] == ["sendMessage", "editMessageText"]
    assert calls[1][1]["message_id"] == "123"
