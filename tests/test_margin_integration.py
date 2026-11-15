import asyncio
import importlib.util
import sys
import time
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from auto_hedge.control import Control, execution_lock

FIXTURES = Path(__file__).parent / "fixtures"
sys.path.insert(0, str(FIXTURES))
spec = importlib.util.spec_from_file_location(
    "hedge_guard.guard", Path(__file__).parents[1] / "integration/margin_guard/guard.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
from hedge_guard.config import AppConfig, MonitorConfig, ExecutionConfig, NotificationConfig
from hedge_guard.models import AccountSnapshot, GuardState, RiskLevel
from hedge_guard.risk import Decision


def test_risk_pauses_before_waiting_and_uses_fresh_snapshots(tmp_path):
    c = Control(tmp_path)
    c.heartbeat(True)
    c.resume("test", 35)
    guard = object.__new__(module.HedgeGuard)
    guard.live, guard.interlock = True, c
    fresh = ("new snapshot",)
    guard.read_all = AsyncMock(return_value=fresh)
    guard._flatten_both_unlocked = AsyncMock()
    decision = Decision(RiskLevel.EMERGENCY, D(10), True, "test danger")
    async def scenario():
        async with execution_lock(tmp_path):
            task = asyncio.create_task(guard.flatten_both(("old snapshot",), decision))
            await asyncio.sleep(0.05)
            assert c.snapshot()["paused"]
            guard._flatten_both_unlocked.assert_not_called()
        await task
        guard._flatten_both_unlocked.assert_awaited_once_with(fresh, decision)
        assert c.snapshot()["paused"]
    asyncio.run(scenario())


def test_live_guard_requires_shared_directory(monkeypatch):
    monkeypatch.delenv("AUTO_HEDGE_STATE_DIR", raising=False)
    cfg = SimpleNamespace(execution=SimpleNamespace(enabled=True))
    with pytest.raises(RuntimeError, match="AUTO_HEDGE_STATE_DIR"):
        module.HedgeGuard(cfg, cli_live=True)


def test_guard_only_heartbeats_after_successful_read(tmp_path):
    c = Control(tmp_path)
    guard = object.__new__(module.HedgeGuard)
    monitor = MonitorConfig(10, 150, 100, 75, 175)
    guard.cfg = SimpleNamespace(monitor=monitor)
    guard.state = GuardState()
    guard.live, guard.interlock = True, c
    snapshot = AccountSnapshot("core", (), D(1000), D(1000), D(1000), D(0), time.time())
    guard.read_all = AsyncMock(return_value=(snapshot,))
    guard.live_status = SimpleNamespace(maybe_update=AsyncMock())
    guard._notify_if_due = AsyncMock()
    asyncio.run(guard.run_once())
    assert c.snapshot()["healthy"]
    guard.read_all.side_effect = RuntimeError("network failed")
    heartbeat = c.snapshot()["heartbeat"]
    with pytest.raises(RuntimeError):
        asyncio.run(guard.run_once())
    assert c.snapshot()["heartbeat"] == heartbeat
