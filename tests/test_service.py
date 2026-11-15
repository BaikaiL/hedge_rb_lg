import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from auto_hedge.service import Service
from auto_hedge.control import Control
from auto_hedge.store import Store


def test_backfill_is_paginated_sorted_and_checkpointed(tmp_path):
    c = Control(tmp_path)
    c.heartbeat(True)
    c.resume("test", 35)
    armed = c.snapshot()["armed_ms"]
    s = Store(tmp_path, {"test": True})
    service = object.__new__(Service)
    service.cfg = SimpleNamespace(max_pages=3, source=SimpleNamespace(index=11))
    service.control, service.store = c, s
    service.source_ready, service.synced = asyncio.Event(), asyncio.Event()
    service.source_ready.set()
    service.source = SimpleNamespace(get=AsyncMock(side_effect=[
        {"trades": [{"trade_id": 2, "timestamp": armed + 2}], "next_cursor": "page2"},
        {"trades": [{"trade_id": 1, "timestamp": armed + 1}, {"trade_id": 0, "timestamp": armed - 1}]},
    ]))
    service.engine = SimpleNamespace(ingest=AsyncMock())
    asyncio.run(service.backfill())
    assert [call.args[0]["trade_id"] for call in service.engine.ingest.call_args_list] == [1, 2]
    assert s.get("rest_complete_ms:1") is not None
    assert service.synced.is_set()
    s.close()


def test_backfill_page_limit_never_sends_incomplete_history(tmp_path):
    c = Control(tmp_path)
    c.heartbeat(True)
    c.resume("test", 35)
    s = Store(tmp_path, {"test": True})
    service = object.__new__(Service)
    service.cfg = SimpleNamespace(max_pages=1, source=SimpleNamespace(index=11))
    service.control, service.store = c, s
    service.source_ready, service.synced = asyncio.Event(), asyncio.Event()
    service.source = SimpleNamespace(get=AsyncMock(return_value={
        "trades": [{"trade_id": 1, "timestamp": int(time.time() * 1000)}], "next_cursor": "more"}))
    service.engine = SimpleNamespace(ingest=AsyncMock())
    with pytest.raises(RuntimeError, match="incomplete replay"):
        asyncio.run(service.backfill())
    service.engine.ingest.assert_not_called()
    assert s.get("rest_complete_ms:1") is None
    s.close()
