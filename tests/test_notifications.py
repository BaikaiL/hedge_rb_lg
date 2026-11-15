import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import io

import pytest

from auto_hedge.config import NotificationConfig
from auto_hedge.margin.guard import MarginGuard
from auto_hedge.margin.notifier import Notifier
from auto_hedge.store import Store


def test_result_is_saved_atomically_and_survives_restart(tmp_path):
    s = Store(tmp_path, {"test": True})
    cid = s.new_attempt("BTC", 1, Decimal("-0.001"))
    original = s._enqueue_notification
    s._enqueue_notification = Mock(side_effect=RuntimeError("database write interrupted"))
    with pytest.raises(RuntimeError):
        s.settle(cid, Decimal("-0.001"), notification=("confirmed", "fill details"))
    assert s.uncertain()[0]["id"] == cid
    assert not s.pending_notifications()
    s._enqueue_notification = original
    s.settle(cid, Decimal("-0.001"), notification=("confirmed", "fill details"))
    s.close()
    s = Store(tmp_path, {"test": True})
    try:
        assert not s.uncertain()
        assert len(s.pending_notifications()) == 1
        # Repeated state reconciliation must not create duplicate result events.
        s.settle(cid, Decimal("-0.001"), notification=("confirmed", "fill details"))
        assert len(s.pending_notifications()) == 1
        event_id = s.pending_notifications()[0]["event_id"]
        s.notification_delivery(event_id, False)
        assert not s.pending_notifications()
        with s.db:
            s.db.execute("UPDATE hedge_notifications SET next_try_at=0")
        retry = s.pending_notifications()[0]
        assert retry["delivery_attempts"] == 1
        s.notification_delivery(event_id, True)
        s.enqueue_notification(event_id, "duplicate", "duplicate")
        assert not s.pending_notifications()
    finally:
        s.close()


def test_result_sender_retries_without_resubmitting_a_trade(tmp_path):
    s = Store(tmp_path, {"test": True})
    s.enqueue_notification("event-1", "confirmed fill", "details")
    guard = object.__new__(MarginGuard)
    guard.store = s
    guard.notifier = SimpleNamespace(send=AsyncMock(side_effect=[False, True]))
    done = asyncio.Event()
    delivery = s.notification_delivery
    def record(event_id, delivered):
        delivery(event_id, delivered)
        if delivered:
            done.set()
        else:
            with s.db:
                s.db.execute("UPDATE hedge_notifications SET next_try_at=0")
    s.notification_delivery = record
    async def scenario():
        task = asyncio.create_task(guard.hedge_notification_loop())
        try:
            await asyncio.wait_for(done.wait(), 3)
            assert guard.notifier.send.call_count == 2
            assert not s.pending_notifications()
            assert not s.uncertain()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    try:
        asyncio.run(scenario())
    finally:
        s.close()


@pytest.mark.parametrize("fails,expected", [(False, True), (True, False)])
def test_transport_reports_delivery_result_without_propagating_network_error(monkeypatch, fails, expected):
    cfg = NotificationConfig(webhook_url_env="TEST_RESULT_WEBHOOK", telegram_bot_token_env="TEST_RESULT_TOKEN",
                             telegram_chat_id_env="TEST_RESULT_CHAT")
    monkeypatch.delenv("TEST_RESULT_WEBHOOK", raising=False)
    monkeypatch.setenv("TEST_RESULT_TOKEN", "test-token")
    monkeypatch.setenv("TEST_RESULT_CHAT", "test-chat")
    notifier = Notifier(cfg)
    notifier._post_form = Mock(side_effect=OSError("test transport failure") if fails else None)
    assert asyncio.run(notifier.send("test", "body")) is expected


def test_missing_notification_channel_is_not_marked_as_delivered(monkeypatch):
    cfg = NotificationConfig(webhook_url_env="TEST_RESULT_WEBHOOK", telegram_bot_token_env="TEST_RESULT_TOKEN",
                             telegram_chat_id_env="TEST_RESULT_CHAT")
    for name in ["TEST_RESULT_WEBHOOK", "TEST_RESULT_TOKEN", "TEST_RESULT_CHAT"]:
        monkeypatch.delenv(name, raising=False)
    assert asyncio.run(Notifier(cfg).send("test", "body")) is False


def test_telegram_response_must_acknowledge_message():
    with patch("urllib.request.urlopen", return_value=io.BytesIO(b'{"ok":false}')):
        with pytest.raises(RuntimeError, match="Telegram"):
            Notifier._post_form("https://example.invalid", {})
