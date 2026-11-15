import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from auto_hedge.config import Account
from auto_hedge.exchange import Exchange
from auto_hedge.network import configure_proxy, validate_proxy
from auto_hedge.service import Service


PROXY = "http://127.0.0.1:7897"


def test_proxy_covers_sdk_nonce_native_http_notifications_and_websockets():
    with patch.dict(os.environ):
        configure_proxy(PROXY)
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "WS_PROXY", "WSS_PROXY"):
            assert os.environ[key] == PROXY
        assert os.environ["NO_PROXY"] == "localhost,127.0.0.1,::1"


@pytest.mark.parametrize("url", ["socks5://127.0.0.1:7897", "http://name:password@127.0.0.1:7897",
                                "http://127.0.0.1", "http://127.0.0.1:7897/path"])
def test_proxy_requires_supported_http_endpoint(url):
    with pytest.raises(ValueError):
        validate_proxy(url)


def test_rest_call_explicitly_uses_the_configured_proxy():
    exchange = Exchange(Account("https://api.rh.lighter.xyz", 466324, 11, 5, "UNUSED"), PROXY)
    class Response:
        status = 200
        async def json(self):
            return {"code": 200, "order_books": []}
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
    exchange.http = SimpleNamespace(get=Mock(return_value=Response()))
    data = asyncio.run(exchange.get("orderBooks"))
    assert data["code"] == 200
    assert exchange.http.get.call_args.kwargs["proxy"] == PROXY


def test_account_stream_explicitly_uses_the_configured_proxy():
    service = object.__new__(Service)
    service.cfg = SimpleNamespace(proxy_url=PROXY)
    service.source_ready, service.target_ready, service.synced = asyncio.Event(), asyncio.Event(), asyncio.Event()
    exchange = SimpleNamespace(account=SimpleNamespace(index=22, ws_url="wss://example.invalid/stream"),
                               token=lambda: "test-auth", cache_orders=lambda orders: None)
    class Socket:
        count = 0
        async def send(self, value):
            pass
        async def recv(self):
            self.count += 1
            if self.count == 1:
                return json.dumps({"type": "subscribed/account_all_orders", "orders": {}})
            await asyncio.Event().wait()
    class Connection:
        async def __aenter__(self):
            return Socket()
        async def __aexit__(self, *args):
            pass
    connect = Mock(return_value=Connection())
    async def scenario():
        with patch("auto_hedge.service.connect", connect):
            task = asyncio.create_task(service.stream(exchange, False))
            try:
                await asyncio.wait_for(service.target_ready.wait(), 1)
                assert connect.call_args.kwargs["proxy"] == PROXY
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
    asyncio.run(scenario())
