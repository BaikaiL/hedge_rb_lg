from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import aiohttp

from .config import Account
from .network import configure_proxy

log = logging.getLogger(__name__)


def rows(value) -> list[dict]:
    """WS collections may be arrays or maps keyed by market index."""
    if value is None:
        return []
    if isinstance(value, list):
        if not all(isinstance(x, dict) for x in value):
            raise ValueError("invalid WebSocket collection")
        return value
    if isinstance(value, dict):
        return [r for group in value.values() for r in rows(group)]
    raise ValueError("invalid WebSocket collection")


@dataclass(frozen=True)
class Market:
    symbol: str
    index: int
    size_decimals: int
    price_decimals: int
    min_base: Decimal
    min_quote: Decimal
    multiplier: Decimal

    def units(self, amount: Decimal) -> int:
        return int((amount * Decimal(10) ** self.size_decimals).to_integral_value(rounding=ROUND_FLOOR))

    def amount(self, units: int) -> Decimal:
        return Decimal(units) / Decimal(10) ** self.size_decimals

    def cap(self, price: Decimal, bps: Decimal, sell: bool) -> int:
        scaled = price * (1 - bps / 10000 if sell else 1 + bps / 10000) * Decimal(10) ** self.price_decimals
        # Round inward so tick rounding never increases the configured slippage.
        return int(scaled.to_integral_value(rounding=ROUND_CEILING if sell else ROUND_FLOOR))


class Exchange:
    def __init__(self, account: Account, proxy_url: str | None = None):
        self.account = account
        self.proxy_url = proxy_url
        self.signer = None
        self.http = None
        self.order_cache: dict[str, dict] = {}
        self.source_store = None

    async def start(self) -> None:
        configure_proxy(self.proxy_url)
        import lighter
        self.signer = lighter.SignerClient(
            url=self.account.url, chain_id=self.account.chain_id,
            account_index=self.account.index,
            api_private_keys={self.account.key_index: self.account.private_key()},
        )
        if self.proxy_url:
            self.signer.api_client.configuration.proxy = self.proxy_url
            self.signer.api_client.rest_client.proxy = self.proxy_url
        error = await asyncio.to_thread(self.signer.check_client)
        if error:
            raise RuntimeError(f"API key validation failed for {self.account.url}")
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=True)

    async def close(self) -> None:
        if self.http:
            await self.http.close()
        if self.signer:
            await self.signer.close()

    def token(self) -> str:
        token, error = self.signer.create_auth_token_with_expiry(api_key_index=self.account.key_index)
        if error or not token:
            raise RuntimeError("could not generate account auth token")
        return token

    async def get(self, endpoint: str, *, private=False, **params) -> dict:
        params = {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in params.items() if v is not None}
        headers = {"authorization": self.token()} if private else {}
        async with self.http.get(self.account.url + "/api/v1/" + endpoint, params=params, headers=headers,
                                 proxy=self.proxy_url) as r:
            if r.status != 200:
                # Do not log response bodies or request headers containing auth.
                raise RuntimeError(f"{endpoint}: HTTP {r.status}")
            payload = await r.json()
        if payload.get("code") != 200:
            raise RuntimeError(f"{endpoint}: venue code {payload.get('code')}")
        return payload

    async def markets(self) -> dict[str, Market]:
        data = await self.get("orderBookDetails")
        result = {}
        for m in data["order_book_details"]:
            if m.get("market_type") != "perp" or m["status"] != "active":
                continue
            if m["symbol"] in result:
                raise RuntimeError("ambiguous perp symbol; explicit market selection required")
            result[m["symbol"]] = Market(
                m["symbol"], int(m["market_id"]), int(m["size_decimals"]), int(m["price_decimals"]),
                Decimal(m["min_base_amount"]), Decimal(m["min_quote_amount"]), Decimal(m.get("multiplier", "1")),
            )
        return result

    def cache_orders(self, data: list[dict]) -> None:
        for order in data:
            cid = str(order.get("client_order_id") or order.get("client_order_index", 0))
            # REST polling must not regress a terminal WS state to a stale open state.
            old = self.order_cache.get(cid)
            old_terminal = old and (old["status"] == "filled" or old["status"].startswith("canceled"))
            new_terminal = order["status"] == "filled" or order["status"].startswith("canceled")
            if (not old_terminal or new_terminal) and (old is None or int(order.get("updated_at", 0)) >= int(old.get("updated_at", 0))):
                self.order_cache[cid] = order
            if self.source_store:
                self.source_store.cache_order(order)

    async def find_source_order(self, market: int, oid: str, cid: str) -> dict:
        for _ in range(3):
            order = self.source_store.order(market, oid)
            if order:
                return order
            if cid != "0":
                data = await self.get("accountOrders", private=True, account_index=self.account.index,
                                      client_order_indexes=cid)
                self.cache_orders(data["orders"])
                order = self.source_store.order(market, oid)
                if order:
                    return order
            data = await self.get("accountActiveOrders", private=True, account_index=self.account.index, market_id=market)
            self.cache_orders(data["orders"])
            order = self.source_store.order(market, oid)
            if order:
                return order
            cursor = None
            for _page in range(20):
                data = await self.get("accountInactiveOrders", private=True, account_index=self.account.index,
                                      market_id=market, limit=100, cursor=cursor)
                self.cache_orders(data["orders"])
                order = self.source_store.order(market, oid)
                if order:
                    return order
                nxt = data.get("next_cursor")
                if not nxt:
                    break
                if nxt == cursor:
                    raise RuntimeError("order history pagination did not advance")
                cursor = nxt
            await asyncio.sleep(0.1)
        raise RuntimeError(f"cannot establish source order type: market={market}, order={oid}")

    async def best_price(self, market: Market, sell: bool) -> Decimal:
        book = await self.get("orderBookOrders", market_id=market.index, limit=10)
        levels = book["bids" if sell else "asks"]
        prices = [Decimal(x["price"]) for x in levels if Decimal(x.get("remaining_base_amount", x.get("size", "0"))) > 0]
        if not prices:
            raise RuntimeError("target order book has no executable liquidity")
        return max(prices) if sell else min(prices)

    async def submit(self, market: Market, cid: int, amount: Decimal, price_cap: int) -> None:
        # Called only under execution.lock. No retry on an ambiguous submission.
        _, response, error = await self.signer.create_market_order(
            market_index=market.index, client_order_index=cid,
            base_amount=market.units(abs(amount)), avg_execution_price=price_cap,
            is_ask=amount < 0, reduce_only=False,
        )
        if error or response is None or int(response.code) != 200:
            raise RuntimeError("hedge submission not accepted; reconcile client order ID before continuing")

    async def terminal(self, cid: int, market: Market, requested: Decimal, timeout: float) -> Decimal:
        deadline = time_monotonic() + timeout
        next_rest = time_monotonic() + 0.25
        while time_monotonic() < deadline:
            order = self.order_cache.get(str(cid))
            if order and (order["status"] == "filled" or order["status"].startswith("canceled")):
                if int(order["market_index"]) != market.index or bool(order["is_ask"]) != (requested < 0):
                    raise RuntimeError("client order ID collision or mismatched order")
                size = Decimal(order["filled_base_amount"])
                if size < 0 or size > abs(requested):
                    raise RuntimeError("unexpected hedge fill quantity")
                # A canceled IOC can have a nonzero filled_base_amount.
                return -size if requested < 0 else size
            if time_monotonic() >= next_rest:
                data = await self.get("accountOrders", private=True, account_index=self.account.index,
                                      client_order_indexes=str(cid))
                self.cache_orders(data["orders"])
                next_rest = time_monotonic() + 1.0
            await asyncio.sleep(0.02)
        raise RuntimeError(f"hedge result unresolved: client_order_index={cid}")


def time_monotonic() -> float:
    return asyncio.get_running_loop().time()
