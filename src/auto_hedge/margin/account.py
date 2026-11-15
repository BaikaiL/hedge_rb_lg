from __future__ import annotations

import asyncio
import time
from decimal import Decimal, ROUND_DOWN

from ..config import MarginExecutionConfig
from ..exchange import Exchange
from ..store import Store
from .models import AccountSnapshot, Position


def decimal(value) -> Decimal:
    result = Decimal(str(value if value is not None else "0"))
    if not result.is_finite():
        raise ValueError("non-finite margin data")
    return result


class MarginAccount:
    """Risk reads/writes through the exact Exchange and signer used by hedging."""

    def __init__(self, name: str, exchange: Exchange, execution: MarginExecutionConfig, store: Store, live: bool):
        self.name, self.exchange, self.execution = name, exchange, execution
        self.store, self.live = store, live

    async def snapshot(self) -> AccountSnapshot:
        observed = time.time()
        accounts, books = await asyncio.gather(
            self.exchange.get("account", by="index", value=self.exchange.account.index, active_only=True),
            self.exchange.get("orderBookDetails"),
        )
        if len(accounts["accounts"]) != 1:
            raise RuntimeError(f"{self.name}: expected exactly one account")
        account = accounts["accounts"][0]
        if "index" in account and int(account["index"]) != self.exchange.account.index:
            raise RuntimeError(f"{self.name}: account index mismatch")
        if not isinstance(account.get("positions"), list):
            raise RuntimeError(f"{self.name}: missing positions; cannot assume account is flat")
        metadata = {int(b["market_id"]): b for b in books["order_book_details"]}
        positions = []
        for p in account["positions"]:
            size = abs(decimal(p["position"]))
            if not size:
                continue
            sign = int(p["sign"])
            mode = int(p["margin_mode"])
            if sign not in (-1, 1) or mode not in (0, 1):
                raise RuntimeError(f"{self.name}: invalid position sign or margin mode")
            market_id = int(p["market_id"])
            book = metadata.get(market_id)
            if book is None:
                raise RuntimeError(f"{self.name}: missing market metadata for {market_id}")
            positions.append(Position(
                self.name, market_id, str(p.get("symbol") or book["symbol"]), size * sign,
                decimal(book.get("mark_price")), decimal(p.get("liquidation_price")),
                int(book["size_decimals"]), isolated=mode == 1,
            ))
        return AccountSnapshot(
            self.name, tuple(positions), decimal(account.get("collateral")),
            decimal(account.get("available_balance")), decimal(account.get("cross_asset_value")),
            decimal(account.get("cross_maintenance_margin_requirement")), observed,
        )

    @staticmethod
    def accepted(result, action: str) -> None:
        _, response, error = result
        if error or response is None or int(response.code) != 200:
            raise RuntimeError(f"{action}: submission rejected or uncertain; verify actual positions")

    async def cancel_all(self) -> None:
        if not self.live:
            return
        signer = self.exchange.signer
        async with asyncio.timeout(self.execution.order_timeout_seconds):
            result = await signer.cancel_all_orders(signer.CANCEL_ALL_TIF_IMMEDIATE, 0)
        # SDK's decorated helper shares nonce management with normal hedge orders.
        self.accepted(result, f"{self.name}: cancel all")

    async def flatten_once(self, snapshot: AccountSnapshot) -> list[str]:
        actions = []
        for position in snapshot.positions:
            amount = int((abs(position.signed_size) * Decimal(10) ** position.size_decimals)
                         .to_integral_value(rounding=ROUND_DOWN))
            if amount <= 0:
                continue
            action = f"{self.name}: close {position.symbol} size={position.signed_size} reduce-only"
            actions.append(action)
            if not self.live:
                continue
            async with asyncio.timeout(self.execution.order_timeout_seconds):
                result = await self.exchange.signer.create_market_order_limited_slippage(
                    market_index=position.market_id, client_order_index=self.store.next_client_id(),
                    base_amount=amount, max_slippage=self.execution.max_slippage_pct / 100,
                    is_ask=position.signed_size > 0, reduce_only=True,
                )
            self.accepted(result, action)
        return actions
