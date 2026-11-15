from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

from .config import Config, Route
from .control import Control, execution_lock
from .exchange import Exchange
from .store import Store

log = logging.getLogger(__name__)


class Engine:
    def __init__(self, cfg: Config, control: Control, store: Store, source: Exchange, target: Exchange, live: bool):
        self.cfg, self.control, self.store = cfg, control, store
        self.source, self.target, self.live = source, target, live
        self.source_markets = {}
        self.target_markets = {}
        self.routes = list(cfg.routes)
        self.unavailable: set[str] = set()
        self.dust_since: dict[tuple[str, int], float] = {}

    async def start(self) -> None:
        self.source_markets, self.target_markets = await asyncio.gather(self.source.markets(), self.target.markets())
        if self.cfg.auto_markets:
            self.routes = [Route(symbol, symbol) for symbol in self.source_markets]
        self.unavailable.clear()
        for r in self.routes:
            if r.target_symbol not in self.target_markets:
                if not self.cfg.auto_markets:
                    raise RuntimeError(f"{r.name}: no corresponding target perpetual contract")
                self.unavailable.add(r.name)
                continue
            s, t = self.source_markets[r.source_symbol], self.target_markets[r.target_symbol]
            if s.multiplier != t.multiplier:
                if not self.cfg.auto_markets:
                    raise RuntimeError(f"{r.name}: unequal contract multipliers")
                self.unavailable.add(r.name)
                continue
            if r.max_order_base is not None and t.units(r.max_order_base) <= 0:
                raise RuntimeError(f"{r.name}: max_order_base is below target precision")
            log.info("route=%s RH_market=%s Core_market=%s size_decimals=%s", r.name, s.index, t.index, t.size_decimals)
        if self.unavailable:
            log.warning("RH perps without equivalent Core contract (an eligible fill will pause hedging): %s", ",".join(sorted(self.unavailable)))

    async def ingest(self, trade: dict) -> None:
        market = int(trade["market_id"])
        route = next((r for r in self.routes if self.source_markets[r.source_symbol].index == market), None)
        if route is None and self.cfg.auto_markets and trade.get("market_kind") not in ("spot", "bo"):
            state = self.control.snapshot()
            # Subscription snapshots can include fills in old, already settled markets.
            if state["paused"] or int(trade["timestamp"]) < state["armed_ms"]:
                return
            await self.start()  # Discover newly listed perpetual contracts on their first fill.
            route = next((r for r in self.routes if self.source_markets[r.source_symbol].index == market), None)
            if route is None:
                raise RuntimeError(f"unknown source perpetual market={market}; cannot establish hedge mapping")
        if route is None:
            return
        tid = str(trade.get("trade_id_str") or trade["trade_id"])
        if self.store.has_fill(market, tid):
            return
        ask, bid = int(trade["ask_account_id"]), int(trade["bid_account_id"])
        if self.cfg.source.index not in (ask, bid):
            raise RuntimeError("account trade stream returned another account's fill")
        snapshot = self.control.snapshot()
        if not snapshot["paused"] and not self.control.ready(self.cfg.heartbeat_timeout):
            raise RuntimeError("margin guard heartbeat expired while receiving source fills")
        timestamp = int(trade["timestamp"])
        status, delta = "ignored", Decimal(0)
        if self.control.ready(self.cfg.heartbeat_timeout) and timestamp >= snapshot["armed_ms"]:
            if trade["type"] != "trade":
                raise RuntimeError("source liquidation/deleverage/settlement detected")
            if ask != bid:
                side = "ask" if ask == self.cfg.source.index else "bid"
                oid = str(trade.get(f"{side}_id_str") or trade[f"{side}_id"])
                cid = str(trade.get(f"{side}_client_id_str") or trade.get(f"{side}_client_id", 0))
                order = await self.source.find_source_order(market, oid, cid)
                eligible = order["type"] == "limit"
                if order["type"] == "market":
                    reduce_only = order.get("reduce_only")
                    if not isinstance(reduce_only, bool):
                        raise RuntimeError("cannot establish whether source market order is reduce-only")
                    # The margin guard submits reduce-only market closes, not new entries.
                    eligible = not reduce_only
                if eligible:
                    if route.name in self.unavailable:
                        raise RuntimeError(f"{route.name}: source {order['type']} fill has no equivalent active Core contract")
                    size = Decimal(trade["size"])
                    if not size.is_finite() or size <= 0:
                        raise ValueError("invalid fill size")
                    delta, status = (size if side == "ask" else -size), "eligible"
        self.store.add_fill(market, tid, route.name, snapshot["epoch"], delta, status, trade)
        if status == "eligible":
            log.info("source_fill route=%s trade=%s hedge_delta=%s", route.name, tid, delta)

    async def reconcile(self) -> None:
        for a in self.store.uncertain():
            route = next(r for r in self.routes if r.name == a["route"])
            filled = await self.target.terminal(a["id"], self.target_markets[route.target_symbol],
                                                Decimal(a["requested"]), self.cfg.confirm_timeout)
            self.store.settle(a["id"], filled)
            log.warning("reconciled client_order_index=%s actual_fill=%s", a["id"], filled)

    async def hedge(self) -> None:
        state = self.control.snapshot()
        epoch = state["epoch"]
        if not self.control.ready(self.cfg.heartbeat_timeout, epoch):
            return
        if self.store.uncertain():
            raise RuntimeError("unresolved previous submission; run reconcile before resuming")
        for route in self.routes:
            if not self.store.pending(route.name, epoch):
                continue
            m = self.target_markets[route.target_symbol]
            anchor: Decimal | None = None
            for _ in range(self.cfg.max_attempts):
                pending = self.store.pending(route.name, epoch)
                if not pending:
                    self.dust_since.pop((route.name, epoch), None)
                    break
                if route.max_unhedged_base is not None and abs(pending) > route.max_unhedged_base:
                    raise RuntimeError(f"{route.name}: max_unhedged_base exceeded ({pending})")
                sell = pending < 0
                best = await self.target.best_price(m, sell)
                # Retries share the original price bound; repeated retries cannot walk the market.
                if anchor is None:
                    anchor = best
                desired = abs(pending) if route.max_order_base is None else min(abs(pending), route.max_order_base)
                amount = m.amount(m.units(desired))
                if not amount or amount < m.min_base or amount * best < m.min_quote:
                    since = self.dust_since.setdefault((route.name, epoch), time.monotonic())
                    if time.monotonic() - since >= self.cfg.max_dust_seconds:
                        raise RuntimeError(f"{route.name}: residual {pending} remains below venue minimum/precision")
                    log.debug("dust route=%s unhedged_base=%s", route.name, pending)
                    break
                self.dust_since.pop((route.name, epoch), None)
                signed = -amount if sell else amount
                cap = m.cap(anchor, self.cfg.max_slippage_bps, sell)
                if not 0 < cap <= (1 << 32) - 1:
                    raise RuntimeError("price bound exceeds protocol range")
                async with execution_lock(self.control.directory):
                    if not self.control.ready(self.cfg.heartbeat_timeout, epoch):
                        return
                    cid = self.store.new_attempt(route.name, epoch, signed)
                    if not self.live:
                        self.store.settle(cid, signed, "DRY-RUN simulated; no target order")
                        log.info("DRY-RUN route=%s signed_base=%s price_bound=%s", route.name, signed, cap)
                        continue
                    # Uncertain is durably written BEFORE the network send, including crash windows.
                    async with asyncio.timeout(self.cfg.confirm_timeout):
                        await self.target.submit(m, cid, signed, cap)
                        filled = await self.target.terminal(cid, m, signed, self.cfg.confirm_timeout)
                    self.store.settle(cid, filled)
                    log.info("hedge route=%s client_id=%s requested=%s actual=%s", route.name, cid, signed, filled)
                    if not filled:
                        raise RuntimeError(f"{route.name}: IOC filled zero; price/liquidity/margin check required")
            else:
                if self.store.pending(route.name, epoch):
                    raise RuntimeError(f"{route.name}: remaining exposure after max_attempts")
