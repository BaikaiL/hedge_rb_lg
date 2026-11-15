from __future__ import annotations

import asyncio
import json
import logging
import time

from websockets.asyncio.client import connect

from .config import Config
from .control import Control
from .engine import Engine
from .exchange import Exchange, rows
from .store import Store

log = logging.getLogger(__name__)


def failure_reason(exc: BaseException) -> str:
    if isinstance(exc, BaseExceptionGroup):
        return "; ".join(failure_reason(e) for e in exc.exceptions)
    if type(exc) in (RuntimeError, ValueError, KeyError):
        return str(exc)
    return type(exc).__name__


def identity(cfg: Config, live: bool) -> dict:
    return {
        "source": [cfg.source.url, cfg.source.index],
        "target": [cfg.target.url, cfg.target.index],
        "routes": "auto" if cfg.auto_markets else [[r.source_symbol, r.target_symbol] for r in cfg.routes],
        "live": live,
    }


class Service:
    def __init__(self, cfg: Config, live: bool):
        self.cfg = cfg
        self.control = Control(cfg.state_dir)
        self.store = Store(cfg.state_dir, identity(cfg, live), "ledger.live.sqlite3" if live else "ledger.paper.sqlite3")
        self.source, self.target = Exchange(cfg.source), Exchange(cfg.target)
        self.source.source_store = self.store
        self.engine = Engine(cfg, self.control, self.store, self.source, self.target, live)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.source_ready, self.target_ready, self.synced = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.resync = asyncio.Event()

    async def start(self) -> None:
        await self.source.start()
        await self.target.start()
        await self.engine.start()

    async def close(self) -> None:
        await asyncio.gather(self.source.close(), self.target.close(), return_exceptions=True)
        self.store.close()

    async def stream(self, exchange: Exchange, source: bool) -> None:
        ready = self.source_ready if source else self.target_ready
        channels = [f"account_all_orders/{exchange.account.index}"]
        if source:
            channels.append(f"account_all_trades/{exchange.account.index}")
        delay = 1
        while True:
            ready.clear()
            if source:
                self.synced.clear()
            try:
                async with connect(exchange.account.ws_url, ping_interval=20, ping_timeout=20,
                                   open_timeout=10, max_size=8 * 1024 * 1024) as ws:
                    token = exchange.token()
                    for channel in channels:
                        await ws.send(json.dumps({"type": "subscribe", "channel": channel, "auth": token}))
                    acknowledged = set()
                    connected = time.monotonic()
                    while time.monotonic() - connected < 480:
                        message = json.loads(await asyncio.wait_for(ws.recv(), timeout=40))
                        if not isinstance(message, dict):
                            continue
                        kind = message.get("type", "")
                        if kind == "ping":
                            await ws.send('{"type":"pong"}')
                        elif kind in ("error", "shutdown") or message.get("error"):
                            raise ConnectionError(f"account stream {kind}")
                        elif kind.endswith("/account_all_orders"):
                            exchange.cache_orders(rows(message.get("orders")))
                            if kind.startswith("subscribed/"):
                                acknowledged.add("account_all_orders")
                        elif source and kind.endswith("/account_all_trades"):
                            for trade in rows(message.get("trades")):
                                self.queue.put_nowait(trade)
                            if kind.startswith("subscribed/"):
                                acknowledged.add("account_all_trades")
                        required = {c.split("/")[0] for c in channels}
                        if acknowledged == required and not ready.is_set():
                            ready.set()
                            delay = 1
                            if source:
                                self.resync.set()
                            log.info("account stream ready: %s", "source" if source else "target")
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, ConnectionError) as exc:
                log.warning("account stream reconnecting: %s (%s)", "source" if source else "target", type(exc).__name__)
            except Exception as exc:
                # WS protocol/queue errors are not safe to reinterpret as missing fills.
                from websockets.exceptions import ConnectionClosed
                if not isinstance(exc, ConnectionClosed):
                    raise
                log.warning("account stream disconnected: %s", "source" if source else "target")
            finally:
                ready.clear()
                if source:
                    self.synced.clear()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)

    async def backfill(self) -> None:
        state = self.control.snapshot()
        if state["armed_ms"] == 0:
            self.synced.set()
            return
        epoch = state["epoch"]
        previous = self.store.get(f"rest_complete_ms:{epoch}")
        since = max(state["armed_ms"], int(previous or state["armed_ms"]) - 60000)
        started = int(time.time() * 1000)
        if started - since > 6 * 60 * 60 * 1000:
            raise RuntimeError("source history gap exceeds six hours; reconcile both accounts before a new session")
        trades, cursor, seen_cursors = [], None, set()
        for _ in range(self.cfg.max_pages):
            data = await self.source.get("trades", private=True, account_index=self.cfg.source.index,
                                         sort_by="timestamp", sort_dir="desc", limit=100,
                                         cursor=cursor, aggregate=False)
            page = data["trades"]
            trades.extend(t for t in page if int(t["timestamp"]) >= since)
            if any(int(t["timestamp"]) < since for t in page) or not data.get("next_cursor"):
                break
            cursor = data["next_cursor"]
            if cursor in seen_cursors:
                raise RuntimeError("trade history pagination did not advance")
            seen_cursors.add(cursor)
        else:
            raise RuntimeError("source history exceeds max_backfill_pages; refusing incomplete replay")
        for trade in sorted(trades, key=lambda t: (int(t["timestamp"]), int(t["trade_id"]))):
            await self.engine.ingest(trade)
        self.store.set(f"rest_complete_ms:{epoch}", str(started))
        if self.source_ready.is_set():
            self.synced.set()

    async def backfill_loop(self) -> None:
        while True:
            await self.source_ready.wait()
            self.resync.clear()
            self.synced.clear()
            # Read failures pause sends until a successful complete backfill.
            try:
                await self.backfill()
            except (OSError, TimeoutError) as exc:
                self.synced.clear()
                log.warning("source backfill retry: %s", type(exc).__name__)
                await asyncio.sleep(2)
                continue
            try:
                await asyncio.wait_for(self.resync.wait(), timeout=self.cfg.backfill_seconds)
            except TimeoutError:
                pass

    async def worker(self) -> None:
        while True:
            await self.synced.wait()
            await self.source_ready.wait()
            await self.target_ready.wait()
            # Bound each batch to retain prompt heartbeat/pause handling.
            for _ in range(500):
                try:
                    trade = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await self.engine.ingest(trade)
                if self.synced.is_set() and self.source_ready.is_set() and self.target_ready.is_set():
                    await self.engine.hedge()
            if self.synced.is_set() and self.source_ready.is_set() and self.target_ready.is_set():
                await self.engine.hedge()
            await asyncio.sleep(0.02)

    async def watchdog(self) -> None:
        missing_since = time.monotonic()
        while True:
            s = self.control.snapshot()
            if not s["paused"] and not self.control.ready(self.cfg.heartbeat_timeout):
                self.control.pause("margin guard heartbeat absent/unhealthy/stale")
                log.critical("hedging paused: margin guard heartbeat lost")
            if self.synced.is_set() and self.source_ready.is_set() and self.target_ready.is_set():
                missing_since = time.monotonic()
            elif not s["paused"] and time.monotonic() - missing_since > 30:
                self.control.pause("account stream/backfill unavailable for over 30 seconds")
                log.critical("hedging paused: account stream/backfill unavailable")
            await asyncio.sleep(0.2)

    async def run(self) -> None:
        try:
            await self.start()
            if self.store.uncertain():
                self.control.pause("recovering unresolved hedge submission")
                await self.engine.reconcile()
            # Worker and backfill both mutate the ledger on this single event loop.
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.stream(self.source, True))
                tg.create_task(self.stream(self.target, False))
                tg.create_task(self.backfill_loop())
                tg.create_task(self.worker())
                tg.create_task(self.watchdog())
        except BaseException as exc:
            reason = failure_reason(exc)
            self.control.pause("hedger stopped: " + reason)
            log.critical("hedging paused: %s; reconcile exposure before resuming", reason)
            raise
        finally:
            await self.close()
