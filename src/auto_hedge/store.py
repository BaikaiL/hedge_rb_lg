from __future__ import annotations

import json
import sqlite3
import time
from decimal import Decimal
from pathlib import Path


class Store:
    def __init__(self, directory: Path, identity: dict, filename: str = "ledger.sqlite3"):
        self.db = sqlite3.connect(directory / filename)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS orders (
                market INTEGER, order_id TEXT, payload TEXT, PRIMARY KEY(market,order_id)
            );
            CREATE TABLE IF NOT EXISTS fills (
                market INTEGER, trade_id TEXT, route TEXT, epoch INTEGER,
                target_delta TEXT, status TEXT, observed REAL, payload TEXT,
                PRIMARY KEY(market,trade_id)
            );
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY, route TEXT, epoch INTEGER,
                requested TEXT, filled TEXT NOT NULL DEFAULT '0', status TEXT,
                created REAL, note TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS hedge_notifications (
                event_id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL,
                created REAL NOT NULL, delivered_at REAL,
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                next_try_at REAL NOT NULL DEFAULT 0
            );
        """)
        encoded = json.dumps(identity, sort_keys=True)
        old = self.get("identity")
        if old is not None and old != encoded:
            self.db.close()
            raise RuntimeError("state identity changed (accounts/routes/mode); use a separate state directory")
        self.set("identity", encoded)

    def get(self, key: str) -> str | None:
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

    def set(self, key: str, value: str) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, value))

    def order(self, market: int, order_id: str) -> dict | None:
        r = self.db.execute("SELECT payload FROM orders WHERE market=? AND order_id=?", (market, order_id)).fetchone()
        return json.loads(r[0]) if r else None

    def cache_order(self, order: dict) -> None:
        oid = str(order.get("order_id") or order["order_index"])
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO orders VALUES(?,?,?)",
                            (int(order["market_index"]), oid, json.dumps(order)))

    def has_fill(self, market: int, trade_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM fills WHERE market=? AND trade_id=?", (market, trade_id)).fetchone() is not None

    def add_fill(self, market: int, trade_id: str, route: str, epoch: int,
                 delta: Decimal, status: str, payload: dict) -> None:
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO fills VALUES(?,?,?,?,?,?,?,?)",
                            (market, trade_id, route, epoch, str(delta), status, time.time(), json.dumps(payload)))

    def pending(self, route: str, epoch: int) -> Decimal:
        # SQLite REAL would lose base-asset precision; sum strings using Decimal.
        wanted = sum((Decimal(r[0]) for r in self.db.execute(
            "SELECT target_delta FROM fills WHERE route=? AND epoch=? AND status='eligible'", (route, epoch))), Decimal(0))
        done = sum((Decimal(r[0]) for r in self.db.execute(
            "SELECT filled FROM attempts WHERE route=? AND epoch=?", (route, epoch))), Decimal(0))
        return wanted - done

    def routes(self) -> list[str]:
        return [r[0] for r in self.db.execute("SELECT DISTINCT route FROM fills ORDER BY route")]

    def new_attempt(self, route: str, epoch: int, requested: Decimal) -> int:
        with self.db:
            cid = self._allocate_client_id()
            self.db.execute("INSERT INTO attempts(id,route,epoch,requested,status,created) VALUES(?,?,?,?,?,?)",
                            (cid, route, epoch, str(requested), "uncertain", time.time()))
        return cid

    def _allocate_client_id(self) -> int:
        last = int(self.get("last_client_id") or "0")
        cid = max(int(time.time() * 1000) * 100 + 50, last + 1)
        if cid > (1 << 48) - 1:
            raise RuntimeError("client order ID exceeds protocol range")
        self.db.execute("INSERT OR REPLACE INTO meta VALUES('last_client_id',?)", (str(cid),))
        return cid

    def next_client_id(self) -> int:
        # Emergency closes share the ID counter, but never count as hedge fills.
        with self.db:
            return self._allocate_client_id()

    def settle(self, cid: int, filled: Decimal, note: str = "venue terminal order",
               notification: tuple[str, str] | None = None) -> None:
        row = self.db.execute("SELECT * FROM attempts WHERE id=?", (cid,)).fetchone()
        if row is None:
            raise ValueError("unknown attempt")
        requested = Decimal(row["requested"])
        if not filled.is_finite() or abs(filled) > abs(requested) or filled * requested < 0:
            raise ValueError("filled quantity must have the requested sign and cannot exceed requested")
        with self.db:
            self.db.execute("UPDATE attempts SET filled=?,status='settled',note=? WHERE id=?",
                            (str(filled), note, cid))
            if notification:
                self._enqueue_notification(f"hedge:{cid}:result", *notification)
                # A late retry must not announce "unknown" after a confirmed result.
                self.db.execute(
                    "DELETE FROM hedge_notifications WHERE event_id=? AND delivered_at IS NULL",
                    (f"hedge:{cid}:unknown",),
                )

    def _enqueue_notification(self, event_id: str, title: str, body: str) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO hedge_notifications(event_id,title,body,created) VALUES(?,?,?,?)",
            (event_id, title, body, time.time()),
        )

    def enqueue_notification(self, event_id: str, title: str, body: str) -> None:
        with self.db:
            self._enqueue_notification(event_id, title, body)

    def pending_notifications(self, limit: int = 20) -> list[dict]:
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM hedge_notifications WHERE delivered_at IS NULL AND next_try_at<=? ORDER BY created,event_id LIMIT ?",
            (time.time(), limit),
        )]

    def unsent_notification_count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM hedge_notifications WHERE delivered_at IS NULL").fetchone()[0]

    def notification_delivery(self, event_id: str, delivered: bool) -> None:
        row = self.db.execute("SELECT delivery_attempts FROM hedge_notifications WHERE event_id=?", (event_id,)).fetchone()
        if row is None:
            return  # Superseded while the HTTP notification was in flight.
        delay = min(300, 5 * 2 ** min(row[0], 6))
        now = time.time()
        with self.db:
            self.db.execute(
                "UPDATE hedge_notifications SET delivery_attempts=delivery_attempts+1,delivered_at=?,next_try_at=? WHERE event_id=?",
                (now if delivered else None, now + delay, event_id),
            )

    def uncertain(self) -> list[dict]:
        return [dict(r) for r in self.db.execute("SELECT * FROM attempts WHERE status='uncertain'")]

    def close(self) -> None:
        self.db.close()
