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
            last = int(self.get("last_client_id") or "0")
            # 2^48-1 is the protocol limit. Dedicated API slot avoids other writers.
            cid = max(int(time.time() * 1000) * 100 + 50, last + 1)
            if cid > (1 << 48) - 1:
                raise RuntimeError("client order ID exceeds protocol range")
            self.db.execute("INSERT INTO attempts(id,route,epoch,requested,status,created) VALUES(?,?,?,?,?,?)",
                            (cid, route, epoch, str(requested), "uncertain", time.time()))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES('last_client_id',?)", (str(cid),))
        return cid

    def settle(self, cid: int, filled: Decimal, note: str = "venue terminal order") -> None:
        row = self.db.execute("SELECT * FROM attempts WHERE id=?", (cid,)).fetchone()
        if row is None:
            raise ValueError("unknown attempt")
        requested = Decimal(row["requested"])
        if not filled.is_finite() or abs(filled) > abs(requested) or filled * requested < 0:
            raise ValueError("filled quantity must have the requested sign and cannot exceed requested")
        with self.db:
            self.db.execute("UPDATE attempts SET filled=?,status='settled',note=? WHERE id=?",
                            (str(filled), note, cid))

    def uncertain(self) -> list[dict]:
        return [dict(r) for r in self.db.execute("SELECT * FROM attempts WHERE status='uncertain'")]

    def close(self) -> None:
        self.db.close()
