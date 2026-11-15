from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import time
from pathlib import Path


class FileLock:
    """Advisory process lock. Every participating writer must use the same path."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.file = path.open("a+b")
        if self.file.seek(0, 2) == 0:
            self.file.write(b"\0")
            self.file.flush()
        self.locked = False

    def acquire(self) -> bool:
        try:
            if os.name == "nt":
                import msvcrt
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.locked = True
            return True
        except (BlockingIOError, OSError):
            return False

    def close(self) -> None:
        if self.locked:
            if os.name == "nt":
                import msvcrt
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file, fcntl.LOCK_UN)
        self.file.close()


@contextlib.asynccontextmanager
async def execution_lock(directory: Path):
    lock = FileLock(Path(directory) / "execution.lock")
    try:
        while not lock.acquire():
            await asyncio.sleep(0.02)
        yield
    finally:
        lock.close()


class Control:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS control (
                    id INTEGER PRIMARY KEY CHECK(id=1), paused INTEGER NOT NULL,
                    epoch INTEGER NOT NULL, armed_ms INTEGER NOT NULL,
                    heartbeat REAL NOT NULL, healthy INTEGER NOT NULL, reason TEXT NOT NULL
                );
                INSERT OR IGNORE INTO control VALUES(1,1,0,0,0,0,'initially paused');
                CREATE TABLE IF NOT EXISTS control_events (
                    id INTEGER PRIMARY KEY, at REAL, action TEXT, reason TEXT
                );
            """)

    @contextlib.contextmanager
    def connect(self):
        db = sqlite3.connect(self.directory / "control.sqlite3", timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def snapshot(self) -> dict:
        with self.connect() as db:
            return dict(db.execute("SELECT * FROM control WHERE id=1").fetchone())

    def pause(self, reason: str) -> None:
        # Persist first: risk handling stops new sends before waiting for a lock.
        with self.connect() as db:
            db.execute("UPDATE control SET paused=1, reason=? WHERE id=1", (reason,))
            db.execute("INSERT INTO control_events(at,action,reason) VALUES(?,?,?)",
                       (time.time(), "pause", reason))

    def heartbeat(self, healthy: bool) -> None:
        with self.connect() as db:
            db.execute("UPDATE control SET heartbeat=?,healthy=? WHERE id=1", (time.time(), int(healthy)))
        if not healthy:
            self.pause("built-in margin monitor is unhealthy or stale")

    def ready(self, timeout: float, epoch: int | None = None) -> bool:
        s = self.snapshot()
        return bool(not s["paused"] and s["healthy"] and 0 <= time.time() - s["heartbeat"] <= timeout
                    and (epoch is None or epoch == s["epoch"]))

    def resume(self, reason: str, timeout: float) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            s = db.execute("SELECT * FROM control WHERE id=1").fetchone()
            if not s["healthy"] or not 0 <= time.time() - s["heartbeat"] <= timeout:
                raise RuntimeError("margin monitor heartbeat is absent, unhealthy or stale; start the service first")
            if not s["paused"]:
                raise RuntimeError("already armed; pause before starting a new session")
            db.execute("UPDATE control SET paused=0,epoch=epoch+1,armed_ms=?,reason=? WHERE id=1",
                       (int(time.time() * 1000), reason))
            db.execute("INSERT INTO control_events(at,action,reason) VALUES(?,?,?)",
                       (time.time(), "resume", reason))
