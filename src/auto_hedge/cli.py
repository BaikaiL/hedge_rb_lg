from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from logging.handlers import RotatingFileHandler
from decimal import Decimal

from dotenv import load_dotenv

from .config import load_config
from .control import Control, FileLock, execution_lock
from .service import Service, identity, failure_reason
from .store import Store
from .network import configure_proxy


async def execute(args) -> None:
    load_dotenv(args.env_file, override=False)
    cfg = load_config(args.config)
    configure_proxy(cfg.proxy_url)
    control = Control(cfg.state_dir)
    if args.action == "run":
        handler = RotatingFileHandler(cfg.state_dir / "auto-hedge.log", maxBytes=5 * 1024 * 1024,
                                      backupCount=3, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logging.getLogger().addHandler(handler)
    live = bool(args.live and cfg.enabled)
    if args.live and not cfg.enabled:
        raise ValueError("--live requires execution.enabled=true")
    if args.action == "reconcile" and cfg.enabled and not args.live:
        raise ValueError("LIVE ledger reconciliation requires --live (queries only; sends no orders)")
    # Paper and live have distinct ledgers; their simulated fills can never mix.
    if args.action in ("run", "check", "reconcile"):
        process_lock = FileLock(cfg.state_dir / "process.lock")
        if not process_lock.acquire():
            process_lock.close()
            raise RuntimeError("another hedger/check/reconcile process uses this state directory")
        service = None
        try:
            service = Service(cfg, live)
            if args.action == "run":
                logging.info("starting mode=%s state_dir=%s", "LIVE" if live else "DRY-RUN", cfg.state_dir)
                task = asyncio.create_task(service.run())
                loop = asyncio.get_running_loop()
                if os.name != "nt":
                    loop.add_signal_handler(signal.SIGTERM, task.cancel)
                try:
                    await task
                finally:
                    if os.name != "nt":
                        loop.remove_signal_handler(signal.SIGTERM)
            else:
                await service.start()
                await service.engine.start()
                decision, snapshots = await service.margin.inspect()
                if args.action == "reconcile":
                    control.pause("manual reconcile")
                    async with execution_lock(cfg.state_dir):
                        await service.engine.reconcile()
                print(json.dumps({"check": "ok", "live": live, "control": control.snapshot(),
                                  "margin": service.margin.describe(decision, snapshots)}, ensure_ascii=False))
        finally:
            if service and args.action != "run":
                await service.close()
            process_lock.close()
        return

    if args.action == "pause":
        control.pause(args.reason)
    # Control commands inspect the mode selected by execution.enabled in config.
    ledger_name = "ledger.live.sqlite3" if cfg.enabled else "ledger.paper.sqlite3"
    ledger = cfg.state_dir / ledger_name
    if not ledger.exists():
        if args.action != "status":
            raise RuntimeError("initialize the ledger with check/run first")
        print(json.dumps(control.snapshot(), ensure_ascii=False))
        return
    store = Store(cfg.state_dir, identity(cfg, cfg.enabled), ledger_name)
    try:
        if args.action in ("resume", "settle-attempt"):
            if not control.snapshot()["paused"]:
                raise RuntimeError("pause hedging before resume/settle-attempt")
            async with execution_lock(cfg.state_dir):
                if args.action == "resume":
                    if store.uncertain():
                        raise RuntimeError("unresolved hedge submission; stop service then run reconcile")
                    # Explicit acknowledgement is required because resume starts a fresh epoch.
                    control.resume(args.reason, cfg.heartbeat_timeout)
                else:
                    if not any(a["id"] == args.id for a in store.uncertain()):
                        raise ValueError("only an uncertain attempt may be manually settled")
                    store.settle(args.id, Decimal(args.filled), "MANUAL: " + args.reason)
        state = control.snapshot()
        print(json.dumps({"mode": "LIVE" if cfg.enabled else "DRY-RUN", "control": state, "uncertain": store.uncertain(),
                          "margin": json.loads(store.get("margin_status") or "null"),
                          "pending_result_notifications": store.unsent_notification_count(),
                          "pending_base": {r: str(store.pending(r, state["epoch"])) for r in store.routes()}}, ensure_ascii=False, indent=2))
    finally:
        store.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Hedge Robinhood Lighter fills and monitor/flatten both accounts in one service")
    p.add_argument("--config", default="config.toml")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--live", action="store_true")
    subs = p.add_subparsers(dest="action", required=True)
    for action in ("run", "check", "reconcile", "status"):
        subs.add_parser(action)
    for action in ("pause", "resume", "settle-attempt"):
        sub = subs.add_parser(action)
        sub.add_argument("--reason", required=True)
        if action == "resume":
            sub.add_argument("--acknowledge-exposure", action="store_true", required=True,
                             help="confirm both actual positions and all discarded prior-session fills have been reviewed")
        if action == "settle-attempt":
            sub.add_argument("--id", required=True, type=int)
            sub.add_argument("--filled", required=True, help="verified signed actual filled base quantity")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Official SDK debug logging contains signed transaction payloads; keep it disabled.
    logging.getLogger("lighter").setLevel(logging.WARNING)
    try:
        asyncio.run(execute(args))
    except KeyboardInterrupt:
        pass
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        # Avoid SDK/HTTP exceptions containing auth tokens or signed payloads.
        logging.error("%s", failure_reason(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
