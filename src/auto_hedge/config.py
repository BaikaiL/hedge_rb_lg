from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


@dataclass(frozen=True)
class Account:
    url: str
    chain_id: int
    index: int
    key_index: int
    key_env: str

    def private_key(self) -> str:
        key = os.environ.get(self.key_env, "").strip()
        if not key:
            raise ValueError(f"missing environment variable: {self.key_env}")
        return key

    @property
    def ws_url(self) -> str:
        return self.url.replace("https://", "wss://") + "/stream"


@dataclass(frozen=True)
class Route:
    source_symbol: str
    target_symbol: str
    max_order_base: Decimal | None = None
    max_unhedged_base: Decimal | None = None

    @property
    def name(self) -> str:
        return self.source_symbol


@dataclass(frozen=True)
class Config:
    source: Account
    target: Account
    routes: tuple[Route, ...]
    state_dir: Path
    enabled: bool = False
    max_slippage_bps: Decimal = Decimal("30")
    heartbeat_timeout: float = 35
    backfill_seconds: float = 30
    confirm_timeout: float = 8
    max_attempts: int = 3
    max_dust_seconds: float = 30
    max_pages: int = 100
    auto_markets: bool = False


def load_config(path: str | Path) -> Config:
    with Path(path).open("rb") as f:
        raw = tomllib.load(f)

    def account(section: str) -> Account:
        a = raw[section]
        result = Account(
            a["url"].rstrip("/"), int(a["chain_id"]),
            int(os.environ[a["account_index_env"]]),
            int(os.environ[a["api_key_index_env"]]), a["private_key_env"],
        )
        if result.index < 0 or not 4 <= result.key_index <= 254:
            raise ValueError("account index must be >=0; use a dedicated API key slot in [4,254]")
        return result

    source, target = account("source"), account("target")
    deployments = {
        ("https://api.rh.lighter.xyz", "https://mainnet.zklighter.elliot.ai"): (466324, 304),
        ("https://api.rh-testnet.lighter.xyz", "https://testnet.zklighter.elliot.ai"): (300, 300),
    }
    if deployments.get((source.url, target.url)) != (source.chain_id, target.chain_id):
        raise ValueError("use matching official RH -> Core mainnet or testnet deployments and chain IDs")
    if (source.index, source.key_index) == (target.index, target.key_index):
        raise ValueError("native signer requires distinct (account_index, api_key_index) slots across deployments")
    def optional_decimal(value):
        return None if value is None else Decimal(str(value))

    routes = tuple(Route(
        r["source_symbol"], r.get("target_symbol", r["source_symbol"]),
        optional_decimal(r.get("max_order_base")), optional_decimal(r.get("max_unhedged_base")),
    ) for r in raw.get("routes", []))
    auto_markets = bool(raw.get("markets", {}).get("auto", not routes))
    if auto_markets and routes:
        raise ValueError("automatic markets and explicit routes cannot be combined")
    if (not routes and not auto_markets) or len({r.name for r in routes}) != len(routes):
        raise ValueError("enable automatic markets or configure unique source symbols")
    if len({r.target_symbol for r in routes}) != len(routes):
        raise ValueError("target symbols must be unique")
    for r in routes:
        for limit in (r.max_order_base, r.max_unhedged_base):
            if limit is not None and (not limit.is_finite() or limit <= 0):
                raise ValueError("optional base quantity limits must be finite and positive")
        if r.max_order_base is not None and r.max_unhedged_base is not None and r.max_order_base > r.max_unhedged_base:
            raise ValueError("max_order_base cannot exceed max_unhedged_base")
    e = raw.get("execution", {})
    c = Config(
        source, target, routes, Path(raw.get("state", {}).get("directory", "state")).resolve(),
        enabled=e.get("enabled", False),
        max_slippage_bps=Decimal(str(e.get("max_slippage_bps", 30))),
        heartbeat_timeout=float(e.get("heartbeat_timeout_seconds", 35)),
        backfill_seconds=float(e.get("backfill_seconds", 30)),
        confirm_timeout=float(e.get("confirm_timeout_seconds", 8)),
        max_attempts=int(e.get("max_attempts", 3)),
        max_dust_seconds=float(e.get("max_dust_seconds", 30)),
        max_pages=int(e.get("max_backfill_pages", 100)),
        auto_markets=auto_markets,
    )
    if not c.max_slippage_bps.is_finite() or not 0 < c.max_slippage_bps <= 1000:
        raise ValueError("max_slippage_bps must be in (0,1000]")
    if not (1 <= c.max_attempts <= 10 and 1 <= c.max_pages <= 1000
            and 1 <= c.confirm_timeout <= 15 and 1 <= c.backfill_seconds <= 120
            and 1 <= c.heartbeat_timeout <= 120 and c.max_dust_seconds > 0):
        raise ValueError("invalid timing/attempt/page limits")
    return c
