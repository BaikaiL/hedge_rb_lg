from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MonitorConfig:
    poll_seconds: float
    # 全仓域的「距强平还剩多少美元」阈值（= 权益 − 维持保证金），不是百分比、不是单腿。
    warning_buffer_usd: float
    critical_buffer_usd: float
    flatten_buffer_usd: float
    recovery_buffer_usd: float
    # 逐仓腿的强平由价格触发（保证金被划进那条腿，别的权益救不了它），所以按「距强平百分比」
    # 判定：与腿的规模、杠杆无关，腿大腿小一视同仁。2026-10-10 起用，见 README。
    isolated_warning_distance_pct: float = 6.0
    isolated_critical_distance_pct: float = 4.5
    isolated_flatten_distance_pct: float = 3.0
    isolated_recovery_distance_pct: float = 7.0
    notification_cooldown_seconds: float = 300.0
    max_data_age_seconds: float = 45.0


@dataclass(frozen=True)
class ExecutionConfig:
    enabled: bool
    max_slippage_pct: float
    close_retry_rounds: int
    verify_delay_seconds: float
    cancel_open_orders: bool


@dataclass(frozen=True)
class AccountConfig:
    name: str
    base_url: str
    chain_id: int
    account_index: int
    api_key_index: int
    api_private_key_env: str

    def private_key(self) -> str:
        value = os.getenv(self.api_private_key_env, "").strip()
        if not value:
            raise ValueError(f"missing environment variable: {self.api_private_key_env}")
        return value


@dataclass(frozen=True)
class NotificationConfig:
    webhook_url_env: str = "HEDGE_GUARD_WEBHOOK_URL"
    telegram_bot_token_env: str = "TELEGRAM_BOT_TOKEN"
    telegram_chat_id_env: str = "TELEGRAM_CHAT_ID"
    # 实时状态：在 Telegram 聊天里持续刷新同一条消息（首次发送后只编辑，不刷屏）。
    live_status_enabled: bool = True
    live_status_interval_seconds: float = 15.0


@dataclass(frozen=True)
class AppConfig:
    monitor: MonitorConfig
    execution: ExecutionConfig
    accounts: tuple[AccountConfig, ...]
    notifications: NotificationConfig


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("rb") as handle:
        raw = tomllib.load(handle)
    monitor = MonitorConfig(**raw["monitor"])
    execution = ExecutionConfig(**raw["execution"])
    accounts = tuple(AccountConfig(**item) for item in raw["accounts"])
    notifications = NotificationConfig(**raw.get("notifications", {}))
    if len(accounts) != 2:
        raise ValueError("exactly two hedge accounts are required")
    official_deployments = {
        "https://mainnet.zklighter.elliot.ai": 304,
        "https://api.rh.lighter.xyz": 466324,
    }
    configured_urls = {a.base_url.rstrip("/") for a in accounts}
    if configured_urls != set(official_deployments):
        raise ValueError("accounts must contain exactly the official Lighter Core and Robinhood mainnet URLs")
    for account in accounts:
        expected_chain_id = official_deployments[account.base_url.rstrip("/")]
        if account.chain_id != expected_chain_id:
            raise ValueError(f"{account.name}: chain_id must be {expected_chain_id} for {account.base_url}")
    signer_slots = {(a.account_index, a.api_key_index) for a in accounts}
    if len(signer_slots) != len(accounts):
        raise ValueError(
            "the two domains must use different (account_index, api_key_index) pairs; "
            "the official native signer indexes clients by this pair"
        )
    # 全部按美元计的剩余缓冲，必须严格单调：平仓 < 再次提醒 < 提醒 < 解除警戒。
    if not (
        0
        < monitor.flatten_buffer_usd
        < monitor.critical_buffer_usd
        < monitor.warning_buffer_usd
        < monitor.recovery_buffer_usd
    ):
        raise ValueError(
            "buffer thresholds must satisfy 0 < flatten_buffer_usd < critical_buffer_usd "
            "< warning_buffer_usd < recovery_buffer_usd (all in USD)"
        )
    # 逐仓腿的百分比阈值同样必须严格单调。
    if not (
        0
        < monitor.isolated_flatten_distance_pct
        < monitor.isolated_critical_distance_pct
        < monitor.isolated_warning_distance_pct
        < monitor.isolated_recovery_distance_pct
    ):
        raise ValueError(
            "isolated distance thresholds must satisfy 0 < isolated_flatten_distance_pct "
            "< isolated_critical_distance_pct < isolated_warning_distance_pct "
            "< isolated_recovery_distance_pct (all in percent)"
        )
    if not (0 < execution.max_slippage_pct <= 10):
        raise ValueError("max_slippage_pct must be in (0, 10]")
    if notifications.live_status_interval_seconds < 5:
        raise ValueError("live_status_interval_seconds must be >= 5")
    return AppConfig(monitor, execution, accounts, notifications)
