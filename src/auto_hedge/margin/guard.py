from __future__ import annotations

import asyncio
import json
import logging
import time

from ..config import Config
from ..control import Control, execution_lock
from ..errors import failure_reason
from ..exchange import Exchange
from ..store import Store
from .account import MarginAccount
from .models import AccountSnapshot, GuardState, RiskLevel
from .notifier import LiveStatusPublisher, Notifier
from .risk import Decision, Thresholds, evaluate

log = logging.getLogger(__name__)


def format_usd(value) -> str:
    return "未知" if value is None else f"${value:.2f}"


class MarginGuard:
    def __init__(self, cfg: Config, control: Control, store: Store, source: Exchange, target: Exchange, live: bool):
        self.cfg, self.control, self.store, self.live = cfg, control, store, live
        self.clients = (
            MarginAccount("lighter-core", target, cfg.margin_execution, store, live),
            MarginAccount("robinhood-lighter", source, cfg.margin_execution, store, live),
        )
        self.notifier = Notifier(cfg.notifications)
        self.live_status = LiveStatusPublisher(cfg.notifications, "LIVE" if live else "DRY-RUN",
                                               Thresholds.from_config(cfg.monitor).describe())
        self.state = GuardState()
        self.notifications: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.latest_body = "risk=unknown; 等待保证金数据"

    def notify(self, title: str, body: str) -> None:
        # Network notifications never delay cancellation or emergency close orders.
        log.warning("%s | %s", title, body)
        if self.notifications.full():
            self.notifications.get_nowait()
        self.notifications.put_nowait((title, body))

    async def notification_loop(self) -> None:
        while True:
            title, body = await self.notifications.get()
            try:
                await self.notifier.send(title, body)
            except Exception as exc:
                log.error("notification failed: %s", failure_reason(exc))

    async def status_loop(self) -> None:
        while True:
            state = self.control.snapshot()
            label = "暂停" if state["paused"] else "运行"
            await self.live_status.maybe_update(self.latest_body + f"; hedging={label}")
            await asyncio.sleep(1)

    async def read_all(self) -> tuple[AccountSnapshot, ...]:
        return tuple(await asyncio.gather(*(c.snapshot() for c in self.clients)))

    def describe(self, decision: Decision, snapshots) -> str:
        rows = []
        for s in snapshots:
            head = "无仓位" if not s.positions else (
                "数据缺失" if s.liquidation_buffer_usd is None else f"距强平 {format_usd(s.liquidation_buffer_usd)}")
            row = f"{s.name}: {head}, collateral={s.collateral:.2f}, available={s.available_balance:.2f}"
            if s.weakest_leg_distance_pct is not None:
                row += f", 最弱腿={s.weakest_leg_distance_pct:.2f}%"
            isolated = [(p.liquidation_buffer_usd, p) for p in s.positions
                        if p.isolated and p.liquidation_buffer_usd is not None]
            if isolated:
                buffer, position = min(isolated, key=lambda item: item[0])
                row += f", 逐仓腿={position.symbol} {format_usd(buffer)}"
            rows.append(row)
        return f"risk={decision.level.value}; {decision.reason}; " + " | ".join(rows)

    def alert_title(self, decision: Decision) -> str:
        m = self.cfg.monitor
        isolated = decision.binding_distance_pct is not None
        remaining = f"{decision.binding_distance_pct:.2f}%" if isolated else format_usd(decision.liquidation_buffer_usd)
        limits = {
            RiskLevel.EMERGENCY: m.isolated_flatten_distance_pct if isolated else m.flatten_buffer_usd,
            RiskLevel.CRITICAL: m.isolated_critical_distance_pct if isolated else m.critical_buffer_usd,
            RiskLevel.WARNING: m.isolated_warning_distance_pct if isolated else m.warning_buffer_usd,
        }
        if decision.level not in limits:
            return f"❔ 风控数据不可用：{decision.reason}"
        threshold = f"{limits[decision.level]:g}%" if isolated else f"${limits[decision.level]:g}"
        label = {RiskLevel.EMERGENCY: "🚨 启动双账户全平", RiskLevel.CRITICAL: "🔴 再次提醒",
                 RiskLevel.WARNING: "⚠️ 提醒"}[decision.level]
        return f"{label}：{decision.binding_domain} 距强平 {remaining} ≤ {threshold}"

    def save_status(self, decision: Decision, snapshots) -> None:
        self.store.set("margin_status", json.dumps({
            "risk": decision.level.value, "reason": decision.reason, "observed_at": time.time(),
            "mode": "LIVE" if self.live else "DRY-RUN",
            "accounts": [{"name": s.name, "positions": len(s.positions),
                          "collateral": str(s.collateral), "available_balance": str(s.available_balance),
                          "liquidation_buffer_usd": str(s.liquidation_buffer_usd) if s.liquidation_buffer_usd is not None else None,
                          "weakest_leg_distance_pct": str(s.weakest_leg_distance_pct) if s.weakest_leg_distance_pct is not None else None}
                         for s in snapshots],
        }, ensure_ascii=False))

    async def inspect(self):
        snapshots = await self.read_all()
        return evaluate(snapshots, self.state, self.cfg.monitor), snapshots

    async def run_once(self) -> Decision:
        decision, snapshots = await self.inspect()
        safe = decision.level in (RiskLevel.HEALTHY, RiskLevel.WARNING, RiskLevel.CRITICAL)
        # A paper run also has its own real monitor; all its signed writes remain disabled.
        self.control.heartbeat(safe and not decision.should_flatten and not self.state.flatten_started)
        self.latest_body = self.describe(decision, snapshots)
        self.save_status(decision, snapshots)
        log.info(self.latest_body)
        if decision.level != RiskLevel.HEALTHY:
            now = time.time()
            last = self.state.last_notification_at.get(decision.level.value, 0)
            if now - last >= self.cfg.monitor.notification_cooldown_seconds:
                self.state.last_notification_at[decision.level.value] = now
                self.notify(self.alert_title(decision), self.latest_body)
        if decision.should_flatten:
            await self.flatten_both(snapshots, decision)
        return decision

    async def flatten_both(self, snapshots, decision: Decision) -> None:
        self.control.pause("margin emergency flatten: " + decision.reason)
        self.state.flatten_started = True
        self.notify("🚨 开始双账户全平", f"{decision.reason}; 模式={'LIVE' if self.live else 'DRY-RUN'}; 先撤单，再 reduce-only 平仓")
        paper_completed = False
        try:
            async with execution_lock(self.control.directory):
                if self.cfg.margin_execution.cancel_open_orders:
                    results = await asyncio.gather(*(c.cancel_all() for c in self.clients), return_exceptions=True)
                    for c, result in zip(self.clients, results):
                        if isinstance(result, Exception):
                            self.notify("撤单失败", f"{c.name}: {failure_reason(result)}; 继续尝试平仓")
                # Refresh AFTER draining an in-flight hedge and canceling resting orders.
                try:
                    snapshots = await self.read_all()
                except Exception as exc:
                    self.notify("平仓前复查失败", failure_reason(exc) + "; 使用最后一次有效仓位并复查重试")
                for round_index in range(self.cfg.margin_execution.close_retry_rounds):
                    results = await asyncio.gather(*(c.flatten_once(s) for c, s in zip(self.clients, snapshots)), return_exceptions=True)
                    actions = []
                    for c, result in zip(self.clients, results):
                        if isinstance(result, Exception):
                            self.notify("平仓提交失败", f"round={round_index + 1}; {c.name}: {failure_reason(result)}")
                        else:
                            actions.extend(result)
                    if not self.live:
                        self.notify("DRY-RUN 全平清单", "\n".join(actions) or "当前无仓位")
                        paper_completed = True
                        return
                    await asyncio.sleep(self.cfg.margin_execution.verify_delay_seconds)
                    snapshots = await self.read_all()
                    if all(not s.positions for s in snapshots):
                        self.notify("双账户全平完成", f"rounds={round_index + 1}; 已复查两边均无持仓；对冲保持暂停")
                        return
                remaining = [f"{s.name}:{','.join(p.symbol for p in s.positions)}" for s in snapshots if s.positions]
                self.notify("紧急：全平未完成", "残余仓位=" + " | ".join(remaining))
        finally:
            # Retry on the next emergency poll if submission or verification failed.
            self.state.flatten_started = paper_completed

    async def run_forever(self) -> None:
        self.notify("自动对冲与保证金监控已启动", f"mode={'LIVE' if self.live else 'DRY-RUN'}; 两个账户共用此进程；对冲需手动启用")
        while True:
            started = time.monotonic()
            try:
                await self.run_once()
            except Exception as exc:
                self.control.heartbeat(False)
                reason = failure_reason(exc)
                self.latest_body = "risk=unknown; " + reason
                self.store.set("margin_status", json.dumps({"risk": "unknown", "reason": reason, "observed_at": time.time()}))
                log.error("margin monitor iteration failed: %s", reason)
                self.notify("保证金监控读取或平仓失败", reason + "; 对冲保持暂停，继续监控并重试")
            await asyncio.sleep(max(0.1, self.cfg.monitor.poll_seconds - (time.monotonic() - started)))
