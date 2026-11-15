from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from auto_hedge.control import Control, execution_lock
import time

from .config import AppConfig
from .lighter_client import LighterAccount
from .models import AccountSnapshot, GuardState, RiskLevel
from .notifier import LiveStatusPublisher, Notifier
from .risk import Decision, Thresholds, evaluate


def format_usd(value) -> str:
    return "未知" if value is None else f"${value:.2f}"


class HedgeGuard:
    def __init__(self, cfg: AppConfig, cli_live: bool):
        self.cfg = cfg
        shared = os.environ.get("AUTO_HEDGE_STATE_DIR")
        if cli_live and cfg.execution.enabled and not shared:
            raise RuntimeError("LIVE margin guard requires AUTO_HEDGE_STATE_DIR for hedger coordination")
        self.interlock = Control(Path(shared)) if shared else None
        if self.interlock:
            self.interlock.pause("margin guard starting; manual resume required")
        self.live = bool(cli_live and cfg.execution.enabled)
        self.clients = tuple(LighterAccount(a, cfg.execution, self.live) for a in cfg.accounts)
        self.notifier = Notifier(cfg.notifications)
        mode = "LIVE" if self.live else "DRY-RUN"
        # 两种口径都要显示：全仓域按美元，逐仓腿按距强平百分比（2026-10-10 起）。
        thresholds = Thresholds.from_config(cfg.monitor).describe()
        self.live_status = LiveStatusPublisher(cfg.notifications, mode, thresholds_text=thresholds)
        self.state = GuardState()

    async def start(self) -> None:
        await asyncio.gather(*(client.start() for client in self.clients))
        mode = "LIVE" if self.live else "DRY-RUN"
        await self.notifier.send("Hedge Guard 已启动", f"mode={mode}; accounts={', '.join(a.name for a in self.cfg.accounts)}")

    async def close(self) -> None:
        if self.interlock:
            self.interlock.heartbeat(False)
        await asyncio.gather(*(client.close() for client in self.clients), return_exceptions=True)

    async def read_all(self) -> tuple[AccountSnapshot, ...]:
        return tuple(await asyncio.gather(*(client.snapshot() for client in self.clients)))

    def _describe(self, decision: Decision, snapshots) -> str:
        rows = []
        for snapshot in snapshots:
            buffer = snapshot.liquidation_buffer_usd
            # 「无仓位」只能表示真的没有仓位：算不出数字时必须显式说「数据缺失」，
            # 否则风控瞎了而状态消息看起来像账户是空的（2026-10-02 逐仓腿事件的教训）。
            if not snapshot.positions:
                head = "无仓位"
            elif buffer is None:
                head = "数据缺失"
            else:
                head = f"距强平 {format_usd(buffer)}"
            row = f"{snapshot.name}: {head}, collateral={snapshot.collateral:.2f}, available={snapshot.available_balance:.2f}"
            leg = snapshot.weakest_leg_distance_pct
            if leg is not None:
                row += f", 最弱腿={leg:.2f}%"
            isolated = [
                (position.liquidation_buffer_usd, position)
                for position in snapshot.positions
                if position.isolated and position.liquidation_buffer_usd is not None
            ]
            if isolated:
                leg_buffer, leg_position = min(isolated, key=lambda item: item[0])
                row += f", 逐仓腿={leg_position.symbol} {format_usd(leg_buffer)}"
            rows.append(row)
        return f"risk={decision.level.value}; {decision.reason}; " + " | ".join(rows)

    def _alert_title(self, decision: Decision) -> str:
        """标题按触发域的口径显示：逐仓腿是「距强平 2.87% ≤ 3%」，全仓域是「距强平 $28.4 ≤ $30」。"""
        monitor = self.cfg.monitor
        if decision.binding_distance_pct is not None:
            remaining = f"{decision.binding_distance_pct:.2f}%"
            limits = {
                RiskLevel.EMERGENCY: f"{monitor.isolated_flatten_distance_pct:g}%",
                RiskLevel.CRITICAL: f"{monitor.isolated_critical_distance_pct:g}%",
                RiskLevel.WARNING: f"{monitor.isolated_warning_distance_pct:g}%",
            }
        else:
            remaining = format_usd(decision.liquidation_buffer_usd)
            limits = {
                RiskLevel.EMERGENCY: f"${monitor.flatten_buffer_usd:g}",
                RiskLevel.CRITICAL: f"${monitor.critical_buffer_usd:g}",
                RiskLevel.WARNING: f"${monitor.warning_buffer_usd:g}",
            }
        where = f"{decision.binding_domain} " if decision.binding_domain else ""
        if decision.level == RiskLevel.EMERGENCY:
            return f"🚨 {where}距强平 {remaining} ≤ {limits[RiskLevel.EMERGENCY]}：启动双账户全平"
        if decision.level == RiskLevel.CRITICAL:
            return f"🔴 {where}距强平 {remaining} ≤ {limits[RiskLevel.CRITICAL]}（再次提醒）"
        if decision.level == RiskLevel.WARNING:
            return f"⚠️ {where}距强平 {remaining} ≤ {limits[RiskLevel.WARNING]}（提醒）"
        return f"❔ 风控数据不可用：{decision.reason}"

    async def _notify_if_due(self, decision: Decision, body: str) -> None:
        if decision.level == RiskLevel.HEALTHY:
            return
        now = time.time()
        key = decision.level.value
        last = self.state.last_notification_at.get(key, 0)
        if now - last >= self.cfg.monitor.notification_cooldown_seconds:
            self.state.last_notification_at[key] = now
            await self.notifier.send(self._alert_title(decision), body)

    async def flatten_both(self, initial_snapshots, decision: Decision) -> None:
        if not self.interlock or not self.live:
            await self._flatten_both_unlocked(initial_snapshots, decision)
            return
        # Pause is committed before waiting for any in-flight hedge to resolve.
        self.interlock.pause("margin guard emergency flatten: " + decision.reason)
        async with execution_lock(self.interlock.directory):
            # The snapshot that triggered liquidation may predate an in-flight hedge.
            try:
                initial_snapshots = await self.read_all()
            except Exception:
                logging.exception("fresh snapshot failed; emergency flatten uses last known positions")
            await self._flatten_both_unlocked(initial_snapshots, decision)
        # Remain paused after successful liquidation and after all error paths.

    async def _flatten_both_unlocked(self, initial_snapshots, decision: Decision) -> None:
        self.state.flatten_started = True
        mode = "真实执行" if self.live else "演练（DRY-RUN，不会真的下单）"
        await self.notifier.send(
            "🚨 开始双账户全平",
            f"触发={decision.reason}; 距强平={format_usd(decision.liquidation_buffer_usd)}"
            f"{f' ({decision.binding_distance_pct:.2f}%)' if decision.binding_distance_pct is not None else ''}; 模式={mode}; "
            "先撤销全部挂单，再逐仓 reduce-only 平仓",
        )
        if self.cfg.execution.cancel_open_orders:
            results = await asyncio.gather(*(c.cancel_all() for c in self.clients), return_exceptions=True)
            for client, result in zip(self.clients, results):
                if isinstance(result, Exception):
                    await self.notifier.send("撤单失败", f"{client.cfg.name}: {result}; 将继续尝试平仓")

        snapshots = initial_snapshots
        all_actions: list[str] = []
        for round_index in range(self.cfg.execution.close_retry_rounds):
            results = await asyncio.gather(
                *(client.flatten_once(snapshot) for client, snapshot in zip(self.clients, snapshots)),
                return_exceptions=True,
            )
            for client, result in zip(self.clients, results):
                if isinstance(result, Exception):
                    await self.notifier.send("平仓提交失败", f"round={round_index + 1}; {client.cfg.name}: {result}")
                else:
                    all_actions.extend(result)
            if not self.live:
                await self.notifier.send("DRY-RUN 全平清单", "\n".join(all_actions) or "当前无仓位")
                return
            await asyncio.sleep(self.cfg.execution.verify_delay_seconds)
            try:
                snapshots = await self.read_all()
            except Exception:
                # A later monitor iteration must be allowed to retry the entire
                # emergency procedure if verification temporarily fails.
                self.state.flatten_started = False
                raise
            if all(not snapshot.positions for snapshot in snapshots):
                await self.notifier.send("双账户全平完成", f"rounds={round_index + 1}; 已复查两边均无持仓")
                return
        remaining = [f"{s.name}:{','.join(p.symbol for p in s.positions)}" for s in snapshots if s.positions]
        await self.notifier.send("紧急：全平未完成", "残余仓位=" + " | ".join(remaining))
        self.state.flatten_started = False

    async def run_once(self) -> Decision:
        snapshots = await self.read_all()
        decision = evaluate(snapshots, self.state, self.cfg.monitor)
        if self.interlock:
            fresh = all(0 <= time.time() - s.observed_at_s <= self.cfg.monitor.max_data_age_seconds for s in snapshots)
            self.interlock.heartbeat(bool(self.live and fresh
                and decision.level in (RiskLevel.HEALTHY, RiskLevel.WARNING, RiskLevel.CRITICAL)
                and not decision.should_flatten and not self.state.flatten_started))
        body = self._describe(decision, snapshots)
        logging.info(body)
        await self.live_status.maybe_update(body)
        await self._notify_if_due(decision, body)
        if decision.should_flatten:
            await self.flatten_both(snapshots, decision)
        return decision

    async def _report_startup_failure(self, exc: Exception) -> None:
        """启动失败必须立刻可见：systemd 会自动重启，但重启循环期间风控是不设防的。"""
        try:
            await self.notifier.send(
                "🚨 Hedge Guard 启动失败",
                f"mode={'LIVE' if self.live else 'DRY-RUN'}; error={exc!r}; "
                "风控当前不设防（无法平仓），请立刻处理",
            )
        except Exception:  # noqa: BLE001 - 通知失败不能掩盖原始错误
            logging.exception("could not deliver the startup-failure notification")

    async def run_forever(self) -> None:
        try:
            try:
                await self.start()
            except Exception as exc:
                logging.exception("startup failed")
                await self._report_startup_failure(exc)
                raise
            while True:
                started = time.monotonic()
                try:
                    await self.run_once()
                except Exception as exc:
                    if self.interlock:
                        self.interlock.heartbeat(False)
                    logging.exception("monitor iteration failed")
                    await self.notifier.send("监控读取失败", str(exc))
                delay = max(0.1, self.cfg.monitor.poll_seconds - (time.monotonic() - started))
                await asyncio.sleep(delay)
        finally:
            await self.close()
