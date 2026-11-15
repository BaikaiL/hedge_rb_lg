from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from ..config import MonitorConfig
from .models import AccountSnapshot, GuardState, RiskDomain, RiskLevel


def _usd(value: Decimal) -> str:
    """把阈值显示成人看得懂的样子：$95 而不是 $95.0。"""
    return f"{float(value):g}"


def _pct(value: Decimal) -> str:
    """同样把百分比阈值显示干净：6% 而不是 6.0。"""
    return f"{float(value):g}%"


#: 严重程度排序：EMERGENCY > CRITICAL > WARNING > HEALTHY（UNKNOWN 不参与比较）。
_SEVERITY = {
    RiskLevel.HEALTHY: 0,
    RiskLevel.WARNING: 1,
    RiskLevel.CRITICAL: 2,
    RiskLevel.EMERGENCY: 3,
}

_SUFFIX = {
    RiskLevel.WARNING: "reminder",
    RiskLevel.CRITICAL: "second reminder",
    RiskLevel.EMERGENCY: "flatten now",
}


@dataclass(frozen=True)
class Decision:
    level: RiskLevel
    liquidation_buffer_usd: Decimal | None
    should_flatten: bool
    reason: str
    #: 触发判定的那个清算域（哪个账户的全仓域 / 哪条逐仓腿），用于告警标题。
    binding_domain: str = ""
    #: 逐仓腿判定时它的距强平百分比（全仓域判定时为 None，标题改用美元口径）。
    binding_distance_pct: Decimal | None = None


@dataclass(frozen=True)
class Thresholds:
    """两种判定口径的四档阈值：全仓域按美元，逐仓腿按距强平百分比。"""

    usd_warning: Decimal
    usd_critical: Decimal
    usd_flatten: Decimal
    usd_recovery: Decimal
    pct_warning: Decimal
    pct_critical: Decimal
    pct_flatten: Decimal
    pct_recovery: Decimal

    @classmethod
    def from_config(cls, cfg: MonitorConfig) -> "Thresholds":
        return cls(
            usd_warning=Decimal(str(cfg.warning_buffer_usd)),
            usd_critical=Decimal(str(cfg.critical_buffer_usd)),
            usd_flatten=Decimal(str(cfg.flatten_buffer_usd)),
            usd_recovery=Decimal(str(cfg.recovery_buffer_usd)),
            pct_warning=Decimal(str(cfg.isolated_warning_distance_pct)),
            pct_critical=Decimal(str(cfg.isolated_critical_distance_pct)),
            pct_flatten=Decimal(str(cfg.isolated_flatten_distance_pct)),
            pct_recovery=Decimal(str(cfg.isolated_recovery_distance_pct)),
        )

    def describe(self) -> str:
        return (
            f"全仓 ${_usd(self.usd_warning)}/{_usd(self.usd_critical)}/{_usd(self.usd_flatten)} · "
            f"逐仓 {_pct(self.pct_warning)}/{_pct(self.pct_critical)}/{_pct(self.pct_flatten)}"
        )


def _judge(domain: RiskDomain, thresholds: Thresholds) -> tuple[RiskLevel, str]:
    """单个清算域的等级 + 命中的阈值文本。

    逐仓腿看距强平百分比（价格事件，与腿的大小/杠杆无关）；全仓域看美元缓冲（保证金事件）。
    """
    if domain.uses_distance:
        value = domain.distance_pct
        warning, critical, flatten = (
            thresholds.pct_warning,
            thresholds.pct_critical,
            thresholds.pct_flatten,
        )
        fmt = _pct
    else:
        value = domain.buffer_usd
        warning, critical, flatten = (
            thresholds.usd_warning,
            thresholds.usd_critical,
            thresholds.usd_flatten,
        )
        fmt = _usd
    if value <= flatten:
        return RiskLevel.EMERGENCY, fmt(flatten)
    if value <= critical:
        return RiskLevel.CRITICAL, fmt(critical)
    if value <= warning:
        return RiskLevel.WARNING, fmt(warning)
    return RiskLevel.HEALTHY, fmt(warning)


def _explain(domain: RiskDomain, threshold: str) -> str:
    if domain.uses_distance:
        return f"{domain.label} 距强平 {domain.distance_pct:.2f}% <= {threshold}"
    return f"{domain.label} 距强平 ${domain.buffer_usd:.2f} <= ${threshold}"


def _recovered(domain: RiskDomain, thresholds: Thresholds) -> bool:
    if domain.uses_distance:
        return domain.distance_pct >= thresholds.pct_recovery
    return domain.buffer_usd >= thresholds.usd_recovery


def evaluate(
    snapshots: tuple[AccountSnapshot, ...],
    state: GuardState,
    cfg: MonitorConfig,
    now: float | None = None,
) -> Decision:
    """按每个清算域各自的「离强平还有多远」判定，取最危险的那一档。

    全仓（cross）域：账户级缓冲 = 权益 − 维持保证金，美元阈值来自配置。逐仓（isolated）腿：强平是价格事件，按距强平百分比（6/4.5/3，解除 7），
    与腿的规模、杠杆无关 —— 大额腿不会被固定美元数坑，小额腿也不会过早平仓。

    各域互不救济（权益不能互相调用），所以取最危险的域，绝不能相加 —— 一边亏钱的同时
    另一边赚钱时，"合计"会平稳得看不出危险。
    """
    thresholds = Thresholds.from_config(cfg)
    now = time.time() if now is None else now
    if not snapshots:
        state.level = RiskLevel.UNKNOWN
        return Decision(RiskLevel.UNKNOWN, None, False, "no account data")

    # 陈旧或缺失的数据永远不能授权下单。粘性的警戒标记保留不动，等数据恢复后再判定。
    for snapshot in snapshots:
        age = now - snapshot.observed_at_s
        if snapshot.observed_at_s <= 0 or age < -5 or age > cfg.max_data_age_seconds:
            state.level = RiskLevel.UNKNOWN
            return Decision(RiskLevel.UNKNOWN, None, False, f"stale data: {snapshot.name}")
        if snapshot.buffer_is_unknown:
            state.level = RiskLevel.UNKNOWN
            return Decision(RiskLevel.UNKNOWN, None, False, f"missing margin data: {snapshot.name}")

    domains = [domain for snapshot in snapshots for domain in (snapshot.domains or ())]
    if not domains:
        state.level = RiskLevel.HEALTHY
        state.warning_started_at = None
        state.flatten_started = False
        return Decision(RiskLevel.HEALTHY, None, False, "no open positions")

    worst = max(domains, key=lambda domain: _SEVERITY[_judge(domain, thresholds)[0]])
    level, threshold = _judge(worst, thresholds)
    # 展示与告警仍带上「最危险的美元缓冲」，方便和权益对齐看。
    buffer_usd = min(domain.buffer_usd for domain in domains)
    distance_pct = worst.distance_pct if worst.uses_distance else None

    if level == RiskLevel.HEALTHY:
        if all(_recovered(domain, thresholds) for domain in domains):
            state.level = RiskLevel.HEALTHY
            state.warning_started_at = None
            state.flatten_started = False
            return Decision(
                RiskLevel.HEALTHY,
                buffer_usd,
                False,
                f"recovered (>= 全仓 ${_usd(thresholds.usd_recovery)} / 逐仓 {_pct(thresholds.pct_recovery)})",
                worst.label,
                None,
            )
        if state.warning_started_at is None:
            state.level = RiskLevel.HEALTHY
            state.flatten_started = False
            return Decision(RiskLevel.HEALTHY, buffer_usd, False, "healthy", worst.label, None)
        # 迟滞：回到 recovery 线以上之前保持警戒，避免在阈值附近来回抖。
        state.level = RiskLevel.WARNING
        return Decision(RiskLevel.WARNING, buffer_usd, False, "recovery hysteresis", worst.label, None)

    if state.warning_started_at is None:
        state.warning_started_at = now
    state.level = level
    return Decision(
        level,
        buffer_usd,
        level == RiskLevel.EMERGENCY and not state.flatten_started,
        f"{_explain(worst, threshold)}: {_SUFFIX[level]}",
        worst.label,
        distance_pct,
    )
