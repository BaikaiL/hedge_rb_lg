from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


class RiskLevel(str, Enum):
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Position:
    account: str
    market_id: int
    symbol: str
    signed_size: Decimal
    mark_price: Decimal
    liquidation_price: Decimal
    size_decimals: int
    # 逐仓（isolated，Lighter 的 margin_mode=1）腿自带的保证金与全仓池互相隔离，
    # 风险必须单独算：见 AccountSnapshot.liquidation_buffer_usd。
    isolated: bool = False

    @property
    def liquidation_distance_pct(self) -> Decimal | None:
        """单腿标记价到强平价的百分比距离（仅供显示，不参与风控判定）。"""
        if self.mark_price <= 0 or self.liquidation_price <= 0 or self.signed_size == 0:
            return None
        if self.signed_size > 0:
            distance = (self.mark_price - self.liquidation_price) / self.mark_price
        else:
            distance = (self.liquidation_price - self.mark_price) / self.mark_price
        return max(Decimal("0"), distance * 100)

    @property
    def liquidation_buffer_usd(self) -> Decimal | None:
        """逐仓腿自己还能亏多少美元才被强平：多头 (标记价 − 强平价) × 张数，空头反之。

        逐仓的亏损封顶在这条腿上（保证金是划过去的固定额度），所以这个价差 × 张数就是
        这条腿的「距强平还剩多少钱」，等价于 划拨保证金 + 未实现盈亏 − 维持保证金。
        拿不到标记价/强平价 -> None，调用方按「未知」处理。
        """
        if self.mark_price <= 0 or self.liquidation_price <= 0 or self.signed_size == 0:
            return None
        size = abs(self.signed_size)
        if self.signed_size > 0:
            return (self.mark_price - self.liquidation_price) * size
        return (self.liquidation_price - self.mark_price) * size


@dataclass(frozen=True)
class RiskDomain:
    """一个彼此不救济的清算域：某账户的全仓域，或它的某一条逐仓腿。

    两个域的强平触发条件不同，所以判定口径也不同：
    - 全仓域：账户级强平（权益 − 维持保证金），没有单一价格可映射 -> 用 buffer_usd；
    - 逐仓腿：价格打到强平价就爆，与腿有多大无关 -> 用 distance_pct（距强平百分比）。
    """

    label: str
    buffer_usd: Decimal
    distance_pct: Decimal | None = None

    @property
    def uses_distance(self) -> bool:
        return self.distance_pct is not None


@dataclass(frozen=True)
class AccountSnapshot:
    name: str
    positions: tuple[Position, ...]
    collateral: Decimal
    available_balance: Decimal
    cross_asset_value: Decimal
    cross_maintenance_margin: Decimal
    observed_at_s: float

    @property
    def has_positions(self) -> bool:
        return bool(self.positions)

    @property
    def domains(self) -> tuple[RiskDomain, ...] | None:
        """本账户所有清算域；None = 有仓位但某个含仓位的域算不出数字（必须按「未知」处理）。

        无仓位 -> 空元组（不存在强平风险）。
        """
        if not self.positions:
            return ()
        domains: list[RiskDomain] = []
        if any(not position.isolated for position in self.positions):
            # 账户级字段只代表全仓域；账户里还有全仓腿却拿不到这两个数 -> 未知。
            if self.cross_asset_value <= 0 or self.cross_maintenance_margin <= 0:
                return None
            domains.append(
                RiskDomain(f"{self.name} 全仓", self.cross_asset_value - self.cross_maintenance_margin)
            )
        for position in self.positions:
            if not position.isolated:
                continue
            leg_buffer = position.liquidation_buffer_usd
            leg_distance = position.liquidation_distance_pct
            if leg_buffer is None or leg_distance is None:
                return None
            domains.append(RiskDomain(f"{self.name} {position.symbol} 逐仓腿", leg_buffer, leg_distance))
        return tuple(domains)

    @property
    def liquidation_buffer_usd(self) -> Decimal | None:
        """距账户被强平还剩多少美元 = 各清算域里最危险的那一个（取最小值，绝不求和）。

        全仓域（cross）：权益 − 维持保证金，账户级强平按这个触发。
        逐仓域（isolated）：每条腿只靠自己划过去的保证金，按这条腿的标记价 vs 强平价强平，
        别的腿的权益救不了它，它也救不了别人，所以逐仓腿单独算
        `Position.liquidation_buffer_usd`。

        一个账户里两种域可以同时存在，且互相不救济：任何一边先爆都会强平。所以账户口径 =
        min(全仓缓冲, 各逐仓腿缓冲) —— 相加会把「一边很危险 + 一边很健康」夸大成安全。

        没有仓位时不存在强平风险 -> None（调用方视作无风险）。
        有仓位但某个含仓位的域算不出 -> 同样返回 None，调用方必须按「未知」处理，
        绝不允许用猜出来的数字授权平仓。
        """
        domains = self.domains
        if not domains:
            return None
        return min(domain.buffer_usd for domain in domains)

    @property
    def buffer_is_unknown(self) -> bool:
        return bool(self.positions) and self.liquidation_buffer_usd is None

    @property
    def weakest_leg_distance_pct(self) -> Decimal | None:
        """最脆弱那条腿的百分比距离（仅供显示，让人一眼看出是哪条腿在逼近）。"""
        values = [position.liquidation_distance_pct for position in self.positions]
        known = [value for value in values if value is not None]
        return min(known) if known else None


@dataclass
class GuardState:
    level: RiskLevel = RiskLevel.HEALTHY
    # 粘性标记：一旦进入警戒就保留，直到缓冲回到 recovery 线以上（迟滞）。
    warning_started_at: float | None = None
    last_notification_at: dict[str, float] = field(default_factory=dict)
    flatten_started: bool = False
