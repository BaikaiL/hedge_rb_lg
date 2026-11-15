from __future__ import annotations

import asyncio
import json
import time
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN

from .config import AccountConfig, ExecutionConfig
from .models import AccountSnapshot, Position


def _decimal(value: object) -> Decimal:
    return Decimal(str(value or "0"))


# 原生 signer（Go）硬校验 0 <= client_order_index <= 2^48-1，并且要求同组内唯一。
# 微秒时间戳（≈1.79e15）超上限 6.37 倍，会让每一张平仓单在签名阶段就被拒 —— 2026-10-10
# 真单演练实测：error='ClientOrderIndex should not be larger than 281474976710655'。
# 毫秒时间戳 × 100 + 序号：上限内（1.79e14），同一轮里多张单也不会撞号。
MAX_CLIENT_ORDER_INDEX = (1 << 48) - 1


def client_order_index(seq: int = 0) -> int:
    return (int(time.time() * 1000) * 100 + (seq % 100)) & MAX_CLIENT_ORDER_INDEX


# CancelAllTime 必须是 NIL（0）：IMMEDIATE 语义下传毫秒时间戳会被原生 signer 拒 ——
# 'CancelAllTime should be nil'（2026-10-10 实测，两个 venue 都一样）。原始代码传的是
# int(time.time() * 1000)，所以撤单从来没有成功过；只有 CANCEL_ALL_TIF_SCHEDULED 才用时间戳。
NIL_CANCEL_ALL_TIME = 0


class LighterAccount:
    def __init__(self, cfg: AccountConfig, execution: ExecutionConfig, live: bool):
        self.cfg = cfg
        self.execution = execution
        self.live = live
        self._lighter = None
        self._api_client = None
        self._signer = None

    async def start(self) -> None:
        if self.live:
            try:
                import lighter
            except ImportError as exc:
                raise RuntimeError("lighter SDK is not installed; run: pip install -e .") from exc
            self._lighter = lighter
            self._signer = lighter.SignerClient(
                url=self.cfg.base_url,
                account_index=self.cfg.account_index,
                api_private_keys={self.cfg.api_key_index: self.cfg.private_key()},
                chain_id=self.cfg.chain_id,
            )
            error = self._signer.check_client()
            if error:
                raise RuntimeError(f"{self.cfg.name}: API key validation failed: {error}")

    async def close(self) -> None:
        if self._signer is not None:
            await self._signer.close()
        if self._api_client is not None:
            await self._api_client.close()

    async def snapshot(self) -> AccountSnapshot:
        account_payload, books_payload = await asyncio.gather(
            asyncio.to_thread(
                self._get_json,
                "/api/v1/account",
                {"by": "index", "value": str(self.cfg.account_index), "active_only": "true"},
            ),
            asyncio.to_thread(self._get_json, "/api/v1/orderBookDetails", None),
        )
        observed_at = time.time()
        raw_accounts = account_payload["accounts"]
        if len(raw_accounts) != 1:
            raise RuntimeError(f"{self.cfg.name}: expected one account, got {len(raw_accounts)}")
        account = raw_accounts[0]
        books = {int(item["market_id"]): item for item in books_payload["order_book_details"]}
        positions = []
        for item in account.get("positions", []):
            size = _decimal(item.get("position"))
            sign = int(item.get("sign", 0))
            signed_size = abs(size) * (Decimal(1) if sign > 0 else Decimal(-1))
            if signed_size == 0:
                continue
            market_id = int(item["market_id"])
            book = books.get(market_id)
            if book is None:
                raise RuntimeError(f"{self.cfg.name}: missing market metadata for {market_id}")
            positions.append(Position(
                account=self.cfg.name,
                market_id=market_id,
                symbol=str(item.get("symbol") or book.get("symbol") or market_id),
                signed_size=signed_size,
                mark_price=_decimal(book.get("mark_price")),
                liquidation_price=_decimal(item.get("liquidation_price")),
                size_decimals=int(book["size_decimals"]),
                # margin_mode: 0 = 全仓(CROSS), 1 = 逐仓(ISOLATED)，与 SDK 常量一致。
                # 逐仓腿的保证金被划进单子，账户级 cross_* 字段会变成 0/零头，不能当成「没数据」。
                isolated=int(item.get("margin_mode", 0) or 0) == 1,
            ))
        return AccountSnapshot(
            name=self.cfg.name,
            positions=tuple(positions),
            collateral=_decimal(account.get("collateral")),
            available_balance=_decimal(account.get("available_balance")),
            cross_asset_value=_decimal(account.get("cross_asset_value")),
            cross_maintenance_margin=_decimal(account.get("cross_maintenance_margin_requirement")),
            observed_at_s=observed_at,
        )

    def _get_json(self, path: str, query: dict[str, str] | None) -> dict:
        url = self.cfg.base_url.rstrip("/") + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "hedge-guard/0.1"})
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.load(response)
        if int(payload.get("code", 0)) != 200:
            raise RuntimeError(f"{self.cfg.name}: API error: {payload.get('message', payload.get('code'))}")
        return payload

    async def cancel_all(self) -> None:
        if not self.live:
            return
        tx_type, tx_info, _, error = self._signer.sign_cancel_all_orders(
            self._signer.CANCEL_ALL_TIF_IMMEDIATE,
            NIL_CANCEL_ALL_TIME,
            api_key_index=self.cfg.api_key_index,
        )
        if error:
            raise RuntimeError(f"{self.cfg.name}: cancel-all signing failed: {error}")
        response = await self._signer.send_tx(tx_type=tx_type, tx_info=tx_info)
        if response is None or int(response.code) != 200:
            raise RuntimeError(
                f"{self.cfg.name}: cancel-all rejected: code={getattr(response, 'code', None)} "
                f"message={getattr(response, 'message', None)}"
            )

    async def flatten_once(self, snapshot: AccountSnapshot) -> list[str]:
        actions = []
        for offset, position in enumerate(snapshot.positions):
            base_amount = int(
                (abs(position.signed_size) * (Decimal(10) ** position.size_decimals)).to_integral_value(rounding=ROUND_DOWN)
            )
            if base_amount <= 0:
                continue
            action = f"{self.cfg.name}: close {position.symbol} size={position.signed_size} reduce-only"
            actions.append(action)
            if not self.live:
                continue
            _, response, error = await self._signer.create_market_order_limited_slippage(
                market_index=position.market_id,
                client_order_index=client_order_index(offset),
                base_amount=base_amount,
                max_slippage=self.execution.max_slippage_pct / 100,
                is_ask=position.signed_size > 0,
                reduce_only=True,
                api_key_index=self.cfg.api_key_index,
            )
            if error:
                raise RuntimeError(f"{action} failed: {error}")
            # 签名成功 ≠ 被接受：venue 侧拒单只体现在 resp.code/message 上，不检查就会
            # 白等一个 verify_delay 才发现仓位还在（2026-10-10 起显式检查）。
            if response is None or int(response.code) != 200:
                code = getattr(response, "code", None)
                message = getattr(response, "message", None)
                raise RuntimeError(f"{action} rejected by venue: code={code} message={message}")
        return actions
