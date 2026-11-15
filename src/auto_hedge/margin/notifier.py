from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
import urllib.error
import urllib.parse
import urllib.request

from ..config import NotificationConfig

_ACCOUNT_RE = re.compile(
    r"(?P<name>[A-Za-z0-9_.\-]+):\s*(?:(?P<flat>无仓位)|(?P<missing>数据缺失)|距强平 \$(?P<buf>-?[\d.]+))"
    r"(?:,\s*collateral=(?P<coll>[\d.]+),\s*available=(?P<avail>[\d.]+))?"
    r"(?:,\s*最弱腿=(?P<leg>[\d.]+)%)?"
    r"(?:,\s*逐仓腿=(?P<iso_sym>[A-Za-z0-9_.\-]+) \$(?P<iso_buf>-?[\d.]+))?"
)
_LEVEL_RE = re.compile(r"risk=(?P<level>[a-z]+)")


class Notifier:
    def __init__(self, cfg: NotificationConfig):
        self.cfg = cfg

    async def send(self, title: str, body: str) -> bool:
        logging.warning("%s | %s", title, body)
        tasks = []
        webhook = os.getenv(self.cfg.webhook_url_env, "").strip()
        if webhook:
            tasks.append(asyncio.to_thread(self._post_json, webhook, {"title": title, "text": body}))
        token = os.getenv(self.cfg.telegram_bot_token_env, "").strip()
        chat_id = os.getenv(self.cfg.telegram_chat_id_env, "").strip()
        if token and chat_id:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            tasks.append(asyncio.to_thread(self._post_form, url, {"chat_id": chat_id, "text": f"{title}\n{body}"}))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logging.error("notification failed: %s", type(result).__name__)
            return any(not isinstance(result, Exception) for result in results)
        return False

    @staticmethod
    def _post_json(url: str, payload: dict) -> None:
        data = json.dumps(payload).encode()
        request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    @staticmethod
    def _post_form(url: str, payload: dict) -> None:
        data = urllib.parse.urlencode(payload).encode()
        with urllib.request.urlopen(url, data=data, timeout=10) as response:
            result = json.load(response)
        if result.get("ok") is not True:
            raise RuntimeError("Telegram did not accept the notification")


class LiveStatusPublisher:
    """Keep ONE Telegram message updated with the latest monitor snapshot.

    Every process start sends a FRESH message (silent: ``disable_notification``) and
    edits that same message from then on. The message therefore belongs to the run it
    was started by — its Telegram send-time is the run's start time — while steady-state
    updates cost nothing but an edit, so the chat is never flooded.

    Pushes are best-effort: a Telegram failure is logged and never propagates into the
    monitoring loop, because alerting must not be able to break risk handling.
    """

    def __init__(self, cfg: NotificationConfig, mode_label: str = "DRY-RUN", thresholds_text: str = ""):
        self.cfg = cfg
        self.mode_label = mode_label
        self.thresholds_text = thresholds_text
        self._message_id: int | None = None
        self._last_push: float = 0.0
        self._last_text: str | None = None
        self._lock = asyncio.Lock()
        self.sent_count = 0
        self.edit_count = 0

    async def maybe_update(self, body: str) -> None:
        if not self.cfg.live_status_enabled:
            return
        if not all(self.credentials()):
            return
        now = time.monotonic()
        interval = float(self.cfg.live_status_interval_seconds)
        # Fire slightly early: the monitor loop only ticks every `poll_seconds`, so a strict
        # ">= interval" gate makes the cadence snap to 2x the interval (measured 14.5s for a
        # 10s setting). The tolerance keeps the real refresh rate at the configured value.
        tolerance = min(3.0, interval * 0.25)
        if now - self._last_push < interval - tolerance:
            return
        self._last_push = now
        text = self.render(body)
        if text == self._last_text:
            return
        async with self._lock:
            try:
                await asyncio.to_thread(self.push, text)
            except Exception as exc:  # noqa: BLE001 - must never break the monitor loop
                logging.warning("live status push failed: %s", type(exc).__name__)
                self._message_id = None
                return
        self._last_text = text

    def render(self, body: str) -> str:
        """Format the snapshot so that a refresh is obvious at a glance.

        Telegram keeps showing the *original send time* of an edited message, so the
        update counter and the explicit timestamp have to live in the body.
        """
        stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%m-%d %H:%M:%S")
        refresh = self.sent_count + self.edit_count + 1
        interval = f"{float(self.cfg.live_status_interval_seconds):g}"
        lines = [
            f"🔄 刷新 #{refresh} · 最后更新 {stamp}（北京时间）",
            f"（约每 {interval} 秒刷新一次；上方是 Telegram 的首次发送时间，不会变）",
        ]
        level_match = _LEVEL_RE.search(body)
        level = level_match.group("level") if level_match else "?"
        header = f"🛡️ 自动对冲与保证金监控 ({self.mode_label}) · risk={level}"
        hedge_match = re.search(r"hedging=([^;]+)", body)
        if hedge_match:
            header += f" · 对冲={hedge_match.group(1)}"
        if self.thresholds_text:
            header += f" · 阈值 {self.thresholds_text}"
        rows = list(_ACCOUNT_RE.finditer(body))
        if not rows:
            lines += [header, body]
            return "\n".join(lines)
        lines.append(header)
        for row in rows:
            if row.group("buf") is None:
                # 「无仓位」和「数据缺失」是两件完全不同的事：后者说明风控当前算不出数字，
                # 绝不能渲染成账户是空的。
                label = row.group("flat") or row.group("missing") or "无仓位"
                lines.append(f"• {row.group('name')}: {label}")
                continue
            detail = f"• {row.group('name')}: 距强平 ${float(row.group('buf')):.2f}"
            if row.group("coll"):
                detail += f" | 权益 {float(row.group('coll')):.2f} | 可用 {float(row.group('avail')):.2f}"
            if row.group("leg"):
                detail += f" | 最弱腿 {float(row.group('leg')):.2f}%"
            if row.group("iso_sym"):
                detail += f" | 逐仓腿 {row.group('iso_sym')} ${float(row.group('iso_buf')):.2f}"
            lines.append(detail)
        return "\n".join(lines)

    def push(self, text: str) -> None:
        token, chat_id = self.credentials()
        if not token or not chat_id:
            raise RuntimeError("telegram bot token / chat id not configured")
        if self._message_id is not None:
            reply = self._call(token, "editMessageText", {
                "chat_id": chat_id,
                "message_id": str(self._message_id),
                "text": text,
            })
            if reply.get("ok"):
                self.edit_count += 1
                if self.edit_count == 1:
                    logging.info("live status: first edit applied to message id=%s", self._message_id)
                self._log_progress()
                return
            description = str(reply.get("description") or reply.get("error_code"))
            if "not modified" in description.lower():
                return
            logging.info("live status edit failed (%s); sending a new message", description)
            self._message_id = None
        reply = self._call(token, "sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "disable_notification": "true",
        })
        if not reply.get("ok"):
            raise RuntimeError(f"sendMessage failed: {reply.get('description') or reply.get('error_code')}")
        self._message_id = (reply.get("result") or {}).get("message_id")
        self.sent_count += 1
        logging.info("live status message created: id=%s (silent)", self._message_id)

    def _log_progress(self) -> None:
        if self.edit_count and self.edit_count % 20 == 0:
            logging.info("live status editor: %d edits on message id=%s", self.edit_count, self._message_id)

    def credentials(self) -> tuple[str, str]:
        token = os.getenv(self.cfg.telegram_bot_token_env, "").strip()
        chat_id = os.getenv(self.cfg.telegram_chat_id_env, "").strip()
        return token, chat_id

    @staticmethod
    def _call(token: str, method: str, payload: dict) -> dict:
        url = f"https://api.telegram.org/bot{token}/{method}"
        data = urllib.parse.urlencode(payload).encode()
        request = urllib.request.Request(url, data=data)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            # Telegram reports 400s (e.g. "message is not modified") in the body.
            try:
                return json.load(error)
            except Exception:  # noqa: BLE001
                raise RuntimeError(f"{method} HTTP {error.code}") from error
