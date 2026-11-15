# Robinhood Lighter 自动对冲与保证金监控

一个 Python 程序、一个配置文件、一个 systemd 服务，同时完成 RH → Lighter Core 自动对冲和双账户保证金保护。不再运行独立的保证金脚本，也不再需要替换原脚本文件。

RH 普通限价单或非 reduce-only 市价单成交后，在 Core 的相同永续合约提交反向、相同实际基础币数量的市价 IOC 单。自动识别品种，不需要手工填写币种或固定数量。

本版本已完成离线回归，未使用你的账户发送真实订单，未部署到 VPS。

## 合并保留的保证金功能

- 每 10 秒读取 RH、Core 两个账户，监控全部永续仓位；不局限于对冲程序跟随的品种。
- 全仓按权益减去维持保证金的剩余美元缓冲判断，逐仓按距离强平价的百分比判断，取最危险的清算域，两边资金不相加。
- 预警、再次提醒、恢复迟滞、通知冷却、数据缺失/过期判断，与所提供的原保证金脚本保持一致。
- 触发紧急条件后：先暂停对冲，等待在途对冲订单退出执行锁，再撤两边挂单、重新读取仓位、提交 reduce-only 市价平仓、复查残仓并重试。
- Telegram/Webhook 告警，以及在 Telegram 中持续编辑同一条状态消息；时间显示为北京时间，同时显示保证金风险和对冲运行/暂停状态。
- 控制台/systemd 日志及状态目录中的自动轮转日志文件 auto-hedge.log。
- 告警网络请求独立运行，不阻塞撤单或平仓。对冲模块故障会暂停对冲并重试连接，保证金监控继续运行。

以下默认值来自所提供的原监控配置；若你后来在 VPS 调过阈值，请将当前值带入新配置。

| 风险口径 | 提醒 | 再次提醒 | 全平 | 解除警戒 |
| --- | --- | --- | --- | --- |
| 全仓剩余缓冲 | ≤ $150 | ≤ $100 | ≤ $75 | ≥ $175 |
| 逐仓距强平 | ≤ 6% | ≤ 4.5% | ≤ 3% | ≥ 7% |

紧急平仓默认滑点边界 2%、最多 3 轮、每轮 3 秒后复查。对冲单的滑点边界单独配置，默认 30 bps（0.30%）。

## 配置

Python 3.11+，Linux x86_64/arm64，git。项目位于 `/opt/auto-hedge`，状态位于 `/var/lib/auto-hedge`。下面以服务用户 `hedge` 为例，替换为你实际使用的用户。

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin hedge
sudo install -d -o hedge -g hedge /opt/auto-hedge /var/lib/auto-hedge
sudo -u hedge git clone https://github.com/BaikaiL/hedge_rb_lg.git /opt/auto-hedge
cd /opt/auto-hedge
sudo -u hedge python3 -m venv .venv
sudo -u hedge .venv/bin/pip install -e '.[test]'
sudo -u hedge cp config.example.toml config.toml
sudo -u hedge cp .env.example .env
sudo chmod 600 .env
```

已有用户和目录时跳过对应创建操作。下述项目命令使用服务用户执行。

填写 `.env` 的两个账户 index、API key index 和对应 API 私钥。两边 API key 均须可撤单和进行市价交易；这里使用 Lighter API 私钥，不是钱包私钥或助记词。不要向聊天发送密钥。

每个部署只用一套 key：对冲、撤单和平仓共用同一个客户端、签名器和 nonce 管理。不要同时运行旧程序或其他使用同一 key 的交易进程。如果两边 account index 相同，API key index 必须不同，避免原生 signer 槽位冲突。示例使用 Core 4、RH 5，需要在对应部署实际登记。

可选通知沿用原变量：`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`、`HEDGE_GUARD_WEBHOOK_URL`，把原值放进唯一的 `.env` 即可。未配置时只记录日志。

`config.toml` 包含：

- `[source]` / `[target]`：RH 和 Core 的部署、账户环境变量。
- `[execution]`：对冲滑点、超时、重试；`enabled=true` 与启动参数 `--live` 共同控制全部真实交易。
- `[markets] auto=true`：自动识别来源全部永续合约。
- `[monitor]`：原保证金监控阈值、轮询、数据有效期和告警冷却。
- `[margin_execution]`：紧急撤单、平仓滑点、轮数、复查间隔和单次请求超时。
- `[notifications]`：原通知变量名称和 Telegram 状态刷新间隔。

未添加新段落的旧配置会采用上述原脚本默认值；建议显式补齐段落并与 VPS 原设置核对。DRY-RUN 与 LIVE 分别保存在 paper/live 账本，已有订单与成交账本可继续使用。

本机使用 Clash 时，可在 config.toml 中增加 [network] 段，设置 proxy_url = "http://127.0.0.1:7897"，端口以本机实际监听端口为准。代理覆盖 API、SDK、WebSocket 和通知请求。是否配置代理取决于运行主机的网络；本机的 Clash 地址不会自动迁移到 VPS。

## 只运行一个服务

首次切换前暂停新的来源交易，处理仍可能成交的挂单，并核对两边实际仓位。**停用原独立保证金服务，不要与合并版并行运行。**

从 0.1.x 更新时：

```bash
cd /opt/auto-hedge
sudo systemctl stop auto-hedge
# 将下面占位名称替换成原保证金服务的真实名称。
sudo systemctl disable --now YOUR_OLD_MARGIN_GUARD.service
git pull --ff-only
.venv/bin/pip install -e .
# 保留自己的 .env、config.toml 和原状态目录；补齐监控/平仓/通知段落。
```

独立运行原脚本时应停止它的 Python 进程；若原来由其他进程管理器拉起，也要关闭其自动重启。原服务的修改只在 VPS 上进行，本项目不会自动停止远程进程。

只读检查（不发订单、不平仓）：

```bash
.venv/bin/auto-hedge --live check
```

上面的 `--live` 要求配置已经 `execution.enabled=true`，检查操作本身仍只读。默认 `enabled=false` 时去掉 `--live` 即可。检查输出包含两边风险状态；检查账户、阈值和通知配置后再启动。

```bash
sudo cp deploy/auto-hedge.service /etc/systemd/system/auto-hedge.service
sudo systemctl daemon-reload
sudo systemctl enable --now auto-hedge
sudo journalctl -u auto-hedge -f
```

同一进程立即开始监控保证金，对冲保持暂停。**启动后**在另一个终端检查内部监控数据正常，并确认现有仓位和挂单已经核对，再启用对冲：

```bash
cd /opt/auto-hedge
.venv/bin/auto-hedge status
.venv/bin/auto-hedge resume --reason 'both accounts reviewed' --acknowledge-exposure
```

不使用 systemd 时，唯一的常驻命令是 `.venv/bin/auto-hedge --live run`。`status`、`pause`、`resume` 是同一程序的短时管理命令，不需要第二个监控守护进程。

默认不带 `--live` 的 `run` 是 DRY-RUN：同时读取真实市场和保证金数据，模拟对冲/全平，不会发送任何交易。主网 RH 成交本身仍是真实风险，DRY-RUN 不会替你建立实际 Core 对冲。

## 暂停、故障与恢复

`pause` 只暂停对冲，内置保证金监控和紧急全平继续运行：

```bash
.venv/bin/auto-hedge pause --reason 'manual maintenance'
.venv/bin/auto-hedge status
```

监控数据缺失、过期或读取失败会暂停对冲；数据恢复后继续监控，但不自动解除暂停。紧急全平完成、程序重启后也需要人工恢复。

暂停本身不撤来源挂单；只有触发保证金紧急全平时才按配置撤两边挂单。对冲异常时需及时处理仍可能成交的来源订单。恢复前核对两边实际仓位、暂停期间成交和旧会话剩余敞口。恢复开启新会话，旧会话及暂停期间成交不自动补单。

如果有 `uncertain` 订单，对冲模块会按同一 client order ID 尝试查询，查询确认前不会重发。需要手动查单时，先处理维护期间风险，再停止合并服务运行 `.venv/bin/auto-hedge --live reconcile`；这段时间内置保证金监控也会停止。查不到订单不等于零成交。

仅取得真实订单/成交证据后，可用 `settle-attempt --id CLIENT_ORDER_ID --filled=-0.001 --reason 'verified evidence ...'` 登记带方向的实际基础币成交量（买正、卖负），再重启服务、等待有效内部监控数据、核对仓位后 `resume`。

## 成交与执行限制

- 来源限价单和 `type=market, reduce_only=false` 的普通市价单触发；reduce-only 市价平仓、止盈止损、TWAP 不触发。未标记 reduce-only 的市价减仓/反手也按实际成交跟随；限价成交保留原有行为。
- Core 必须有同名、相同 multiplier 的活跃永续合约。没有对应合约就暂停，不换成其他资产。
- 数量使用 Decimal；每笔实际成交、重复推送、断线历史回补和重启共用持久化去重。目标 IOC 根据实际终态成交量补剩余数量，超时不盲目重发。
- 不足最小数量、最小名义金额或目标精度的碎量先积累，超过默认 30 秒仍不能成交就暂停；断线回补和碎量可能按净额合并。
- 两个交易所的操作不具备原子性。风险模块取得执行锁前，在途对冲发送与确认可能占用默认最多 8 秒；已送达交易所的请求不能靠本地暂停撤回。请以两边实际订单和仓位为准。
- 不接管启动前仓位，不自动修复其他程序或人工操作导致的仓位差异。保证金监控始终覆盖读取到的全部永续仓位，紧急全平会处理两边全部挂单和仓位。
- 对冲和内置监控共用 API 配额；通知、网络、交易所拥堵、流动性和速率限制仍可能延迟操作。脚本不能保证先于交易所强平完成。

## 验证

`.venv/bin/python -m pytest -q` 覆盖限价/市价对冲、部分成交、去重、超时、全仓/逐仓风险、恢复迟滞、撤单/平仓/残仓重试、共享签名器、暂停竞争、DRY-RUN、Telegram 单条消息刷新，以及对冲故障后监控继续运行。

官方 SDK 固定版本：[lighter-python](https://github.com/elliottech/lighter-python/tree/38a34672e7cb43c67d807ece25f125fda2dfdecf)。API 入口：[Lighter API 文档](https://docs.lighter.xyz/api)。请在东京 VPS 完成小额真实成交与平仓验证后再用于日常交易。
