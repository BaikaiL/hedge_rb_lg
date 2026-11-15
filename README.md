# Robinhood Lighter 自动对冲

在 **Robinhood Chain 上的 Lighter** 限价单或普通市价开仓单每次实际成交后，在 **Lighter Core** 的相同永续合约提交反向、相同基础币数量的市价 IOC 单。例如 RH 市价买入 0.001 BTC，Core 市价卖出 0.001 BTC。默认自动发现全部来源永续合约：下什么合约就跟随什么，成交多少就按实际数量对冲，无需逐个填币种或预设数量。

本项目同时提供与你现有 `lg_rh_margin_guard` 源码配套的风控替换文件。当前只在本机完成离线测试和公开接口验证，**没有连接你的账户、发送真实订单或部署到 VPS**。

## 工作方式

- 来源：`account_all_trades` WebSocket；同时订阅 `account_all_orders` 确认来源订单类型。`type=limit` 限价成交（包括主动吃单）和 `type=market, reduce_only=false` 普通市价成交触发对冲。`reduce_only=true` 的市价平仓、止盈止损和 TWAP 不触发；来源永续的强平/减仓/结算事件触发暂停。市价单的 `reduce_only` 标记缺失或格式异常时暂停，不猜测是否开仓。
- 数量：按基础币数量计算，不按保证金或杠杆倍数计算。用 Decimal 和合约元数据转换整数数量，两边分别查询市场编号。只支持永续合约，要求两边合约 multiplier 相同。
- 记账：SQLite 保存来源 trade ID、订单类型、每次下单的 client order ID 和实际成交量。重复推送、重启与 REST 补历史走同一去重逻辑。
- 确认：`sendTx` 接受不等于成交。通过目标订单 WS/REST 的终态及 `filled_base_amount` 核实。取消的 IOC 也可能部分成交，只补真实剩余数量。
- 限制：市价 IOC 带滑点边界，重试保持第一次目标盘口价格的边界。超时、结果不明、零成交或重试耗尽都会暂停。结果不明时不会盲目重发。
- 精度/最小额：不足目标最小数量、最小名义金额或数量精度的碎量先积累，超过 `max_dust_seconds` 仍无法对冲就暂停。因此无法保证每一笔极小成交立即做到完全等量。
- 对账：启动、断线重连及每 30 秒分页回补来源成交；回补完成前暂停发送。来源/目标 WS 短暂断线会自动重连；超过 30 秒不可用会锁定暂停。历史缺口超过六小时或超过分页上限时需要人工核对。

正常推送会逐笔成交触发对冲；断线回补或不足最小额的碎量可能合并为净额对冲，买卖相互抵消时只处理剩余数量。Core 必须有同名、相同 multiplier 的活跃永续合约，否则该来源限价或普通市价成交触发暂停，不能换成其他资产。新合约的首次成交会重新查询市场列表。程序只处理本次启用时间以后的成交，**不接管启动前已有仓位，也不自动修复其他程序/人工交易造成的持仓偏差**。两边账户应专用于这组策略；保证金脚本仍负责紧急减仓。

市价单按交易所的 `reduce_only` 标记筛选，不从当前仓位推测开平；未标记 reduce-only 的市价减仓/反手也会按实际成交量跟随。限价成交保留原有行为，包括 reduce-only 限价成交。风控平仓前仍先暂停对冲，并通过共享执行锁等待在途订单完成。

## 从 0.1.0 更新到 0.1.1

无需新增配置，已有 SQLite 账本可以继续使用。旧版本已记为忽略的市价成交不会因升级而补单；先核对两边当前实际仓位，并处理旧会话剩余敞口。维护期间暂停来源交易，使用现有服务用户执行项目命令，保留自己的 `.env`、`config.toml` 和状态目录：

```bash
cd /opt/auto-hedge
.venv/bin/auto-hedge pause --reason 'upgrade to market entry hedging'
sudo systemctl stop auto-hedge
git pull --ff-only
.venv/bin/pip install --no-deps -e .
.venv/bin/auto-hedge status
# 核对实际仓位、来源挂单及暂停期间成交后，开启新的会话。
.venv/bin/auto-hedge resume --reason 'positions reconciled after upgrade' --acknowledge-exposure
sudo systemctl start auto-hedge
```

更新后对冲仍需有效的 LIVE 风控心跳。此次不需要再次替换风控文件；已有暂停与共享锁协作继续使用。

## 与保证金脚本协作

两个进程必须在同一 VPS、同一文件系统使用相同绝对状态目录。建议都用同一个 Linux 服务账户运行，避免权限问题。

1. 风控每次成功读取两边状态并完成风险评估后才写心跳。它必须处于真实执行模式，且配置 `execution.enabled=true`、启动带 `--live`。DRY-RUN 风控不能为对冲程序提供有效保护心跳。
2. 对冲程序每次下单前检查心跳和暂停状态。风控读取失败、状态未知、进程停止或心跳超时会锁定暂停。
3. 风控开始平仓时，**先持久化暂停，再取得共享执行锁**；等待已在发送/确认中的对冲单完成后，重新读取仓位，执行原来的撤单与双边 reduce-only 全平。
4. 原有风控阈值、通知与平仓重试逻辑保持在替换文件中。风控全平后不自动恢复对冲，需要人工检查两边实际仓位并重新启用。

共享锁只协调参与此协议的两个进程，不会取消已经到交易所的请求。进程崩溃、交易所超时或迟到的交易仍需依据实际订单和仓位核对；不能把它看作跨交易所原子成交保证。每笔对冲的网络发送加确认预算默认为 8 秒，这段时间可能推迟风控获得执行锁。请结合自己的强平缓冲和 VPS 延迟调整。

风控替换文件基于配套的 `lg_rh_margin_guard` 源码生成。交付的 `integration/margin_guard/guard.py` 替换 VPS 上包内的 `src/hedge_guard/guard.py`。原始源码没有修改。风控主循环会在异常时标记心跳无效，退出时也会标记无效；仅启动一个假心跳进程不能替代风控。

## 东京 VPS 安装

要求 Linux、Python 3.11+、git；官方 signer 支持 Linux x86_64/arm64。下面以代码在 `/opt/auto-hedge`、两个服务均使用 `hedge` 用户为例。若现有服务使用其他用户，请统一替换用户及所属组。

```bash
# 管理员先建立服务账户和目录；已有账户则跳过 useradd。
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

编辑 `.env` 的两边 account index、**新建的 API key index** 和对应 API 私钥。`config.toml` 默认 `[markets] auto=true`，不需要设置币种或数量；可按实际网络情况修改滑点边界与确认超时。不要把私钥发到聊天、写进配置、放入版本库；这里使用 Lighter API 私钥，不是钱包助记词或 L1 钱包私钥。

**给对冲程序使用独立 API key 槽位，不要复用现有风控脚本使用的 key。** 两个 Python 进程若复用相同账户的同一 key 会争用 nonce。示例使用 Core key 4、RH key 5；需在各自部署创建并登记，不能只修改数字。程序要求新 key index 在 4–254 范围内，实际可用槽位以各部署的 API key 页面为准。如果两个部署的 account index 相同，key index 也必须不同，避免官方原生 signer 的客户端槽位冲突。

独立 key 不会把同 IP/账户的 API 配额隔开。现有保证金脚本也会轮询，成交频繁时需检查两个部署对自己账户类型的限流；本脚本收到限流/查询异常会暂停，不会持续重发订单。

默认不设置固定下单量、单笔基础币数量上限或累计未对冲基础币数量上限，目标数量由来源实际成交量决定。可选的显式 `[[routes]]` 模式供未来需要币种白名单或自设限制时使用，当前示例没有启用。

## 安装风控协作文件

先暂停新的来源限价单和市价交易，在维护窗口操作。将下面路径替换成 VPS 的真实路径：

```bash
# 安装到现有风控服务使用的 Python 虚拟环境；仅安装本项目，不改变其 SDK 版本。
/PATH/TO/MARGIN_GUARD/.venv/bin/pip install --no-deps -e /opt/auto-hedge

# 备份现有文件，再复制已修改版本；保留自己的 config.toml 和原有 .env。
cp /PATH/TO/MARGIN_GUARD/src/hedge_guard/guard.py /PATH/TO/MARGIN_GUARD/src/hedge_guard/guard.py.before-auto-hedge
cp /opt/auto-hedge/integration/margin_guard/guard.py /PATH/TO/MARGIN_GUARD/src/hedge_guard/guard.py
```

给原风控服务添加环境变量 `AUTO_HEDGE_STATE_DIR=/var/lib/auto-hedge`，并允许该用户读写这个目录。使用 `systemctl edit 原风控服务名`，可参考 `deploy/margin-guard-override.conf`。如果使用不可编辑的 wheel 安装，请在源码目录替换后用原环境重新 `pip install --no-deps -e 源码目录`，确保运行的是替换后的文件。

重新启动原风控服务，确认它处于 LIVE、能读取两边仓位并能平仓。风控重启会把对冲状态置为暂停，随后更新心跳。不要改动它原有的风险阈值或已有凭据。此安装方式的协作模块只依赖 Python 标准库。

## 首次验证与启动

下面命令的工作目录为 `/opt/auto-hedge`，建议用 `sudo -u hedge` 执行。全局参数（`--config`、`--env-file`、`--live`）放在子命令前。

```bash
.venv/bin/python -m pytest -q

# 默认 enabled=false，check 只读取市场并校验 API key，不发送订单。
.venv/bin/auto-hedge check
.venv/bin/auto-hedge status
```

推荐先在对应测试网验证真实订单行为：配置同时改为 RH 测试网 `https://api.rh-testnet.lighter.xyz`（chain 300）和 Core 测试网 `https://testnet.zklighter.elliot.ai`（chain 300），换成该环境的账户和 API key。现有风控配置校验只接受两个主网，不能直接拿它给测试网提供保护；测试网需有实际监控对应测试账户的风控集成。不要伪造主网保护心跳。

主网 DRY-RUN 可以使用当前真实风控心跳：

```bash
# 检查两边现有实际仓位后，记录启用时间。
.venv/bin/auto-hedge resume --reason 'dry-run positions reviewed' --acknowledge-exposure
.venv/bin/auto-hedge run
# RH 限价/普通市价实际成交会出现在日志中，Core 订单仅模拟，不发送。
```

**DRY-RUN 并没有对冲 RH 的真实成交。** 若主网演练，来源实际风险仍由你和现有风控承担；可先只运行 `check`，随后以自己确定的最小可交易数量验证 LIVE。

切换 LIVE 时，先停止对冲程序，`pause`，人工核对/处理演练期间的实际仓位和来源挂单。模拟与真实成交分别记在 `ledger.paper.sqlite3` 与 `ledger.live.sqlite3`，不会混用。`status`、`resume`、`settle-attempt` 根据配置中的 `execution.enabled` 选择要检查的账本；`run` 还必须加 `--live` 才能真实下单。两种模式不能同时使用同一个状态目录运行。

将 `execution.enabled` 改为 `true` 后：

```bash
# --live check 仍然只读，同时建立 LIVE 模式账本。
.venv/bin/auto-hedge --live check
.venv/bin/auto-hedge resume --reason 'live positions and source orders reviewed' --acknowledge-exposure
.venv/bin/auto-hedge --live run
```

先分别验证 RH 一笔已确认可成交的小额限价单和普通市价开仓单，对照两边订单和基础币数量；检查部分成交、取消挂单，以及风控暂停时不再下对冲单。然后设置 systemd：

```bash
sudo cp deploy/auto-hedge.service /etc/systemd/system/auto-hedge.service
sudo systemctl daemon-reload
sudo systemctl enable --now auto-hedge
sudo journalctl -u auto-hedge -f
```

systemd 自动重启不解除暂停。如果发生故障，先读状态、核对实际账户，再人工恢复。东京 VPS 上实测「收到来源成交 → 提交目标单 → 确认实际成交」延迟，网络位置不能保证固定延迟；本机测试不能代替这个检查。

## 暂停、对账、恢复

```bash
.venv/bin/auto-hedge pause --reason 'manual maintenance'
.venv/bin/auto-hedge status
```

有 `uncertain` 订单时，先停止对冲服务再查询同一个 client order ID：

```bash
sudo systemctl stop auto-hedge
.venv/bin/auto-hedge --live reconcile
```

如果交易所保留历史不足或订单一直查不到，必须从交易所实际订单/成交记录与仓位确定是否成交。查不到不能自动解释为零成交。只有已取得证据时，才手动登记**带方向的实际基础币成交数量**（买为正、卖为负）：

```bash
.venv/bin/auto-hedge settle-attempt --id CLIENT_ORDER_ID --filled=-0.001 --reason 'verified order/fill evidence ...'
```

恢复会开启新的会话 epoch，旧会话的未对冲量和暂停期间的成交不会被自动补单。**先检查两边实际仓位、仍存在的来源挂单、暂停期间的新成交，以及旧会话剩余敞口。** 需要时人工处理完再恢复；`--acknowledge-exposure` 明确确认这一点。

```bash
.venv/bin/auto-hedge resume --reason 'both accounts reconciled after incident' --acknowledge-exposure
sudo systemctl start auto-hedge
```

如果目标订单被拒、来源成交超限或任何错误导致暂停，来源的已有挂单仍可能继续成交。**本脚本暂停本身不撤来源挂单，也不代替保证金脚本的紧急全平。** 故障时应及时处理来源挂单；原风控触发紧急全平时仍会按原逻辑撤两边挂单。建议为 systemd 日志的 `CRITICAL`/服务异常接入 VPS 已有的运维告警。

## 文件与验证

- `src/auto_hedge/`：成交监听、记账、对冲、跨进程协作和 CLI。
- `integration/margin_guard/guard.py`：基于你提供的源码改好的风控文件。
- `deploy/`：systemd 服务和已有风控服务的环境设置示例。
- `tests/`：离线回归，使用用户提供的风控辅助模块作为测试夹具，不含真实配置或私钥。

测试覆盖限价与普通市价单的重复推送、两种成交方向、部分成交、超时后按 ID 查单、暂停竞争、旧会话隔离、reduce-only 市价平仓过滤、数量精度、最小额累积、分页回补和风控协作。公开 REST 响应验证了两边 BTC 的市场元数据和目标盘口解析。没有验证真实账户权限、真实 IOC 成交、交易所历史完整性或东京 VPS 的运行环境。

实现依据：用户提供的 [Lighter API 文档](https://docs.lighter.xyz/api)，可读取的官方 [Python SDK](https://github.com/elliottech/lighter-python/tree/38a34672e7cb43c67d807ece25f125fda2dfdecf)、[部署配置](https://github.com/elliottech/lighter-python/blob/38a34672e7cb43c67d807ece25f125fda2dfdecf/lighter/endpoint_profiles.py) 和 [下单接口](https://github.com/elliottech/lighter-python/blob/38a34672e7cb43c67d807ece25f125fda2dfdecf/lighter/signer_client.py)。文档站本次访问返回 403，因此以该固定版本的官方 SDK 和实际公开响应核对接口。
