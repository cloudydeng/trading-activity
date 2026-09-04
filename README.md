# Binance Spot Trading Activity Bot

基于公开 WebSocket 行情信号执行小额 Binance 现货交易。系统默认 `OBSERVE`，真实交易必须同时启用服务器 `LIVE` 配置、从控制台临时解锁并手动启动。项目不做自成交。

## 安全边界

- BUY 部分成交会立即进入 SELL 管理，成交以账户事件流和 REST 对账为准。
- 买入只使用买一价 `LIMIT_MAKER`；每笔最多 `12 USDT`，20 秒后若订单仍是买一则继续挂，只有不再是买一时才撤单；不转 IOC、不因短线信号提前撤单。
- BUY 一旦真实成交（包括达到最小可卖额的部分成交），立即撤销剩余买单并按实际加权买入均价（按 tick 向上取整）挂 `LIMIT GTC SELL`；例如买入均价 `0.862` 就先挂卖价 `0.862`。
- 每张卖单有效管理窗口为 2 分钟；未完全成交时先撤单并完成成交与库存对账，再将剩余数量按最新卖一价挂新的 `LIMIT GTC SELL`，之后每 2 分钟重复，直至空仓。
- 自动交易流程没有价格止损、止损冷却或 MARKET 卖出；MARKET 只保留给人工授权清仓。库存对账与防重复卖出始终是强制保护。
- 每个 API 账户拥有独立运行时、交易对、订单、持仓、风控和统计；交易对切换前必须停止该账户、锁定 LIVE，并确认无活动订单和当前标的持仓。
- 所有账户共享服务器公网 IP 的 Binance 请求权重；系统动态读取每分钟上限，在 80% 处暂停新开仓并保留退出、撤单和对账余量。
- API Key、Secret、管理密码仅从服务器环境变量加载，不通过浏览器提交或返回。
- 每日成交量、手续费、净盈亏和成本按 `稳定账户 ID + UTC 日期 + 交易对` 写入 SQLite。

## 环境变量

账户配置是以稳定账户 ID 为键的 JSON，不限制为三组；同一配置可承载 1～10 或更多账户：

```bash
BOT_ACCOUNT_PROFILES_JSON='{
  "account-a":{"alias":"bot-a","apiKey":"...","secretKey":"...","enabled":true},
  "account-b":{"alias":"bot-b","apiKey":"...","secretKey":"...","enabled":true,
                "orderAmountsUsdt":{"ENSOUSDT":6,"BTCUSDT":12},
                "symbolStrategies":{"ENSOUSDT":{"mode":"CURRENT"},
                                     "BTCUSDT":{"mode":"BID_ASK_MAKER","orderAmountUsdt":6,
                                                 "entryTimeoutMs":180000,"exitTimeoutMs":180000}}}
}'
```

`orderAmountsUsdt` 可为每个账户按交易对设置单笔 USDT 名义金额；未配置的交易对回退到全局
`binance.strategy.order-amount-usdt`。单笔金额仍不能超过 `max-live-order-notional-usdt`，并会在控制台显示当前生效值。

`symbolStrategies` 可为每个账户的每个交易对选择策略：`CURRENT` 保留现有按成本/手续费保护的卖出逻辑；
`BID_ASK_MAKER` 在买一挂买单、成交后按卖一挂限价卖单。买单或卖单达到各自超时时，先对账撤单，
再按最新盘口价继续挂单；买单若超时但仍是当前买一，则不撤单继续等待。未配置的交易对回退到 `CURRENT`。

单账户旧配置仍作为兼容回退，仅在未配置 `BOT_ACCOUNT_PROFILES_JSON` 时生效：

```bash
BINANCE_API_KEY_ALIAS=bot-a
BINANCE_API_API_KEY=...
BINANCE_API_SECRET_KEY=...
```

每个启用账户的 API Key 和 Secret 必须同时配置。网页只显示账户 ID 和别名，绝不返回密钥。各账户切换后的交易对按账户 ID 写入 SQLite，服务重启后独立恢复。

## 构建与启动

```bash
mvn clean package
java -jar target/binance-spot-competition-bot-3.0.0.jar
```

控制接口均受浏览器登录会话或 `X-Bot-Admin-Token` 保护：

- `GET /api/accounts`
- `GET /api/accounts/stats/summary?days=10`（按账户 + 交易对分别汇总近 N 天成交）
- `GET /api/accounts/{accountId}/status`
- `POST /api/accounts/{accountId}/live/arm`
- `POST /api/accounts/{accountId}/start`
- `POST /api/accounts/{accountId}/stop`
- `POST /api/accounts/{accountId}/symbol`
- `POST /api/accounts/arm-all`
- `POST /api/accounts/start-all`
- `POST /api/accounts/stop-all`
- `POST /api/accounts/reload`（从服务器受保护环境文件热加载新增账户；不会替换或停止已有账户）

控制台的“API 账户10天汇总”页面展示上述汇总，只保留窗口内有真实成交的账户/交易对组合。

账户热加载只读取 `BOT_ACCOUNT_PROFILES_ENV_FILE` 指向的服务器环境文件（默认 `/etc/trading-activity.env`）。
它仅为清单中尚未运行的启用账户创建 User Data Stream；新账户始终以 `running=false`、`liveArmed=false` 加入，
不会触碰已有账户的持仓、活动 SELL 订单或 LIVE 状态。修改环境文件后可从控制台点击“热加载账户”，无需重启 JVM。
