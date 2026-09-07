# Binance Spot Trading Activity Bot

基于公开 WebSocket 行情信号执行小额 Binance 现货交易。系统只有 `LIVE` 运行模式；服务器端 `BINANCE_LIVE_TRADING_ENABLED=true` 后，从控制台手动启动才会真实下单。项目不做自成交。

产品、运营和测试请先阅读 [两种 Maker 策略产品说明](docs/strategy-behavior.md)。

## 安全边界

- BUY 部分成交会立即进入 SELL 管理，成交以账户事件流和 REST 对账为准。
- 买入只使用买一价 `LIMIT_MAKER`；每笔最多 `12 USDT`，20 秒后若订单仍是买一则继续挂，只有不再是买一时才撤单；不转 IOC、不因短线信号提前撤单。
- BUY 一旦真实成交（包括达到最小可卖额的部分成交），立即撤销剩余买单并对账，再按当前策略挂第一张卖单：`BID_ASK_MAKER` 使用卖一/买入价上方底线，`FEE_AWARE_MAKER` 使用手续费保护价。
- 每张卖单按配置时间检查（默认 2 分钟）；若原单仍在卖一，则保留原单并重新计时；若不在卖一，则撤单、对账并直接按最新卖一重挂。超时重挂不再使用买入价或手续费保护价底线。
- 自动交易流程没有价格止损、止损冷却或 MARKET 卖出；MARKET 只保留给人工授权清仓。库存对账与防重复卖出始终是强制保护。
- 每个 API 账户拥有独立运行时、交易对、订单、持仓、风控和统计；交易对切换前必须停止该账户，并确认无活动订单和当前标的持仓。
- 所有账户共享服务器公网 IP 的 Binance 请求权重；系统动态读取每分钟上限，在 80% 处暂停新开仓并保留退出、撤单和对账余量。
- API Key、Secret、管理密码仅从服务器环境变量加载，不通过浏览器提交或返回。
- 每日成交量、手续费、净盈亏和成本按 `稳定账户 ID + UTC 日期 + 交易对` 写入 SQLite。
- 每个账户/交易对可配置“每日成交量上限”，默认 `510 USDT`。达到上限后立即停止新买入；已有仓位继续卖出，确认空仓后自动停止当前账户，不影响其他账户。
- 每个账户每 30 秒检查一次 BNB 总余额价值；低于 `1 USDT` 时停止新买入，已有仓位完成退出并确认空仓后自动停止当前账户。

## 环境变量

账户配置是以稳定账户 ID 为键的 JSON，不限制为三组；同一配置可承载 1～10 或更多账户：

```bash
BOT_ACCOUNT_PROFILES_JSON='{
  "account-a":{"alias":"bot-a","apiKey":"...","secretKey":"...","enabled":true},
  "account-b":{"alias":"bot-b","apiKey":"...","secretKey":"...","enabled":true,
                "orderAmountsUsdt":{"ENSOUSDT":6,"BTCUSDT":12},
                "symbolStrategies":{"ENSOUSDT":{"mode":"BID_ASK_MAKER"},
                                     "BTCUSDT":{"mode":"FEE_AWARE_MAKER","orderAmountUsdt":6,
                                                 "entryTimeoutMs":180000,"exitTimeoutMs":600000}}}
}'
```

`orderAmountsUsdt` 可为每个账户按交易对设置单笔 USDT 名义金额；未配置的交易对回退到全局
`binance.strategy.order-amount-usdt`。单笔金额仍不能超过 `max-live-order-notional-usdt`，并会在控制台显示当前生效值。

`symbolStrategies` 可为每个账户的每个交易对选择两种策略：
`BID_ASK_MAKER` 在买一挂买单，初始卖价取卖一和买入价上方 1 tick 的较高值；
`FEE_AWARE_MAKER` 同样在买一挂买单，但增加买入锚点，并让初始卖价不低于手续费保护价。
卖单到达检查时间时，如果仍在卖一就保留并重新计时；如果不在卖一，则撤单对账并直接按最新卖一重挂。
超时阶段优先释放仓位，不再强制买入价或手续费保护价底线。未配置时默认使用 `FEE_AWARE_MAKER`。

控制台的“运行时策略切换”可在不重启的情况下修改当前账户/交易对的策略。切换请求会写入 SQLite
`runtime_setting`，重启后优先于环境变量配置恢复；如果当前处于 BUYING 或 SELLING，修改会排队到订单完成并回到
`IDLE` 后应用，绝不会中途改变正在执行的订单。

对应接口为 `POST /api/accounts/{accountId}/strategy`（旧版默认账户也支持
`POST /api/bot/strategy`），请求体字段为 `symbol`、`mode`、`orderAmountUsdt`、`entryTimeoutMs`、
`exitTimeoutMs`、`postSellEntryDelayMs`、`dailyVolumeLimitUsdt`、`makerFeeBps`、锚点相关字段和兼容旧请求的
`targetNetProfitBps`。`makerFeeBps` 留空时按账户和交易对从币安读取
实际 Maker 卖出费率，读取失败才回退到全局保守估值；金额不能超过生产上限，超时时间限制为 1 秒至 30 分钟。

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
- `GET /api/accounts/open-orders`（所有账户当前活动买单/卖单）
- `GET /api/accounts/stats/summary?days=10`（按账户 + 交易对分别汇总近 N 天成交）
- `GET /api/accounts/{accountId}/status`
- `POST /api/accounts/{accountId}/start`
- `POST /api/accounts/{accountId}/stop`
- `POST /api/accounts/{accountId}/symbol`
- `POST /api/accounts/start-all`
- `POST /api/accounts/stop-all`
- `POST /api/accounts/reload`（从服务器受保护环境文件热加载新增账户；不会替换或停止已有账户）

控制台的“API 账户10天汇总”页面展示上述汇总，只保留窗口内有真实成交的账户/交易对组合。

应用启动前会自动读取 `BOT_ACCOUNT_PROFILES_ENV_FILE` 指向的服务器环境文件（默认 `/etc/trading-activity.env`），
将其中缺失的 `BOT_*`、`BINANCE_*` 配置加载到启动环境；已有进程环境变量优先。账户热加载也会重新读取同一文件。
热加载仅为清单中尚未运行的启用账户创建 User Data Stream；新账户始终以 `running=false` 加入，
不会触碰已有账户的持仓或活动 SELL 订单。修改环境文件后可从控制台点击“热加载账户”，无需重启 JVM。
