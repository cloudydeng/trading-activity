# Binance Spot Trading Activity Bot

基于公开 WebSocket 行情信号执行小额 Binance 现货交易。系统只有 `LIVE` 运行模式；服务器端 `BINANCE_LIVE_TRADING_ENABLED=true` 后，从控制台手动启动才会真实下单。项目不做自成交。

产品、运营和测试请先阅读 [三种 Maker 策略产品说明](docs/strategy-behavior.md)。

## 安全边界

- BUY 部分成交会立即进入 SELL 管理，成交以账户事件流和 REST 对账为准。
- 买入只使用 `LIMIT_MAKER`；`BID_ASK_MAKER` 可配置买一至买五，另外两种策略固定买一。默认每笔 `12 USDT`、最高可配置 `30 USDT`；超时后若订单仍在所选档位则继续挂，否则撤单，不转 IOC。
- BUY 一旦真实成交（包括达到最小可卖额的部分成交），立即撤销剩余买单并对账，再按当前策略挂第一张卖单：`BID_ASK_MAKER` 使用卖一/买入价上方可配置 tick 底线，`BUY_PRICE_MAKER` 使用实际买入均价，`FEE_AWARE_MAKER` 使用手续费保护价。
- 每张卖单按配置时间检查（默认 2 分钟）；若原单仍在卖一，则保留原单并重新计时；若不在卖一，则撤单、对账并直接按最新卖一重挂。超时重挂不再使用买入价或手续费保护价底线。
- 自动交易流程没有价格止损、止损冷却或 MARKET 卖出；MARKET 只保留给人工授权清仓。库存对账与防重复卖出始终是强制保护。
- 每个 API 账户拥有一个账户会话和一条 User Data WebSocket，可配置最多 5 个交易对同时运行。每个交易对拥有独立策略状态机、行情流、订单、持仓恢复和统计；账户余额、BNB 检查、买单提交、总持仓风险与总回撤保护由同一账户共享协调。
- 多币种账户启动任一币种前只读取一次账户余额，并同时核对全部已配置币种；任何远程持仓与当日本地账本不一致时拒绝新开仓。可用 USDT 与待提交买单也按账户统一预留，避免多个币种争抢同一余额。
- 所有账户共享服务器公网 IP 的 Binance 请求权重；系统动态读取每分钟上限，在 80% 处暂停新开仓并保留退出、撤单和对账余量。
- 控制台“全部账号最近 10 条成交记录”只接收账户 WebSocket 成交事件，并通过控制台 WebSocket 实时推送；不轮询 `/api/v3/myTrades`，服务重启后从新成交开始显示。
- 控制台“所有账户当前买单 / 卖单”在每个账户策略启动成功时用 REST 读取一次初始快照，之后只由账户 WebSocket 订单事件实时更新；账户流重连成功后再用 REST 对账一次，不做固定 30 秒轮询。
- 当前订单按账户隔离容错：单个账户快照读取失败时保留其上一份数据，不影响其他账户；异常订单记录会被忽略，订单快照发送失败也不会拖断最近成交流。
- 行情流或账户流收到 `1001` 瞬断时先自动重连，不立即停止账户。重连成功后继续运行；重连失败、再次断开或超过健康检查时限才按原有安全逻辑停机。
- API Key、Secret、管理密码仅从服务器环境变量加载，不通过浏览器提交或返回。
- 每笔真实成交及每日成交量、手续费、净盈亏和成本按 `稳定账户 ID + UTC 日期 + 交易对` 写入 SQLite。策略启动时分页对账一次，之后由账户 WebSocket 实时增量写入；控制台状态轮询不查询 Binance。
- 每个账户/交易对可配置“每日成交量上限”，默认 `510 USDT`。达到上限后立即停止该币种的新买入；已有仓位继续卖出，确认空仓后自动停止该币种策略，不影响同账户其他币种。
- 同一账户的 BNB 余额结果由所有币种共享，最多每 30 秒读取一次；低于 `1 USDT` 时，各运行币种都停止新买入，已有仓位完成退出并确认空仓后分别停止。

## SQLite 持久化结构

应用使用同一个 SQLite 文件，当前主动读写四张业务表：

- `daily_trade_stats`：按账户、UTC 日期和交易对保存成交量、手续费、盈亏、持仓成本及闭环次数等每日聚合值。
- `trade_fill`：保存每笔成交的订单 ID、成交 ID、方向、价格、数量、成交额、手续费币种/金额和成交时间；一个订单拆成多笔成交时分别保存。
- `processed_trade`：每笔已记账成交占一行，以账户、交易对和成交身份作为唯一主键，防止 WebSocket 与 REST 对账重复记账。
- `runtime_setting`：保存当前交易对、非敏感策略覆盖配置和必要的订单/锚点恢复状态。

旧版 `daily_trade_stats.processed_trade_ids` JSON 字段会在启动时事务化迁移到 `processed_trade`，迁移成功后从每日聚合表移除；API Key 和 Secret 不写入 SQLite。

## 环境变量

账户配置是以稳定账户 ID 为键的 JSON，不限制为三组；同一配置可承载 1～10 或更多账户：

```bash
BOT_ACCOUNT_PROFILES_JSON='{
  "account-a":{"alias":"bot-a","apiKey":"...","secretKey":"...","enabled":true,
                "symbols":["ENSOUSDT"]},
  "account-b":{"alias":"bot-b","apiKey":"...","secretKey":"...","enabled":true,
                "symbols":["ENSOUSDT","BTCUSDT"],
                "orderAmountsUsdt":{"ENSOUSDT":6,"BTCUSDT":12},
                "symbolStrategies":{"ENSOUSDT":{"mode":"BID_ASK_MAKER"},
                                     "BTCUSDT":{"mode":"FEE_AWARE_MAKER","orderAmountUsdt":6,
                                                 "entryTimeoutMs":180000,"exitTimeoutMs":600000}}}
}'
```

`symbols` 是该账户要创建的并发交易对清单，按配置顺序展示，当前只接受 USDT 现货交易对，去重后最多 5 个。清单中的每个交易对都可以在控制台独立启动、停止和切换策略。同一账户只建立一条账户成交流；成交和订单事件按 `symbol` 分发给对应策略。未配置 `symbols` 时保持旧版单币种行为：优先恢复该账户保存的活动交易对，否则使用全局 `BINANCE_STRATEGY_SYMBOL`。

多币种账户必须使用带 `symbol` 的启动接口，避免旧账户级接口含义不明确；旧账户级“停止”接口会停止该账户的全部币种，不会只停配置中的第一个币种。

已有账户的热加载不会替换正在运行的账户实例，因此给已有账户新增或删除 `symbols` 后需要重启服务；这避免热加载时误动现有订单。只有单币种账户支持原来的“安全切换交易对”，多币种账户通过修改 `symbols` 清单管理交易对。

`orderAmountsUsdt` 可为每个账户按交易对设置单笔 USDT 名义金额；未配置的交易对回退到全局
`binance.strategy.order-amount-usdt`。单笔金额仍不能超过 `max-live-order-notional-usdt`，并会在控制台显示当前生效值。

生产环境可在受保护配置文件中用 `BINANCE_STRATEGY_MAX_DAILY_DRAWDOWN_USDT` 覆盖日内最大回撤；未配置时默认 `8 USDT`，修改后需重启服务。

`symbolStrategies` 可为每个账户的每个交易对选择三种策略：
`BID_ASK_MAKER` 按 `bidAskEntryBookLevel` 选择买一至买五挂买单（默认买一），初始卖价取卖一和“买入均价 + `bidAskInitialSellMarkupTicks` 个 tick”的较高值，加价默认 `1 tick`；
`BUY_PRICE_MAKER` 在买一挂买单，初始卖价直接取实际买入均价并按 tick 向上取整；
`FEE_AWARE_MAKER` 同样在买一挂买单，但增加买入锚点，并让初始卖价不低于手续费保护价。
卖单到达检查时间时，如果仍在卖一就保留并重新计时；如果不在卖一，则撤单对账并直接按最新卖一重挂。
超时阶段优先释放仓位，不再强制买入价或手续费保护价底线。控制台新建策略配置时默认选择 `FEE_AWARE_MAKER`；生产账户应为当前交易对保存明确的策略配置。

控制台的“运行时策略切换”可在不重启的情况下修改当前账户/交易对的策略。切换请求会写入 SQLite
`runtime_setting`，重启后优先于环境变量配置恢复；如果当前处于 BUYING 或 SELLING，修改会排队到订单完成并回到
`IDLE` 后应用，绝不会中途改变正在执行的订单。

多币种控制台使用 `POST /api/accounts/{accountId}/symbols/{symbol}/strategy`；单币种兼容接口仍为 `POST /api/accounts/{accountId}/strategy`（旧版默认账户也支持
`POST /api/bot/strategy`），请求体字段为 `symbol`、`mode`、`orderAmountUsdt`、`entryTimeoutMs`、
`exitTimeoutMs`、`postSellEntryDelayMs`、`dailyVolumeLimitUsdt`、`bidAskEntryBookLevel`、`bidAskInitialSellMarkupTicks`、`makerFeeBps`、锚点相关字段和兼容旧请求的
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
- `GET /api/accounts/stats/today`（按账户汇总 UTC 今日各交易对成交量、手续费和净盈亏；单账户错误隔离）
- `GET /api/accounts/{accountId}/status`
- `POST /api/accounts/{accountId}/start`
- `POST /api/accounts/{accountId}/stop`
- `POST /api/accounts/{accountId}/symbol`
- `GET /api/accounts/{accountId}/symbols/{symbol}/status`
- `GET /api/accounts/{accountId}/symbols/{symbol}/account`
- `GET /api/accounts/{accountId}/symbols/{symbol}/stats/daily`
- `POST /api/accounts/{accountId}/symbols/{symbol}/start`
- `POST /api/accounts/{accountId}/symbols/{symbol}/stop`
- `POST /api/accounts/{accountId}/symbols/{symbol}/strategy`
- `POST /api/accounts/{accountId}/symbols/{symbol}/liquidate`
- `POST /api/accounts/start-all`
- `POST /api/accounts/stop-all`
- `POST /api/accounts/reload`（从服务器受保护环境文件热加载新增账户；不会替换或停止已有账户）

控制台的“API 账户10天汇总”页面展示上述汇总，只保留窗口内有真实成交的账户/交易对组合。
控制台的“今日账户汇总”页面按 UTC 今日、账户和交易对展示本地每日聚合中的成交量、手续费和已实现净盈亏；每 10 秒刷新，但不直接请求 Binance，单账户读取失败不会影响其他账户。

应用启动前会自动读取 `BOT_ACCOUNT_PROFILES_ENV_FILE` 指向的服务器环境文件（默认 `/etc/trading-activity.env`），
将其中缺失的 `BOT_*`、`BINANCE_*` 配置加载到启动环境；已有进程环境变量优先。账户热加载也会重新读取同一文件。
热加载仅为清单中尚未运行的启用账户创建 User Data Stream；新账户始终以 `running=false` 加入，
不会触碰已有账户的持仓或活动 SELL 订单。修改环境文件后可从控制台点击“热加载账户”，无需重启 JVM。
