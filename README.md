# Binance Spot Trading Activity Bot

基于公开 WebSocket 行情信号执行小额 Binance 现货交易。系统默认 `OBSERVE`，真实交易必须同时启用服务器 `LIVE` 配置、从控制台临时解锁并手动启动。项目不做自成交。

## 安全边界

- BUY 部分成交会立即进入 SELL 管理，成交以账户事件流和 REST 对账为准。
- 买入只使用 `LIMIT_MAKER`；最长挂在买一 20 秒，超时后撤单并等待下一次信号，不再转为 IOC 追价成交；明确卖压、快速下跌或行情过期仍会提前撤单。
- BUY 一旦真实成交（包括达到最小可卖额的部分成交），立即撤销剩余买单并按实际可用余额提交 MARKET SELL；库存对账与防重复卖出仍为强制保护。
- 止损平仓后暂停新开仓 3 分钟；普通的“买入后立即市价卖出”闭环不触发止损冷却。
- 交易对或 API 账户切换前必须停止策略、锁定 LIVE、确认无活动订单和当前标的持仓。
- API Key、Secret、管理密码仅从服务器环境变量加载，不通过浏览器提交或返回。
- 每日成交量、手续费、净盈亏和成本按 `UTC 日期 + API 别名 + 交易对` 写入 SQLite。

## 环境变量

主 API 账户：

```bash
BINANCE_API_KEY_ALIAS=huaqin-bot
BINANCE_API_API_KEY=...
BINANCE_API_SECRET_KEY=...
```

可选的第二 API 账户：

```bash
BINANCE_SECONDARY_API_KEY_ALIAS=second-bot
BINANCE_SECONDARY_API_API_KEY=...
BINANCE_SECONDARY_API_SECRET_KEY=...

BINANCE_TERTIARY_API_KEY_ALIAS=third-bot
BINANCE_TERTIARY_API_API_KEY=...
BINANCE_TERTIARY_API_SECRET_KEY=...
```

第二套凭据必须三项同时配置，否则服务拒绝启动。网页只会显示别名。成功切换的别名写入 SQLite，服务重启后会恢复；如果对应环境变量不再存在，则回退到主账户。

## 构建与启动

```bash
mvn clean package
java -jar target/binance-spot-competition-bot-3.0.0.jar
```

控制接口均受浏览器登录会话或 `X-Bot-Admin-Token` 保护：

- `GET /api/bot/status`
- `POST /api/bot/start`
- `POST /api/bot/stop`
- `POST /api/bot/symbol`
- `GET /api/bot/api-profiles`
- `POST /api/bot/api-profile`
- `GET /api/bot/stats/daily`
