# Binance Spot Competition Pure Churning Bot (v3.0 Ultra)

专为**币安现货交易赛低磨损、极速刷交易量**设计的工业级量化工程。

---

## ⚡ 核心实战逻辑

1. **深度插针挂单（Anti-Adverse Selection）：**
   - 买单挂在 `Bid 2 ~ Bid 3`，绝不贴脸买一接飞刀，专吃市场微小插针。
2. **0 持仓毫秒级出货（Ping-Pong Zero Exposure）：**
   - 买单成交瞬间以 0 延迟反手将卖单挂在 `Ask 1` 快速脱手回本，单笔持仓时间控制在毫秒/秒级。
3. **小单化整为零抗单边（Randomized Micro Sizing）：**
   - 单笔严格限制在 `$15~$25 USDT` 并附带随机抖动，单边行情发生时单笔损失仅 $0.1~$0.2，被大数定律彻底摊薄。
4. **原生 cancelReplace + 权重熔断：**
   - 单请求原子撤换单，节省 50% API 权重，使用率达 80% 自动降速，杜绝 429/418 封 IP。

---

## 🚀 启动与控制

```bash
# 1. 打包
mvn clean package -DskipTests

# 2. 启动 (替换真实密钥)
java -jar target/binance-spot-competition-bot-3.0.0.jar \
  --binance.api.api-key="YOUR_KEY" \
  --binance.api.secret-key="YOUR_SECRET"
```

- **启动刷量：** `curl -X POST http://localhost:8080/api/bot/start`
- **监控看板：** `curl -X GET http://localhost:8080/api/bot/status`
- **安全停止：** `curl -X POST http://localhost:8080/api/bot/stop`
