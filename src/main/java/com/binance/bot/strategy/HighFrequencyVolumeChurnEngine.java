package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.config.BinanceCredentialManager;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.service.BinanceOptimizedTradeService;
import com.binance.bot.service.UserDataStreamService;
import com.binance.bot.util.PrecisionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class HighFrequencyVolumeChurnEngine implements WebSocket.Listener {
    private final BinanceProperties properties;
    private final BinanceOptimizedTradeService tradeService;
    private final SymbolRuleManager ruleManager;
    private final UserDataStreamService userDataStreamService;
    private final MarketSignalEvaluator marketSignalEvaluator;
    private final PostFillOutcomeTracker postFillOutcomeTracker;
    private final TradingRiskGuard riskGuard;
    private final DailyTradeStatsStore dailyStatsStore;
    private final BinanceCredentialManager credentialManager;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final AtomicReference<BigDecimal> lastBestBid = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastBestAsk = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastMidPrice = new AtomicReference<>();
    private final AtomicLong lastMarketDataTimestamp = new AtomicLong(0);
    private final AtomicLong lastMarketFrameTimestamp = new AtomicLong(0);

    public enum ChurnStatus { IDLE, BUYING, SELLING, HALTED }

    @Getter private final AtomicBoolean isRunning = new AtomicBoolean(false);
    @Getter private final AtomicBoolean liveArmed = new AtomicBoolean(false);
    @Getter private final AtomicReference<ChurnStatus> currentStatus = new AtomicReference<>(ChurnStatus.IDLE);
    @Getter private final AtomicReference<BigDecimal> totalVolumeUsdt = new AtomicReference<>(BigDecimal.ZERO);
    @Getter private final AtomicLong roundTripsCompleted = new AtomicLong(0);
    private final AtomicReference<Long> activeOrderId = new AtomicReference<>();
    private final AtomicReference<BigDecimal> activeOrderPrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> holdingInventory = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> targetEntryQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> filledEntryQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> filledEntryQuoteQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<String> activeClientOrderId = new AtomicReference<>();
    private final AtomicReference<Long> replacingOrderId = new AtomicReference<>();
    @Getter private final AtomicReference<String> statusReason = new AtomicReference<>("等待启动");
    private final Set<Long> knownOrderIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> restReconciledOrderIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pendingClientOrderIds = ConcurrentHashMap.newKeySet();
    private final TradeAccountingLedger accountingLedger = new TradeAccountingLedger();
    private final ConcurrentHashMap<String, CommissionPrice> commissionPriceCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> orderReconcileFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> tradeReconcileFailures = new ConcurrentHashMap<>();
    private final AtomicLong orderPlacedTimestamp = new AtomicLong(0);
    private final AtomicLong nextOrderAttemptAt = new AtomicLong(0);
    private final AtomicLong clientOrderSequence = new AtomicLong(0);
    private final AtomicBoolean entryCancellationPending = new AtomicBoolean(false);
    private final AtomicBoolean exitSubmissionInFlight = new AtomicBoolean(false);
    private final AtomicBoolean marketConnectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean acceptingMarketConnections = new AtomicBoolean(true);
    private final AtomicReference<WebSocket> activeMarketWebSocket = new AtomicReference<>();
    private final ScheduledExecutorService marketWatchdog = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "binance-market-stream-watchdog");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicReference<String> activeEntrySignalReason = new AtomicReference<>("UNKNOWN");
    private final AtomicReference<MarketSignalEvaluator.MarketContext> activeEntryContext = new AtomicReference<>();
    private final StringBuilder inboundMarketMessage = new StringBuilder();
    private final AtomicLong lastBenchmarkObservationTimestamp = new AtomicLong(0);
    private final AtomicLong lastPaperCandidateTimestamp = new AtomicLong(0);

    public HighFrequencyVolumeChurnEngine(BinanceProperties properties, BinanceOptimizedTradeService tradeService,
                                           SymbolRuleManager ruleManager, UserDataStreamService userDataStreamService,
                                           MarketSignalEvaluator marketSignalEvaluator, PostFillOutcomeTracker postFillOutcomeTracker,
                                           TradingRiskGuard riskGuard, DailyTradeStatsStore dailyStatsStore,
                                           BinanceCredentialManager credentialManager) {
        this.properties = properties;
        this.tradeService = tradeService;
        this.ruleManager = ruleManager;
        this.userDataStreamService = userDataStreamService;
        this.marketSignalEvaluator = marketSignalEvaluator;
        this.postFillOutcomeTracker = postFillOutcomeTracker;
        this.riskGuard = riskGuard;
        this.dailyStatsStore = dailyStatsStore;
        this.credentialManager = credentialManager;
    }

    @PostConstruct
    public void init() {
        if (ruleManager.refreshRule(properties.getStrategy().getSymbol()) == null) {
            statusReason.set("当前交易对不可交易或规则加载失败: " + properties.getStrategy().getSymbol());
            currentStatus.set(ChurnStatus.HALTED);
        }
        syncDailyCounters();
        restoreDailyRisk();
        userDataStreamService.setExecutionCallback(this::onOrderUpdate);
        userDataStreamService.setStreamLifecycleCallback(this::handleUserStreamLoss);
        connectMarketData();
        marketWatchdog.scheduleWithFixedDelay(this::checkMarketStreamHealth, 1, 1, TimeUnit.SECONDS);
    }

    private void connectMarketData() {
        if (!acceptingMarketConnections.get()) return;
        if (!marketConnectInProgress.compareAndSet(false, true)) return;
        String symbol = properties.getStrategy().getSymbol().toLowerCase();
        String baseUrl = properties.getApi().getWsMarketUrl().replaceFirst("/ws/?$", "");
        String wsUrl = baseUrl + "/stream?streams=" + symbol + "@bookTicker/" + symbol + "@aggTrade/" + symbol + "@depth5@100ms";
        httpClient.newWebSocketBuilder().buildAsync(URI.create(wsUrl), this)
                .thenAccept(ws -> {
                    if (!acceptingMarketConnections.get()) {
                        marketConnectInProgress.set(false);
                        reconnectScheduled.set(false);
                        ws.abort();
                        return;
                    }
                    lastMarketFrameTimestamp.set(System.currentTimeMillis());
                    lastMarketDataTimestamp.set(0);
                    WebSocket previous = activeMarketWebSocket.getAndSet(ws);
                    if (previous != null && previous != ws) previous.abort();
                    // Publish the active socket before clearing the connection guards so the
                    // watchdog can never observe a false "no connection" gap and queue a duplicate.
                    marketConnectInProgress.set(false);
                    reconnectScheduled.set(false);
                    log.info("已连接盘口数据流: {}", wsUrl);
                })
                .exceptionally(ex -> {
                    marketConnectInProgress.set(false);
                    reconnectScheduled.set(false);
                    handleMarketStreamLoss("连接失败: " + ex.getMessage());
                    return null;
                });
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        if (activeMarketWebSocket.compareAndSet(webSocket, null)) {
            handleMarketStreamLoss("连接关闭 " + statusCode + ": " + reason);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        if (activeMarketWebSocket.compareAndSet(webSocket, null)) {
            handleMarketStreamLoss("连接错误: " + error.getMessage());
        }
    }

    @Override
    public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
        if (webSocket.equals(activeMarketWebSocket.get())) lastMarketFrameTimestamp.set(System.currentTimeMillis());
        return WebSocket.Listener.super.onPing(webSocket, message);
    }

    @Override
    public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
        if (webSocket.equals(activeMarketWebSocket.get())) lastMarketFrameTimestamp.set(System.currentTimeMillis());
        return WebSocket.Listener.super.onPong(webSocket, message);
    }

    private void handleMarketStreamLoss(String reason) {
        if (!acceptingMarketConnections.get()) return;
        marketSignalEvaluator.reset();
        log.warn("行情流不可用: {}", reason);
        if (isRunning.get() && !properties.getStrategy().isObserveMode()) protectOnStreamLoss("行情流不可用: " + reason);
        if (reconnectScheduled.compareAndSet(false, true)) {
            CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
                if (!acceptingMarketConnections.get()) {
                    reconnectScheduled.set(false);
                    return;
                }
                connectMarketData();
            });
        }
    }

    private void checkMarketStreamHealth() {
        if (!acceptingMarketConnections.get()) return;
        WebSocket socket = activeMarketWebSocket.get();
        long lastFrame = lastMarketFrameTimestamp.get();
        // Business data can legitimately be quiet for several seconds. Connection liveness is
        // based on any inbound frame (including Binance's ~20-second ping), while signal freshness
        // remains independently guarded by marketDataStaleMs in MarketSignalEvaluator.
        long timeoutMs = Math.max(45_000, properties.getStrategy().getMarketStreamWatchdogMs());
        if (socket != null && lastFrame > 0 && System.currentTimeMillis() - lastFrame > timeoutMs) {
            if (activeMarketWebSocket.compareAndSet(socket, null)) {
                socket.abort();
                lastMarketDataTimestamp.set(0);
                handleMarketStreamLoss("连续 " + timeoutMs + " ms 未收到行情帧");
            }
        } else if (socket == null && !marketConnectInProgress.get() && !reconnectScheduled.get()) {
            handleMarketStreamLoss("行情连接不存在");
        }
    }

    private void forceMarketReconnect(String reason) {
        WebSocket socket = activeMarketWebSocket.getAndSet(null);
        if (socket != null) socket.abort();
        lastMarketDataTimestamp.set(0);
        handleMarketStreamLoss(reason);
    }

    public synchronized boolean startTrading() {
        if (isRunning.get()) return true;
        if (properties.getStrategy().isObserveMode()) {
            isRunning.set(true);
            currentStatus.set(ChurnStatus.IDLE);
            statusReason.set("OBSERVE 运行中，不会发送订单");
            orderPlacedTimestamp.set(0);
            log.info("OBSERVE 模式已启动：只记录虚拟候选入场，不发送订单");
            return true;
        }
        if (!properties.getStrategy().isLiveMode() || !properties.getStrategy().isLiveTradingEnabled() || !liveArmed.get()) {
            log.error("拒绝启动：真实执行必须同时设置 execution-mode=LIVE 与 live-trading-enabled=true");
            return false;
        }
        BinanceCredentialManager.CredentialSnapshot credentials = credentialManager.current();
        if (credentials.apiKey().isBlank() || credentials.secretKey().isBlank()) {
            log.error("拒绝启动：真实执行缺少 API 凭据");
            return false;
        }
        if (!userDataStreamService.isReady()) {
            halt("账户成交流未就绪，拒绝启动");
            liveArmed.set(false);
            return false;
        }
        long marketAgeMs = System.currentTimeMillis() - lastMarketFrameTimestamp.get();
        long startupMaxAgeMs = Math.max(45_000, properties.getStrategy().getMarketStreamWatchdogMs());
        if (lastMarketFrameTimestamp.get() <= 0 || lastMarketDataTimestamp.get() <= 0
                || marketAgeMs > startupMaxAgeMs) {
            isRunning.set(false);
            currentStatus.set(ChurnStatus.HALTED);
            statusReason.set("盘口行情正在重连，请稍后再次启动");
            forceMarketReconnect("启动检查发现行情已过期");
            return false;
        }
        if (properties.getStrategy().getOrderAmountUsdt().compareTo(properties.getStrategy().getMaxLiveOrderNotionalUsdt()) > 0) {
            log.error("拒绝启动：单笔名义金额超过 LIVE 上限");
            return false;
        }
        if (getBaselineOutcomes().completedObservations() < properties.getStrategy().getMinBaselineObservationsForLive()
                || getQualifiedSignalOutcomes().completedObservations() < properties.getStrategy().getMinQualifiedObservationsForLive()) {
            log.error("拒绝启动：观察样本不足（需要基准 {}、合格信号 {}）",
                    properties.getStrategy().getMinBaselineObservationsForLive(), properties.getStrategy().getMinQualifiedObservationsForLive());
            return false;
        }
        if (!calibrateHoldings()) {
            halt("无法确认账户余额，拒绝启动");
            liveArmed.set(false);
            return false;
        }
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (openOrders == null) {
            halt("无法确认交易所活动订单，拒绝启动");
            return false;
        }
        if (!openOrders.isEmpty()) {
            halt("发现未由本进程恢复的活动订单，需先人工对账");
            return false;
        }
        if (holdingInventory.get().signum() > 0) {
            halt("发现既有标的持仓，成本未知；拒绝自动接管");
            return false;
        }
        SymbolRuleManager.SymbolRule currentRule = ruleManager.getRule(properties.getStrategy().getSymbol());
        DailyTradeStatsStore.DailyStatsSnapshot durableStats = getDailyStatsSnapshot();
        if (currentRule == null || durableStats.positionQty().compareTo(currentRule.stepSize()) >= 0) {
            halt("本地每日账本仍记录有持仓，和交易所空仓状态不一致；拒绝启动，需人工对账");
            return false;
        }
        clearTrackedOrders();
        isRunning.set(true);
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set("运行中，等待入场信号");
        resetEntryTarget();
        orderPlacedTimestamp.set(0);
        log.info("引擎启动，当前标的持仓: {}", holdingInventory.get());
        return true;
    }

    public synchronized boolean stopTrading() {
        isRunning.set(false);
        Long orderId = activeOrderId.get();
        if (properties.getStrategy().isObserveMode()) {
            clearActiveOrder();
            currentStatus.set(ChurnStatus.IDLE);
            statusReason.set("OBSERVE 已停止");
            return true;
        }
        if (orderId != null) {
            JsonNode cancel = tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId);
            JsonNode finalOrder = tradeService.getOrder(properties.getStrategy().getSymbol(), orderId);
            if (cancel == null || finalOrder == null || !isTerminal(finalOrder.path("status").asText())) {
                halt("停止时无法确认订单 " + orderId + " 已终止；保留本地订单状态");
                return false;
            }
        }
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (openOrders == null || !openOrders.isEmpty() || !calibrateHoldings()) {
            halt("停止后交易所订单或余额状态无法确认");
            return false;
        }
        clearActiveOrder();
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule != null && holdingInventory.get().compareTo(rule.stepSize()) >= 0) {
            halt("活动订单已清理，但仍有标的持仓 " + holdingInventory.get() + "，不可报告为安全空仓");
            return false;
        }
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set("已安全停止");
        resetEntryTarget();
        log.info("引擎已停止。总交易量: {} USDT, 闭环轮数: {}", totalVolumeUsdt.get(), roundTripsCompleted.get());
        return true;
    }

    public synchronized boolean armLiveTrading() {
        if (!properties.getStrategy().isLiveMode() || !properties.getStrategy().isLiveTradingEnabled()) return false;
        if (!userDataStreamService.isReady()) return false;
        liveArmed.set(true);
        return true;
    }

    public synchronized boolean disarmLiveTrading() {
        boolean stopped = stopTrading();
        liveArmed.set(false);
        return stopped;
    }

    /** Operator-authorized, reduce-only-style liquidation of the currently free base-asset balance. */
    public synchronized LiquidationResult liquidateExistingPosition() {
        isRunning.set(false);
        liveArmed.set(false);
        if (!properties.getStrategy().isLiveMode() || !properties.getStrategy().isLiveTradingEnabled()) {
            return LiquidationResult.rejected("服务器未配置 LIVE 双开关");
        }
        if (!userDataStreamService.isReady()) {
            halt("账户成交流未就绪，拒绝提交清仓单");
            return LiquidationResult.rejected(statusReason.get());
        }
        String symbol = properties.getStrategy().getSymbol();
        JsonNode openOrders = tradeService.getOpenOrders(symbol);
        if (openOrders == null || !openOrders.isEmpty()) {
            halt(openOrders == null ? "无法确认活动订单，拒绝清仓" : "仍有活动订单，拒绝重复提交清仓单");
            return LiquidationResult.rejected(statusReason.get());
        }
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
        BigDecimal freeBalance = tradeService.getFreeAssetBalance(baseAsset());
        if (rule == null || freeBalance == null) {
            halt("无法确认交易规则或可卖余额，拒绝清仓");
            return LiquidationResult.rejected(statusReason.get());
        }
        BigDecimal qty = PrecisionUtil.roundDownToStep(freeBalance, rule.stepSize());
        if (qty.compareTo(rule.stepSize()) < 0) {
            holdingInventory.set(freeBalance);
            currentStatus.set(ChurnStatus.IDLE);
            statusReason.set("账户已无可卖标的余额");
            return LiquidationResult.rejected(statusReason.get());
        }
        holdingInventory.set(freeBalance);
        String clientOrderId = nextClientOrderId("SELLM");
        pendingClientOrderIds.add(clientOrderId);
        activeClientOrderId.set(clientOrderId);
        JsonNode response = tradeService.placeMarketSell(symbol, qty, clientOrderId);
        if (response != null && response.has("orderId")) {
            long orderId = response.get("orderId").asLong();
            trackOrder(orderId, clientOrderId, ChurnStatus.SELLING);
            statusReason.set("已提交人工授权市价清仓单，等待账户成交流确认");
            log.warn("人工授权清仓：已提交市价卖单 ID={} qty={} {}", orderId, qty, baseAsset());
            return new LiquidationResult(true, orderId, qty, statusReason.get());
        }
        pendingClientOrderIds.remove(clientOrderId);
        activeClientOrderId.compareAndSet(clientOrderId, null);
        halt(response == null ? "市价清仓单结果未知，需人工核对" :
                "市价清仓单被交易所拒绝: " + response.path("code").asText("unknown") + " "
                        + response.path("msg").asText("unknown"));
        return LiquidationResult.rejected(statusReason.get());
    }

    @PreDestroy public void onShutdown() {
        acceptingMarketConnections.set(false);
        WebSocket socket = activeMarketWebSocket.getAndSet(null);
        if (socket != null) socket.abort();
        marketWatchdog.shutdownNow();
        stopTrading();
    }

    private synchronized void handleUserStreamLoss(String reason) {
        if (!properties.getStrategy().isObserveMode()) protectOnStreamLoss("账户成交流不可用: " + reason);
    }

    private void protectOnStreamLoss(String reason) {
        boolean wasRunning = isRunning.getAndSet(false);
        boolean wasArmed = liveArmed.getAndSet(false);
        boolean wasActive = wasRunning || wasArmed;
        Long orderId = activeOrderId.get();
        if (orderId != null && currentStatus.get() == ChurnStatus.BUYING) {
            tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId);
        }
        currentStatus.set(ChurnStatus.HALTED);
        statusReason.set(reason);
        if (wasActive || orderId != null) log.error("{}；已停机并解除 LIVE，重连后不会自动恢复", reason);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        WebSocket activeSocket = activeMarketWebSocket.get();
        if (activeSocket != null && webSocket != activeSocket) {
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }
        lastMarketFrameTimestamp.set(System.currentTimeMillis());
        String payload;
        synchronized (inboundMarketMessage) {
            inboundMarketMessage.append(data);
            if (!last) return WebSocket.Listener.super.onText(webSocket, data, false);
            payload = inboundMarketMessage.toString();
            inboundMarketMessage.setLength(0);
        }
        try {
            JsonNode envelope = objectMapper.readTree(payload);
            JsonNode node = envelope.has("data") ? envelope.get("data") : envelope;
            if (node.has("b") && node.has("B") && node.has("a") && node.has("A")) {
                BigDecimal bid = new BigDecimal(node.get("b").asText());
                BigDecimal bidQty = new BigDecimal(node.get("B").asText());
                BigDecimal ask = new BigDecimal(node.get("a").asText());
                BigDecimal askQty = new BigDecimal(node.get("A").asText());
                long now = System.currentTimeMillis();
                BigDecimal mid = bid.add(ask).divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP);
                lastBestBid.set(bid);
                lastBestAsk.set(ask);
                lastMidPrice.set(mid);
                lastMarketDataTimestamp.set(now);
                postFillOutcomeTracker.recordMarketPrice(mid, now);
                riskGuard.recordMark(mid, now, properties.getStrategy());
                marketSignalEvaluator.recordQuote(bid, bidQty, ask, askQty, now, properties.getStrategy());
                if (isRunning.get() && properties.getStrategy().isObserveMode() && properties.getStrategy().isCollectObservations()) recordMarketBaseline(mid, now);
                if (isRunning.get()) driveChurnStateMachine(bid, ask);
            } else if (node.has("q") && node.has("m")) {
                marketSignalEvaluator.recordAggTrade(new BigDecimal(node.get("q").asText()), node.get("m").asBoolean(), System.currentTimeMillis(), properties.getStrategy());
            } else if ((node.has("bids") && node.has("asks")) || (node.has("b") && node.has("a"))) {
                // Binance depth payloads use bids/asks on partial-depth streams and b/a on diff-depth streams.
                JsonNode bids = node.has("bids") ? node.get("bids") : node.get("b");
                JsonNode asks = node.has("asks") ? node.get("asks") : node.get("a");
                marketSignalEvaluator.recordDepth(sumDepth(bids), sumDepth(asks), System.currentTimeMillis());
            }
        } catch (Exception e) {
            log.error("Tick 解析异常", e);
        }
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    private synchronized void driveChurnStateMachine(BigDecimal bestBid, BigDecimal bestAsk) {
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null || !isRunning.get()) return;
        long now = System.currentTimeMillis();
        if (now < nextOrderAttemptAt.get()) return;
        String symbol = properties.getStrategy().getSymbol();
        switch (currentStatus.get()) {
            case IDLE -> {
                MarketSignalEvaluator.EntryDecision decision = marketSignalEvaluator.markBestBidMakerReady();
                activeEntrySignalReason.set(decision.reason());
                activeEntryContext.set(marketSignalEvaluator.getMarketContext(now));
                BigDecimal price = PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
                BigDecimal qty = capEntryQuantity(buyQuantity(bestBid, rule), price, rule);
                if (!isValidOrder(qty, price, rule)) return;
                if (properties.getStrategy().isObserveMode()) {
                    if (properties.getStrategy().isCollectObservations()) recordPaperCandidate(price, now);
                    return;
                }
                if (!riskGuard.permitsNewEntry(qty, price, now, properties.getStrategy())) {
                    log.warn("新开仓被风险熔断阻止: {}", riskGuard.getEntryBlockReason());
                    return;
                }
                targetEntryQuantity.set(qty);
                filledEntryQuantity.set(BigDecimal.ZERO);
                submitMakerOrder(symbol, "BUY", price, qty, null, ChurnStatus.BUYING);
            }
            case BUYING -> {
                Long orderId = activeOrderId.get();
                if (orderId == null) { halt("买单状态没有活动订单"); return; }
                long restingMs = now - orderPlacedTimestamp.get();
                long makerTimeoutMs = Math.max(properties.getStrategy().getOrderTtlMs(),
                        properties.getStrategy().getMinEntryOrderRestMs());
                if (!entryCancellationPending.get() && restingMs >= makerTimeoutMs) {
                    cancelActiveEntryOrder("Maker 买单超时，取消等待下一次信号（不转 IOC）");
                }
            }
            case SELLING -> {
                BigDecimal qty = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
                if (qty.compareTo(rule.stepSize()) < 0) { halt("持仓不足以创建有效卖单"); return; }
                Long activeId = activeOrderId.get();
                if (activeId != null) {
                    if (now - orderPlacedTimestamp.get() >= properties.getStrategy().getLimitSellTimeoutMs()) {
                        rollTimedOutExitToBestAsk(symbol, bestAsk, rule, activeId);
                    }
                    return;
                }
                submitImmediateExit(rule);
            }
            case HALTED -> { }
        }
    }

    private synchronized void onOrderUpdate(UserDataStreamService.ExecutionUpdate update) {
        long orderId = update.orderId();
        String clientOrderId = update.clientOrderId();
        String side = update.side();
        String executionType = update.executionType();
        String orderStatus = update.orderStatus();
        if (restReconciledOrderIds.contains(orderId)) {
            // REST reconciliation already consumed every trade for this order. Keep the marker
            // until its delayed expiry so duplicate/late execution reports cannot mutate
            // inventory or submit a second exit order.
            return;
        }
        boolean replacingOldEvent = Long.valueOf(orderId).equals(replacingOrderId.get())
                && (clientOrderId == null || !clientOrderId.equals(activeClientOrderId.get()));
        boolean activeEvent = !replacingOldEvent && (Long.valueOf(orderId).equals(activeOrderId.get())
                || (clientOrderId != null && clientOrderId.equals(activeClientOrderId.get())));
        boolean knownEvent = activeEvent || knownOrderIds.contains(orderId)
                || (clientOrderId != null && pendingClientOrderIds.contains(clientOrderId));
        if (!knownEvent) {
            if ("TRADE".equals(executionType) && update.lastExecutedQty().signum() > 0) {
                liveArmed.set(false);
                halt("收到未关联订单的成交回报 " + orderId + "，需人工对账");
            }
            return;
        }
        knownOrderIds.add(orderId);
        if (activeEvent && activeOrderId.get() == null) activeOrderId.set(orderId);
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null) { halt("未加载交易规则"); return; }
        TradeAccountingLedger.AppliedTrade appliedTrade = null;
        if ("TRADE".equals(executionType) && update.lastExecutedQty().signum() > 0) {
            appliedTrade = applyExecutionTrade(update);
        }
        if (activeEvent && appliedTrade != null && appliedTrade.applied()
                && "BUY".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.BUYING
                && "PARTIALLY_FILLED".equals(orderStatus)) {
            if (canSubmitImmediateExit(rule)) {
                cancelPartiallyFilledEntryAndExit(orderId, rule);
                return;
            }
            statusReason.set("BUY 已部分成交 " + holdingInventory.get().toPlainString()
                    + " " + baseAsset() + "，尚未达到最小可卖额，继续等待成交");
        }
        if (activeEvent && "BUY".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.BUYING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus) || "EXPIRED".equals(orderStatus))) {
            if (!reconcileTerminalEvent(orderId, side, update.cumulativeExecutedQty(), update.cumulativeQuoteQty(), rule)) return;
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) >= 0) {
                currentStatus.set(ChurnStatus.SELLING);
                statusReason.set("BUY 已成交，按实际成交均价挂限价卖单");
                submitImmediateExit(rule);
            }
            else { currentStatus.set(ChurnStatus.IDLE); resetEntryTarget(); }
        } else if (activeEvent && "SELL".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.SELLING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus)
                || "EXPIRED".equals(orderStatus) || "EXPIRED_IN_MATCH".equals(orderStatus))) {
            if (!reconcileTerminalEvent(orderId, side, update.cumulativeExecutedQty(), update.cumulativeQuoteQty(), rule)) return;
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) < 0) {
                completeFlatExit(false);
            } else {
                // A partial fill leaves one position and no active order; the next market tick
                // routes it through the same target/stop/time exit policy.
                currentStatus.set(ChurnStatus.SELLING);
            }
        }
        if (isTerminal(orderStatus)) {
            knownOrderIds.remove(orderId);
            orderReconcileFailures.remove(orderId);
            if (clientOrderId != null) pendingClientOrderIds.remove(clientOrderId);
        }
    }

    /** Compatibility entry point retained for focused unit tests and operational tooling. */
    private void onOrderUpdate(long orderId, String clientOrderId, String side, String executionType,
                               String orderStatus, BigDecimal lastFilledQty, BigDecimal lastFilledPrice,
                               BigDecimal commission, String commissionAsset) {
        BigDecimal cumulativeQuote = lastFilledQty.multiply(lastFilledPrice);
        onOrderUpdate(new UserDataStreamService.ExecutionUpdate(orderId, -1, clientOrderId, side, executionType,
                orderStatus, lastFilledQty, lastFilledPrice, lastFilledQty, cumulativeQuote,
                commission, commissionAsset, System.currentTimeMillis()));
    }

    private TradeAccountingLedger.AppliedTrade applyExecutionTrade(UserDataStreamService.ExecutionUpdate update) {
        BigDecimal quoteQuantity = update.lastExecutedQty().multiply(update.lastExecutedPrice());
        return applyTrade(update.orderId(), update.tradeId(), update.side(), update.lastExecutedQty(),
                update.lastExecutedPrice(), quoteQuantity, update.commission(), update.commissionAsset(),
                update.tradeTimeMs());
    }

    private TradeAccountingLedger.AppliedTrade applyTrade(long orderId, long tradeId, String side,
                                                          BigDecimal quantity, BigDecimal price,
                                                          BigDecimal quoteQuantity, BigDecimal commission,
                                                          String commissionAsset, long tradeTimeMs) {
        BigDecimal commissionQuote = commissionQuoteEquivalent(commission, commissionAsset, price);
        TradeAccountingLedger.AppliedTrade trade = accountingLedger.record(orderId, tradeId, quantity, price,
                quoteQuantity, commission, commissionAsset, commissionQuote);
        if (!trade.applied()) return trade;

        boolean baseCommission = baseAsset().equalsIgnoreCase(trade.commissionAsset());
        BigDecimal inventoryQuantity = trade.quantity();
        if (baseCommission) {
            inventoryQuantity = "BUY".equalsIgnoreCase(side)
                    ? inventoryQuantity.subtract(trade.commission()).max(BigDecimal.ZERO)
                    : inventoryQuantity.add(trade.commission());
        }
        BigDecimal cashCommissionQuote = baseCommission ? BigDecimal.ZERO : commissionQuote;
        DailyTradeStatsStore.RecordResult persistentResult = dailyStatsStore.recordTrade(
                credentialManager.currentAlias(), properties.getStrategy().getSymbol(), orderId, tradeId,
                side, inventoryQuantity, trade.quoteQuantity(), trade.commission(), commissionQuote,
                cashCommissionQuote, tradeTimeMs);
        if (persistentResult == DailyTradeStatsStore.RecordResult.FAILED) {
            liveArmed.set(false);
            halt("每日交易统计写入失败，已停止真实交易");
            return TradeAccountingLedger.AppliedTrade.ignored();
        }
        if (persistentResult == DailyTradeStatsStore.RecordResult.DUPLICATE) {
            log.warn("忽略数据库已记录的重复成交: orderId={} tradeId={}", orderId, tradeId);
            syncDailyCounters();
            return TradeAccountingLedger.AppliedTrade.ignored();
        }
        if ("BUY".equalsIgnoreCase(side)) {
            filledEntryQuantity.accumulateAndGet(trade.quantity(), BigDecimal::add);
            filledEntryQuoteQuantity.accumulateAndGet(trade.quoteQuantity(), BigDecimal::add);
            holdingInventory.accumulateAndGet(inventoryQuantity, BigDecimal::add);
            if (properties.getStrategy().isCollectObservations()) {
                postFillOutcomeTracker.recordBuyFill(price, activeEntrySignalReason.get(), activeEntryContext.get(),
                        tradeTimeMs > 0 ? tradeTimeMs : System.currentTimeMillis());
            }
        } else {
            holdingInventory.accumulateAndGet(inventoryQuantity, BigDecimal::subtract);
            if (holdingInventory.get().signum() < 0) holdingInventory.set(BigDecimal.ZERO);
        }
        syncDailyCounters();
        riskGuard.recordActualFill(side, inventoryQuantity, trade.quoteQuantity(), cashCommissionQuote,
                tradeTimeMs > 0 ? tradeTimeMs : System.currentTimeMillis(), properties.getStrategy());
        if (riskGuard.getEntryBlockReason() != null) {
            log.warn("风险熔断已触发: {}", riskGuard.getEntryBlockReason());
        }
        return trade;
    }

    private BigDecimal commissionQuoteEquivalent(BigDecimal commission, String commissionAsset, BigDecimal fillPrice) {
        if (commission == null || commission.signum() <= 0) return BigDecimal.ZERO;
        if (quoteAsset().equalsIgnoreCase(commissionAsset)) return commission;
        if (baseAsset().equalsIgnoreCase(commissionAsset)) return commission.multiply(fillPrice);
        if (commissionAsset == null || commissionAsset.isBlank()) return null;
        String conversionSymbol = commissionAsset.toUpperCase() + quoteAsset();
        long now = System.currentTimeMillis();
        CommissionPrice cached = commissionPriceCache.get(conversionSymbol);
        BigDecimal conversionPrice;
        if (cached != null && now - cached.updatedAtMs() < 30_000) {
            conversionPrice = cached.price();
        } else {
            conversionPrice = tradeService.getTickerPrice(conversionSymbol);
            if (conversionPrice != null) commissionPriceCache.put(conversionSymbol, new CommissionPrice(conversionPrice, now));
        }
        return conversionPrice == null ? null : commission.multiply(conversionPrice);
    }

    private boolean reconcileTerminalEvent(long orderId, String side, BigDecimal expectedQuantity,
                                           BigDecimal expectedQuote, SymbolRuleManager.SymbolRule rule) {
        if (accountingLedger.accountedQuantity(orderId).compareTo(expectedQuantity) < 0
                && !reconcileOrderTrades(orderId, side, expectedQuantity, expectedQuote)) {
            statusReason.set("等待订单 " + orderId + " 的 REST 成交明细完成同步");
            scheduleOrderReconciliation(orderId);
            return false;
        }
        if (!reconcileInventory(rule, "订单 " + orderId + " 终态")) return false;
        markRestReconciled(orderId);
        return true;
    }

    private boolean reconcileOrderTrades(long orderId, String fallbackSide, BigDecimal expectedQuantity,
                                         BigDecimal expectedQuote) {
        JsonNode trades = tradeService.getMyTrades(properties.getStrategy().getSymbol(), orderId);
        if (trades == null || !trades.isArray()) {
            log.warn("订单 {} 的真实成交与手续费明细暂不可用，等待 REST 重试", orderId);
            return false;
        }
        for (JsonNode trade : trades) {
            if (trade.path("orderId").asLong(orderId) != orderId) continue;
            String side = trade.has("isBuyer") ? (trade.path("isBuyer").asBoolean() ? "BUY" : "SELL") : fallbackSide;
            BigDecimal quantity = new BigDecimal(trade.path("qty").asText("0"));
            BigDecimal price = new BigDecimal(trade.path("price").asText("0"));
            BigDecimal quote = new BigDecimal(trade.path("quoteQty").asText(quantity.multiply(price).toPlainString()));
            BigDecimal commission = new BigDecimal(trade.path("commission").asText("0"));
            applyTrade(orderId, trade.path("id").asLong(-1), side, quantity, price, quote,
                    commission, trade.path("commissionAsset").asText(""),
                    trade.path("time").asLong(System.currentTimeMillis()));
        }
        BigDecimal accountedQuantity = accountingLedger.accountedQuantity(orderId);
        BigDecimal accountedQuote = accountingLedger.accountedQuote(orderId);
        if (accountedQuantity.compareTo(expectedQuantity) != 0
                || (expectedQuote != null && expectedQuote.signum() > 0 && accountedQuote.compareTo(expectedQuote) != 0)) {
            log.warn("订单 {} 成交明细尚未追平累计值: qty={}/{}, quote={}/{}",
                    orderId, accountedQuantity, expectedQuantity, accountedQuote, expectedQuote);
            return false;
        }
        return true;
    }

    private boolean reconcileInventory(SymbolRuleManager.SymbolRule rule, String context) {
        BinanceOptimizedTradeService.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (balance == null) {
            BigDecimal free = tradeService.getFreeAssetBalance(baseAsset());
            if (free != null) balance = new BinanceOptimizedTradeService.AssetBalance(baseAsset(), free,
                    BigDecimal.ZERO, free);
        }
        if (balance == null) {
            liveArmed.set(false);
            halt(context + " 后无法读取账户总持仓");
            return false;
        }
        BigDecimal expected = holdingInventory.get();
        BigDecimal difference = balance.total().subtract(expected).abs();
        holdingInventory.set(balance.total());
        if (difference.compareTo(rule.stepSize()) >= 0) {
            liveArmed.set(false);
            halt(context + " 后库存不一致: local=" + expected.toPlainString()
                    + ", exchange=" + balance.total().toPlainString());
            return false;
        }
        return true;
    }

    private void cancelPartiallyFilledEntryAndExit(long orderId, SymbolRuleManager.SymbolRule rule) {
        if (!entryCancellationPending.compareAndSet(false, true)) return;
        JsonNode cancel = tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId);
        JsonNode finalOrder = tradeService.getOrder(properties.getStrategy().getSymbol(), orderId);
        if (finalOrder == null || !isTerminal(finalOrder.path("status").asText())) {
            liveArmed.set(false);
            halt("BUY 部分成交后无法确认剩余买单已撤销");
            return;
        }
        if (cancel == null || cancel.has("code")) {
            log.info("BUY 部分成交撤单响应未成功，但订单 {} 已处于终态 {}；按 REST 成交明细对账",
                    orderId, finalOrder.path("status").asText());
        }
        BigDecimal executed = new BigDecimal(finalOrder.path("executedQty").asText("0"));
        BigDecimal quote = new BigDecimal(finalOrder.path("cummulativeQuoteQty").asText("0"));
        if (!reconcileOrderTrades(orderId, "BUY", executed, quote) || !reconcileInventory(rule, "BUY 部分成交撤单")) {
            return;
        }
        markRestReconciled(orderId);
        clearActiveOrder();
        currentStatus.set(ChurnStatus.SELLING);
        statusReason.set("BUY 部分成交已停止追单，立即退出已成交持仓");
        submitImmediateExit(rule);
    }

    private void submitImmediateExit(SymbolRuleManager.SymbolRule rule) {
        if (activeOrderId.get() != null
                || currentStatus.get() != ChurnStatus.SELLING) return;
        BigDecimal quantity = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
        BigDecimal floorPrice = entryAverageFloorPrice(rule);
        if (!isValidOrder(quantity, floorPrice, rule)) {
            halt("已成交持仓不足以创建有效卖单");
            return;
        }
        String clientOrderId = nextClientOrderId("SELLG");
        pendingClientOrderIds.add(clientOrderId);
        activeClientOrderId.set(clientOrderId);
        JsonNode response = tradeService.placeLimitGtcSell(properties.getStrategy().getSymbol(), quantity,
                floorPrice, clientOrderId);
        if (response != null && response.has("orderId")) {
            trackOrder(response.get("orderId").asLong(), clientOrderId, ChurnStatus.SELLING);
            statusReason.set("BUY 成交后按买入均价下限卖出中 @ " + floorPrice.toPlainString());
            log.info("BUY 成交后已挂 GTC 限价卖出 {} {} @ {}", quantity, baseAsset(), floorPrice);
        } else {
            reconcileAmbiguousSubmission(clientOrderId, ChurnStatus.SELLING, "买入均价 GTC 卖单结果未知");
        }
    }

    /**
     * Every sell order has a 60-second working window. Once it expires, cancel and
     * reconcile the old order before placing the remaining inventory at the latest
     * best ask. This repeats for each replacement and never falls back to MARKET.
     */
    private void rollTimedOutExitToBestAsk(String symbol, BigDecimal bestAsk,
                                           SymbolRuleManager.SymbolRule rule, long orderId) {
        if (!exitSubmissionInFlight.compareAndSet(false, true)) return;
        try {
            JsonNode cancel = tradeService.cancelOrder(symbol, orderId);
            JsonNode finalOrder = tradeService.getOrder(symbol, orderId);
            if (finalOrder == null || !isTerminal(finalOrder.path("status").asText())) {
                liveArmed.set(false);
                halt("60 秒卖单超时后无法确认原限价卖单已撤销");
                return;
            }
            if (cancel == null || cancel.has("code")) {
                log.info("卖单超时撤单响应未成功，但订单 {} 已处于终态 {}；先按 REST 对账",
                        orderId, finalOrder.path("status").asText());
            }
            reconcileTrackedOrder(orderId);
            if (Long.valueOf(orderId).equals(activeOrderId.get())
                    || currentStatus.get() != ChurnStatus.SELLING) return;

            BigDecimal quantity = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
            BigDecimal price = PrecisionUtil.roundDownToStep(bestAsk, rule.tickSize());
            if (!isValidOrder(quantity, price, rule)) {
                halt("剩余持仓不足以按卖一价创建 LIMIT 卖单");
                return;
            }
            String clientOrderId = nextClientOrderId("SELLA");
            pendingClientOrderIds.add(clientOrderId);
            activeClientOrderId.set(clientOrderId);
            JsonNode response = tradeService.placeLimitGtcSell(symbol, quantity, price, clientOrderId);
            if (response != null && response.has("orderId")) {
                trackOrder(response.get("orderId").asLong(), clientOrderId, ChurnStatus.SELLING);
                statusReason.set("上一张卖单满 60 秒，剩余持仓已按卖一价挂 LIMIT @ "
                        + price.toPlainString());
                log.info("卖单满 60 秒，已按最新卖一价重新挂 LIMIT {} {} @ {}",
                        quantity, baseAsset(), price);
            } else {
                reconcileAmbiguousSubmission(clientOrderId, ChurnStatus.SELLING,
                        "卖一价 LIMIT 卖单结果未知");
            }
        } finally {
            exitSubmissionInFlight.set(false);
        }
    }

    private boolean canSubmitImmediateExit(SymbolRuleManager.SymbolRule rule) {
        BigDecimal quantity = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
        return isValidOrder(quantity, entryAverageFloorPrice(rule), rule);
    }

    private BigDecimal entryAverageFloorPrice(SymbolRuleManager.SymbolRule rule) {
        BigDecimal filledQuantity = filledEntryQuantity.get();
        BigDecimal filledQuote = filledEntryQuoteQuantity.get();
        if (filledQuantity.signum() <= 0 || filledQuote.signum() <= 0) return BigDecimal.ZERO;
        return PrecisionUtil.roundUpToStep(filledQuote.divide(filledQuantity,
                java.math.MathContext.DECIMAL64), rule.tickSize());
    }

    private void submitExitMakerOnce(String symbol, BigDecimal price, BigDecimal quantity, Long cancelOrderId) {
        if (cancelOrderId == null && activeOrderId.get() != null) return;
        if (!exitSubmissionInFlight.compareAndSet(false, true)) return;
        try {
            submitMakerOrder(symbol, "SELL", price, quantity, cancelOrderId, ChurnStatus.SELLING);
        } finally {
            exitSubmissionInFlight.set(false);
        }
    }

    private void markRestReconciled(long orderId) {
        restReconciledOrderIds.add(orderId);
        CompletableFuture.delayedExecutor(60, TimeUnit.SECONDS)
                .execute(() -> restReconciledOrderIds.remove(orderId));
    }

    private BigDecimal initialExitPrice(BigDecimal ask, SymbolRuleManager.SymbolRule rule) {
        BigDecimal target = feeAwareTargetPrice(rule);
        BigDecimal configuredAsk = ask.add(rule.tickSize().multiply(BigDecimal.valueOf(
                Math.max(0, properties.getStrategy().getAskDepthOffsetTicks()))));
        return PrecisionUtil.roundUpToStep(configuredAsk.max(target), rule.tickSize());
    }

    private BigDecimal passiveExitPrice(BigDecimal bestBid, BigDecimal bestAsk,
                                        SymbolRuleManager.SymbolRule rule) {
        BigDecimal target = feeAwareTargetPrice(rule);
        BigDecimal minimumPostOnly = bestBid.add(rule.tickSize());
        return PrecisionUtil.roundUpToStep(target.max(bestAsk).max(minimumPostOnly), rule.tickSize());
    }

    private BigDecimal feeAwareTargetPrice(SymbolRuleManager.SymbolRule rule) {
        var risk = riskGuard.snapshot();
        if (risk.positionQty() == null || risk.positionQty().signum() <= 0
                || risk.positionCostUsdt() == null || risk.positionCostUsdt().signum() <= 0) {
            throw new IllegalStateException("无法在空仓状态计算退出目标价");
        }
        BigDecimal unitCost = risk.positionCostUsdt().divide(
                risk.positionQty(), java.math.MathContext.DECIMAL64);
        BigDecimal desiredNetRate = BigDecimal.valueOf(properties.getStrategy().getTakeProfitBps())
                .movePointLeft(4);
        BigDecimal estimatedExitFeeRate = properties.getStrategy().getAssumedMakerFeeBps().movePointLeft(4);
        BigDecimal feeDenominator = BigDecimal.ONE.subtract(estimatedExitFeeRate);
        if (feeDenominator.signum() <= 0) throw new IllegalStateException("预计卖出费率配置无效");
        BigDecimal target = unitCost.multiply(BigDecimal.ONE.add(desiredNetRate))
                .divide(feeDenominator, java.math.MathContext.DECIMAL64);
        return PrecisionUtil.roundUpToStep(target, rule.tickSize());
    }

    private BigDecimal buyQuantity(BigDecimal bid, SymbolRuleManager.SymbolRule rule) {
        BigDecimal base = properties.getStrategy().getOrderAmountUsdt().divide(bid, 8, RoundingMode.DOWN);
        return PrecisionUtil.roundDownToStep(applyJitter(base), rule.stepSize());
    }
    private BigDecimal sumDepth(JsonNode levels) {
        BigDecimal total = BigDecimal.ZERO;
        for (JsonNode level : levels) {
            if (level.isArray() && level.size() > 1) total = total.add(new BigDecimal(level.get(1).asText()));
        }
        return total;
    }
    private boolean isValidOrder(BigDecimal qty, BigDecimal price, SymbolRuleManager.SymbolRule rule) {
        return qty != null && qty.compareTo(rule.stepSize()) >= 0 && price != null && price.signum() > 0 && qty.multiply(price).compareTo(rule.minNotional()) >= 0;
    }

    static boolean isHardEntryRisk(String reason) {
        return switch (reason) {
            case "STALE_MARKET_DATA", "STALE_DEPTH_DATA", "EMPTY_TOP_OF_BOOK", "EMPTY_DEPTH_BOOK",
                    "SELL_TAKER_PRESSURE", "SHORT_TERM_DOWNMOVE", "EXCESS_SHORT_TERM_VOLATILITY",
                    "POST_SELLOFF_COOLDOWN" -> true;
            default -> false;
        };
    }

    private void submitMakerOrder(String symbol, String side, BigDecimal price, BigDecimal qty,
                                  Long cancelOrderId, ChurnStatus status) {
        String clientOrderId = nextClientOrderId(side);
        pendingClientOrderIds.add(clientOrderId);
        activeClientOrderId.set(clientOrderId);
        replacingOrderId.set(cancelOrderId);
        try {
            JsonNode response = tradeService.cancelAndReplaceOrder(symbol, side, price, qty, cancelOrderId, clientOrderId);
            handleMakerResponse(response, cancelOrderId, clientOrderId, status);
            JsonNode acceptedOrder = cancelOrderId == null ? response
                    : response == null ? null : response.path("newOrderResponse");
            if (acceptedOrder != null && acceptedOrder.has("orderId")) {
                long acceptedOrderId = acceptedOrder.get("orderId").asLong();
                if (Long.valueOf(acceptedOrderId).equals(activeOrderId.get())) activeOrderPrice.set(price);
                log.info("Maker 报单已接受: ID={} side={} qty={} price={} clientOrderId={}",
                        acceptedOrderId, side, qty, price, clientOrderId);
                if ("SELL".equalsIgnoreCase(side)) {
                    statusReason.set("持仓退出中：Maker 卖单已挂出 @ " + price.toPlainString());
                }
            }
        } finally {
            replacingOrderId.compareAndSet(cancelOrderId, null);
        }
    }

    private BigDecimal capEntryQuantity(BigDecimal quantity, BigDecimal price,
                                        SymbolRuleManager.SymbolRule rule) {
        BigDecimal maxQuantity = properties.getStrategy().getMaxLiveOrderNotionalUsdt()
                .divide(price, 8, RoundingMode.DOWN);
        return PrecisionUtil.roundDownToStep(quantity.min(maxQuantity), rule.stepSize());
    }

    private void handleMakerResponse(JsonNode response, Long oldOrderId, String clientOrderId, ChurnStatus status) {
        if (response == null) {
            reconcileAmbiguousSubmission(clientOrderId, status, "报单请求结果未知");
            return;
        }
        // Binance wraps cancelReplace failure details in `data`, while successful responses
        // expose the same result fields at the root. Normalize both shapes before deciding.
        JsonNode result = response.path("data").isObject() ? response.path("data") : response;
        JsonNode order = oldOrderId == null ? response : result.path("newOrderResponse");
        boolean newOrderSucceeded = oldOrderId == null ? response.has("orderId")
                : "SUCCESS".equals(result.path("newOrderResult").asText()) && order.has("orderId");
        if (newOrderSucceeded) {
            long orderId = order.get("orderId").asLong();
            if (clientOrderId.equals(activeClientOrderId.get())) trackOrder(orderId, clientOrderId, status);
            return;
        }
        pendingClientOrderIds.remove(clientOrderId);
        if (oldOrderId != null && "SUCCESS".equals(result.path("cancelResult").asText())) {
            BigDecimal exchangeExecuted = new BigDecimal(
                    result.path("cancelResponse").path("executedQty").asText("0"));
            if (exchangeExecuted.signum() > 0
                    && filledEntryQuantity.get().compareTo(exchangeExecuted) < 0) {
                activeClientOrderId.compareAndSet(clientOrderId, null);
                nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
                CompletableFuture.delayedExecutor(750, TimeUnit.MILLISECONDS)
                        .execute(() -> reconcileCancelledMakerFill(oldOrderId, exchangeExecuted));
                log.info("撤换单的原订单终态包含成交 {}，等待账户成交流对账", exchangeExecuted);
                return;
            }
            knownOrderIds.remove(oldOrderId);
            clearActiveOrder();
            nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
            var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
            if (status == ChurnStatus.BUYING) {
                if (rule != null && holdingInventory.get().compareTo(rule.stepSize()) >= 0) currentStatus.set(ChurnStatus.SELLING);
                else { currentStatus.set(ChurnStatus.IDLE); resetEntryTarget(); }
            } else {
                currentStatus.set(ChurnStatus.SELLING);
            }
            log.warn("旧订单已撤销但新订单未创建，已退避 1 秒: code={}, msg={}",
                    response.path("code").asText("unknown"), response.path("msg").asText("unknown"));
            return;
        }
        if (oldOrderId != null && "FAILURE".equals(result.path("cancelResult").asText())) {
            activeClientOrderId.set(null);
            nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
            log.warn("撤换单中的撤单失败；保留旧订单 {} 并退避", oldOrderId);
            return;
        }
        if (oldOrderId == null) {
            activeClientOrderId.compareAndSet(clientOrderId, null);
            nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
            if (status == ChurnStatus.BUYING) resetEntryTarget();
            log.warn("新订单未被交易所接受，已退避 1 秒: code={}, msg={}",
                    response.path("code").asText("unknown"), response.path("msg").asText("unknown"));
            return;
        }
        reconcileAmbiguousSubmission(clientOrderId, status, "无法解释 cancelReplace 组合结果");
    }

    private synchronized void reconcileCancelledMakerFill(long makerOrderId, BigDecimal exchangeExecuted) {
        if (!Long.valueOf(makerOrderId).equals(activeOrderId.get())
                || currentStatus.get() != ChurnStatus.BUYING) return;
        if (filledEntryQuantity.get().compareTo(exchangeExecuted) >= 0
                && holdingInventory.get().signum() > 0) {
            clearActiveOrder();
            currentStatus.set(ChurnStatus.SELLING);
            statusReason.set("BUY 撤单包含成交，立即退出已成交持仓");
            log.info("已撤销 Maker 买单的成交已由账户成交流对账，转入持仓退出管理");
            return;
        }
        liveArmed.set(false);
        halt("Maker 买单撤销后的成交回报超时，需人工对账");
    }

    private void reconcileAmbiguousSubmission(String clientOrderId, ChurnStatus status, String reason) {
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (openOrders == null) {
            liveArmed.set(false);
            halt(reason + "，且无法查询活动订单");
            return;
        }
        JsonNode matched = null;
        for (JsonNode order : openOrders) {
            if (clientOrderId.equals(order.path("clientOrderId").asText())) {
                if (matched != null) {
                    liveArmed.set(false);
                    halt("发现重复客户端订单 ID，需人工对账");
                    return;
                }
                matched = order;
            }
        }
        if (matched != null) {
            trackOrder(matched.path("orderId").asLong(), clientOrderId, status);
            BigDecimal matchedPrice = new BigDecimal(matched.path("price").asText("0"));
            if (matchedPrice.signum() > 0) activeOrderPrice.set(matchedPrice);
            return;
        }
        pendingClientOrderIds.remove(clientOrderId);
        activeClientOrderId.compareAndSet(clientOrderId, null);
        liveArmed.set(false);
        halt(reason + "；交易所未发现对应活动订单，需核对成交历史");
    }

    private void cancelActiveEntryOrder(String reason) {
        Long orderId = activeOrderId.get();
        if (orderId == null || !entryCancellationPending.compareAndSet(false, true)) return;
        JsonNode cancel = tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId);
        if (cancel != null) {
            log.info("撤销活动买单 {}: {}", orderId, reason);
            CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS)
                    .execute(() -> reconcileCancelledEntry(orderId));
        } else {
            entryCancellationPending.set(false);
            liveArmed.set(false);
            halt("撤销活动买单 " + orderId + " 的结果未知: " + reason);
        }
    }
    private synchronized void reconcileCancelledEntry(long orderId) {
        if (!Long.valueOf(orderId).equals(activeOrderId.get()) || currentStatus.get() != ChurnStatus.BUYING) return;
        JsonNode order = tradeService.getOrder(properties.getStrategy().getSymbol(), orderId);
        if (order == null) { halt("撤单后无法确认订单最终状态"); return; }
        String status = order.path("status").asText();
        BigDecimal executedQty = new BigDecimal(order.path("executedQty").asText("0"));
        if ("CANCELED".equals(status) || "EXPIRED".equals(status) || "EXPIRED_IN_MATCH".equals(status)) {
            var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
            if (rule == null) { halt("撤单后交易规则不可用"); return; }
            if (executedQty.signum() > 0) {
                BigDecimal cumulativeQuote = new BigDecimal(order.path("cummulativeQuoteQty").asText("0"));
                if (!reconcileOrderTrades(orderId, "BUY", executedQty, cumulativeQuote)
                        || !reconcileInventory(rule, "撤销部分成交 BUY")) return;
                markRestReconciled(orderId);
            }
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) >= 0) {
                currentStatus.set(ChurnStatus.SELLING);
                statusReason.set("BUY 撤单包含成交，立即退出已成交持仓");
                submitImmediateExit(rule);
            }
            else { currentStatus.set(ChurnStatus.IDLE); resetEntryTarget(); }
            log.info("撤单 {} 已确认，状态机恢复为 {}", orderId, currentStatus.get());
        }
    }
    private void trackOrder(long orderId, String clientOrderId, ChurnStatus status) {
        knownOrderIds.add(orderId);
        pendingClientOrderIds.add(clientOrderId);
        activeOrderId.set(orderId);
        activeClientOrderId.set(clientOrderId);
        entryCancellationPending.set(false);
        orderPlacedTimestamp.set(System.currentTimeMillis());
        currentStatus.set(status);
        scheduleOrderReconciliation(orderId);
    }

    private void scheduleOrderReconciliation(long orderId) {
        CompletableFuture.delayedExecutor(1_500, TimeUnit.MILLISECONDS)
                .execute(() -> reconcileTrackedOrder(orderId));
    }

    private synchronized void reconcileTrackedOrder(long orderId) {
        if (!Long.valueOf(orderId).equals(activeOrderId.get())) return;
        JsonNode order = tradeService.getOrder(properties.getStrategy().getSymbol(), orderId);
        if (order == null) {
            int failures = orderReconcileFailures.merge(orderId, 1, Integer::sum);
            if (failures >= 3) {
                liveArmed.set(false);
                halt("连续三次无法查询活动订单 " + orderId + "，已停止真实交易");
            } else {
                scheduleOrderReconciliation(orderId);
            }
            return;
        }
        orderReconcileFailures.remove(orderId);
        String orderStatus = order.path("status").asText();
        if (!isTerminal(orderStatus)) {
            scheduleOrderReconciliation(orderId);
            return;
        }

        String side = order.path("side").asText();
        BigDecimal executedQty = new BigDecimal(order.path("executedQty").asText("0"));
        BigDecimal cumulativeQuote = new BigDecimal(order.path("cummulativeQuoteQty").asText("0"));
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null) {
            liveArmed.set(false);
            halt("订单终态已确认，但交易规则不可用");
            return;
        }
        if (executedQty.signum() > 0 && !reconcileOrderTrades(orderId, side, executedQty, cumulativeQuote)) {
            int failures = tradeReconcileFailures.merge(orderId, 1, Integer::sum);
            if (failures < 3) {
                statusReason.set("订单 " + orderId + " 对账同步中（" + failures + "/3）");
                scheduleOrderReconciliation(orderId);
                return;
            }
            tradeReconcileFailures.remove(orderId);
            if (continueAfterConfirmedFlatSell(orderId, side, rule)) return;
            liveArmed.set(false);
            halt("订单 " + orderId + " 连续三次成交明细不一致，且无法确认安全空仓");
            return;
        }
        if (!reconcileInventory(rule, "订单 " + orderId + " REST 对账")) return;
        tradeReconcileFailures.remove(orderId);
        BigDecimal reconciledInventory = holdingInventory.get();
        markRestReconciled(orderId);
        clearActiveOrder();
        if ("BUY".equalsIgnoreCase(side)) {
            if (reconciledInventory.compareTo(rule.stepSize()) >= 0) currentStatus.set(ChurnStatus.SELLING);
            else { currentStatus.set(ChurnStatus.IDLE); resetEntryTarget(); }
        } else if (reconciledInventory.compareTo(rule.stepSize()) < 0) {
            completeFlatExit(true);
        } else {
            currentStatus.set(ChurnStatus.SELLING);
        }
    }

    private void completeFlatExit(boolean restReconciled) {
        holdingInventory.set(BigDecimal.ZERO);
        riskGuard.reconcileExchangeFlat(System.currentTimeMillis(), properties.getStrategy());
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null || !dailyStatsStore.reconcileFlatDust(credentialManager.currentAlias(),
                properties.getStrategy().getSymbol(), rule.stepSize())) {
            liveArmed.set(false);
            halt("交易所已空仓，但每日账本无法安全归零");
            return;
        }
        syncDailyCounters();
        currentStatus.set(ChurnStatus.IDLE);
        resetEntryTarget();
        statusReason.set(isRunning.get() ? "运行中，等待入场信号"
                : "人工授权市价清仓已完成" + (restReconciled ? "（REST 对账）" : ""));
    }

    private boolean continueAfterConfirmedFlatSell(long orderId, String side,
                                                    SymbolRuleManager.SymbolRule rule) {
        if (!"SELL".equalsIgnoreCase(side)) return false;
        BinanceOptimizedTradeService.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (balance == null) {
            BigDecimal free = tradeService.getFreeAssetBalance(baseAsset());
            if (free != null) balance = new BinanceOptimizedTradeService.AssetBalance(
                    baseAsset(), free, BigDecimal.ZERO, free);
        }
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (balance == null || openOrders == null || !openOrders.isEmpty()
                || balance.total().compareTo(rule.stepSize()) >= 0) return false;
        holdingInventory.set(balance.total());
        markRestReconciled(orderId);
        clearActiveOrder();
        completeFlatExit(true);
        if (currentStatus.get() != ChurnStatus.HALTED) {
            log.warn("订单 {} 的成交明细延迟，但交易所已确认空仓且无活动订单；继续运行", orderId);
        }
        return true;
    }

    private void clearActiveOrder() {
        Long orderId = activeOrderId.getAndSet(null);
        if (orderId != null) knownOrderIds.remove(orderId);
        if (orderId != null) tradeReconcileFailures.remove(orderId);
        String clientOrderId = activeClientOrderId.getAndSet(null);
        if (clientOrderId != null) pendingClientOrderIds.remove(clientOrderId);
        entryCancellationPending.set(false);
        activeOrderPrice.set(null);
        orderPlacedTimestamp.set(0);
    }

    private void clearTrackedOrders() {
        clearActiveOrder();
        knownOrderIds.clear();
        pendingClientOrderIds.clear();
        restReconciledOrderIds.clear();
        orderReconcileFailures.clear();
        tradeReconcileFailures.clear();
        replacingOrderId.set(null);
    }

    private void resetEntryTarget() {
        targetEntryQuantity.set(BigDecimal.ZERO);
        filledEntryQuantity.set(BigDecimal.ZERO);
        filledEntryQuoteQuantity.set(BigDecimal.ZERO);
    }

    private String nextClientOrderId(String side) {
        return "churn-" + side + "-" + Long.toUnsignedString(clientOrderSequence.incrementAndGet(), 36)
                + "-" + Long.toUnsignedString(System.currentTimeMillis(), 36);
    }

    private boolean isTerminal(String status) {
        return "FILLED".equals(status) || "CANCELED".equals(status) || "EXPIRED".equals(status)
                || "EXPIRED_IN_MATCH".equals(status) || "REJECTED".equals(status);
    }


    public record LiquidationResult(boolean accepted, Long orderId, BigDecimal quantity, String message) {
        private static LiquidationResult rejected(String message) {
            return new LiquidationResult(false, null, BigDecimal.ZERO, message);
        }
    }
    private void recordPaperCandidate(BigDecimal price, long nowMs) {
        long previous = lastPaperCandidateTimestamp.get();
        if (nowMs - previous < properties.getStrategy().getPaperEntryIntervalMs()) return;
        if (!lastPaperCandidateTimestamp.compareAndSet(previous, nowMs)) return;
        postFillOutcomeTracker.recordPaperCandidate(price, activeEntrySignalReason.get(), activeEntryContext.get(), nowMs);
        log.info("记录虚拟候选入场 @ {}；当前仅观测，不发送订单", price);
    }
    private void recordMarketBaseline(BigDecimal midPrice, long nowMs) {
        long previous = lastBenchmarkObservationTimestamp.get();
        if (nowMs - previous < properties.getStrategy().getBenchmarkObservationIntervalMs()) return;
        if (!lastBenchmarkObservationTimestamp.compareAndSet(previous, nowMs)) return;
        var context = marketSignalEvaluator.getMarketContext(nowMs);
        postFillOutcomeTracker.recordMarketBaseline(midPrice, context.decisionReason(), context, nowMs);
    }
    private void halt(String reason) { isRunning.set(false); currentStatus.set(ChurnStatus.HALTED); statusReason.set(reason); log.error("引擎进入保护停机: {}", reason); }
    private BigDecimal applyJitter(BigDecimal qty) { double j = properties.getStrategy().getRandomSizeJitter(); return j <= 0 ? qty : qty.multiply(BigDecimal.valueOf(1 + ThreadLocalRandom.current().nextDouble(-j, j))); }
    private boolean calibrateHoldings() {
        BinanceOptimizedTradeService.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (balance == null) {
            BigDecimal free = tradeService.getFreeAssetBalance(baseAsset());
            if (free == null) return false;
            balance = new BinanceOptimizedTradeService.AssetBalance(baseAsset(), free, BigDecimal.ZERO, free);
        }
        holdingInventory.set(balance.total());
        return true;
    }

    /**
     * Hot-switches only while fully stopped and disarmed. In LIVE mode both symbols are reconciled
     * against Binance before any local setting changes, so an old order can never be orphaned.
     */
    public synchronized SymbolSwitchResult switchSymbol(String requestedSymbol) {
        String target = requestedSymbol == null ? "" : requestedSymbol.trim().toUpperCase();
        String current = properties.getStrategy().getSymbol().toUpperCase();
        if (!target.matches("[A-Z0-9]{5,20}") || !target.endsWith("USDT")) {
            return SymbolSwitchResult.rejected(current, "交易对格式无效；当前策略仅支持 USDT 现货交易对");
        }
        if (target.equals(current)) return new SymbolSwitchResult(true, current, "交易对未变化");
        if (isRunning.get() || liveArmed.get() || activeOrderId.get() != null) {
            return SymbolSwitchResult.rejected(current, "请先停止策略并解除 LIVE，确认没有活动订单后再切换");
        }

        SymbolRuleManager.SymbolRule targetRule = ruleManager.refreshRule(target);
        if (targetRule == null) {
            return SymbolSwitchResult.rejected(current, "目标交易对不可交易或无法加载交易规则: " + target);
        }
        BigDecimal orderNotional = properties.getStrategy().getOrderAmountUsdt();
        if (orderNotional == null || orderNotional.compareTo(targetRule.minNotional()) < 0) {
            return SymbolSwitchResult.rejected(current, "当前单笔金额低于 " + target + " 的最小名义额 "
                    + targetRule.minNotional().toPlainString());
        }

        if (!properties.getStrategy().isObserveMode()) {
            JsonNode currentOrders = tradeService.getOpenOrders(current);
            JsonNode targetOrders = tradeService.getOpenOrders(target);
            if (currentOrders == null || targetOrders == null) {
                return SymbolSwitchResult.rejected(current, "无法确认旧/新交易对的活动订单，已拒绝切换");
            }
            if (!currentOrders.isEmpty() || !targetOrders.isEmpty()) {
                return SymbolSwitchResult.rejected(current, "旧或新交易对仍有活动订单，已拒绝切换");
            }
            SymbolRuleManager.SymbolRule currentRule = ruleManager.getRule(current);
            BinanceOptimizedTradeService.AssetBalance currentBalance = tradeService.getAssetBalance(baseAsset(current));
            BinanceOptimizedTradeService.AssetBalance targetBalance = tradeService.getAssetBalance(baseAsset(target));
            if (currentRule == null || currentBalance == null || targetBalance == null) {
                return SymbolSwitchResult.rejected(current, "无法确认旧/新标的账户余额，已拒绝切换");
            }
            if (currentBalance.total().compareTo(currentRule.stepSize()) >= 0) {
                return SymbolSwitchResult.rejected(current, "旧标的仍有持仓 " + currentBalance.total().toPlainString()
                        + " " + baseAsset(current) + "，必须先卖出");
            }
            if (targetBalance.total().compareTo(targetRule.stepSize()) >= 0) {
                return SymbolSwitchResult.rejected(current, "目标标的已有持仓 " + targetBalance.total().toPlainString()
                        + " " + baseAsset(target) + "，成本未知，拒绝自动接管");
            }
            DailyTradeStatsStore.DailyStatsSnapshot currentStats = dailyStatsStore.today(
                    credentialManager.currentAlias(), current);
            DailyTradeStatsStore.DailyStatsSnapshot targetStats = dailyStatsStore.today(
                    credentialManager.currentAlias(), target);
            if (currentStats.positionQty().compareTo(currentRule.stepSize()) >= 0
                    || targetStats.positionQty().compareTo(targetRule.stepSize()) >= 0) {
                return SymbolSwitchResult.rejected(current, "每日账本仍记录旧或新标的持仓，需先人工对账");
            }
        }

        try {
            dailyStatsStore.saveActiveSymbol(target);
        } catch (RuntimeException e) {
            log.error("保存目标交易对失败", e);
            return SymbolSwitchResult.rejected(current, "无法持久化目标交易对，已保持原交易对");
        }
        properties.getStrategy().setSymbol(target);
        clearTrackedOrders();
        resetEntryTarget();
        accountingLedger.reset();
        riskGuard.resetForFlatSymbol();
        marketSignalEvaluator.reset();
        postFillOutcomeTracker.reset();
        holdingInventory.set(BigDecimal.ZERO);
        commissionPriceCache.clear();
        lastBestBid.set(null);
        lastBestAsk.set(null);
        lastMidPrice.set(null);
        lastMarketDataTimestamp.set(0);
        lastBenchmarkObservationTimestamp.set(0);
        lastPaperCandidateTimestamp.set(0);
        activeEntryContext.set(null);
        activeEntrySignalReason.set("UNKNOWN");
        synchronized (inboundMarketMessage) { inboundMarketMessage.setLength(0); }
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set("已切换到 " + target + "，等待新行情后可启动");
        syncDailyCounters();
        restoreDailyRisk();
        forceMarketReconnect("交易对已切换到 " + target);
        log.warn("交易对已由 {} 切换为 {}；策略保持停止且 LIVE 未解锁", current, target);
        return new SymbolSwitchResult(true, target, statusReason.get());
    }

    /** Safely moves this stopped engine between server-configured Binance accounts. */
    public synchronized ApiProfileSwitchResult switchApiProfile(String requestedAlias) {
        String currentAlias = credentialManager.currentAlias();
        String targetAlias = requestedAlias == null ? "" : requestedAlias.trim();
        if (!credentialManager.contains(targetAlias)) {
            return ApiProfileSwitchResult.rejected(currentAlias, "未配置 API 别名: " + targetAlias);
        }
        if (targetAlias.equals(currentAlias)) {
            return new ApiProfileSwitchResult(true, currentAlias, "API 凭据未变化");
        }
        if (isRunning.get() || liveArmed.get() || activeOrderId.get() != null) {
            return ApiProfileSwitchResult.rejected(currentAlias, "请先停止策略并解除 LIVE，再切换 API");
        }
        String symbol = properties.getStrategy().getSymbol();
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
        if (rule == null) return ApiProfileSwitchResult.rejected(currentAlias, "当前交易规则不可用");

        JsonNode oldOrders = tradeService.getAllOpenOrders();
        BinanceOptimizedTradeService.AssetBalance oldBalance = tradeService.getAssetBalance(baseAsset());
        DailyTradeStatsStore.DailyStatsSnapshot oldStats = dailyStatsStore.today(currentAlias, symbol);
        if (oldOrders == null || !oldOrders.isArray() || oldBalance == null) {
            return ApiProfileSwitchResult.rejected(currentAlias, "无法确认当前账户订单或余额，已拒绝切换");
        }
        if (!oldOrders.isEmpty()) {
            return ApiProfileSwitchResult.rejected(currentAlias, "当前账户仍有活动订单，已拒绝切换");
        }
        if (holdingInventory.get().compareTo(rule.stepSize()) >= 0
                || oldBalance.total().compareTo(rule.stepSize()) >= 0
                || oldStats.positionQty().compareTo(rule.stepSize()) >= 0) {
            return ApiProfileSwitchResult.rejected(currentAlias, "当前账户或每日账本仍有标的持仓，必须先卖出并对账");
        }

        BinanceCredentialManager.CredentialSnapshot previous = credentialManager.current();
        credentialManager.activate(targetAlias);
        String failure = validateSelectedAccount(rule, symbol, targetAlias);
        if (failure == null && !userDataStreamService.reconnectForCredentialSwitch(12_000)) {
            failure = "目标账户成交流订阅失败";
        }
        if (failure != null) return rollbackApiProfile(previous, failure);

        try {
            dailyStatsStore.saveActiveApiAlias(targetAlias);
        } catch (RuntimeException e) {
            log.error("持久化 API 别名失败", e);
            return rollbackApiProfile(previous, "无法持久化 API 选择");
        }

        clearTrackedOrders();
        resetEntryTarget();
        accountingLedger.reset();
        riskGuard.resetForFlatSymbol();
        holdingInventory.set(BigDecimal.ZERO);
        commissionPriceCache.clear();
        tradeService.resetRequestWeight();
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set("已切换 API 到 " + targetAlias + "，策略保持停止");
        syncDailyCounters();
        restoreDailyRisk();
        log.warn("API 凭据已由 {} 切换为 {}；策略保持停止且 LIVE 未解锁", currentAlias, targetAlias);
        return new ApiProfileSwitchResult(true, targetAlias, statusReason.get());
    }

    private String validateSelectedAccount(SymbolRuleManager.SymbolRule rule, String symbol, String alias) {
        JsonNode account = tradeService.getAccountInfo();
        JsonNode orders = tradeService.getAllOpenOrders();
        BinanceOptimizedTradeService.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (account == null || account.has("code") || !account.path("canTrade").asBoolean(false)) {
            return "目标 API 无法读取账户或没有现货交易权限";
        }
        if (orders == null || !orders.isArray()) return "无法确认目标账户的活动订单";
        if (!orders.isEmpty()) return "目标账户存在活动订单，拒绝自动接管";
        if (balance == null) return "无法确认目标账户的标的余额";
        if (balance.total().compareTo(rule.stepSize()) >= 0) return "目标账户已有标的持仓，成本未知";
        DailyTradeStatsStore.DailyStatsSnapshot stats = dailyStatsStore.today(alias, symbol);
        if (stats.positionQty().compareTo(rule.stepSize()) >= 0) return "目标 API 的每日账本仍记录有持仓";
        return null;
    }

    private ApiProfileSwitchResult rollbackApiProfile(BinanceCredentialManager.CredentialSnapshot previous,
                                                       String failure) {
        credentialManager.restore(previous);
        boolean restored = userDataStreamService.reconnectForCredentialSwitch(12_000);
        liveArmed.set(false);
        if (restored) {
            currentStatus.set(ChurnStatus.IDLE);
            statusReason.set("API 切换失败，已回滚到 " + previous.alias() + ": " + failure);
        } else {
            currentStatus.set(ChurnStatus.HALTED);
            statusReason.set("API 切换失败且旧账户流恢复失败，需人工检查: " + failure);
        }
        log.error(statusReason.get());
        return ApiProfileSwitchResult.rejected(previous.alias(), statusReason.get());
    }

    private void syncDailyCounters() {
        try {
            DailyTradeStatsStore.DailyStatsSnapshot today = dailyStatsStore.today(
                    credentialManager.currentAlias(), properties.getStrategy().getSymbol());
            totalVolumeUsdt.set(today.totalVolumeQuote());
            roundTripsCompleted.set(today.roundTrips());
        } catch (RuntimeException e) {
            log.error("读取今日统计计数失败；成交写入结果不受影响", e);
        }
    }

    private void restoreDailyRisk() {
        DailyTradeStatsStore.DailyStatsSnapshot today = dailyStatsStore.today(
                credentialManager.currentAlias(), properties.getStrategy().getSymbol());
        riskGuard.restoreFlatDaily(today.netRealizedPnlQuote(), today.totalCommissionQuoteEquivalent(),
                today.date(), properties.getStrategy());
    }

    private String baseAsset() {
        return baseAsset(properties.getStrategy().getSymbol());
    }
    private String baseAsset(String requestedSymbol) {
        String symbol = requestedSymbol.toUpperCase();
        for (String quote : new String[]{"FDUSD", "USDT", "USDC", "BUSD", "BTC", "ETH"}) {
            if (symbol.endsWith(quote)) return symbol.substring(0, symbol.length() - quote.length());
        }
        return symbol;
    }
    private String quoteAsset() {
        String symbol = properties.getStrategy().getSymbol().toUpperCase();
        for (String quote : new String[]{"FDUSD", "USDT", "USDC", "BUSD", "BTC", "ETH"}) {
            if (symbol.endsWith(quote)) return quote;
        }
        return "USDT";
    }
    public String getSymbol() { return properties.getStrategy().getSymbol(); }
    public int getUsedApiWeight() { return tradeService.getUsedWeight1m().get(); }
    public MarketSignalEvaluator.EntryDecision getLastEntryDecision() { return marketSignalEvaluator.getLastDecision(); }
    public PostFillOutcomeTracker.OutcomeSummary getBaselineOutcomes() { return postFillOutcomeTracker.getBaselineSummary(); }
    public PostFillOutcomeTracker.OutcomeSummary getQualifiedSignalOutcomes() { return postFillOutcomeTracker.getQualifiedSignalSummary(); }
    public TradingRiskGuard.RiskSnapshot getRiskSnapshot() { return riskGuard.snapshot(); }
    public TradeAccountingLedger.AccountingSnapshot getAccountingSnapshot() { return accountingLedger.snapshot(); }
    public DailyTradeStatsStore.DailyStatsSnapshot getDailyStatsSnapshot() {
        return dailyStatsStore.today(credentialManager.currentAlias(), properties.getStrategy().getSymbol());
    }
    public java.util.List<DailyTradeStatsStore.DailyStatsSnapshot> getRecentDailyStats(int limit) {
        return dailyStatsStore.recent(credentialManager.currentAlias(), properties.getStrategy().getSymbol(), limit);
    }
    public String getApiKeyAlias() { return credentialManager.currentAlias(); }
    public java.util.List<BinanceCredentialManager.ProfileView> getApiProfiles() {
        return credentialManager.profileViews();
    }
    public String getRiskBlockReason() { return riskGuard.getEntryBlockReason(); }
    public String getExecutionMode() { return properties.getStrategy().getExecutionMode(); }
    public boolean isAccountStreamReady() { return userDataStreamService.isReady(); }
    public int getMinimumPaperObservations() { return properties.getStrategy().getMinPaperObservations(); }
    public MarketDataSnapshot getMarketDataSnapshot() {
        return new MarketDataSnapshot(lastBestBid.get(), lastBestAsk.get(), lastMidPrice.get(),
                lastMarketDataTimestamp.get(), lastMarketFrameTimestamp.get());
    }

    public record MarketDataSnapshot(BigDecimal bestBid, BigDecimal bestAsk, BigDecimal midPrice,
                                     long updatedAtMs, long lastFrameAtMs) { }
    public record SymbolSwitchResult(boolean accepted, String symbol, String message) {
        private static SymbolSwitchResult rejected(String current, String message) {
            return new SymbolSwitchResult(false, current, message);
        }
    }
    public record ApiProfileSwitchResult(boolean accepted, String alias, String message) {
        private static ApiProfileSwitchResult rejected(String current, String message) {
            return new ApiProfileSwitchResult(false, current, message);
        }
    }
    private record CommissionPrice(BigDecimal price, long updatedAtMs) { }
}
