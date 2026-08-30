package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
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
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicReference<BigDecimal> lastBestBid = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastBestAsk = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastMidPrice = new AtomicReference<>();
    private final AtomicLong lastMarketDataTimestamp = new AtomicLong(0);

    public enum ChurnStatus { IDLE, BUYING, SELLING, HALTED }

    @Getter private final AtomicBoolean isRunning = new AtomicBoolean(false);
    @Getter private final AtomicBoolean liveArmed = new AtomicBoolean(false);
    @Getter private final AtomicReference<ChurnStatus> currentStatus = new AtomicReference<>(ChurnStatus.IDLE);
    @Getter private final AtomicReference<BigDecimal> totalVolumeUsdt = new AtomicReference<>(BigDecimal.ZERO);
    @Getter private final AtomicLong roundTripsCompleted = new AtomicLong(0);
    private final AtomicReference<Long> activeOrderId = new AtomicReference<>();
    private final AtomicReference<BigDecimal> holdingInventory = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> targetEntryQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> filledEntryQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<String> activeClientOrderId = new AtomicReference<>();
    private final AtomicReference<Long> replacingOrderId = new AtomicReference<>();
    @Getter private final AtomicReference<String> statusReason = new AtomicReference<>("等待启动");
    private final Set<Long> knownOrderIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pendingClientOrderIds = ConcurrentHashMap.newKeySet();
    private final AtomicLong orderPlacedTimestamp = new AtomicLong(0);
    private final AtomicLong nextOrderAttemptAt = new AtomicLong(0);
    private final AtomicLong clientOrderSequence = new AtomicLong(0);
    private final AtomicBoolean entryCancellationPending = new AtomicBoolean(false);
    private final AtomicBoolean marketConnectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean acceptingMarketConnections = new AtomicBoolean(true);
    private final AtomicReference<String> activeEntrySignalReason = new AtomicReference<>("UNKNOWN");
    private final AtomicReference<MarketSignalEvaluator.MarketContext> activeEntryContext = new AtomicReference<>();
    private final StringBuilder inboundMarketMessage = new StringBuilder();
    private final AtomicLong lastBenchmarkObservationTimestamp = new AtomicLong(0);
    private final AtomicLong lastPaperCandidateTimestamp = new AtomicLong(0);

    public HighFrequencyVolumeChurnEngine(BinanceProperties properties, BinanceOptimizedTradeService tradeService,
                                           SymbolRuleManager ruleManager, UserDataStreamService userDataStreamService,
                                           MarketSignalEvaluator marketSignalEvaluator, PostFillOutcomeTracker postFillOutcomeTracker,
                                           TradingRiskGuard riskGuard) {
        this.properties = properties;
        this.tradeService = tradeService;
        this.ruleManager = ruleManager;
        this.userDataStreamService = userDataStreamService;
        this.marketSignalEvaluator = marketSignalEvaluator;
        this.postFillOutcomeTracker = postFillOutcomeTracker;
        this.riskGuard = riskGuard;
    }

    @PostConstruct
    public void init() {
        userDataStreamService.setExecutionCallback(this::onOrderUpdate);
        userDataStreamService.setStreamLifecycleCallback(this::handleUserStreamLoss);
        connectMarketData();
    }

    private void connectMarketData() {
        if (!acceptingMarketConnections.get()) return;
        if (!marketConnectInProgress.compareAndSet(false, true)) return;
        String symbol = properties.getStrategy().getSymbol().toLowerCase();
        String baseUrl = properties.getApi().getWsMarketUrl().replaceFirst("/ws/?$", "");
        String wsUrl = baseUrl + "/stream?streams=" + symbol + "@bookTicker/" + symbol + "@aggTrade/" + symbol + "@depth5@100ms";
        HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create(wsUrl), this)
                .thenAccept(ws -> {
                    marketConnectInProgress.set(false);
                    log.info("已连接盘口数据流: {}", wsUrl);
                })
                .exceptionally(ex -> {
                    marketConnectInProgress.set(false);
                    handleMarketStreamLoss("连接失败: " + ex.getMessage());
                    return null;
                });
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        handleMarketStreamLoss("连接关闭 " + statusCode + ": " + reason);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        handleMarketStreamLoss("连接错误: " + error.getMessage());
    }

    private void handleMarketStreamLoss(String reason) {
        if (!acceptingMarketConnections.get()) return;
        marketSignalEvaluator.reset();
        log.warn("行情流不可用: {}", reason);
        if (isRunning.get() && !properties.getStrategy().isObserveMode()) protectOnStreamLoss("行情流不可用: " + reason);
        if (reconnectScheduled.compareAndSet(false, true)) {
            CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
                reconnectScheduled.set(false);
                connectMarketData();
            });
        }
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
        if (properties.getApi().getApiKey() == null || properties.getApi().getApiKey().isBlank()
                || properties.getApi().getSecretKey() == null || properties.getApi().getSecretKey().isBlank()) {
            log.error("拒绝启动：真实执行缺少 API 凭据");
            return false;
        }
        if (!userDataStreamService.isReady()) {
            halt("账户成交流未就绪，拒绝启动");
            liveArmed.set(false);
            return false;
        }
        long marketAgeMs = System.currentTimeMillis() - lastMarketDataTimestamp.get();
        if (lastMarketDataTimestamp.get() <= 0 || marketAgeMs > properties.getStrategy().getMarketDataStaleMs()) {
            halt("盘口行情未就绪或已过期，拒绝启动");
            liveArmed.set(false);
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

    @PreDestroy public void onShutdown() {
        acceptingMarketConnections.set(false);
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
        String payload;
        synchronized (inboundMarketMessage) {
            inboundMarketMessage.append(data);
            if (!last) return CompletableFuture.completedFuture(null);
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
                MarketSignalEvaluator.EntryDecision decision = marketSignalEvaluator.evaluate(now, properties.getStrategy());
                if (!decision.allowed()) {
                    log.debug("新开仓被信号层阻止: {} (book={}, depth={}, flow={}, returnBps={}, rangeBps={})", decision.reason(), decision.bookImbalance(), decision.depthImbalance(), decision.takerFlowImbalance(), decision.returnBps(), decision.rangeBps());
                    return;
                }
                activeEntrySignalReason.set(decision.reason());
                activeEntryContext.set(marketSignalEvaluator.getMarketContext(now));
                BigDecimal qty = buyQuantity(bestBid, rule);
                BigDecimal price = PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
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
                MarketSignalEvaluator.EntryDecision decision = marketSignalEvaluator.evaluate(now, properties.getStrategy());
                if (!decision.allowed()) {
                    long restingMs = now - orderPlacedTimestamp.get();
                    long softCancelAfterMs = Math.max(properties.getStrategy().getOrderTtlMs(),
                            properties.getStrategy().getMinEntryOrderRestMs());
                    if (isHardEntryRisk(decision.reason())) {
                        cancelActiveEntryOrder("入场硬风险转为 " + decision.reason());
                    } else if (restingMs >= softCancelAfterMs) {
                        cancelActiveEntryOrder("软信号转弱且挂单已到 TTL: " + decision.reason());
                    } else {
                        log.debug("忽略买单短时软信号噪声: {}，已驻留 {} ms", decision.reason(), restingMs);
                    }
                    return;
                }
                long restingMs = now - orderPlacedTimestamp.get();
                if (entryCancellationPending.get()
                        || restingMs < properties.getStrategy().getMinEntryOrderRestMs()
                        || restingMs <= properties.getStrategy().getOrderTtlMs()) return;
                BigDecimal qty = PrecisionUtil.roundDownToStep(
                        targetEntryQuantity.get().subtract(filledEntryQuantity.get()).max(BigDecimal.ZERO), rule.stepSize());
                BigDecimal price = PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
                if (!isValidOrder(qty, price, rule)) {
                    cancelActiveEntryOrder("目标数量的剩余部分不足以继续挂单");
                    return;
                }
                if (!riskGuard.permitsNewEntry(qty, price, now, properties.getStrategy())) {
                    cancelActiveEntryOrder("风险熔断: " + riskGuard.getEntryBlockReason());
                    return;
                }
                submitMakerOrder(symbol, "BUY", price, qty, orderId, ChurnStatus.BUYING);
            }
            case SELLING -> {
                ExitReason exitReason = exitReason(bestBid, now);
                if (exitReason == ExitReason.NONE) return;
                BigDecimal qty = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
                if (qty.compareTo(rule.stepSize()) < 0) { halt("持仓不足以创建有效卖单"); return; }
                if (exitReason.emergency) {
                    emergencyExit(symbol, qty, rule, exitReason);
                    return;
                }
                if (activeOrderId.get() != null && now - orderPlacedTimestamp.get() <= properties.getStrategy().getOrderTtlMs()) return;
                BigDecimal price = exitPrice(bestBid, bestAsk, rule);
                if (!isValidOrder(qty, price, rule)) { halt("止盈卖单低于交易所最小名义额"); return; }
                submitMakerOrder(symbol, "SELL", price, qty, activeOrderId.get(), ChurnStatus.SELLING);
            }
            case HALTED -> { }
        }
    }

    private synchronized void onOrderUpdate(long orderId, String clientOrderId, String side, String executionType,
                                            String orderStatus, BigDecimal lastFilledQty, BigDecimal lastFilledPrice,
                                            BigDecimal commission, String commissionAsset) {
        boolean replacingOldEvent = Long.valueOf(orderId).equals(replacingOrderId.get())
                && (clientOrderId == null || !clientOrderId.equals(activeClientOrderId.get()));
        boolean activeEvent = !replacingOldEvent && (Long.valueOf(orderId).equals(activeOrderId.get())
                || (clientOrderId != null && clientOrderId.equals(activeClientOrderId.get())));
        boolean knownEvent = activeEvent || knownOrderIds.contains(orderId)
                || (clientOrderId != null && pendingClientOrderIds.contains(clientOrderId));
        if (!knownEvent) {
            if ("TRADE".equals(executionType) && lastFilledQty.signum() > 0) {
                liveArmed.set(false);
                halt("收到未关联订单的成交回报 " + orderId + "，需人工对账");
            }
            return;
        }
        knownOrderIds.add(orderId);
        if (activeEvent && activeOrderId.get() == null) activeOrderId.set(orderId);
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null) { halt("未加载交易规则"); return; }
        if ("TRADE".equals(executionType) && lastFilledQty.signum() > 0) {
            totalVolumeUsdt.accumulateAndGet(lastFilledQty.multiply(lastFilledPrice), BigDecimal::add);
            if ("BUY".equalsIgnoreCase(side)) {
                filledEntryQuantity.accumulateAndGet(lastFilledQty, BigDecimal::add);
                BigDecimal netQty = lastFilledQty;
                if (baseAsset().equalsIgnoreCase(commissionAsset)) netQty = netQty.subtract(commission).max(BigDecimal.ZERO);
                holdingInventory.accumulateAndGet(netQty, BigDecimal::add);
                if (properties.getStrategy().isCollectObservations()) postFillOutcomeTracker.recordBuyFill(lastFilledPrice, activeEntrySignalReason.get(), activeEntryContext.get(), System.currentTimeMillis());
            } else {
                BigDecimal inventoryReduction = lastFilledQty;
                if (baseAsset().equalsIgnoreCase(commissionAsset)) inventoryReduction = inventoryReduction.add(commission);
                holdingInventory.accumulateAndGet(inventoryReduction, BigDecimal::subtract);
                if (holdingInventory.get().signum() < 0) holdingInventory.set(BigDecimal.ZERO);
            }
            riskGuard.recordFill(side, lastFilledQty, lastFilledPrice, System.currentTimeMillis(), properties.getStrategy());
            if (riskGuard.getEntryBlockReason() != null) log.warn("风险熔断已触发: {}", riskGuard.getEntryBlockReason());
        }
        if (activeEvent && "BUY".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.BUYING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus) || "EXPIRED".equals(orderStatus))) {
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) >= 0) currentStatus.set(ChurnStatus.SELLING);
            else { currentStatus.set(ChurnStatus.IDLE); resetEntryTarget(); }
        } else if (activeEvent && "SELL".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.SELLING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus)
                || "EXPIRED".equals(orderStatus) || "EXPIRED_IN_MATCH".equals(orderStatus))) {
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) < 0) {
                holdingInventory.set(BigDecimal.ZERO);
                roundTripsCompleted.incrementAndGet();
                currentStatus.set(ChurnStatus.IDLE);
                resetEntryTarget();
            } else {
                // A partial fill leaves one position and no active order; the next market tick
                // routes it through the same target/stop/time exit policy.
                currentStatus.set(ChurnStatus.SELLING);
            }
        }
        if (isTerminal(orderStatus)) {
            knownOrderIds.remove(orderId);
            if (clientOrderId != null) pendingClientOrderIds.remove(clientOrderId);
        }
    }

    private ExitReason exitReason(BigDecimal bestBid, long nowMs) {
        var risk = riskGuard.snapshot();
        if (risk.positionQty() == null || risk.positionQty().signum() <= 0 || risk.positionCostUsdt() == null) return ExitReason.NONE;
        BigDecimal cost = risk.positionCostUsdt().divide(risk.positionQty(), java.math.MathContext.DECIMAL64);
        BigDecimal takeProfit = cost.multiply(BigDecimal.ONE.add(BigDecimal.valueOf(properties.getStrategy().getTakeProfitBps()).movePointLeft(4)));
        BigDecimal stopLoss = cost.multiply(BigDecimal.ONE.subtract(BigDecimal.valueOf(properties.getStrategy().getStopLossBps()).movePointLeft(4)));
        boolean timedOut = risk.positionOpenedAtMs() > 0 && nowMs - risk.positionOpenedAtMs() >= properties.getStrategy().getMaxHoldingMs();
        if (bestBid.compareTo(stopLoss) <= 0) return ExitReason.STOP_LOSS;
        if (timedOut) return ExitReason.MAX_HOLDING;
        if (bestBid.compareTo(takeProfit) >= 0) return ExitReason.TAKE_PROFIT;
        return ExitReason.NONE;
    }
    private BigDecimal exitPrice(BigDecimal bestBid, BigDecimal ask, SymbolRuleManager.SymbolRule rule) {
        var risk = riskGuard.snapshot();
        BigDecimal cost = risk.positionCostUsdt().divide(risk.positionQty(), java.math.MathContext.DECIMAL64);
        BigDecimal target = cost.multiply(BigDecimal.ONE.add(BigDecimal.valueOf(properties.getStrategy().getTakeProfitBps()).movePointLeft(4)));
        // At target, preserve the desired minimum gain; on stop/time exit, stay passive at best ask.
        return PrecisionUtil.roundDownToStep(bestBid.compareTo(target) >= 0 ? ask.max(target) : ask, rule.tickSize());
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
        } finally {
            replacingOrderId.compareAndSet(cancelOrderId, null);
        }
    }

    private void handleMakerResponse(JsonNode response, Long oldOrderId, String clientOrderId, ChurnStatus status) {
        if (response == null) {
            reconcileAmbiguousSubmission(clientOrderId, status, "报单请求结果未知");
            return;
        }
        JsonNode order = oldOrderId == null ? response : response.path("newOrderResponse");
        boolean newOrderSucceeded = oldOrderId == null ? response.has("orderId")
                : "SUCCESS".equals(response.path("newOrderResult").asText()) && order.has("orderId");
        if (newOrderSucceeded) {
            long orderId = order.get("orderId").asLong();
            if (clientOrderId.equals(activeClientOrderId.get())) trackOrder(orderId, clientOrderId, status);
            return;
        }
        pendingClientOrderIds.remove(clientOrderId);
        if (oldOrderId != null && "SUCCESS".equals(response.path("cancelResult").asText())) {
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
        if (oldOrderId != null && "FAILURE".equals(response.path("cancelResult").asText())) {
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
            return;
        }
        pendingClientOrderIds.remove(clientOrderId);
        activeClientOrderId.compareAndSet(clientOrderId, null);
        liveArmed.set(false);
        halt(reason + "；交易所未发现对应活动订单，需核对成交历史");
    }

    private void emergencyExit(String symbol, BigDecimal requestedQty, SymbolRuleManager.SymbolRule rule, ExitReason reason) {
        Long makerOrderId = activeOrderId.get();
        if (makerOrderId != null) {
            JsonNode cancel = tradeService.cancelOrder(symbol, makerOrderId);
            JsonNode finalOrder = tradeService.getOrder(symbol, makerOrderId);
            if (cancel == null || finalOrder == null || !isTerminal(finalOrder.path("status").asText())) {
                liveArmed.set(false);
                halt("紧急退出前无法确认原卖单已撤销");
                return;
            }
            clearActiveOrder();
        }
        BigDecimal freeBalance = tradeService.getFreeAssetBalance(baseAsset());
        if (freeBalance == null) {
            liveArmed.set(false);
            halt("紧急退出前无法确认可卖余额");
            return;
        }
        BigDecimal qty = PrecisionUtil.roundDownToStep(freeBalance.min(requestedQty), rule.stepSize());
        if (qty.compareTo(rule.stepSize()) < 0) {
            halt("紧急退出余额不足，需人工核对");
            return;
        }
        String clientOrderId = nextClientOrderId("SELLM");
        pendingClientOrderIds.add(clientOrderId);
        activeClientOrderId.set(clientOrderId);
        JsonNode response = tradeService.placeMarketSell(symbol, qty, clientOrderId);
        if (response != null && response.has("orderId")) {
            trackOrder(response.get("orderId").asLong(), clientOrderId, ChurnStatus.SELLING);
            log.warn("触发 {}，已提交紧急市价减仓 {} {}", reason, qty, baseAsset());
        } else {
            reconcileAmbiguousSubmission(clientOrderId, ChurnStatus.SELLING, "紧急市价单结果未知");
        }
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
        if (executedQty.signum() > 0) {
            halt("撤单回报遗漏了部分成交，需人工核对持仓");
            return;
        }
        if ("CANCELED".equals(status) || "EXPIRED".equals(status) || "EXPIRED_IN_MATCH".equals(status)) {
            clearActiveOrder();
            if (holdingInventory.get().signum() > 0) currentStatus.set(ChurnStatus.SELLING);
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
    }

    private void clearActiveOrder() {
        Long orderId = activeOrderId.getAndSet(null);
        if (orderId != null) knownOrderIds.remove(orderId);
        String clientOrderId = activeClientOrderId.getAndSet(null);
        if (clientOrderId != null) pendingClientOrderIds.remove(clientOrderId);
        entryCancellationPending.set(false);
        orderPlacedTimestamp.set(0);
    }

    private void clearTrackedOrders() {
        clearActiveOrder();
        knownOrderIds.clear();
        pendingClientOrderIds.clear();
        replacingOrderId.set(null);
    }

    private void resetEntryTarget() {
        targetEntryQuantity.set(BigDecimal.ZERO);
        filledEntryQuantity.set(BigDecimal.ZERO);
    }

    private String nextClientOrderId(String side) {
        return "churn-" + side + "-" + Long.toUnsignedString(clientOrderSequence.incrementAndGet(), 36)
                + "-" + Long.toUnsignedString(System.currentTimeMillis(), 36);
    }

    private boolean isTerminal(String status) {
        return "FILLED".equals(status) || "CANCELED".equals(status) || "EXPIRED".equals(status)
                || "EXPIRED_IN_MATCH".equals(status) || "REJECTED".equals(status);
    }

    private enum ExitReason {
        NONE(false), TAKE_PROFIT(false), STOP_LOSS(true), MAX_HOLDING(true);
        private final boolean emergency;
        ExitReason(boolean emergency) { this.emergency = emergency; }
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
        BigDecimal balance = tradeService.getFreeAssetBalance(baseAsset());
        if (balance == null) return false;
        holdingInventory.set(balance);
        return true;
    }
    private String baseAsset() {
        String symbol = properties.getStrategy().getSymbol().toUpperCase();
        for (String quote : new String[]{"FDUSD", "USDT", "USDC", "BUSD", "BTC", "ETH"}) {
            if (symbol.endsWith(quote)) return symbol.substring(0, symbol.length() - quote.length());
        }
        return symbol;
    }
    public String getSymbol() { return properties.getStrategy().getSymbol(); }
    public int getUsedApiWeight() { return tradeService.getUsedWeight1m().get(); }
    public MarketSignalEvaluator.EntryDecision getLastEntryDecision() { return marketSignalEvaluator.getLastDecision(); }
    public PostFillOutcomeTracker.OutcomeSummary getBaselineOutcomes() { return postFillOutcomeTracker.getBaselineSummary(); }
    public PostFillOutcomeTracker.OutcomeSummary getQualifiedSignalOutcomes() { return postFillOutcomeTracker.getQualifiedSignalSummary(); }
    public TradingRiskGuard.RiskSnapshot getRiskSnapshot() { return riskGuard.snapshot(); }
    public String getRiskBlockReason() { return riskGuard.getEntryBlockReason(); }
    public String getExecutionMode() { return properties.getStrategy().getExecutionMode(); }
    public boolean isAccountStreamReady() { return userDataStreamService.isReady(); }
    public int getMinimumPaperObservations() { return properties.getStrategy().getMinPaperObservations(); }
    public MarketDataSnapshot getMarketDataSnapshot() {
        return new MarketDataSnapshot(lastBestBid.get(), lastBestAsk.get(), lastMidPrice.get(), lastMarketDataTimestamp.get());
    }

    public record MarketDataSnapshot(BigDecimal bestBid, BigDecimal bestAsk, BigDecimal midPrice, long updatedAtMs) { }
}
