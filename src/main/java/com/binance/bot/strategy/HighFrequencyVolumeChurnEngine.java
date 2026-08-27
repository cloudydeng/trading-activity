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

@Slf4j
@Component
public class HighFrequencyVolumeChurnEngine implements WebSocket.Listener {
    private final BinanceProperties properties;
    private final BinanceOptimizedTradeService tradeService;
    private final SymbolRuleManager ruleManager;
    private final UserDataStreamService userDataStreamService;
    private final MarketSignalEvaluator marketSignalEvaluator;
    private final PostFillOutcomeTracker postFillOutcomeTracker;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicReference<BigDecimal> lastBestAsk = new AtomicReference<>();

    public enum ChurnStatus { IDLE, BUYING, SELLING, HALTED }

    @Getter private final AtomicBoolean isRunning = new AtomicBoolean(false);
    @Getter private final AtomicReference<ChurnStatus> currentStatus = new AtomicReference<>(ChurnStatus.IDLE);
    @Getter private final AtomicReference<BigDecimal> totalVolumeUsdt = new AtomicReference<>(BigDecimal.ZERO);
    @Getter private final AtomicLong roundTripsCompleted = new AtomicLong(0);
    private final AtomicReference<Long> activeOrderId = new AtomicReference<>();
    private final AtomicReference<BigDecimal> holdingInventory = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicLong orderPlacedTimestamp = new AtomicLong(0);
    private final AtomicBoolean entryCancellationPending = new AtomicBoolean(false);
    private final AtomicBoolean marketConnectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean acceptingMarketConnections = new AtomicBoolean(true);
    private final AtomicReference<String> activeEntrySignalReason = new AtomicReference<>("UNKNOWN");

    public HighFrequencyVolumeChurnEngine(BinanceProperties properties, BinanceOptimizedTradeService tradeService,
                                           SymbolRuleManager ruleManager, UserDataStreamService userDataStreamService,
                                           MarketSignalEvaluator marketSignalEvaluator, PostFillOutcomeTracker postFillOutcomeTracker) {
        this.properties = properties;
        this.tradeService = tradeService;
        this.ruleManager = ruleManager;
        this.userDataStreamService = userDataStreamService;
        this.marketSignalEvaluator = marketSignalEvaluator;
        this.postFillOutcomeTracker = postFillOutcomeTracker;
    }

    @PostConstruct
    public void init() {
        userDataStreamService.setExecutionCallback(this::onOrderUpdate);
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
        if (isRunning.get() && currentStatus.get() == ChurnStatus.BUYING) cancelActiveEntryOrder("行情流不可用");
        if (reconnectScheduled.compareAndSet(false, true)) {
            CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
                reconnectScheduled.set(false);
                connectMarketData();
            });
        }
    }

    public synchronized void startTrading() {
        if (isRunning.get()) return;
        calibrateHoldings();
        isRunning.set(true);
        currentStatus.set(holdingInventory.get().signum() > 0 ? ChurnStatus.SELLING : ChurnStatus.IDLE);
        orderPlacedTimestamp.set(0);
        log.info("引擎启动，当前标的持仓: {}", holdingInventory.get());
    }

    public synchronized void stopTrading() {
        isRunning.set(false);
        Long orderId = activeOrderId.get();
        if (orderId != null && !tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId)) {
            currentStatus.set(ChurnStatus.HALTED);
            log.error("停止时撤单失败；保留订单 ID {} 以避免误报安全停止", orderId);
            return;
        }
        activeOrderId.set(null);
        currentStatus.set(ChurnStatus.IDLE);
        log.info("引擎已停止。总交易量: {} USDT, 闭环轮数: {}", totalVolumeUsdt.get(), roundTripsCompleted.get());
    }

    @PreDestroy public void onShutdown() {
        acceptingMarketConnections.set(false);
        stopTrading();
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        if (!last) {
            log.warn("收到分片盘口消息，忽略以避免使用不完整行情");
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }
        try {
            JsonNode envelope = objectMapper.readTree(data.toString());
            JsonNode node = envelope.has("data") ? envelope.get("data") : envelope;
            if (node.has("b") && node.has("B") && node.has("a") && node.has("A")) {
                BigDecimal bid = new BigDecimal(node.get("b").asText());
                BigDecimal bidQty = new BigDecimal(node.get("B").asText());
                BigDecimal ask = new BigDecimal(node.get("a").asText());
                BigDecimal askQty = new BigDecimal(node.get("A").asText());
                lastBestAsk.set(ask);
                postFillOutcomeTracker.recordMarketPrice(bid.add(ask).divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP), System.currentTimeMillis());
                marketSignalEvaluator.recordQuote(bid, bidQty, ask, askQty, System.currentTimeMillis(), properties.getStrategy());
                if (isRunning.get()) driveChurnStateMachine(bid, ask);
            } else if (node.has("q") && node.has("m")) {
                marketSignalEvaluator.recordAggTrade(new BigDecimal(node.get("q").asText()), node.get("m").asBoolean(), System.currentTimeMillis(), properties.getStrategy());
            } else if (node.has("bids") && node.has("asks")) {
                marketSignalEvaluator.recordDepth(sumDepth(node.get("bids")), sumDepth(node.get("asks")), System.currentTimeMillis());
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
        String symbol = properties.getStrategy().getSymbol();
        switch (currentStatus.get()) {
            case IDLE -> {
                if (bestAsk.subtract(bestBid).compareTo(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getMinSpreadTicks()))) < 0) return;
                MarketSignalEvaluator.EntryDecision decision = marketSignalEvaluator.evaluate(now, properties.getStrategy());
                if (!decision.allowed()) {
                    log.debug("新开仓被信号层阻止: {} (book={}, depth={}, flow={}, returnBps={}, rangeBps={})", decision.reason(), decision.bookImbalance(), decision.depthImbalance(), decision.takerFlowImbalance(), decision.returnBps(), decision.rangeBps());
                    return;
                }
                activeEntrySignalReason.set(decision.reason());
                BigDecimal qty = buyQuantity(bestBid, rule);
                BigDecimal price = PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
                if (!isValidOrder(qty, price, rule)) return;
                JsonNode res = tradeService.cancelAndReplaceOrder(symbol, "BUY", price, qty, null);
                if (res != null && res.has("orderId")) trackOrder(res.get("orderId").asLong(), ChurnStatus.BUYING);
            }
            case BUYING -> {
                Long orderId = activeOrderId.get();
                if (orderId == null) { halt("买单状态没有活动订单"); return; }
                MarketSignalEvaluator.EntryDecision decision = marketSignalEvaluator.evaluate(now, properties.getStrategy());
                if (!decision.allowed()) {
                    cancelActiveEntryOrder("入场信号转为 " + decision.reason());
                    return;
                }
                if (entryCancellationPending.get() || now - orderPlacedTimestamp.get() <= properties.getStrategy().getOrderTtlMs()) return;
                BigDecimal qty = buyQuantity(bestBid, rule);
                BigDecimal price = PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
                if (!isValidOrder(qty, price, rule)) return;
                JsonNode res = tradeService.cancelAndReplaceOrder(symbol, "BUY", price, qty, orderId);
                if (res != null && res.has("newOrderResponse")) trackOrder(res.get("newOrderResponse").get("orderId").asLong(), ChurnStatus.BUYING);
            }
            case SELLING -> {
                if (now - orderPlacedTimestamp.get() <= properties.getStrategy().getOrderTtlMs()) return;
                BigDecimal qty = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
                BigDecimal price = askPrice(bestAsk, rule);
                if (!isValidOrder(qty, price, rule)) { halt("持仓不足以创建有效卖单"); return; }
                JsonNode res = tradeService.cancelAndReplaceOrder(symbol, "SELL", price, qty, activeOrderId.get());
                if (res != null && res.has("newOrderResponse")) trackOrder(res.get("newOrderResponse").get("orderId").asLong(), ChurnStatus.SELLING);
                else if (activeOrderId.get() == null && res != null && res.has("orderId")) trackOrder(res.get("orderId").asLong(), ChurnStatus.SELLING);
            }
            case HALTED -> { }
        }
    }

    private synchronized void onOrderUpdate(long orderId, String side, String executionType, String orderStatus,
                                            BigDecimal lastFilledQty, BigDecimal lastFilledPrice) {
        if (!isRunning.get() || !Long.valueOf(orderId).equals(activeOrderId.get())) return;
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null) { halt("未加载交易规则"); return; }
        if ("TRADE".equals(executionType) && lastFilledQty.signum() > 0) {
            totalVolumeUsdt.accumulateAndGet(lastFilledQty.multiply(lastFilledPrice), BigDecimal::add);
            if ("BUY".equalsIgnoreCase(side)) {
                holdingInventory.accumulateAndGet(lastFilledQty, BigDecimal::add);
                postFillOutcomeTracker.recordBuyFill(lastFilledPrice, activeEntrySignalReason.get(), System.currentTimeMillis());
            }
            else holdingInventory.accumulateAndGet(lastFilledQty, BigDecimal::subtract);
        }
        if ("BUY".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.BUYING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus) || "EXPIRED".equals(orderStatus))) {
            entryCancellationPending.set(false);
            activeOrderId.set(null);
            placeRecoverySell(rule, lastFilledPrice);
        } else if ("SELL".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.SELLING && "FILLED".equals(orderStatus)) {
            activeOrderId.set(null);
            if (holdingInventory.get().compareTo(rule.stepSize()) < 0) {
                holdingInventory.set(BigDecimal.ZERO);
                roundTripsCompleted.incrementAndGet();
                currentStatus.set(ChurnStatus.IDLE);
            } else placeRecoverySell(rule, lastFilledPrice);
        }
    }

    private void placeRecoverySell(SymbolRuleManager.SymbolRule rule, BigDecimal fallbackPrice) {
        BigDecimal qty = PrecisionUtil.roundDownToStep(holdingInventory.get(), rule.stepSize());
        BigDecimal price = lastBestAsk.get() == null ? fallbackPrice.add(rule.tickSize()) : askPrice(lastBestAsk.get(), rule);
        if (!isValidOrder(qty, price, rule)) { halt("成交后无法创建有效平仓单"); return; }
        JsonNode res = tradeService.cancelAndReplaceOrder(properties.getStrategy().getSymbol(), "SELL", price, qty, null);
        if (res != null && res.has("orderId")) trackOrder(res.get("orderId").asLong(), ChurnStatus.SELLING);
        else halt("成交后的平仓单被交易所拒绝");
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
    private BigDecimal askPrice(BigDecimal ask, SymbolRuleManager.SymbolRule rule) {
        return PrecisionUtil.roundDownToStep(ask.add(rule.tickSize().multiply(BigDecimal.valueOf(Math.max(0, properties.getStrategy().getAskDepthOffsetTicks() - 1)))), rule.tickSize());
    }
    private boolean isValidOrder(BigDecimal qty, BigDecimal price, SymbolRuleManager.SymbolRule rule) {
        return qty != null && qty.compareTo(rule.stepSize()) >= 0 && price != null && price.signum() > 0 && qty.multiply(price).compareTo(rule.minNotional()) >= 0;
    }
    private void cancelActiveEntryOrder(String reason) {
        Long orderId = activeOrderId.get();
        if (orderId == null || !entryCancellationPending.compareAndSet(false, true)) return;
        if (tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId)) {
            log.info("撤销活动买单 {}: {}", orderId, reason);
        } else {
            entryCancellationPending.set(false);
            log.error("撤销活动买单 {} 失败: {}", orderId, reason);
        }
    }
    private void trackOrder(long orderId, ChurnStatus status) { activeOrderId.set(orderId); entryCancellationPending.set(false); orderPlacedTimestamp.set(System.currentTimeMillis()); currentStatus.set(status); }
    private void halt(String reason) { isRunning.set(false); currentStatus.set(ChurnStatus.HALTED); log.error("引擎进入保护停机: {}", reason); }
    private BigDecimal applyJitter(BigDecimal qty) { double j = properties.getStrategy().getRandomSizeJitter(); return j <= 0 ? qty : qty.multiply(BigDecimal.valueOf(1 + ThreadLocalRandom.current().nextDouble(-j, j))); }
    private void calibrateHoldings() { String base = properties.getStrategy().getSymbol().replace("USDT", "").replace("FDUSD", "").replace("USDC", ""); holdingInventory.set(tradeService.getFreeAssetBalance(base)); }
    public String getSymbol() { return properties.getStrategy().getSymbol(); }
    public int getUsedApiWeight() { return tradeService.getUsedWeight1m().get(); }
    public MarketSignalEvaluator.EntryDecision getLastEntryDecision() { return marketSignalEvaluator.getLastDecision(); }
    public PostFillOutcomeTracker.OutcomeSummary getPostFillOutcomes() { return postFillOutcomeTracker.getSummary(); }
}
