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
import java.util.concurrent.ThreadLocalRandom;
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

    public HighFrequencyVolumeChurnEngine(BinanceProperties properties, BinanceOptimizedTradeService tradeService,
                                           SymbolRuleManager ruleManager, UserDataStreamService userDataStreamService) {
        this.properties = properties;
        this.tradeService = tradeService;
        this.ruleManager = ruleManager;
        this.userDataStreamService = userDataStreamService;
    }

    @PostConstruct
    public void init() {
        userDataStreamService.setExecutionCallback(this::onOrderUpdate);
        connectMarketData();
    }

    private void connectMarketData() {
        String wsUrl = properties.getApi().getWsMarketUrl() + "/" + properties.getStrategy().getSymbol().toLowerCase() + "@bookTicker";
        HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create(wsUrl), this)
                .thenAccept(ws -> log.info("已连接盘口数据流: {}", wsUrl))
                .exceptionally(ex -> { log.error("盘口数据流连接失败", ex); return null; });
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

    @PreDestroy public void onShutdown() { stopTrading(); }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        if (!last) {
            log.warn("收到分片盘口消息，忽略以避免使用不完整行情");
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }
        try {
            JsonNode node = objectMapper.readTree(data.toString());
            if (node.has("b") && node.has("a")) {
                BigDecimal bid = new BigDecimal(node.get("b").asText());
                BigDecimal ask = new BigDecimal(node.get("a").asText());
                lastBestAsk.set(ask);
                if (isRunning.get()) driveChurnStateMachine(bid, ask);
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
                BigDecimal qty = buyQuantity(bestBid, rule);
                BigDecimal price = PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
                if (!isValidOrder(qty, price, rule)) return;
                JsonNode res = tradeService.cancelAndReplaceOrder(symbol, "BUY", price, qty, null);
                if (res != null && res.has("orderId")) trackOrder(res.get("orderId").asLong(), ChurnStatus.BUYING);
            }
            case BUYING -> {
                if (now - orderPlacedTimestamp.get() <= properties.getStrategy().getOrderTtlMs()) return;
                Long orderId = activeOrderId.get();
                if (orderId == null) { halt("买单状态没有活动订单"); return; }
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
            if ("BUY".equalsIgnoreCase(side)) holdingInventory.accumulateAndGet(lastFilledQty, BigDecimal::add);
            else holdingInventory.accumulateAndGet(lastFilledQty, BigDecimal::subtract);
        }
        if ("BUY".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.BUYING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus) || "EXPIRED".equals(orderStatus))) {
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
    private BigDecimal askPrice(BigDecimal ask, SymbolRuleManager.SymbolRule rule) {
        return PrecisionUtil.roundDownToStep(ask.add(rule.tickSize().multiply(BigDecimal.valueOf(Math.max(0, properties.getStrategy().getAskDepthOffsetTicks() - 1)))), rule.tickSize());
    }
    private boolean isValidOrder(BigDecimal qty, BigDecimal price, SymbolRuleManager.SymbolRule rule) {
        return qty != null && qty.compareTo(rule.stepSize()) >= 0 && price != null && price.signum() > 0 && qty.multiply(price).compareTo(rule.minNotional()) >= 0;
    }
    private void trackOrder(long orderId, ChurnStatus status) { activeOrderId.set(orderId); orderPlacedTimestamp.set(System.currentTimeMillis()); currentStatus.set(status); }
    private void halt(String reason) { isRunning.set(false); currentStatus.set(ChurnStatus.HALTED); log.error("引擎进入保护停机: {}", reason); }
    private BigDecimal applyJitter(BigDecimal qty) { double j = properties.getStrategy().getRandomSizeJitter(); return j <= 0 ? qty : qty.multiply(BigDecimal.valueOf(1 + ThreadLocalRandom.current().nextDouble(-j, j))); }
    private void calibrateHoldings() { String base = properties.getStrategy().getSymbol().replace("USDT", "").replace("FDUSD", "").replace("USDC", ""); holdingInventory.set(tradeService.getFreeAssetBalance(base)); }
    public String getSymbol() { return properties.getStrategy().getSymbol(); }
    public int getUsedApiWeight() { return tradeService.getUsedWeight1m().get(); }
}
