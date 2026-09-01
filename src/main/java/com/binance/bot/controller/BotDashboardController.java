package com.binance.bot.controller;

import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.service.BinanceOptimizedTradeService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/bot")
public class BotDashboardController {

    private final HighFrequencyVolumeChurnEngine engine;
    private final BinanceProperties properties;
    private final BinanceOptimizedTradeService tradeService;

    public BotDashboardController(HighFrequencyVolumeChurnEngine engine, BinanceProperties properties,
                                  BinanceOptimizedTradeService tradeService) {
        this.engine = engine;
        this.properties = properties;
        this.tradeService = tradeService;
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.ofEntries(
                Map.entry("running", engine.getIsRunning().get()),
                Map.entry("status", engine.getCurrentStatus().get().name()),
                Map.entry("statusReason", engine.getStatusReason().get()),
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("liveArmed", engine.getLiveArmed().get()),
                Map.entry("accountStreamReady", engine.isAccountStreamReady()),
                Map.entry("apiKeyAlias", engine.getApiKeyAlias()),
                Map.entry("apiProfiles", engine.getApiProfiles()),
                Map.entry("minimumPaperObservations", engine.getMinimumPaperObservations()),
                Map.entry("symbol", engine.getSymbol()),
                Map.entry("totalVolumeUsdt", engine.getTotalVolumeUsdt().get()),
                Map.entry("roundTripsCompleted", engine.getRoundTripsCompleted().get()),
                Map.entry("usedApiWeight1m", engine.getUsedApiWeight()),
                Map.entry("marketData", engine.getMarketDataSnapshot()),
                Map.entry("entrySignal", engine.getLastEntryDecision()),
                Map.entry("marketBaseline", engine.getBaselineOutcomes()),
                Map.entry("qualifiedSignals", engine.getQualifiedSignalOutcomes()),
                Map.entry("risk", engine.getRiskSnapshot()),
                Map.entry("accounting", engine.getAccountingSnapshot()),
                Map.entry("dailyStats", engine.getDailyStatsSnapshot())
        );
    }

    @GetMapping("/stats/daily")
    public List<com.binance.bot.strategy.DailyTradeStatsStore.DailyStatsSnapshot> getDailyStats(
            @RequestParam(defaultValue = "30") int limit) {
        return engine.getRecentDailyStats(limit);
    }

    @GetMapping("/account")
    public ResponseEntity<?> getAccountSnapshot() {
        JsonNode account = tradeService.getAccountInfo();
        JsonNode allOrders = tradeService.getAllOrders(engine.getSymbol(), 100);
        JsonNode openOrders = tradeService.getOpenOrders(engine.getSymbol());
        if (account == null || allOrders == null || openOrders == null) {
            return ResponseEntity.status(502).body(Map.of("message", "账户或订单数据暂时不可用，请稍后重试"));
        }
        return ResponseEntity.ok(new AccountSnapshot(
                engine.getSymbol(),
                engine.getApiKeyAlias(),
                account.path("accountType").asText("SPOT"),
                account.path("canTrade").asBoolean(false),
                account.path("updateTime").asLong(0),
                nonZeroBalances(account.path("balances")),
                executedOrders(allOrders),
                orderViews(openOrders),
                engine.getUsedApiWeight()));
    }

    private List<BalanceView> nonZeroBalances(JsonNode balances) {
        List<BalanceView> result = new ArrayList<>();
        if (!balances.isArray()) return result;
        for (JsonNode balance : balances) {
            BigDecimal free = decimal(balance.path("free").asText("0"));
            BigDecimal locked = decimal(balance.path("locked").asText("0"));
            if (free.signum() == 0 && locked.signum() == 0) continue;
            result.add(new BalanceView(balance.path("asset").asText(), balance.path("free").asText("0"),
                    balance.path("locked").asText("0"), free.add(locked).toPlainString()));
        }
        result.sort(Comparator.comparing(BalanceView::total, this::compareDecimal).reversed());
        return result;
    }

    private List<OrderView> executedOrders(JsonNode orders) {
        List<OrderView> result = new ArrayList<>();
        if (!orders.isArray()) return result;
        for (JsonNode order : orders) {
            if (decimal(order.path("executedQty").asText("0")).signum() > 0) result.add(orderView(order));
        }
        result.sort(Comparator.comparingLong(OrderView::timeMs).reversed());
        return result;
    }

    private List<OrderView> orderViews(JsonNode orders) {
        List<OrderView> result = new ArrayList<>();
        if (!orders.isArray()) return result;
        for (JsonNode order : orders) result.add(orderView(order));
        result.sort(Comparator.comparingLong(OrderView::timeMs).reversed());
        return result;
    }

    private OrderView orderView(JsonNode order) {
        return new OrderView(order.path("orderId").asLong(0), order.path("clientOrderId").asText(""),
                order.path("side").asText(""), order.path("type").asText(""), order.path("status").asText(""),
                order.path("price").asText("0"), order.path("origQty").asText("0"),
                order.path("executedQty").asText("0"), order.path("cummulativeQuoteQty").asText("0"),
                order.path("time").asLong(order.path("updateTime").asLong(0)));
    }

    private BigDecimal decimal(String value) {
        try { return new BigDecimal(value); } catch (Exception ignored) { return BigDecimal.ZERO; }
    }

    private int compareDecimal(String left, String right) {
        return decimal(left).compareTo(decimal(right));
    }

    @PostMapping("/start")
    public ResponseEntity<String> startEngine() {
        return engine.startTrading() ? ResponseEntity.ok("引擎已启动")
                : ResponseEntity.status(409).body("引擎拒绝启动；请查看控制台的当前状态说明");
    }

    @PostMapping("/stop")
    public ResponseEntity<String> stopEngine() {
        boolean clean = engine.disarmLiveTrading();
        return clean ? ResponseEntity.ok("引擎已停止，LIVE 已解除，交易所确认无活动订单和标的持仓")
                : ResponseEntity.status(409).body("LIVE 已解除，但未能确认安全空仓；系统已保护停机，请人工对账");
    }

    @PostMapping("/live/arm")
    public ResponseEntity<String> armLive() {
        // The authenticated dashboard session or admin-token filter remains the authorization
        // boundary. LIVE arming intentionally has no additional password/confirmation prompt.
        return engine.armLiveTrading() ? ResponseEntity.ok("LIVE 已临时解锁；服务重启或停止后自动解除")
                : ResponseEntity.status(409).body("无法解锁：LIVE 双开关未配置或账户成交流未就绪");
    }

    @PostMapping("/live/disarm")
    public ResponseEntity<String> disarmLive() {
        boolean clean = engine.disarmLiveTrading();
        return clean ? ResponseEntity.ok("LIVE 已解除，交易所状态已确认")
                : ResponseEntity.status(409).body("LIVE 已解除，但订单或持仓仍需人工核对");
    }

    @PostMapping("/symbol")
    public ResponseEntity<HighFrequencyVolumeChurnEngine.SymbolSwitchResult> switchSymbol(
            @RequestBody SymbolSwitchRequest request) {
        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = engine.switchSymbol(request.symbol());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @GetMapping("/api-profiles")
    public List<com.binance.bot.config.BinanceCredentialManager.ProfileView> getApiProfiles() {
        return engine.getApiProfiles();
    }

    @PostMapping("/api-profile")
    public ResponseEntity<HighFrequencyVolumeChurnEngine.ApiProfileSwitchResult> switchApiProfile(
            @RequestBody ApiProfileSwitchRequest request) {
        HighFrequencyVolumeChurnEngine.ApiProfileSwitchResult result = engine.switchApiProfile(request.alias());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/liquidate")
    public ResponseEntity<HighFrequencyVolumeChurnEngine.LiquidationResult> liquidate(
            @RequestBody LiquidationRequest request) {
        if (!passwordMatches(request.password()) || !"SELL ALL BASE ASSET".equals(request.confirmation())) {
            return ResponseEntity.status(401).body(new HighFrequencyVolumeChurnEngine.LiquidationResult(
                    false, null, java.math.BigDecimal.ZERO, "二次验证失败"));
        }
        HighFrequencyVolumeChurnEngine.LiquidationResult result = engine.liquidateExistingPosition();
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    private boolean passwordMatches(String provided) {
        String expected = properties.getSecurity().getAdminPassword();
        return expected != null && provided != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
    }

    public record LiquidationRequest(String password, String confirmation) { }
    public record SymbolSwitchRequest(String symbol) { }
    public record ApiProfileSwitchRequest(String alias) { }
    public record AccountSnapshot(String symbol, String apiKeyAlias, String accountType,
                                  boolean canTrade, long accountUpdateTimeMs,
                                  List<BalanceView> balances, List<OrderView> filledOrders,
                                  List<OrderView> openOrders, int usedApiWeight1m) { }
    public record BalanceView(String asset, String free, String locked, String total) { }
    public record OrderView(long orderId, String clientOrderId, String side, String type, String status,
                            String price, String originalQty, String executedQty, String quoteQty, long timeMs) { }
}
