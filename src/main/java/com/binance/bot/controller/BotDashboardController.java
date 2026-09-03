package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@RestController
public class BotDashboardController {
    private final TradingAccountManager accountManager;
    private final BinanceProperties properties;
    private final TradeNotificationService notificationService;

    public BotDashboardController(TradingAccountManager accountManager, BinanceProperties properties,
                                  TradeNotificationService notificationService) {
        this.accountManager = accountManager;
        this.properties = properties;
        this.notificationService = notificationService;
    }

    @GetMapping("/api/accounts")
    public List<TradingAccountManager.AccountSummary> accounts() { return accountManager.summaries(); }

    @GetMapping("/api/accounts/{accountId}/status")
    public ResponseEntity<?> status(@PathVariable String accountId) {
        return runtime(accountId).<ResponseEntity<?>>map(value -> ResponseEntity.ok(statusOf(value)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/{accountId}/stats/daily")
    public ResponseEntity<?> dailyStats(@PathVariable String accountId,
                                        @RequestParam(defaultValue = "30") int limit) {
        return runtime(accountId).<ResponseEntity<?>>map(value -> ResponseEntity.ok(
                        value.engine().getRecentDailyStats(Math.max(1, Math.min(366, limit)))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/{accountId}/notifications")
    public ResponseEntity<?> notifications(@PathVariable String accountId,
                                           @RequestParam(defaultValue = "100") int limit) {
        if (runtime(accountId).isEmpty()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(notificationService.recentFills(accountId, Math.max(1, Math.min(500, limit))));
    }

    @GetMapping("/api/accounts/{accountId}/account")
    public ResponseEntity<?> account(@PathVariable String accountId) {
        return runtime(accountId).map(this::accountSnapshot).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/api/accounts/{accountId}/start")
    public ResponseEntity<?> start(@PathVariable String accountId) {
        return runtime(accountId).map(value -> value.start()
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "引擎已启动"))
                        : ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", value.engine().getStatusReason().get())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/api/accounts/{accountId}/stop")
    public ResponseEntity<?> stop(@PathVariable String accountId) {
        return runtime(accountId).map(value -> value.stop()
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "引擎已停止并解除 LIVE"))
                        : ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", value.engine().getStatusReason().get())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping({"/api/accounts/{accountId}/arm", "/api/accounts/{accountId}/live/arm"})
    public ResponseEntity<?> arm(@PathVariable String accountId) {
        return runtime(accountId).map(value -> value.arm()
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "LIVE 已为当前账号临时解锁"))
                        : ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", "LIVE 双开关未配置或当前账号成交流未就绪")))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping({"/api/accounts/{accountId}/disarm", "/api/accounts/{accountId}/live/disarm"})
    public ResponseEntity<?> disarm(@PathVariable String accountId) { return stop(accountId); }

    @PostMapping("/api/accounts/{accountId}/symbol")
    public ResponseEntity<?> switchSymbol(@PathVariable String accountId, @RequestBody SymbolSwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = runtime.get().engine().switchSymbol(request.symbol());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/{accountId}/liquidate")
    public ResponseEntity<?> liquidate(@PathVariable String accountId, @RequestBody LiquidationRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        if (!passwordMatches(request.password()) || !"SELL ALL BASE ASSET".equals(request.confirmation())) {
            return ResponseEntity.status(401).body(Map.of("accepted", false, "message", "二次验证失败"));
        }
        HighFrequencyVolumeChurnEngine.LiquidationResult result = runtime.get().engine().liquidateExistingPosition();
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/start-all")
    public Map<String, TradingAccountManager.OperationResult> startAll() { return accountManager.startAll(); }

    @PostMapping("/api/accounts/arm-all")
    public Map<String, TradingAccountManager.OperationResult> armAll() { return accountManager.armAll(); }

    @PostMapping("/api/accounts/stop-all")
    public Map<String, TradingAccountManager.OperationResult> stopAll() { return accountManager.stopAll(); }

    /* Legacy dashboard routes select a stable first runtime; credentials are never hot-switched. */
    @GetMapping("/api/bot/status")
    public ResponseEntity<?> legacyStatus() {
        return defaultRuntime().<ResponseEntity<?>>map(value -> ResponseEntity.ok(statusOf(value)))
                .orElseGet(this::noAccount);
    }

    @GetMapping("/api/bot/stats/daily")
    public ResponseEntity<?> legacyDaily(@RequestParam(defaultValue = "30") int limit) {
        return defaultRuntime().<ResponseEntity<?>>map(value -> ResponseEntity.ok(
                        value.engine().getRecentDailyStats(Math.max(1, Math.min(366, limit)))))
                .orElseGet(this::noAccount);
    }

    @GetMapping("/api/bot/account")
    public ResponseEntity<?> legacyAccount() {
        return defaultRuntime().map(this::accountSnapshot).orElseGet(this::noAccount);
    }

    @PostMapping("/api/bot/start")
    public ResponseEntity<?> legacyStart() {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? start(runtime.get().accountId()) : noAccount();
    }

    @PostMapping("/api/bot/stop")
    public ResponseEntity<?> legacyStop() {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? stop(runtime.get().accountId()) : noAccount();
    }

    @PostMapping("/api/bot/live/arm")
    public ResponseEntity<?> legacyArm() {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? arm(runtime.get().accountId()) : noAccount();
    }

    @PostMapping("/api/bot/live/disarm")
    public ResponseEntity<?> legacyDisarm() { return legacyStop(); }

    @PostMapping("/api/bot/symbol")
    public ResponseEntity<?> legacySymbol(@RequestBody SymbolSwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? switchSymbol(runtime.get().accountId(), request) : noAccount();
    }

    @PostMapping("/api/bot/liquidate")
    public ResponseEntity<?> legacyLiquidate(@RequestBody LiquidationRequest request) {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? liquidate(runtime.get().accountId(), request) : noAccount();
    }

    private Map<String, Object> statusOf(AccountTradingRuntime runtime) {
        HighFrequencyVolumeChurnEngine engine = runtime.engine();
        return Map.ofEntries(
                Map.entry("accountId", runtime.accountId()), Map.entry("apiKeyAlias", runtime.alias()),
                Map.entry("running", engine.getIsRunning().get()),
                Map.entry("status", engine.getCurrentStatus().get().name()),
                Map.entry("statusReason", engine.getStatusReason().get()),
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("liveArmed", engine.getLiveArmed().get()),
                Map.entry("accountStreamReady", engine.isAccountStreamReady()),
                Map.entry("symbol", engine.getSymbol()),
                Map.entry("totalVolumeUsdt", engine.getTotalVolumeUsdt().get()),
                Map.entry("roundTripsCompleted", engine.getRoundTripsCompleted().get()),
                Map.entry("usedApiWeight1m", engine.getUsedApiWeight()),
                Map.entry("apiWeightLimit1m", engine.getApiWeightLimit()),
                Map.entry("apiWeightEntrySafeLimit1m", engine.getApiWeightEntrySafeLimit()),
                Map.entry("marketData", engine.getMarketDataSnapshot()),
                Map.entry("entrySignal", engine.getLastEntryDecision()),
                Map.entry("marketBaseline", engine.getBaselineOutcomes()),
                Map.entry("qualifiedSignals", engine.getQualifiedSignalOutcomes()),
                Map.entry("risk", engine.getRiskSnapshot()),
                Map.entry("accounting", engine.getAccountingSnapshot()),
                Map.entry("dailyStats", engine.getDailyStatsSnapshot()));
    }

    private ResponseEntity<?> accountSnapshot(AccountTradingRuntime runtime) {
        HighFrequencyVolumeChurnEngine engine = runtime.engine();
        BinanceAccountTradeClient tradeService = runtime.tradeClient();
        JsonNode account = tradeService.getAccountInfo();
        JsonNode allOrders = tradeService.getAllOrders(engine.getSymbol(), 100);
        JsonNode openOrders = tradeService.getOpenOrders(engine.getSymbol());
        if (account == null || allOrders == null || openOrders == null)
            return ResponseEntity.status(502).body(Map.of("message", "账户或订单数据暂时不可用，请稍后重试"));
        return ResponseEntity.ok(new AccountSnapshot(runtime.accountId(), engine.getSymbol(), runtime.alias(),
                account.path("accountType").asText("SPOT"), account.path("canTrade").asBoolean(false),
                account.path("updateTime").asLong(0), nonZeroBalances(account.path("balances")),
                executedOrders(allOrders), orderViews(openOrders), engine.getUsedApiWeight()));
    }

    private Optional<AccountTradingRuntime> runtime(String accountId) { return accountManager.find(accountId); }
    private Optional<AccountTradingRuntime> defaultRuntime() { return accountManager.runtimes().stream().findFirst(); }
    private ResponseEntity<?> noAccount() {
        return ResponseEntity.status(503).body(Map.of("message", "没有可用账号"));
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
        if (orders.isArray()) for (JsonNode order : orders)
            if (decimal(order.path("executedQty").asText("0")).signum() > 0) result.add(orderView(order));
        result.sort(Comparator.comparingLong(OrderView::timeMs).reversed());
        return result;
    }

    private List<OrderView> orderViews(JsonNode orders) {
        List<OrderView> result = new ArrayList<>();
        if (orders.isArray()) for (JsonNode order : orders) result.add(orderView(order));
        result.sort(Comparator.comparingLong(OrderView::timeMs).reversed());
        return result;
    }

    private OrderView orderView(JsonNode order) {
        BigDecimal executedQty = decimal(order.path("executedQty").asText("0"));
        BigDecimal quoteQty = decimal(order.path("cummulativeQuoteQty").asText("0"));
        String displayPrice = order.path("price").asText("0");
        if ("MARKET".equalsIgnoreCase(order.path("type").asText()) && executedQty.signum() > 0 && quoteQty.signum() > 0)
            displayPrice = quoteQty.divide(executedQty, 16, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();
        return new OrderView(order.path("orderId").asLong(0), order.path("clientOrderId").asText(""),
                order.path("side").asText(""), order.path("type").asText(""), order.path("status").asText(""),
                displayPrice, order.path("origQty").asText("0"), order.path("executedQty").asText("0"),
                order.path("cummulativeQuoteQty").asText("0"),
                order.path("time").asLong(order.path("updateTime").asLong(0)));
    }

    private boolean passwordMatches(String provided) {
        String expected = properties.getSecurity().getAdminPassword();
        return expected != null && provided != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
    }
    private BigDecimal decimal(String value) {
        try { return new BigDecimal(value); } catch (Exception ignored) { return BigDecimal.ZERO; }
    }
    private int compareDecimal(String left, String right) { return decimal(left).compareTo(decimal(right)); }

    public record LiquidationRequest(String password, String confirmation) { }
    public record SymbolSwitchRequest(String symbol) { }
    public record AccountSnapshot(String accountId, String symbol, String apiKeyAlias, String accountType,
                                  boolean canTrade, long accountUpdateTimeMs, List<BalanceView> balances,
                                  List<OrderView> filledOrders, List<OrderView> openOrders, int usedApiWeight1m) { }
    public record BalanceView(String asset, String free, String locked, String total) { }
    public record OrderView(long orderId, String clientOrderId, String side, String type, String status,
                            String price, String originalQty, String executedQty, String quoteQty, long timeMs) { }
}
