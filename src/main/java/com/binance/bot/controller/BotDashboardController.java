package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.DailyTradeStatsStore;
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
    private static final long RECENT_FILLS_CACHE_MS = 30_000L;
    private static final int RECENT_FILLS_PER_ACCOUNT = 20;
    private final TradingAccountManager accountManager;
    private final BinanceProperties properties;
    private final TradeNotificationService notificationService;
    private final Object recentFillsLock = new Object();
    private volatile RecentFillsCache recentFillsCache = new RecentFillsCache(List.of(), 0L);

    public BotDashboardController(TradingAccountManager accountManager, BinanceProperties properties,
                                  TradeNotificationService notificationService) {
        this.accountManager = accountManager;
        this.properties = properties;
        this.notificationService = notificationService;
    }

    @GetMapping("/api/accounts")
    public List<TradingAccountManager.AccountSummary> accounts() { return accountManager.summaries(); }

    @GetMapping("/api/accounts/open-orders")
    public Map<String, Object> allOpenOrders() {
        List<OpenOrderView> orders = new ArrayList<>();
        Map<String, String> errors = new LinkedHashMap<>();
        accountManager.runtimes().stream()
                .sorted(Comparator.comparing(AccountTradingRuntime::alias, String.CASE_INSENSITIVE_ORDER)
                        .thenComparing(AccountTradingRuntime::accountId, String.CASE_INSENSITIVE_ORDER))
                .forEach(runtime -> {
                    JsonNode openOrders = runtime.tradeClient().getAllOpenOrders();
                    if (openOrders == null) {
                        errors.put(runtime.accountId(), "活动订单读取失败");
                        return;
                    }
                    if (!openOrders.isArray()) return;
                    for (JsonNode order : openOrders) {
                        orders.add(openOrderView(runtime, order));
                    }
                });
        orders.sort(Comparator.comparingLong(OpenOrderView::timeMs).reversed()
                .thenComparing(OpenOrderView::accountAlias, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(OpenOrderView::symbol, String.CASE_INSENSITIVE_ORDER));
        return Map.of("orders", List.copyOf(orders), "errors", Map.copyOf(errors),
                "updatedAtMs", System.currentTimeMillis());
    }

    @GetMapping("/api/accounts/stats/summary")
    public List<DailyTradeStatsStore.AccountSymbolVolumeSummary> accountVolumeSummary(
            @RequestParam(defaultValue = "10") int days) {
        int safeDays = Math.max(1, Math.min(90, days));
        return accountManager.runtimes().stream()
                .flatMap(runtime -> runtime.engine().getAccountSymbolVolumeSummaries(safeDays).stream())
                .sorted(Comparator
                        .comparing(DailyTradeStatsStore.AccountSymbolVolumeSummary::symbol,
                                String.CASE_INSENSITIVE_ORDER)
                        .thenComparing(DailyTradeStatsStore.AccountSymbolVolumeSummary::accountAlias,
                                String.CASE_INSENSITIVE_ORDER)
                        .thenComparing(DailyTradeStatsStore.AccountSymbolVolumeSummary::accountId,
                                String.CASE_INSENSITIVE_ORDER))
                .toList();
    }

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

    @GetMapping("/api/accounts/notifications")
    public List<FillNotification> allNotifications(@RequestParam(defaultValue = "10") int limit) {
        int safeLimit = Math.max(1, Math.min(500, limit));
        return recentRemoteFills(safeLimit);
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
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "引擎已停止"))
                        : ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", value.engine().getStatusReason().get())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/api/accounts/{accountId}/symbol")
    public ResponseEntity<?> switchSymbol(@PathVariable String accountId, @RequestBody SymbolSwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = runtime.get().engine().switchSymbol(request.symbol());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/{accountId}/strategy")
    public ResponseEntity<?> switchStrategy(@PathVariable String accountId,
                                             @RequestBody StrategySwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("accepted", false, "message", "策略配置不能为空"));
        }
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = runtime.get().engine().switchStrategy(
                request.symbol(), request.mode(), request.orderAmountUsdt(), request.entryTimeoutMs(),
                request.exitTimeoutMs(), request.makerFeeBps(), request.targetNetProfitBps(),
                request.entryAnchorWaitMs(), request.maxEntryAnchorDriftBps(),
                request.maxCumulativeEntryAnchorDriftBps(), request.manualEntryAnchorPrice(),
                request.postSellEntryDelayMs(), request.dailyVolumeLimitUsdt());
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

    @PostMapping("/api/accounts/stop-all")
    public Map<String, TradingAccountManager.OperationResult> stopAll() { return accountManager.stopAll(); }

    @PostMapping("/api/accounts/reload")
    public TradingAccountManager.ReloadResult reloadAccounts() { return accountManager.reloadProfiles(); }

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

    @PostMapping("/api/bot/symbol")
    public ResponseEntity<?> legacySymbol(@RequestBody SymbolSwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? switchSymbol(runtime.get().accountId(), request) : noAccount();
    }

    @PostMapping("/api/bot/strategy")
    public ResponseEntity<?> legacyStrategy(@RequestBody StrategySwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? switchStrategy(runtime.get().accountId(), request) : noAccount();
    }

    @PostMapping("/api/bot/liquidate")
    public ResponseEntity<?> legacyLiquidate(@RequestBody LiquidationRequest request) {
        Optional<AccountTradingRuntime> runtime = defaultRuntime();
        return runtime.isPresent() ? liquidate(runtime.get().accountId(), request) : noAccount();
    }

    private Map<String, Object> statusOf(AccountTradingRuntime runtime) {
        HighFrequencyVolumeChurnEngine engine = runtime.engine();
        HighFrequencyVolumeChurnEngine.RemoteTodayStatusSnapshot remoteToday = engine.getRemoteTodayStatusSnapshot();
        return Map.ofEntries(
                Map.entry("accountId", runtime.accountId()), Map.entry("apiKeyAlias", runtime.alias()),
                Map.entry("running", engine.getIsRunning().get()),
                Map.entry("status", engine.getCurrentStatus().get().name()),
                Map.entry("statusReason", engine.getStatusReason().get()),
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("accountStreamReady", engine.isAccountStreamReady()),
                Map.entry("symbol", engine.getSymbol()),
                Map.entry("strategyMode", engine.getStrategyMode()),
                Map.entry("strategyProfile", engine.getStrategyProfile()),
                Map.entry("feeAwareRecommendedEntryAnchorPrice", engine.getFeeAwareRecommendedEntryAnchorPrice()),
                Map.entry("strategyChangePending", engine.hasPendingStrategyChange()),
                Map.entry("orderAmountUsdt", engine.getOrderAmountUsdt()),
                Map.entry("totalVolumeUsdt", engine.getTotalVolumeUsdt().get()),
                Map.entry("roundTripsCompleted", engine.getRoundTripsCompleted().get()),
                Map.entry("usedApiWeight1m", engine.getUsedApiWeight()),
                Map.entry("apiWeightLimit1m", engine.getApiWeightLimit()),
                Map.entry("apiWeightEntrySafeLimit1m", engine.getApiWeightEntrySafeLimit()),
                Map.entry("bnbBalance", Optional.ofNullable(engine.getBnbBalanceSnapshot()).orElse(
                        new HighFrequencyVolumeChurnEngine.BnbBalanceSnapshot(
                                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, false, 0L))),
                Map.entry("marketData", engine.getMarketDataSnapshot()),
                Map.entry("entrySignal", engine.getLastEntryDecision()),
                Map.entry("sellability", engine.getSellabilitySnapshot()),
                Map.entry("risk", remoteToday.risk()),
                Map.entry("accounting", remoteToday.accounting()),
                Map.entry("dailyStats", remoteToday.dailyStats()),
                Map.entry("remoteTodayStats", Map.of(
                        "enabled", remoteToday.remote(),
                        "truncated", remoteToday.truncated(),
                        "updatedAtMs", remoteToday.updatedAtMs())));
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

    private OpenOrderView openOrderView(AccountTradingRuntime runtime, JsonNode order) {
        return new OpenOrderView(runtime.accountId(), runtime.alias(), order.path("symbol").asText(""),
                order.path("side").asText(""), order.path("type").asText(""),
                order.path("status").asText(""), order.path("price").asText("0"),
                order.path("origQty").asText("0"), order.path("orderId").asLong(0),
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

    private List<FillNotification> recentRemoteFills(int limit) {
        long now = System.currentTimeMillis();
        RecentFillsCache cached = recentFillsCache;
        if (now - cached.loadedAtMs() < RECENT_FILLS_CACHE_MS) return first(cached.fills(), limit);
        synchronized (recentFillsLock) {
            cached = recentFillsCache;
            if (now - cached.loadedAtMs() < RECENT_FILLS_CACHE_MS) return first(cached.fills(), limit);

            Map<String, FillNotification> merged = new LinkedHashMap<>();
            cached.fills().forEach(fill -> merged.put(fillIdentity(fill), fill));
            notificationService.recentFills(500).forEach(fill -> merged.put(fillIdentity(fill), fill));
            accountManager.runtimes().forEach(runtime -> {
                String symbol = runtime.engine().getSymbol();
                JsonNode trades = runtime.tradeClient().getRecentMyTrades(symbol, RECENT_FILLS_PER_ACCOUNT);
                if (trades == null || !trades.isArray()) return;
                for (JsonNode trade : trades) {
                    FillNotification fill = remoteFill(runtime, symbol, trade);
                    merged.put(fillIdentity(fill), fill);
                }
            });
            List<FillNotification> fills = merged.values().stream()
                    .sorted(Comparator.comparingLong(FillNotification::eventTime).reversed())
                    .limit(500)
                    .toList();
            recentFillsCache = new RecentFillsCache(fills, now);
            return first(fills, limit);
        }
    }

    private FillNotification remoteFill(AccountTradingRuntime runtime, String symbol, JsonNode trade) {
        BigDecimal quantity = decimal(trade.path("qty").asText("0"));
        BigDecimal price = decimal(trade.path("price").asText("0"));
        BigDecimal quote = decimal(trade.path("quoteQty").asText(quantity.multiply(price).toPlainString()));
        return new FillNotification(runtime.accountId(), runtime.alias(), symbol,
                trade.path("isBuyer").asBoolean(false) ? "BUY" : "SELL",
                trade.path("orderId").asLong(-1), trade.path("id").asLong(-1), "",
                quantity, price, quote, decimal(trade.path("commission").asText("0")),
                trade.path("commissionAsset").asText(""), trade.path("time").asLong(0));
    }

    private String fillIdentity(FillNotification fill) {
        return fill.accountId() + ':' + fill.symbol() + ':' + fill.tradeId();
    }

    private List<FillNotification> first(List<FillNotification> fills, int limit) {
        return List.copyOf(fills.subList(0, Math.min(limit, fills.size())));
    }

    public record LiquidationRequest(String password, String confirmation) { }
    public record SymbolSwitchRequest(String symbol) { }
    public record StrategySwitchRequest(String symbol, String mode, BigDecimal orderAmountUsdt,
                                        Long entryTimeoutMs, Long exitTimeoutMs, BigDecimal makerFeeBps,
                                        BigDecimal targetNetProfitBps, Long entryAnchorWaitMs,
                                        BigDecimal maxEntryAnchorDriftBps,
                                        BigDecimal maxCumulativeEntryAnchorDriftBps,
                                        BigDecimal manualEntryAnchorPrice,
                                        Long postSellEntryDelayMs,
                                        BigDecimal dailyVolumeLimitUsdt) { }
    public record AccountSnapshot(String accountId, String symbol, String apiKeyAlias, String accountType,
                                  boolean canTrade, long accountUpdateTimeMs, List<BalanceView> balances,
                                  List<OrderView> filledOrders, List<OrderView> openOrders, int usedApiWeight1m) { }
    public record BalanceView(String asset, String free, String locked, String total) { }
    public record OrderView(long orderId, String clientOrderId, String side, String type, String status,
                            String price, String originalQty, String executedQty, String quoteQty, long timeMs) { }
    public record OpenOrderView(String accountId, String accountAlias, String symbol, String side, String type,
                                String status, String price, String originalQty, long orderId, long timeMs) { }
    private record RecentFillsCache(List<FillNotification> fills, long loadedAtMs) { }
}
