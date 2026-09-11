package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.SymbolTradeCoordinator;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.DailyTradeStatsStore;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

@RestController
@Slf4j
public class BotDashboardController {
    private final TradingAccountManager accountManager;
    private final BinanceProperties properties;
    private final TradeNotificationService notificationService;
    private final DailyTradeStatsStore dailyStatsStore;
    private final SymbolTradeCoordinator symbolTradeCoordinator;

    public BotDashboardController(TradingAccountManager accountManager, BinanceProperties properties,
                                  TradeNotificationService notificationService) {
        this(accountManager, properties, notificationService, null, null);
    }

    @Autowired
    public BotDashboardController(TradingAccountManager accountManager, BinanceProperties properties,
                                  TradeNotificationService notificationService,
                                  DailyTradeStatsStore dailyStatsStore,
                                  SymbolTradeCoordinator symbolTradeCoordinator) {
        this.accountManager = accountManager;
        this.properties = properties;
        this.notificationService = notificationService;
        this.dailyStatsStore = dailyStatsStore;
        this.symbolTradeCoordinator = symbolTradeCoordinator;
    }

    @GetMapping("/api/accounts")
    public List<TradingAccountManager.AccountSummary> accounts() { return accountManager.summaries(); }

    @GetMapping("/api/settings/trading")
    public TradingSettingsView tradingSettings() {
        int value = dailyStatsStore == null ? properties.getStrategy().getMaxConcurrentEntriesPerSymbol()
                : dailyStatsStore.loadTradingRuntimeSettings()
                .map(DailyTradeStatsStore.TradingRuntimeSettings::maxConcurrentEntriesPerSymbol)
                .orElse(properties.getStrategy().getMaxConcurrentEntriesPerSymbol());
        value = Math.max(1, value);
        if (symbolTradeCoordinator != null) {
            symbolTradeCoordinator.configureMaxConcurrentEntriesPerSymbol(value);
            value = symbolTradeCoordinator.maxConcurrentEntriesPerSymbol();
        }
        return new TradingSettingsView(value);
    }

    @PutMapping("/api/settings/trading")
    public ResponseEntity<?> updateTradingSettings(@RequestBody TradingSettingsRequest request) {
        if (request == null || request.maxConcurrentEntriesPerSymbol() == null) {
            return ResponseEntity.badRequest().body(Map.of("accepted", false, "message", "交易运行设置不能为空"));
        }
        int value = request.maxConcurrentEntriesPerSymbol();
        if (value < 1 || value > 20) {
            return ResponseEntity.badRequest().body(Map.of("accepted", false,
                    "message", "同交易对并发名额必须在 1 到 20 之间"));
        }
        DailyTradeStatsStore.TradingRuntimeSettings settings =
                new DailyTradeStatsStore.TradingRuntimeSettings(value);
        if (dailyStatsStore != null) dailyStatsStore.saveTradingRuntimeSettings(settings);
        properties.getStrategy().setMaxConcurrentEntriesPerSymbol(value);
        if (symbolTradeCoordinator != null) symbolTradeCoordinator.configureMaxConcurrentEntriesPerSymbol(value);
        return ResponseEntity.ok(new TradingSettingsUpdateResult(true, "交易运行设置已保存并立即生效",
                new TradingSettingsView(value)));
    }

    @GetMapping("/api/accounts/open-orders")
    public Map<String, Object> allOpenOrders() {
        return Map.of("orders", notificationService.currentOpenOrders(), "errors", Map.of(),
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

    /**
     * UTC-today dashboard summary backed only by the durable aggregate store. Each account is read
     * independently so a damaged account row cannot hide every other account from the page.
     */
    @GetMapping("/api/accounts/stats/today")
    public TodayTradingSummary todayTradingSummary() {
        List<TodayAccountTradingSummary> accounts = new ArrayList<>();
        for (AccountTradingRuntime runtime : accountManager.runtimes()) {
            try {
                List<DailyTradeStatsStore.AccountSymbolVolumeSummary> symbols =
                        Optional.ofNullable(runtime.engine().getAccountSymbolVolumeSummaries(1))
                                .orElseGet(List::of).stream()
                                .filter(Objects::nonNull)
                                .sorted(Comparator.comparing(summary ->
                                                Objects.toString(summary.symbol(), ""),
                                        String.CASE_INSENSITIVE_ORDER))
                                .toList();
                accounts.add(todayAccountSummary(runtime.accountId(), runtime.alias(), symbols, null));
            } catch (RuntimeException e) {
                log.warn("[accountId={} alias={}] 读取今日账户交易汇总失败，其他账户继续显示: {}",
                        runtime.accountId(), runtime.alias(), e.getMessage());
                accounts.add(todayAccountSummary(runtime.accountId(), runtime.alias(), List.of(),
                        "今日统计暂时不可用"));
            }
        }
        accounts.sort(Comparator.comparing(TodayAccountTradingSummary::accountAlias,
                        String.CASE_INSENSITIVE_ORDER)
                .thenComparing(TodayAccountTradingSummary::accountId, String.CASE_INSENSITIVE_ORDER));

        BigDecimal volume = BigDecimal.ZERO;
        BigDecimal commission = BigDecimal.ZERO;
        BigDecimal netPnl = BigDecimal.ZERO;
        BigDecimal loss = BigDecimal.ZERO;
        for (TodayAccountTradingSummary account : accounts) {
            volume = volume.add(account.totalVolumeQuote());
            commission = commission.add(account.totalCommissionQuoteEquivalent());
            netPnl = netPnl.add(account.netRealizedPnlQuote());
            loss = loss.add(account.lossQuote());
        }
        return new TodayTradingSummary(LocalDate.now(ZoneOffset.UTC), List.copyOf(accounts),
                volume, commission, netPnl, loss, System.currentTimeMillis());
    }

    private TodayAccountTradingSummary todayAccountSummary(
            String accountId, String accountAlias,
            List<DailyTradeStatsStore.AccountSymbolVolumeSummary> symbols, String error) {
        BigDecimal buy = BigDecimal.ZERO;
        BigDecimal sell = BigDecimal.ZERO;
        BigDecimal total = BigDecimal.ZERO;
        BigDecimal commission = BigDecimal.ZERO;
        BigDecimal netPnl = BigDecimal.ZERO;
        boolean commissionComplete = true;
        for (DailyTradeStatsStore.AccountSymbolVolumeSummary symbol : symbols) {
            buy = buy.add(orZero(symbol.buyVolumeQuote()));
            sell = sell.add(orZero(symbol.sellVolumeQuote()));
            total = total.add(orZero(symbol.totalVolumeQuote()));
            commission = commission.add(orZero(symbol.totalCommissionQuoteEquivalent()));
            netPnl = netPnl.add(orZero(symbol.netRealizedPnlQuote()));
            commissionComplete &= symbol.commissionConversionComplete();
        }
        BigDecimal loss = netPnl.signum() < 0 ? netPnl.negate() : BigDecimal.ZERO;
        String safeAccountId = Objects.toString(accountId, "");
        String safeAlias = accountAlias == null || accountAlias.isBlank() ? safeAccountId : accountAlias;
        return new TodayAccountTradingSummary(safeAccountId, safeAlias, List.copyOf(symbols), buy, sell,
                total, commission, netPnl, loss, commissionComplete, error);
    }

    private static BigDecimal orZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    @GetMapping("/api/accounts/{accountId}/status")
    public ResponseEntity<?> status(@PathVariable String accountId) {
        return runtime(accountId).<ResponseEntity<?>>map(value -> ResponseEntity.ok(statusOf(value, value.engine())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/{accountId}/symbols/{symbol}/status")
    public ResponseEntity<?> symbolStatus(@PathVariable String accountId, @PathVariable String symbol) {
        return engineContext(accountId, symbol).<ResponseEntity<?>>map(value ->
                        ResponseEntity.ok(statusOf(value.runtime(), value.engine())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/{accountId}/stats/daily")
    public ResponseEntity<?> dailyStats(@PathVariable String accountId,
                                        @RequestParam(defaultValue = "30") int limit) {
        return runtime(accountId).<ResponseEntity<?>>map(value -> ResponseEntity.ok(
                        value.engine().getRecentDailyStats(Math.max(1, Math.min(366, limit)))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/{accountId}/symbols/{symbol}/stats/daily")
    public ResponseEntity<?> symbolDailyStats(@PathVariable String accountId, @PathVariable String symbol,
                                              @RequestParam(defaultValue = "30") int limit) {
        return engineContext(accountId, symbol).<ResponseEntity<?>>map(value -> ResponseEntity.ok(
                        value.engine().getRecentDailyStats(Math.max(1, Math.min(366, limit)))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/notifications")
    public List<FillNotification> allNotifications(@RequestParam(defaultValue = "10") int limit) {
        int safeLimit = Math.max(1, Math.min(500, limit));
        return notificationService.recentFills(safeLimit);
    }

    @GetMapping("/api/accounts/{accountId}/notifications")
    public ResponseEntity<?> notifications(@PathVariable String accountId,
                                           @RequestParam(defaultValue = "100") int limit) {
        if (runtime(accountId).isEmpty()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(notificationService.recentFills(accountId, Math.max(1, Math.min(500, limit))));
    }

    @GetMapping("/api/accounts/{accountId}/account")
    public ResponseEntity<?> account(@PathVariable String accountId) {
        return runtime(accountId).<ResponseEntity<?>>map(value -> accountSnapshot(value, value.engine()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/api/accounts/{accountId}/symbols/{symbol}/account")
    public ResponseEntity<?> symbolAccount(@PathVariable String accountId, @PathVariable String symbol) {
        return engineContext(accountId, symbol).<ResponseEntity<?>>map(
                        value -> accountSnapshot(value.runtime(), value.engine()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/api/accounts/{accountId}/start")
    public ResponseEntity<?> start(@PathVariable String accountId) {
        return runtime(accountId).map(value -> value.symbolCount() > 1
                        ? ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", "多币种账户请按币种单独启动"))
                        : value.start()
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "引擎已启动"))
                        : ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", value.engine().getStatusReason().get())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/api/accounts/{accountId}/symbols/{symbol}/start")
    public ResponseEntity<?> startSymbol(@PathVariable String accountId, @PathVariable String symbol) {
        return engineContext(accountId, symbol).map(value -> value.runtime().start(value.engine().getSymbol())
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "币种策略已启动"))
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

    @PostMapping("/api/accounts/{accountId}/symbols/{symbol}/stop")
    public ResponseEntity<?> stopSymbol(@PathVariable String accountId, @PathVariable String symbol) {
        return engineContext(accountId, symbol).map(value -> value.runtime().stop(value.engine().getSymbol())
                        ? ResponseEntity.ok(Map.of("accepted", true, "message", "币种策略已停止"))
                        : ResponseEntity.status(409).body(Map.of("accepted", false,
                        "message", value.engine().getStatusReason().get())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/api/accounts/{accountId}/symbol")
    public ResponseEntity<?> switchSymbol(@PathVariable String accountId, @RequestBody SymbolSwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        if (runtime.get().symbolCount() > 1) {
            return ResponseEntity.status(409).body(Map.of("accepted", false,
                    "message", "多币种账户不能在运行时替换交易对，请修改账户 symbols 配置后重启"));
        }
        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = runtime.get().engine().switchSymbol(request.symbol());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/{accountId}/strategy")
    public ResponseEntity<?> switchStrategy(@PathVariable String accountId,
                                             @RequestBody StrategySwitchRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        if (runtime.get().symbolCount() > 1) {
            return ResponseEntity.status(409).body(Map.of("accepted", false,
                    "message", "多币种账户请明确指定交易对后修改策略"));
        }
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("accepted", false, "message", "策略配置不能为空"));
        }
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = runtime.get().engine().switchStrategy(
                request.symbol(), request.mode(), request.orderAmountUsdt(), request.entryTimeoutMs(),
                request.exitTimeoutMs(), request.makerFeeBps(), request.targetNetProfitBps(),
                request.entryAnchorWaitMs(), request.maxEntryAnchorDriftBps(),
                request.maxCumulativeEntryAnchorDriftBps(), request.manualEntryAnchorPrice(),
                request.postSellEntryDelayMs(), request.dailyVolumeLimitUsdt(),
                request.bidAskInitialSellMarkupTicks(), request.bidAskEntryBookLevel(),
                request.entryTimeoutCooldownMs());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/{accountId}/symbols/{symbol}/strategy")
    public ResponseEntity<?> switchSymbolStrategy(@PathVariable String accountId, @PathVariable String symbol,
                                                   @RequestBody StrategySwitchRequest request) {
        Optional<EngineContext> context = engineContext(accountId, symbol);
        if (context.isEmpty()) return ResponseEntity.notFound().build();
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("accepted", false, "message", "策略配置不能为空"));
        }
        HighFrequencyVolumeChurnEngine engine = context.get().engine();
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                engine.getSymbol(), request.mode(), request.orderAmountUsdt(), request.entryTimeoutMs(),
                request.exitTimeoutMs(), request.makerFeeBps(), request.targetNetProfitBps(),
                request.entryAnchorWaitMs(), request.maxEntryAnchorDriftBps(),
                request.maxCumulativeEntryAnchorDriftBps(), request.manualEntryAnchorPrice(),
                request.postSellEntryDelayMs(), request.dailyVolumeLimitUsdt(),
                request.bidAskInitialSellMarkupTicks(), request.bidAskEntryBookLevel(),
                request.entryTimeoutCooldownMs());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/{accountId}/liquidate")
    public ResponseEntity<?> liquidate(@PathVariable String accountId, @RequestBody LiquidationRequest request) {
        Optional<AccountTradingRuntime> runtime = runtime(accountId);
        if (runtime.isEmpty()) return ResponseEntity.notFound().build();
        if (runtime.get().symbolCount() > 1) {
            return ResponseEntity.status(409).body(Map.of("accepted", false,
                    "message", "多币种账户请明确指定交易对后执行清仓"));
        }
        if (!passwordMatches(request.password()) || !"SELL ALL BASE ASSET".equals(request.confirmation())) {
            return ResponseEntity.status(401).body(Map.of("accepted", false, "message", "二次验证失败"));
        }
        HighFrequencyVolumeChurnEngine.LiquidationResult result = runtime.get().engine().liquidateExistingPosition();
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/{accountId}/symbols/{symbol}/liquidate")
    public ResponseEntity<?> liquidateSymbol(@PathVariable String accountId, @PathVariable String symbol,
                                              @RequestBody LiquidationRequest request) {
        Optional<EngineContext> context = engineContext(accountId, symbol);
        if (context.isEmpty()) return ResponseEntity.notFound().build();
        if (!passwordMatches(request.password()) || !"SELL ALL BASE ASSET".equals(request.confirmation())) {
            return ResponseEntity.status(401).body(Map.of("accepted", false, "message", "二次验证失败"));
        }
        HighFrequencyVolumeChurnEngine.LiquidationResult result =
                context.get().engine().liquidateExistingPosition();
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    @PostMapping("/api/accounts/start-all")
    public Map<String, TradingAccountManager.OperationResult> startAll() { return accountManager.startAll(); }

    @PostMapping("/api/accounts/stop-all")
    public Map<String, TradingAccountManager.OperationResult> stopAll() { return accountManager.stopAll(); }

    @PostMapping("/api/accounts/reload")
    public TradingAccountManager.ReloadResult reloadAccounts() { return accountManager.reloadProfiles(); }

    @GetMapping("/api/accounts/{accountId}/configuration/symbols")
    public ResponseEntity<?> configuredSymbols(@PathVariable String accountId) {
        return accountManager.accountSymbolsConfiguration(accountId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/api/accounts/{accountId}/configuration/symbols")
    public ResponseEntity<?> updateConfiguredSymbols(@PathVariable String accountId,
                                                      @RequestBody SymbolsConfigurationRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("accepted", false, "message", "交易对配置不能为空"));
        }
        TradingAccountManager.SymbolsUpdateResult result =
                accountManager.updateAccountSymbols(accountId, request.symbols());
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    /* Legacy dashboard routes select a stable first runtime; credentials are never hot-switched. */
    @GetMapping("/api/bot/status")
    public ResponseEntity<?> legacyStatus() {
        return defaultRuntime().<ResponseEntity<?>>map(value -> ResponseEntity.ok(statusOf(value, value.engine())))
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
        return defaultRuntime().<ResponseEntity<?>>map(value -> accountSnapshot(value, value.engine()))
                .orElseGet(this::noAccount);
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

    private Map<String, Object> statusOf(AccountTradingRuntime runtime, HighFrequencyVolumeChurnEngine engine) {
        HighFrequencyVolumeChurnEngine.RemoteTodayStatusSnapshot remoteToday =
                engine.getRemoteTodayStatusSnapshotForMonitoring();
        return Map.ofEntries(
                Map.entry("accountId", runtime.accountId()), Map.entry("apiKeyAlias", runtime.alias()),
                Map.entry("runtimeId", runtime.accountId() + "::" + engine.getSymbol()),
                Map.entry("symbolCount", runtime.symbolCount()),
                Map.entry("running", engine.getIsRunning().get()),
                Map.entry("status", engine.getCurrentStatus().get().name()),
                Map.entry("statusReason", engine.getStatusReason().get()),
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("accountStreamReady", engine.isAccountStreamReady()),
                Map.entry("symbol", engine.getSymbol()),
                Map.entry("strategyMode", engine.getStrategyMode()),
                Map.entry("strategyProfile", engine.getStrategyProfile()),
                Map.entry("tickSize", engine.getTickSize()),
                Map.entry("feeAwareRecommendedEntryAnchorPrice", engine.getFeeAwareRecommendedEntryAnchorPrice()),
                Map.entry("strategyChangePending", engine.hasPendingStrategyChange()),
                Map.entry("orderAmountUsdt", engine.getOrderAmountUsdt()),
                Map.entry("totalVolumeUsdt", engine.getTotalVolumeUsdt().get()),
                Map.entry("roundTripsCompleted", engine.getRoundTripsCompleted().get()),
                Map.entry("usedApiWeight1m", engine.getUsedApiWeight()),
                Map.entry("apiWeightLimit1m", engine.getApiWeightLimit()),
                Map.entry("apiWeightEntrySafeLimit1m", engine.getApiWeightEntrySafeLimit()),
                Map.entry("bnbBalance", Optional.ofNullable(engine.getBnbBalanceSnapshotForMonitoring()).orElse(
                        new HighFrequencyVolumeChurnEngine.BnbBalanceSnapshot(
                                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, false, 0L))),
                Map.entry("marketData", engine.getMarketDataSnapshot()),
                Map.entry("entrySignal", engine.getLastEntryDecision()),
                Map.entry("sellability", engine.getSellabilitySnapshot()),
                Map.entry("risk", remoteToday.risk()),
                Map.entry("accountRisk", engine.getAccountRiskSnapshot()),
                Map.entry("accounting", remoteToday.accounting()),
                Map.entry("dailyStats", remoteToday.dailyStats()),
                Map.entry("remoteTodayStats", Map.of(
                        "enabled", remoteToday.remote(),
                        "truncated", remoteToday.truncated(),
                        "updatedAtMs", remoteToday.updatedAtMs())));
    }

    private ResponseEntity<?> accountSnapshot(AccountTradingRuntime runtime, HighFrequencyVolumeChurnEngine engine) {
        if (!engine.getIsRunning().get()) {
            return ResponseEntity.status(409).body(Map.of(
                    "message", "策略未运行，未向交易所查询账户详情；启动策略后将自动刷新"));
        }
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

    private Optional<EngineContext> engineContext(String accountId, String symbol) {
        return runtime(accountId).flatMap(value -> value.engine(symbol).map(engine ->
                new EngineContext(value, engine)));
    }

    private record EngineContext(AccountTradingRuntime runtime, HighFrequencyVolumeChurnEngine engine) { }

    public record TodayTradingSummary(LocalDate date, List<TodayAccountTradingSummary> accounts,
                                      BigDecimal totalVolumeQuote,
                                      BigDecimal totalCommissionQuoteEquivalent,
                                      BigDecimal netRealizedPnlQuote, BigDecimal totalLossQuote,
                                      long updatedAtMs) { }

    public record TodayAccountTradingSummary(
            String accountId, String accountAlias,
            List<DailyTradeStatsStore.AccountSymbolVolumeSummary> symbols,
            BigDecimal buyVolumeQuote, BigDecimal sellVolumeQuote, BigDecimal totalVolumeQuote,
            BigDecimal totalCommissionQuoteEquivalent, BigDecimal netRealizedPnlQuote,
            BigDecimal lossQuote, boolean commissionConversionComplete, String error) { }
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
    public record SymbolsConfigurationRequest(List<String> symbols) { }
    public record TradingSettingsRequest(Integer maxConcurrentEntriesPerSymbol) { }
    public record TradingSettingsView(int maxConcurrentEntriesPerSymbol) { }
    public record TradingSettingsUpdateResult(boolean accepted, String message, TradingSettingsView settings) { }
    public record StrategySwitchRequest(String symbol, String mode, BigDecimal orderAmountUsdt,
                                        Long entryTimeoutMs, Long exitTimeoutMs, BigDecimal makerFeeBps,
                                        BigDecimal targetNetProfitBps, Long entryAnchorWaitMs,
                                        BigDecimal maxEntryAnchorDriftBps,
                                        BigDecimal maxCumulativeEntryAnchorDriftBps,
                                        BigDecimal manualEntryAnchorPrice,
                                        Long postSellEntryDelayMs,
                                        BigDecimal dailyVolumeLimitUsdt,
                                        Integer bidAskInitialSellMarkupTicks,
                                        Integer bidAskEntryBookLevel,
                                        Long entryTimeoutCooldownMs) { }
    public record AccountSnapshot(String accountId, String symbol, String apiKeyAlias, String accountType,
                                  boolean canTrade, long accountUpdateTimeMs, List<BalanceView> balances,
                                  List<OrderView> filledOrders, List<OrderView> openOrders, int usedApiWeight1m) { }
    public record BalanceView(String asset, String free, String locked, String total) { }
    public record OrderView(long orderId, String clientOrderId, String side, String type, String status,
                            String price, String originalQty, String executedQty, String quoteQty, long timeMs) { }
}
