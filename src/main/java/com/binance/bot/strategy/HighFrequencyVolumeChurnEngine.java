package com.binance.bot.strategy;

import com.binance.bot.account.AccountCredentials;
import com.binance.bot.account.AccountExecutionEvent;
import com.binance.bot.account.AccountRiskCoordinator;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.OpenOrderNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.util.PrecisionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.nio.ByteBuffer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HighFrequencyVolumeChurnEngine implements WebSocket.Listener {
    private static final long COMMISSION_RATE_CACHE_MS = TimeUnit.MINUTES.toMillis(15);
    private static final long REMOTE_TODAY_ACCOUNTING_CACHE_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long POSITION_RECOVERY_WINDOW_MS = TimeUnit.DAYS.toMillis(1);
    private static final int POSITION_RECOVERY_MAX_WINDOWS = 30;
    private static final BigDecimal MIN_BNB_BALANCE_USDT = BigDecimal.ONE;
    private final String accountId;
    private final String accountAlias;
    private final String accountTag;
    private final String accountEngineKey;
    private final AccountCredentials credentials;
    private final BinanceProperties properties;
    private final BinanceAccountTradeClient tradeService;
    private final SymbolRuleManager ruleManager;
    private final BooleanSupplier accountStreamReady;
    private final MarketSignalEvaluator marketSignalEvaluator;
    private final PostFillOutcomeTracker postFillOutcomeTracker;
    private final TradingRiskGuard riskGuard;
    private final DailyTradeStatsStore dailyStatsStore;
    private final TradeNotificationService notificationService;
    private final AccountRiskCoordinator accountRiskCoordinator;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final AtomicReference<BigDecimal> lastBestBid = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastBestAsk = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastMidPrice = new AtomicReference<>();
    private final AtomicLong lastMarketDataTimestamp = new AtomicLong(0);
    private final AtomicLong lastMarketFrameTimestamp = new AtomicLong(0);
    private final AtomicReference<List<BigDecimal>> latestBidDepthPrices = new AtomicReference<>(List.of());
    private final AtomicLong lastDepthDataTimestamp = new AtomicLong(0);

    public enum ChurnStatus { IDLE, BUYING, SELLING, HALTED }

    @Getter private final AtomicBoolean isRunning = new AtomicBoolean(false);
    @Getter private final AtomicReference<ChurnStatus> currentStatus = new AtomicReference<>(ChurnStatus.IDLE);
    @Getter private final AtomicReference<BigDecimal> totalVolumeUsdt = new AtomicReference<>(BigDecimal.ZERO);
    @Getter private final AtomicLong roundTripsCompleted = new AtomicLong(0);
    private final AtomicReference<Long> activeOrderId = new AtomicReference<>();
    private final AtomicReference<BigDecimal> activeOrderPrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> holdingInventory = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> activeSellCoveredQty = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> lastKnownFreeBaseBalance = new AtomicReference<>();
    private final AtomicReference<BigDecimal> targetEntryQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> filledEntryQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> filledEntryQuoteQuantity = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> filledEntryMaxPrice = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicReference<BigDecimal> feeAwareEntryPriceCeiling = new AtomicReference<>();
    private final AtomicReference<BigDecimal> feeAwareInitialEntryAnchorPrice = new AtomicReference<>();
    private final ArrayDeque<BigDecimal> feeAwareRecentBuyPrices = new ArrayDeque<>();
    private final AtomicLong feeAwareEntryCeilingBlockedSince = new AtomicLong(0);
    private final AtomicLong postSellNextEntryAllowedAtMs = new AtomicLong(0);
    private final AtomicBoolean dailyVolumeStopPending = new AtomicBoolean(false);
    private final AtomicReference<LocalDate> dailyVolumeCounterDate = new AtomicReference<>();
    private final AtomicBoolean bnbBalanceStopPending = new AtomicBoolean(false);
    private final AtomicReference<BnbBalanceSnapshot> bnbBalanceSnapshot = new AtomicReference<>();
    private final AtomicReference<String> activeClientOrderId = new AtomicReference<>();
    private final AtomicReference<Long> replacingOrderId = new AtomicReference<>();
    @Getter private final AtomicReference<String> statusReason = new AtomicReference<>("等待启动");
    private final Set<Long> knownOrderIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> restReconciledOrderIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pendingClientOrderIds = ConcurrentHashMap.newKeySet();
    private final TradeAccountingLedger accountingLedger = new TradeAccountingLedger();
    private final ConcurrentHashMap<String, CommissionPrice> commissionPriceCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CachedCommissionRate> makerSellFeeRateCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> orderReconcileFailures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> tradeReconcileFailures = new ConcurrentHashMap<>();
    private final AtomicReference<RemoteTodayStatusSnapshot> remoteTodayAccountingCache = new AtomicReference<>();
    private final Object remoteTodayAccountingLock = new Object();
    /** Active profiles are replaced atomically so a runtime switch never mutates a profile in use. */
    private final ConcurrentHashMap<String, BinanceProperties.SymbolStrategyProfile> strategyProfiles =
            new ConcurrentHashMap<>();
    /** A profile for the current symbol is queued here until the current order reaches a safe boundary. */
    private final ConcurrentHashMap<String, BinanceProperties.SymbolStrategyProfile> pendingStrategyProfiles =
            new ConcurrentHashMap<>();
    private final AtomicLong orderPlacedTimestamp = new AtomicLong(0);
    private final AtomicLong nextOrderAttemptAt = new AtomicLong(0);
    private final AtomicLong clientOrderSequence = new AtomicLong(0);
    private final AtomicBoolean entryCancellationPending = new AtomicBoolean(false);
    private final AtomicBoolean exitSubmissionInFlight = new AtomicBoolean(false);
    private final AtomicBoolean marketConnectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicInteger marketReconnectAttempts = new AtomicInteger(0);
    private final AtomicBoolean acceptingMarketConnections = new AtomicBoolean(true);
    private final AtomicReference<WebSocket> activeMarketWebSocket = new AtomicReference<>();
    private final AtomicBoolean transientMarketRecoveryPending = new AtomicBoolean(false);
    private final AtomicReference<String> preMarketRecoveryStatusReason = new AtomicReference<>();
    private final AtomicBoolean userStreamOrderReconcilePending = new AtomicBoolean(false);
    private final ScheduledExecutorService marketWatchdog;
    private final AtomicReference<String> activeEntrySignalReason = new AtomicReference<>("UNKNOWN");
    private final AtomicReference<MarketSignalEvaluator.MarketContext> activeEntryContext = new AtomicReference<>();
    private final StringBuilder inboundMarketMessage = new StringBuilder();
    private final AtomicReference<String> lastDustStateSignature = new AtomicReference<>("");
    private final AtomicBoolean accountRiskReconciled = new AtomicBoolean(false);

    public HighFrequencyVolumeChurnEngine(String accountId, String accountAlias, AccountCredentials credentials,
                                           BinanceProperties properties, BinanceAccountTradeClient tradeService,
                                           SymbolRuleManager ruleManager, BooleanSupplier accountStreamReady,
                                           MarketSignalEvaluator marketSignalEvaluator,
                                           PostFillOutcomeTracker postFillOutcomeTracker,
                                           TradingRiskGuard riskGuard, DailyTradeStatsStore dailyStatsStore,
                                           TradeNotificationService notificationService) {
        this(accountId, accountAlias, credentials, properties, tradeService, ruleManager,
                accountStreamReady, marketSignalEvaluator, postFillOutcomeTracker, riskGuard,
                dailyStatsStore, notificationService, null);
    }

    public HighFrequencyVolumeChurnEngine(String accountId, String accountAlias, AccountCredentials credentials,
                                           BinanceProperties properties, BinanceAccountTradeClient tradeService,
                                           SymbolRuleManager ruleManager, BooleanSupplier accountStreamReady,
                                           MarketSignalEvaluator marketSignalEvaluator,
                                           PostFillOutcomeTracker postFillOutcomeTracker,
                                           TradingRiskGuard riskGuard, DailyTradeStatsStore dailyStatsStore,
                                           TradeNotificationService notificationService,
                                           AccountRiskCoordinator accountRiskCoordinator) {
        this.accountId = accountId;
        this.accountAlias = accountAlias;
        this.accountEngineKey = accountId + "::" + properties.getStrategy().getSymbol().toUpperCase();
        this.accountTag = Integer.toUnsignedString(accountEngineKey.hashCode(), 36);
        this.credentials = credentials;
        this.properties = properties;
        this.tradeService = tradeService;
        this.ruleManager = ruleManager;
        this.accountStreamReady = accountStreamReady;
        this.marketSignalEvaluator = marketSignalEvaluator;
        this.postFillOutcomeTracker = postFillOutcomeTracker;
        this.riskGuard = riskGuard;
        this.dailyStatsStore = dailyStatsStore;
        this.notificationService = notificationService;
        // The standalone constructor is retained for backward compatibility and tests. Production
        // runtimes always inject the shared coordinator and reconcile every configured symbol first.
        this.accountRiskReconciled.set(accountRiskCoordinator == null);
        this.accountRiskCoordinator = accountRiskCoordinator == null
                ? new AccountRiskCoordinator() : accountRiskCoordinator;
        this.accountRiskCoordinator.register(accountEngineKey, riskGuard::snapshot, accountRiskReconciled::get);
        if (properties.getStrategy().getSymbolStrategies() != null) {
            properties.getStrategy().getSymbolStrategies().forEach((symbol, profile) -> {
                String normalized = normalizeStrategySymbol(symbol);
                if (profile != null && !normalized.isBlank() && normalized.endsWith("USDT")) {
                    strategyProfiles.put(normalized, copyStrategyProfile(profile));
                }
            });
        }
        this.marketWatchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "binance-market-stream-watchdog-" + accountId + "-"
                    + properties.getStrategy().getSymbol().toLowerCase());
            thread.setDaemon(true);
            return thread;
        });
    }

    public void initialize() {
        if (ruleManager.refreshRule(properties.getStrategy().getSymbol()) == null) {
            statusReason.set("当前交易对不可交易或规则加载失败: " + properties.getStrategy().getSymbol());
            currentStatus.set(ChurnStatus.HALTED);
        }
        syncDailyCounters();
        restoreDailyRisk();
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
                    latestBidDepthPrices.set(List.of());
                    lastDepthDataTimestamp.set(0);
                    WebSocket previous = activeMarketWebSocket.getAndSet(ws);
                    if (previous != null && previous != ws) previous.abort();
                    // Publish the active socket before clearing the connection guards so the
                    // watchdog can never observe a false "no connection" gap and queue a duplicate.
                    marketConnectInProgress.set(false);
                    reconnectScheduled.set(false);
                    marketReconnectAttempts.set(0);
                    log.info("[accountId={} alias={}] 已连接盘口数据流: {}", accountId, accountAlias, wsUrl);
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
            if (statusCode == 1001) {
                handleTransientMarketClose("连接关闭 1001: " + reason);
            } else {
                handleMarketStreamLoss("连接关闭 " + statusCode + ": " + reason);
            }
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
        transientMarketRecoveryPending.set(false);
        preMarketRecoveryStatusReason.set(null);
        marketSignalEvaluator.reset();
        latestBidDepthPrices.set(List.of());
        lastDepthDataTimestamp.set(0);
        log.warn("[accountId={} alias={}] 行情流不可用: {}", accountId, accountAlias, reason);
        if (isRunning.get()) protectOnStreamLoss("行情流不可用: " + reason);
        scheduleMarketReconnect();
    }

    /**
     * Binance occasionally closes a healthy market stream with 1001 while rotating a connection.
     * Keep the strategy running during the first reconnect attempt. A second close before any new
     * market frame, a reconnect failure, or the watchdog timing out is treated as a real outage.
     */
    private void handleTransientMarketClose(String reason) {
        if (!acceptingMarketConnections.get()) return;
        if (!transientMarketRecoveryPending.compareAndSet(false, true)) {
            handleMarketStreamLoss(reason + "；重连后仍未恢复行情");
            return;
        }
        marketSignalEvaluator.reset();
        latestBidDepthPrices.set(List.of());
        lastDepthDataTimestamp.set(0);
        lastMarketDataTimestamp.set(0);
        if (isRunning.get()) {
            preMarketRecoveryStatusReason.set(statusReason.get());
            statusReason.set("行情流 1001 瞬断，正在自动重连");
        }
        log.warn("[accountId={} alias={}] 行情流 1001 瞬断，先自动重连；重连失败才停机: {}",
                accountId, accountAlias, reason);
        scheduleMarketReconnect();
    }

    private void scheduleMarketReconnect() {
        if (reconnectScheduled.compareAndSet(false, true)) {
            int attempt = marketReconnectAttempts.incrementAndGet();
            long delayMs = reconnectDelayMs(attempt);
            marketWatchdog.schedule(() -> {
                if (!acceptingMarketConnections.get()) {
                    reconnectScheduled.set(false);
                    return;
                }
                connectMarketData();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private long reconnectDelayMs(int attempt) {
        int exponent = Math.min(Math.max(0, attempt - 1), 6);
        long baseMs = Math.min(60_000L, 1_000L << exponent);
        return baseMs + ThreadLocalRandom.current().nextLong(Math.max(1L, baseMs / 4));
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
        if (!properties.getStrategy().isLiveTradingEnabled()) {
            log.error("拒绝启动：服务器未配置 BINANCE_LIVE_TRADING_ENABLED=true");
            return false;
        }
        if (credentials.apiKey().isBlank() || credentials.secretKey().isBlank()) {
            log.error("拒绝启动：真实执行缺少 API 凭据");
            return false;
        }
        if (!accountStreamReady.getAsBoolean()) {
            halt("账户成交流未就绪，拒绝启动");
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
        BigDecimal orderAmount = orderAmountUsdt();
        if (orderAmount == null || orderAmount.signum() <= 0) {
            log.error("拒绝启动：当前交易对未配置有效单笔金额");
            return false;
        }
        if (orderAmount.compareTo(properties.getStrategy().getMaxLiveOrderNotionalUsdt()) > 0) {
            log.error("拒绝启动：单笔名义金额超过 LIVE 上限");
            return false;
        }
        if (!calibrateHoldings()) {
            halt("无法确认账户余额，拒绝启动");
            return false;
        }
        // Build/repair today's durable fill cache once at explicit strategy start. Dashboard
        // polling reads this cache and must never turn into a repeated full-day exchange query.
        refreshRemoteTodayStatusSnapshot();
        BnbBalanceSnapshot bnb = refreshBnbBalanceSnapshot(true);
        if (bnb != null && bnb.belowMinimum() && holdingInventory.get().signum() <= 0) {
            isRunning.set(false);
            currentStatus.set(ChurnStatus.IDLE);
            bnbBalanceStopPending.set(false);
            statusReason.set(bnbBalanceMessage(bnb, "当前币种策略保持停止"));
            return false;
        }
        bnbBalanceStopPending.set(bnb != null && bnb.belowMinimum());
        syncDailyCounters();
        if (dailyVolumeLimitReached() && holdingInventory.get().signum() <= 0) {
            isRunning.set(false);
            currentStatus.set(ChurnStatus.IDLE);
            statusReason.set(dailyVolumeLimitMessage("当前币种策略保持停止"));
            return false;
        }
        dailyVolumeStopPending.set(dailyVolumeLimitReached());
        DailyTradeStatsStore.RuntimeState runtimeState = loadRuntimeState();
        restoreFeeAwareEntryPriceCeiling(runtimeState);
        applyConfiguredManualAnchorToCurrentCeiling();
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (openOrders == null) {
            halt("无法确认交易所活动订单，拒绝启动");
            return false;
        }
        if (!openOrders.isEmpty()) {
            if (restoreActiveSellOrder(openOrders, runtimeState)) return true;
            halt("发现未由本进程恢复的活动订单，需先人工对账");
            return false;
        }
        SymbolRuleManager.SymbolRule startupRule = ruleManager.getRule(properties.getStrategy().getSymbol());
        SellabilityResult startupSellability = currentSellability(startupRule, lastBestAsk.get());
        if (holdingInventory.get().signum() > 0 && startupSellability.sellable()) {
            if (restoreRemoteTodayInventoryWithoutActiveOrder(startupRule, startupSellability)) return true;
            halt("发现既有可交易标的持仓，但交易所历史成交无法还原成本或与余额不一致；拒绝自动接管");
            return false;
        }
        if (holdingInventory.get().signum() > 0 && !verifyDustWithinLimit(startupSellability)) {
            return false;
        }
        clearTrackedOrders();
        isRunning.set(true);
        currentStatus.set(ChurnStatus.IDLE);
        if (holdingInventory.get().signum() > 0) {
            updateDustState(startupSellability, "启动时发现残余库存，允许后续 BUY 合并");
        } else {
            statusReason.set("运行中，等待入场信号");
            resetEntryTarget();
            persistRuntimeState(false);
        }
        orderPlacedTimestamp.set(0);
        log.info("[accountId={} alias={}] 引擎启动，当前标的持仓: {}", accountId, accountAlias,
                holdingInventory.get());
        return true;
    }

    private boolean restoreRemoteTodayInventoryWithoutActiveOrder(SymbolRuleManager.SymbolRule rule,
                                                                  SellabilityResult sellability) {
        if (rule == null || sellability == null || !sellability.sellable()) return false;
        RemoteTodayStatusSnapshot remote = getRemoteTodayStatusSnapshot();
        if (remote == null || !remote.remote()) return false;
        DailyTradeStatsStore.DailyStatsSnapshot today = remote.dailyStats();
        TradingRiskGuard.RiskSnapshot recoveredRisk = remote.risk();
        if (recoveredRisk.positionQty().signum() <= 0 || recoveredRisk.positionCostUsdt().signum() <= 0) return false;
        if (holdingInventory.get().subtract(recoveredRisk.positionQty()).abs().compareTo(rule.stepSize()) >= 0) {
            log.warn("[accountId={} alias={}] 启动发现可卖持仓，但交易所余额 {} 与历史成交恢复持仓 {} 不一致，拒绝自动接管",
                    accountId, accountAlias, holdingInventory.get(), recoveredRisk.positionQty());
            return false;
        }

        clearTrackedOrders();
        resetEntryTarget();
        riskGuard.restoreFlatDaily(today.netRealizedPnlQuote(), today.totalCommissionQuoteEquivalent(),
                today.date(), properties.getStrategy());
        riskGuard.restoreOpenPosition(recoveredRisk.positionQty(), recoveredRisk.positionCostUsdt(),
                lastBestAskOrZero(), recoveredRisk.positionOpenedAtMs(), properties.getStrategy());
        isRunning.set(true);
        currentStatus.set(ChurnStatus.SELLING);
        statusReason.set("启动时按交易所历史成交恢复持仓，重新挂卖单");
        submitImmediateExit(rule);
        boolean accepted = activeOrderId.get() != null && currentStatus.get() == ChurnStatus.SELLING;
        if (accepted) {
            log.warn("[accountId={} alias={}] 启动按交易所历史成交恢复无活动订单持仓: qty={} cost={}，已重新挂 SELL",
                    accountId, accountAlias, recoveredRisk.positionQty(), recoveredRisk.positionCostUsdt());
        }
        return accepted;
    }

    private DailyTradeStatsStore.RuntimeState loadRuntimeState() {
        try {
            return dailyStatsStore.loadRuntimeState(accountId, properties.getStrategy().getSymbol()).orElse(null);
        } catch (RuntimeException e) {
            log.warn("[accountId={} alias={}] 读取运行状态快照失败，按无快照处理", accountId, accountAlias);
            return null;
        }
    }

    private void restoreFeeAwareEntryPriceCeiling(DailyTradeStatsStore.RuntimeState state) {
        if (!usesFeeAwareMakerStrategy() || state == null) return;
        restoreFeeAwareRecentBuyPrices(state.feeAwareRecentBuyPrices());
        if (state.feeAwareInitialEntryAnchorPrice() != null
                && state.feeAwareInitialEntryAnchorPrice().signum() > 0) {
            feeAwareInitialEntryAnchorPrice.set(state.feeAwareInitialEntryAnchorPrice());
        }
        if (state.feeAwareEntryPriceCeiling() != null
                && state.feeAwareEntryPriceCeiling().signum() > 0) {
            feeAwareEntryPriceCeiling.set(state.feeAwareEntryPriceCeiling());
            feeAwareInitialEntryAnchorPrice.compareAndSet(null, state.feeAwareEntryPriceCeiling());
        }
    }

    private void applyConfiguredManualAnchorToCurrentCeiling() {
        if (!usesFeeAwareMakerStrategy()) return;
        String symbol = normalizeStrategySymbol(properties.getStrategy().getSymbol());
        BinanceProperties.SymbolStrategyProfile profile = symbolStrategy(symbol);
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
        BigDecimal manual = configuredManualEntryAnchorPrice(profile, rule);
        if (manual == null || manual.signum() <= 0) return;
        feeAwareInitialEntryAnchorPrice.set(manual);
        feeAwareEntryPriceCeiling.set(manual);
        feeAwareEntryCeilingBlockedSince.set(0);
    }

    private void restoreFeeAwareRecentBuyPrices(List<BigDecimal> prices) {
        synchronized (feeAwareRecentBuyPrices) {
            feeAwareRecentBuyPrices.clear();
            if (prices == null) return;
            int start = Math.max(0, prices.size() - 5);
            for (int i = start; i < prices.size(); i++) {
                BigDecimal price = prices.get(i);
                if (price != null && price.signum() > 0) feeAwareRecentBuyPrices.addLast(price);
            }
        }
    }

    private boolean restoreActiveSellOrder(JsonNode openOrders, DailyTradeStatsStore.RuntimeState state) {
        if (state == null || state.orderId() == null
                || state.clientOrderId() == null || state.clientOrderId().isBlank()
                || !"SELLING".equalsIgnoreCase(state.status())
                || !"SELL".equalsIgnoreCase(state.side())
                || !openOrders.isArray() || openOrders.size() != 1) return false;
        JsonNode order = openOrders.get(0);
        if (order.path("orderId").asLong(-1) != state.orderId()) return false;
        if (!state.clientOrderId().equals(order.path("clientOrderId").asText(""))) return false;
        if (!"SELL".equalsIgnoreCase(order.path("side").asText(""))) return false;
        BigDecimal executedQty = decimal(order.path("executedQty").asText("0"));
        if (executedQty.signum() > 0) return false;

        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        TradingRiskGuard.RiskSnapshot recovered = riskGuard.snapshot();
        if (rule == null || recovered.positionQty().signum() <= 0
                || recovered.positionCostUsdt().signum() <= 0
                || holdingInventory.get().compareTo(rule.stepSize()) < 0) return false;
        if (holdingInventory.get().subtract(recovered.positionQty()).abs().compareTo(rule.stepSize()) >= 0) {
            return false;
        }

        BigDecimal orderPrice = decimal(order.path("price").asText(
                state.orderPrice() == null ? "0" : state.orderPrice().toPlainString()));
        BigDecimal originalQty = decimal(order.path("origQty").asText("0"));
        BigDecimal remainingQty = originalQty.subtract(executedQty);
        if (remainingQty.signum() <= 0) remainingQty = state.activeSellCoveredQty();
        if (remainingQty == null || remainingQty.signum() <= 0) return false;

        knownOrderIds.add(state.orderId());
        pendingClientOrderIds.add(state.clientOrderId());
        activeOrderId.set(state.orderId());
        activeClientOrderId.set(state.clientOrderId());
        activeOrderPrice.set(orderPrice.signum() > 0 ? orderPrice : state.orderPrice());
        activeSellCoveredQty.set(remainingQty);
        orderPlacedTimestamp.set(state.orderPlacedAtMs() > 0 ? state.orderPlacedAtMs() : System.currentTimeMillis());
        entryCancellationPending.set(false);
        BigDecimal markPrice = lastBestAskOrZero().signum() > 0 ? lastBestAskOrZero() : activeOrderPrice.get();
        riskGuard.restoreOpenPosition(recovered.positionQty(), recovered.positionCostUsdt(),
                markPrice, recovered.positionOpenedAtMs(), properties.getStrategy());
        isRunning.set(true);
        currentStatus.set(ChurnStatus.SELLING);
        statusReason.set("重启后已恢复本进程卖单 " + state.orderId()
                + "，继续等待成交/检查换单");
        persistRuntimeState(true);
        if (currentStatus.get() == ChurnStatus.HALTED) return false;
        scheduleOrderReconciliation(state.orderId());
        log.warn("[accountId={} alias={}] 重启恢复 SELL 单: orderId={} clientOrderId={} qty={} price={}",
                accountId, accountAlias, state.orderId(), state.clientOrderId(),
                remainingQty, activeOrderPrice.get());
        return true;
    }

    private void persistRuntimeState(boolean includeActiveOrder) {
        String symbol = properties.getStrategy().getSymbol();
        BigDecimal ceiling = runtimeFeeAwareEntryPriceCeiling(symbol);
        Long orderId = includeActiveOrder ? activeOrderId.get() : null;
        ChurnStatus status = includeActiveOrder && orderId != null ? currentStatus.get() : ChurnStatus.IDLE;
        String clientOrderId = includeActiveOrder ? activeClientOrderId.get() : null;
        String side = status == ChurnStatus.BUYING ? "BUY" : status == ChurnStatus.SELLING ? "SELL" : null;
        try {
            if (orderId == null && (ceiling == null || ceiling.signum() <= 0)) {
                dailyStatsStore.clearRuntimeState(accountId, symbol);
                return;
            }
            dailyStatsStore.saveRuntimeState(accountId, symbol, new DailyTradeStatsStore.RuntimeState(
                    accountId, symbol, status.name(), orderId, clientOrderId, side, activeOrderPrice.get(),
                    activeSellCoveredQty.get(), ceiling, orderPlacedTimestamp.get(), System.currentTimeMillis(),
                    feeAwareInitialEntryAnchorPrice.get(), feeAwareRecentBuyPricesSnapshot()));
        } catch (RuntimeException e) {
            log.error("[accountId={} alias={}] 保存运行状态快照失败", accountId, accountAlias, e);
            if (includeActiveOrder && orderId != null) {
                halt("活动订单状态持久化失败，已停止真实交易，需人工对账");
            }
        }
    }

    private BigDecimal runtimeFeeAwareEntryPriceCeiling(String symbol) {
        if (!usesFeeAwareMakerStrategy()) return null;
        BigDecimal ceiling = feeAwareEntryPriceCeiling.get();
        if (currentStatus.get() == ChurnStatus.SELLING && filledEntryQuantity.get().signum() > 0) {
            SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
            BigDecimal currentEntryAnchor = entryMaxFloorPrice(rule);
            if (currentEntryAnchor.signum() > 0) ceiling = currentEntryAnchor;
        }
        return ceiling;
    }

    private List<BigDecimal> feeAwareRecentBuyPricesSnapshot() {
        synchronized (feeAwareRecentBuyPrices) {
            return new ArrayList<>(feeAwareRecentBuyPrices);
        }
    }

    public synchronized boolean stopTrading() {
        isRunning.set(false);
        Long orderId = activeOrderId.get();
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
        if (currentStatus.get() == ChurnStatus.BUYING) accountRiskCoordinator.releaseEntry(accountEngineKey);
        clearActiveOrder();
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        SellabilityResult sellability = currentSellability(rule, lastBestAskOrZero());
        if (rule != null && sellability.sellable()) {
            halt("活动订单已清理，但仍有标的持仓 " + holdingInventory.get() + "，不可报告为安全空仓");
            return false;
        }
        currentStatus.set(ChurnStatus.IDLE);
        if (holdingInventory.get().signum() > 0) {
            updateDustState(sellability, "已停止，保留不足最小卖单条件的 DUST");
        } else {
            statusReason.set("已安全停止");
            resetEntryTarget();
        }
        log.info("[accountId={} alias={}] 引擎已停止。总交易量: {} USDT, 闭环轮数: {}",
                accountId, accountAlias, totalVolumeUsdt.get(), roundTripsCompleted.get());
        return true;
    }

    /** Operator-authorized, reduce-only-style liquidation of the currently free base-asset balance. */
    public synchronized LiquidationResult liquidateExistingPosition() {
        isRunning.set(false);
        if (!properties.getStrategy().isLiveTradingEnabled()) {
            return LiquidationResult.rejected("服务器未配置 BINANCE_LIVE_TRADING_ENABLED=true");
        }
        if (!accountStreamReady.getAsBoolean()) {
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
        reserveActiveSellQuantity(qty);
        JsonNode response = tradeService.placeMarketSell(symbol, qty, clientOrderId);
        if (response != null && response.has("orderId")) {
            long orderId = response.get("orderId").asLong();
            trackOrder(orderId, clientOrderId, ChurnStatus.SELLING);
            activeOrderPrice.set(BigDecimal.ZERO);
            persistRuntimeState(true);
            statusReason.set("已提交人工授权市价清仓单，等待账户成交流确认");
            log.warn("[accountId={} alias={}] 人工授权清仓：已提交市价卖单 ID={} qty={} {}",
                    accountId, accountAlias, orderId, qty, baseAsset());
            return new LiquidationResult(true, orderId, qty, statusReason.get());
        }
        pendingClientOrderIds.remove(clientOrderId);
        activeClientOrderId.compareAndSet(clientOrderId, null);
        releaseActiveSellReservation();
        halt(response == null ? "市价清仓单结果未知，需人工核对" :
                "市价清仓单被交易所拒绝: " + response.path("code").asText("unknown") + " "
                        + response.path("msg").asText("unknown"));
        return LiquidationResult.rejected(statusReason.get());
    }

    public void shutdown() {
        acceptingMarketConnections.set(false);
        WebSocket socket = activeMarketWebSocket.getAndSet(null);
        if (socket != null) socket.abort();
        marketWatchdog.shutdownNow();
        try {
            stopTrading();
        } finally {
            accountRiskCoordinator.unregister(accountEngineKey);
        }
    }

    public synchronized void handleUserStreamLoss(String reason) {
        if (isRunning.get()) userStreamOrderReconcilePending.set(true);
        protectOnStreamLoss("账户成交流不可用: " + reason);
    }

    /** A reconnect may have missed order events, so rebuild the dashboard snapshot once. */
    public void handleUserStreamReady() {
        handleUserStreamReady(true);
    }

    /** Account runtimes share one user stream and request one account-wide order snapshot. */
    public boolean handleUserStreamReady(boolean refreshAccountOpenOrders) {
        boolean reconciliationPending = userStreamOrderReconcilePending.getAndSet(false);
        boolean reconciliationRequired = isRunning.get() || reconciliationPending;
        if (reconciliationRequired) {
            reconcileRemoteTodayTradesIncrementally();
            if (refreshAccountOpenOrders) refreshDashboardOpenOrderSnapshot();
        }
        statusReason.compareAndSet("账户成交流暂不可用，等待自动重连", statusForCurrentState());
        return reconciliationRequired;
    }

    private String statusForCurrentState() {
        return switch (currentStatus.get()) {
            case IDLE -> "运行中，等待入场信号";
            case BUYING -> "买单处理中";
            case SELLING -> "卖单处理中";
            case HALTED -> statusReason.get();
        };
    }

    /** One account-wide REST snapshot at strategy start or after account-stream recovery. */
    public void refreshDashboardOpenOrderSnapshot() {
        JsonNode openOrders;
        try {
            openOrders = tradeService.getAllOpenOrders();
        } catch (RuntimeException e) {
            log.warn("[accountId={} alias={}] 控制台活动订单快照读取异常，保留其他账户和本账户上一份数据: {}",
                    accountId, accountAlias, e.getMessage());
            return;
        }
        if (openOrders == null || !openOrders.isArray()) {
            log.warn("[accountId={} alias={}] 控制台活动订单快照刷新失败，保留上一份 WebSocket 状态",
                    accountId, accountAlias);
            return;
        }
        List<OpenOrderNotification> snapshot = new ArrayList<>();
        for (JsonNode order : openOrders) {
            snapshot.add(new OpenOrderNotification(
                    accountId, accountAlias, order.path("symbol").asText(""),
                    order.path("side").asText(""), order.path("type").asText(""),
                    order.path("status").asText("NEW"), order.path("price").asText("0"),
                    order.path("origQty").asText("0"), order.path("executedQty").asText("0"),
                    order.path("orderId").asLong(0),
                    order.path("time").asLong(order.path("updateTime").asLong(System.currentTimeMillis()))));
        }
        notificationService.replaceOpenOrders(accountId, snapshot);
        log.info("[accountId={} alias={}] 控制台活动订单快照已对账，当前 {} 笔",
                accountId, accountAlias, snapshot.size());
    }

    private void protectOnStreamLoss(String reason) {
        boolean wasRunning = isRunning.getAndSet(false);
        Long orderId = activeOrderId.get();
        if (orderId != null && currentStatus.get() == ChurnStatus.BUYING) {
            tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId);
        }
        currentStatus.set(ChurnStatus.HALTED);
        statusReason.set(reason);
        if (wasRunning || orderId != null) log.error("[accountId={} alias={}] {}；已停机，重连后不会自动恢复",
                accountId, accountAlias, reason);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        WebSocket activeSocket = activeMarketWebSocket.get();
        if (activeSocket != null && webSocket != activeSocket) {
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }
        lastMarketFrameTimestamp.set(System.currentTimeMillis());
        if (transientMarketRecoveryPending.compareAndSet(true, false)) {
            String recoveryMessage = "行情流 1001 瞬断，正在自动重连";
            String previousReason = preMarketRecoveryStatusReason.getAndSet(null);
            statusReason.compareAndSet(recoveryMessage,
                    previousReason == null || previousReason.isBlank() ? "运行中，等待行情" : previousReason);
            log.info("[accountId={} alias={}] 行情流 1001 重连成功，已收到新行情并继续运行",
                    accountId, accountAlias);
        }
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
                if (isRunning.get()) driveChurnStateMachine(bid, ask);
            } else if (node.has("q") && node.has("m")) {
                marketSignalEvaluator.recordAggTrade(new BigDecimal(node.get("q").asText()), node.get("m").asBoolean(), System.currentTimeMillis(), properties.getStrategy());
            } else if ((node.has("bids") && node.has("asks")) || (node.has("b") && node.has("a"))) {
                // Binance depth payloads use bids/asks on partial-depth streams and b/a on diff-depth streams.
                JsonNode bids = node.has("bids") ? node.get("bids") : node.get("b");
                JsonNode asks = node.has("asks") ? node.get("asks") : node.get("a");
                long now = System.currentTimeMillis();
                latestBidDepthPrices.set(depthPrices(bids));
                lastDepthDataTimestamp.set(now);
                marketSignalEvaluator.recordDepth(sumDepth(bids), sumDepth(asks), now);
            }
        } catch (Exception e) {
            log.error("Tick 解析异常", e);
        }
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    private synchronized void driveChurnStateMachine(BigDecimal bestBid, BigDecimal bestAsk) {
        if (bestBid != null && bestBid.signum() > 0) lastBestBid.set(bestBid);
        if (bestAsk != null && bestAsk.signum() > 0) lastBestAsk.set(bestAsk);
        applyPendingStrategyIfSafe();
        var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null || !isRunning.get()) return;
        if (!accountStreamReady.getAsBoolean()) {
            statusReason.set("账户成交流暂不可用，等待自动重连");
            return;
        }
        refreshDailyVolumeCounterDate();
        long now = System.currentTimeMillis();
        if (enforceAccountRiskLimit()) return;
        if (enforceDailyVolumeLimit(rule)) return;
        if (enforceBnbBalanceMinimum(rule)) return;
        if (now < nextOrderAttemptAt.get()) return;
        String symbol = properties.getStrategy().getSymbol();
        switch (currentStatus.get()) {
            case IDLE -> {
                SellabilityResult residual = currentSellability(rule, exitReferencePrice(rule, bestAsk));
                if (holdingInventory.get().signum() > 0) {
                    if (!verifyDustWithinLimit(residual)) return;
                    if (residual.sellable()) {
                        currentStatus.set(ChurnStatus.SELLING);
                        statusReason.set("残余库存已达到可卖条件，合并后挂限价卖单");
                        submitImmediateExit(rule);
                        return;
                    }
                    updateDustState(residual, "残余库存等待后续 BUY 合并");
                }
                if (postSellEntryDelayActive(now)) return;
                MarketSignalEvaluator.EntryDecision decision = entryDecisionForStrategy(now);
                activeEntrySignalReason.set(decision.reason());
                activeEntryContext.set(marketSignalEvaluator.getMarketContext(now));
                if (!decision.allowed()) {
                    statusReason.set("等待入场信号: " + decision.reason());
                    return;
                }
                BigDecimal price = entryPriceForStrategy(bestBid, rule, now);
                if (price == null || price.signum() <= 0) return;
                BigDecimal qty = capEntryQuantity(buyQuantity(price, rule), price, rule);
                if (!isValidOrder(qty, price, rule)) return;
                boolean dustMergeEntry = holdingInventory.get().signum() > 0 && !residual.sellable();
                if (!riskGuard.permitsNewEntry(qty, price, now, properties.getStrategy(), dustMergeEntry)) {
                    log.warn("[accountId={} alias={}] 新开仓被风险熔断阻止: {}",
                            accountId, accountAlias, riskGuard.getEntryBlockReason());
                    return;
                }
                targetEntryQuantity.set(qty);
                if (holdingInventory.get().signum() == 0) {
                    filledEntryQuantity.set(BigDecimal.ZERO);
                    filledEntryQuoteQuantity.set(BigDecimal.ZERO);
                }
                submitMakerOrder(symbol, "BUY", price, qty, null, ChurnStatus.BUYING);
            }
            case BUYING -> {
                Long orderId = activeOrderId.get();
                if (orderId == null) { halt("买单状态没有活动订单"); return; }
                long restingMs = now - orderPlacedTimestamp.get();
                long makerTimeoutMs = Math.max(entryOrderTimeoutMs(),
                        properties.getStrategy().getMinEntryOrderRestMs());
                if (!entryCancellationPending.get() && restingMs >= makerTimeoutMs) {
                    BigDecimal orderPrice = activeOrderPrice.get();
                    BigDecimal currentEntryPrice = configuredEntryBookPrice(bestBid, rule, now);
                    if (currentEntryPrice == null || currentEntryPrice.signum() <= 0) {
                        orderPlacedTimestamp.set(now);
                        persistRuntimeState(true);
                        return;
                    }
                    if (usesFeeAwareMakerStrategy() && !feeAwareEntryAllowedByAnchor(currentEntryPrice, rule)) {
                        BigDecimal maxAllowed = feeAwareAllowedEntryPrice(rule);
                        if (orderPrice != null && maxAllowed != null && orderPrice.compareTo(maxAllowed) <= 0) {
                            statusReason.set(feeAwareAnchorWaitMessage(currentEntryPrice, rule,
                                    "保留较低 Maker 买单 @ " + orderPrice.toPlainString()));
                        } else {
                            cancelActiveEntryOrder("Maker 买单高于允许买入上限，撤单等待价格回落");
                        }
                        return;
                    }
                    if (orderPrice != null && orderPrice.compareTo(currentEntryPrice) == 0) {
                        statusReason.set("Maker 买单已满 " + durationLabel(makerTimeoutMs)
                                + " 但仍处于" + configuredEntryBookLevelLabel() + "，继续挂单 @ "
                                + orderPrice.toPlainString());
                    } else {
                        cancelActiveEntryOrder("Maker 买单已不在" + configuredEntryBookLevelLabel()
                                + "，撤单等待重新挂单（不转 IOC）");
                    }
                }
            }
            case SELLING -> {
                Long activeId = activeOrderId.get();
                if (activeId != null) {
                    if (now - orderPlacedTimestamp.get() >= exitOrderTimeoutMs()) {
                        BigDecimal currentBestAsk = positiveOrZero(bestAsk);
                        if (currentBestAsk.signum() <= 0) currentBestAsk = lastBestAskOrZero();
                        if (deferTimedOutExitIfStillBestAsk(rule, currentBestAsk, now)) return;
                        if (currentBestAsk.signum() <= 0) return;
                        rollTimedOutExitToBestAsk(symbol, currentBestAsk, rule, activeId);
                    }
                    return;
                }
                SellabilityResult sellability = currentSellability(rule, exitReferencePrice(rule, bestAsk));
                if (!sellability.sellable()) {
                    if (!verifyDustWithinLimit(sellability)) return;
                    currentStatus.set(ChurnStatus.IDLE);
                    updateDustState(sellability, "剩余持仓不足以创建有效卖单，等待后续 BUY 合并");
                    return;
                }
                submitImmediateExit(rule);
            }
            case HALTED -> { }
        }
    }

    public synchronized void onOrderUpdate(AccountExecutionEvent update) {
        if (!accountId.equals(update.accountId())) {
            log.error("CRITICAL account isolation violation runtimeAccount={} eventAccount={}",
                    accountId, update.accountId());
            return;
        }
        if (!properties.getStrategy().getSymbol().equalsIgnoreCase(update.symbol())) return;
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
                recordUntrackedExecutionForDailyStats(update);
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
            updateDustState(currentSellability(rule, exitReferencePrice(rule)),
                    "BUY 已部分成交，尚未达到最小可卖额，继续等待成交");
        }
        if (activeEvent && "BUY".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.BUYING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus) || "EXPIRED".equals(orderStatus))) {
            if (!reconcileTerminalEvent(orderId, side, update.cumulativeExecutedQty(), update.cumulativeQuoteQty(), rule)) return;
            clearActiveOrder();
            transitionAfterInventoryChange(rule, "BUY 已成交，按实际成交均价挂限价卖单");
        } else if (activeEvent && "SELL".equalsIgnoreCase(side) && currentStatus.get() == ChurnStatus.SELLING
                && ("FILLED".equals(orderStatus) || "CANCELED".equals(orderStatus)
                || "EXPIRED".equals(orderStatus) || "EXPIRED_IN_MATCH".equals(orderStatus))) {
            if (!reconcileTerminalEvent(orderId, side, update.cumulativeExecutedQty(), update.cumulativeQuoteQty(), rule)) return;
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) < 0) {
                completeFlatExit(false);
            } else {
                transitionAfterInventoryChange(rule, "SELL 终态后剩余持仓重新评估");
            }
        }
        if (isTerminal(orderStatus)) {
            if ("BUY".equalsIgnoreCase(side)) accountRiskCoordinator.releaseEntry(accountEngineKey);
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
        onOrderUpdate(new AccountExecutionEvent(accountId, properties.getStrategy().getSymbol(), orderId, -1,
                clientOrderId, side, executionType, orderStatus, lastFilledQty, lastFilledPrice,
                lastFilledQty, cumulativeQuote, commission, commissionAsset, false,
                System.currentTimeMillis()));
    }

    private TradeAccountingLedger.AppliedTrade applyExecutionTrade(AccountExecutionEvent update) {
        BigDecimal quoteQuantity = update.lastExecutedQty().multiply(update.lastExecutedPrice());
        return applyTrade(update.orderId(), update.tradeId(), update.side(), update.lastExecutedQty(),
                update.lastExecutedPrice(), quoteQuantity, update.commission(), update.commissionAsset(),
                update.clientOrderId(), update.eventTime());
    }

    /**
     * Account trade history is broader than the strategy's in-memory order set. Preserve an
     * untracked fill in the durable daily ledger before the existing safety halt asks for manual
     * reconciliation. This does not mutate strategy inventory or submit follow-up orders.
     */
    private void recordUntrackedExecutionForDailyStats(AccountExecutionEvent update) {
        BigDecimal quantity = update.lastExecutedQty();
        BigDecimal price = update.lastExecutedPrice();
        BigDecimal quoteQuantity = quantity.multiply(price);
        BigDecimal commissionQuote = commissionQuoteEquivalent(
                update.commission(), update.commissionAsset(), price);
        boolean baseCommission = baseAsset().equalsIgnoreCase(update.commissionAsset());
        BigDecimal inventoryQuantity = quantity;
        if (baseCommission) {
            inventoryQuantity = "BUY".equalsIgnoreCase(update.side())
                    ? quantity.subtract(update.commission()).max(BigDecimal.ZERO)
                    : quantity.add(update.commission());
        }
        BigDecimal economicFeeQuote = baseCommission ? BigDecimal.ZERO : commissionQuote;
        DailyTradeStatsStore.RecordResult result = dailyStatsStore.recordTrade(
                accountId, accountAlias, update.symbol(), update.orderId(), update.tradeId(), update.side(),
                inventoryQuantity, quantity, price, quoteQuantity, update.commission(),
                update.commissionAsset(), commissionQuote, economicFeeQuote, update.eventTime());
        if (result == DailyTradeStatsStore.RecordResult.FAILED) {
            log.error("[accountId={} alias={}] 未关联成交写入每日统计失败: orderId={} tradeId={}",
                    accountId, accountAlias, update.orderId(), update.tradeId());
        } else {
            syncDailyCounters();
        }
    }

    private TradeAccountingLedger.AppliedTrade applyTrade(long orderId, long tradeId, String side,
                                                          BigDecimal quantity, BigDecimal price,
                                                          BigDecimal quoteQuantity, BigDecimal commission,
                                                          String commissionAsset, String clientOrderId,
                                                          long tradeTimeMs) {
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
                accountId, accountAlias, properties.getStrategy().getSymbol(), orderId, tradeId,
                side, inventoryQuantity, trade.quantity(), price, trade.quoteQuantity(), trade.commission(),
                trade.commissionAsset(), commissionQuote, cashCommissionQuote, tradeTimeMs);
        if (persistentResult == DailyTradeStatsStore.RecordResult.FAILED) {
            halt("每日交易统计写入失败，已停止真实交易");
            return TradeAccountingLedger.AppliedTrade.ignored();
        }
        if (persistentResult == DailyTradeStatsStore.RecordResult.DUPLICATE) {
            log.warn("[accountId={} alias={}] 忽略数据库已记录的重复成交: orderId={} tradeId={}",
                    accountId, accountAlias, orderId, tradeId);
            syncDailyCounters();
            return TradeAccountingLedger.AppliedTrade.ignored();
        }
        BigDecimal notificationEntryPrice = "SELL".equalsIgnoreCase(side)
                ? currentEntryAverageExecutionPrice() : null;
        BigDecimal notificationEntryQuote = notificationEntryPrice == null
                ? null : notificationEntryPrice.multiply(quantity);
        notificationService.notifyFill(new FillNotification(accountId, accountAlias,
                properties.getStrategy().getSymbol(), side, orderId, tradeId,
                clientOrderId == null ? "" : clientOrderId, quantity, price,
                quoteQuantity, notificationEntryPrice, notificationEntryQuote,
                commission, commissionAsset, tradeTimeMs));
        if ("BUY".equalsIgnoreCase(side)) {
            filledEntryQuantity.accumulateAndGet(trade.quantity(), BigDecimal::add);
            filledEntryQuoteQuantity.accumulateAndGet(trade.quoteQuantity(), BigDecimal::add);
            filledEntryMaxPrice.accumulateAndGet(price, BigDecimal::max);
            rememberFeeAwareRecentBuyPrice(price);
            holdingInventory.accumulateAndGet(inventoryQuantity, BigDecimal::add);
            addKnownFreeBase(inventoryQuantity);
            if (properties.getStrategy().isCollectObservations()) {
                postFillOutcomeTracker.recordBuyFill(price, activeEntrySignalReason.get(), activeEntryContext.get(),
                        tradeTimeMs > 0 ? tradeTimeMs : System.currentTimeMillis());
            }
        } else {
            holdingInventory.accumulateAndGet(inventoryQuantity, BigDecimal::subtract);
            if (holdingInventory.get().signum() < 0) holdingInventory.set(BigDecimal.ZERO);
            activeSellCoveredQty.accumulateAndGet(trade.quantity(), BigDecimal::subtract);
            if (activeSellCoveredQty.get().signum() < 0) activeSellCoveredQty.set(BigDecimal.ZERO);
            subtractKnownFreeBase(inventoryQuantity);
        }
        syncDailyCounters();
        if (dailyVolumeLimitReached() && dailyVolumeStopPending.compareAndSet(false, true)) {
            log.warn("[accountId={} alias={}] 今日真实成交量 {} USDT 已达到上限 {} USDT；停止新买入，空仓后自动停止",
                    accountId, accountAlias, totalVolumeUsdt.get(), dailyVolumeLimitUsdt());
        }
        riskGuard.recordActualFill(side, inventoryQuantity, trade.quoteQuantity(), cashCommissionQuote,
                tradeTimeMs > 0 ? tradeTimeMs : System.currentTimeMillis(), properties.getStrategy());
        if (riskGuard.getEntryBlockReason() != null) {
            log.warn("[accountId={} alias={}] 风险熔断已触发: {}",
                    accountId, accountAlias, riskGuard.getEntryBlockReason());
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
            log.warn("[accountId={} alias={}] 订单 {} 的真实成交与手续费明细暂不可用，等待 REST 重试",
                    accountId, accountAlias, orderId);
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
                    trade.path("clientOrderId").asText(""),
                    trade.path("time").asLong(System.currentTimeMillis()));
        }
        BigDecimal accountedQuantity = accountingLedger.accountedQuantity(orderId);
        BigDecimal accountedQuote = accountingLedger.accountedQuote(orderId);
        if (accountedQuantity.compareTo(expectedQuantity) != 0
                || (expectedQuote != null && expectedQuote.signum() > 0 && accountedQuote.compareTo(expectedQuote) != 0)) {
            log.warn("[accountId={} alias={}] 订单 {} 成交明细尚未追平累计值: qty={}/{}, quote={}/{}",
                    accountId, accountAlias, orderId, accountedQuantity, expectedQuantity, accountedQuote, expectedQuote);
            return false;
        }
        return true;
    }

    private boolean reconcileInventory(SymbolRuleManager.SymbolRule rule, String context) {
        BinanceAccountTradeClient.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (balance == null) {
            BigDecimal free = tradeService.getFreeAssetBalance(baseAsset());
            if (free != null) balance = new BinanceAccountTradeClient.AssetBalance(baseAsset(), free,
                    BigDecimal.ZERO, free);
        }
        if (balance == null) {
            halt(context + " 后无法读取账户总持仓");
            return false;
        }
        BigDecimal expected = holdingInventory.get();
        BigDecimal difference = balance.total().subtract(expected).abs();
        holdingInventory.set(balance.total());
        lastKnownFreeBaseBalance.set(balance.free());
        if (difference.compareTo(rule.stepSize()) >= 0) {
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
        accountRiskCoordinator.releaseEntry(accountEngineKey);
        currentStatus.set(ChurnStatus.SELLING);
        statusReason.set("BUY 部分成交已停止追单，立即退出已成交持仓");
        submitImmediateExit(rule);
    }

    private void submitImmediateExit(SymbolRuleManager.SymbolRule rule) {
        if (activeOrderId.get() != null
                || currentStatus.get() != ChurnStatus.SELLING) return;
        BigDecimal floorPrice = initialStrategyExitPrice(rule);
        SellabilityResult sellability = currentSellability(rule, floorPrice);
        if (!sellability.sellable()) {
            if (!verifyDustWithinLimit(sellability)) return;
            currentStatus.set(ChurnStatus.IDLE);
            updateDustState(sellability, "已成交持仓不足以创建有效卖单，等待后续 BUY 合并");
            return;
        }
        BigDecimal quantity = sellability.normalizedQty();
        if (usesFeeAwareMakerStrategy()) {
            reserveActiveSellQuantity(quantity);
            submitMakerOrder(properties.getStrategy().getSymbol(), "SELL", floorPrice, quantity, null,
                    ChurnStatus.SELLING);
            if (activeOrderId.get() != null) {
                statusReason.set("手续费保护卖单已挂出 @ " + floorPrice.toPlainString()
                        + "（不低于手续费保护价）");
            }
            return;
        }
        String clientOrderId = nextClientOrderId("SELLG");
        pendingClientOrderIds.add(clientOrderId);
        activeClientOrderId.set(clientOrderId);
        reserveActiveSellQuantity(quantity);
        JsonNode response = tradeService.placeLimitGtcSell(properties.getStrategy().getSymbol(), quantity,
                floorPrice, clientOrderId);
        if (response != null && response.has("orderId")) {
            trackOrder(response.get("orderId").asLong(), clientOrderId, ChurnStatus.SELLING);
            activeOrderPrice.set(floorPrice);
            persistRuntimeState(true);
            String initialExitReason = usesBidAskMakerStrategy()
                    ? "BUY 成交后按卖一/买入均价上方 " + bidAskInitialSellMarkupTicks()
                    + " tick 挂 LIMIT 卖单 @ " + floorPrice.toPlainString()
                    : usesBuyPriceMakerStrategy()
                    ? "BUY 成交后按实际买入均价挂 LIMIT 卖单 @ " + floorPrice.toPlainString()
                    : "BUY 成交后按买入均价下限卖出中 @ " + floorPrice.toPlainString();
            statusReason.set(initialExitReason);
            log.info("[accountId={} alias={}] BUY 成交后已挂 GTC 限价卖出 {} {} @ {}",
                    accountId, accountAlias, quantity, baseAsset(), floorPrice);
        } else {
            reconcileAmbiguousSubmission(clientOrderId, ChurnStatus.SELLING, "买入均价 GTC 卖单结果未知");
        }
    }

    /**
     * Every sell order has a configurable working window. Once it expires, keep
     * the order when it is still at the latest best ask and re-arm the timer.
     * Only an order that is no longer at the best ask is canceled, reconciled,
     * and repriced. All strategies use the latest best ask when repricing;
     * FEE_AWARE_MAKER only enforces the fee floor on the initial exit order, while
     * BUY_PRICE_MAKER only enforces the actual buy average on the initial exit order.
     * Neither path falls back to MARKET.
     */
    private boolean deferTimedOutExitIfStillBestAsk(SymbolRuleManager.SymbolRule rule,
                                                    BigDecimal bestAsk, long now) {
        BigDecimal normalizedAsk = positiveOrZero(bestAsk);
        if (normalizedAsk.signum() <= 0) {
            orderPlacedTimestamp.set(now);
            persistRuntimeState(true);
            statusReason.set("卖单已超时但暂时无法确认最新卖一，保留当前 LIMIT 卖单；下次按配置时间复查");
            log.info("[accountId={} alias={}] 卖单超时但最新卖一暂不可用，保留订单 {}，下次按 {} 复查",
                    accountId, accountAlias, activeOrderId.get(), durationLabel(exitOrderTimeoutMs()));
            return true;
        }
        normalizedAsk = PrecisionUtil.roundDownToStep(normalizedAsk, rule.tickSize());
        BigDecimal orderPrice = activeOrderPrice.get();
        if (orderPrice == null || orderPrice.signum() <= 0) return false;
        BigDecimal normalizedOrderPrice = PrecisionUtil.roundDownToStep(orderPrice, rule.tickSize());
        if (normalizedOrderPrice.compareTo(normalizedAsk) != 0) return false;

        orderPlacedTimestamp.set(now);
        persistRuntimeState(true);
        statusReason.set("卖单已超时但仍在卖一，保留当前 LIMIT 卖单 @ "
                + normalizedOrderPrice.toPlainString() + "；下次按配置时间复查");
        log.info("[accountId={} alias={}] 卖单 {} 满 {} 仍在卖一 @ {}，保留原单并重新计时",
                accountId, accountAlias, activeOrderId.get(), durationLabel(exitOrderTimeoutMs()),
                normalizedOrderPrice);
        return true;
    }

    private void rollTimedOutExitToBestAsk(String symbol, BigDecimal bestAsk,
                                           SymbolRuleManager.SymbolRule rule, long orderId) {
        if (!exitSubmissionInFlight.compareAndSet(false, true)) return;
        try {
            JsonNode cancel = tradeService.cancelOrder(symbol, orderId);
            JsonNode finalOrder = tradeService.getOrder(symbol, orderId);
            if (finalOrder == null || !isTerminal(finalOrder.path("status").asText())) {
                halt(durationLabel(exitOrderTimeoutMs()) + "卖单超时后无法确认原限价卖单已撤销");
                return;
            }
            if (cancel == null || cancel.has("code")) {
                log.info("[accountId={} alias={}] 卖单超时撤单响应未成功，但订单 {} 已处于终态 {}；先按 REST 对账",
                        accountId, accountAlias, orderId, finalOrder.path("status").asText());
            }
            String side = finalOrder.path("side").asText("SELL");
            BigDecimal executedQty = new BigDecimal(finalOrder.path("executedQty").asText("0"));
            BigDecimal cumulativeQuote = new BigDecimal(finalOrder.path("cummulativeQuoteQty").asText("0"));
            if (executedQty.signum() > 0
                    && !reconcileOrderTrades(orderId, side, executedQty, cumulativeQuote)) {
                statusReason.set("等待卖单 " + orderId + " 的 REST 成交明细完成同步");
                scheduleOrderReconciliation(orderId);
                return;
            }
            if (!reconcileInventory(rule, durationLabel(exitOrderTimeoutMs()) + "卖单超时撤单")) return;
            markRestReconciled(orderId);
            clearActiveOrder();
            if (holdingInventory.get().compareTo(rule.stepSize()) < 0) {
                completeFlatExit(true);
                return;
            }
            if (currentStatus.get() != ChurnStatus.SELLING) currentStatus.set(ChurnStatus.SELLING);

            BigDecimal price = timedOutExitPrice(rule, bestAsk);
            SellabilityResult sellability = currentSellability(rule, price);
            if (!sellability.sellable()) {
                if (!verifyDustWithinLimit(sellability)) return;
                currentStatus.set(ChurnStatus.IDLE);
                updateDustState(sellability, "剩余持仓不足以按卖一价创建 LIMIT 卖单，等待后续 BUY 合并");
                return;
            }
            BigDecimal quantity = sellability.normalizedQty();
            if (usesFeeAwareMakerStrategy()) {
                reserveActiveSellQuantity(quantity);
                submitMakerOrder(symbol, "SELL", price, quantity, null, ChurnStatus.SELLING);
                if (activeOrderId.get() != null) {
                    statusReason.set("卖单超时后按最新卖一价快速退出 @ " + price.toPlainString());
                }
                return;
            }
            String clientOrderId = nextClientOrderId("SELLA");
            pendingClientOrderIds.add(clientOrderId);
            activeClientOrderId.set(clientOrderId);
            reserveActiveSellQuantity(quantity);
            JsonNode response = tradeService.placeLimitGtcSell(symbol, quantity, price, clientOrderId);
            if (response != null && response.has("orderId")) {
                trackOrder(response.get("orderId").asLong(), clientOrderId, ChurnStatus.SELLING);
                activeOrderPrice.set(price);
                persistRuntimeState(true);
                statusReason.set("上一张卖单满 " + durationLabel(exitOrderTimeoutMs())
                        + "，剩余持仓已按最新卖一挂 LIMIT @ "
                        + price.toPlainString());
                log.info("[accountId={} alias={}] 卖单满 {}，已按最新卖一重新挂 LIMIT {} {} @ {}",
                        accountId, accountAlias, durationLabel(exitOrderTimeoutMs()),
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
        return currentSellability(rule, entryAverageFloorPrice(rule)).sellable();
    }

    private void transitionAfterInventoryChange(SymbolRuleManager.SymbolRule rule, String sellMessage) {
        if (holdingInventory.get().signum() <= 0) {
            currentStatus.set(ChurnStatus.IDLE);
            resetEntryTarget();
            statusReason.set("运行中，等待入场信号");
            return;
        }
        SellabilityResult sellability = currentSellability(rule, exitReferencePrice(rule));
        if (!verifyDustWithinLimit(sellability)) return;
        if (sellability.sellable()) {
            String previousDust = lastDustStateSignature.getAndSet("");
            if (!previousDust.isBlank()) {
                log.info("[accountId={} alias={}] dust merged with new fill and became sellable: totalSellableQty={} notional={}",
                        accountId, accountAlias, sellability.normalizedQty(), sellability.notional());
            }
            currentStatus.set(ChurnStatus.SELLING);
            statusReason.set(sellMessage);
            submitImmediateExit(rule);
        } else {
            currentStatus.set(ChurnStatus.IDLE);
            updateDustState(sellability, "残余库存不足以创建 SELL，等待后续 BUY 合并");
        }
    }

    public SellabilityResult getSellabilitySnapshot() {
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        return currentSellability(rule, exitReferencePrice(rule));
    }

    private SellabilityResult currentSellability(SymbolRuleManager.SymbolRule rule, BigDecimal sellPrice) {
        return evaluateSellability(holdingInventory.get(), activeSellCoveredQty.get(), knownFreeBaseBalance(),
                sellPrice, rule);
    }

    public SellabilityResult evaluateSellability(BigDecimal inventoryQty, BigDecimal coveredByActiveSell,
                                                 BigDecimal freeBalance, BigDecimal sellPrice,
                                                 SymbolRuleManager.SymbolRule rule) {
        BigDecimal rawAvailable = positiveOrZero(inventoryQty).subtract(positiveOrZero(coveredByActiveSell));
        if (rawAvailable.signum() < 0) rawAvailable = BigDecimal.ZERO;
        BigDecimal price = positiveOrZero(sellPrice);
        if (rawAvailable.signum() == 0 || rule == null) {
            return new SellabilityResult(rawAvailable, BigDecimal.ZERO, BigDecimal.ZERO, false,
                    DustReason.NONE);
        }
        BigDecimal free = freeBalance == null ? rawAvailable : positiveOrZero(freeBalance);
        if (free.signum() <= 0) {
            return new SellabilityResult(rawAvailable, BigDecimal.ZERO, BigDecimal.ZERO, false,
                    DustReason.INSUFFICIENT_FREE_BALANCE);
        }
        BigDecimal candidate = rawAvailable.min(free);
        BigDecimal normalized = PrecisionUtil.roundDownToStep(candidate, rule.stepSize());
        BigDecimal notional = normalized.multiply(price);
        if (normalized.signum() <= 0 && candidate.signum() > 0) {
            return new SellabilityResult(rawAvailable, normalized, notional, false, DustReason.BELOW_STEP_SIZE);
        }
        if (normalized.compareTo(rule.minQty()) < 0) {
            return new SellabilityResult(rawAvailable, normalized, notional, false, DustReason.BELOW_MIN_QTY);
        }
        if (price.signum() <= 0 || notional.compareTo(effectiveSellMinNotional(rule)) < 0) {
            return new SellabilityResult(rawAvailable, normalized, notional, false, DustReason.BELOW_MIN_NOTIONAL);
        }
        return new SellabilityResult(rawAvailable, normalized, notional, true, DustReason.NONE);
    }

    private BigDecimal effectiveSellMinNotional(SymbolRuleManager.SymbolRule rule) {
        BigDecimal bufferPercent = properties.getStrategy().getSellMinNotionalBufferPercent();
        if (bufferPercent == null || bufferPercent.signum() <= 0) return rule.minNotional();
        return rule.minNotional().multiply(BigDecimal.valueOf(100).add(bufferPercent))
                .divide(BigDecimal.valueOf(100), java.math.MathContext.DECIMAL64);
    }

    private boolean verifyDustWithinLimit(SellabilityResult sellability) {
        if (sellability == null || sellability.sellable() || sellability.rawAvailableQty().signum() <= 0) return true;
        BigDecimal maxDust = properties.getStrategy().getMaxDustNotionalUsdt();
        if (maxDust == null || maxDust.signum() <= 0 || sellability.notional().signum() <= 0) return true;
        if (sellability.notional().compareTo(maxDust) <= 0) return true;
        halt("DUST 残余库存名义额超过上限 "
                + sellability.notional().toPlainString() + " USDT，停止继续买入等待人工处理");
        return false;
    }

    private void updateDustState(SellabilityResult sellability, String prefix) {
        if (sellability == null || sellability.rawAvailableQty().signum() <= 0) {
            statusReason.set("运行中，等待入场信号");
            return;
        }
        String signature = sellability.dustReason() + "|" + sellability.rawAvailableQty().toPlainString()
                + "|" + sellability.notional().toPlainString();
        if (!signature.equals(lastDustStateSignature.getAndSet(signature))) {
            log.info("[accountId={} alias={}] residual inventory entered DUST state: symbol={} qty={} notional={} minNotional={} effectiveMinNotional={} minQty={} stepSize={} reason={}",
                    accountId, accountAlias, properties.getStrategy().getSymbol(),
                    sellability.rawAvailableQty(), sellability.notional(),
                    ruleManager.getRule(properties.getStrategy().getSymbol()) == null ? null
                            : ruleManager.getRule(properties.getStrategy().getSymbol()).minNotional(),
                    ruleManager.getRule(properties.getStrategy().getSymbol()) == null ? null
                            : effectiveSellMinNotional(ruleManager.getRule(properties.getStrategy().getSymbol())),
                    ruleManager.getRule(properties.getStrategy().getSymbol()) == null ? null
                            : ruleManager.getRule(properties.getStrategy().getSymbol()).minQty(),
                    ruleManager.getRule(properties.getStrategy().getSymbol()) == null ? null
                            : ruleManager.getRule(properties.getStrategy().getSymbol()).stepSize(),
                    sellability.dustReason());
        }
        statusReason.set(prefix + "：DUST qty=" + sellability.rawAvailableQty().toPlainString()
                + ", notional=" + sellability.notional().toPlainString()
                + ", reason=" + sellability.dustReason());
    }

    private BigDecimal knownFreeBaseBalance() {
        BigDecimal free = lastKnownFreeBaseBalance.get();
        if (free != null) return positiveOrZero(free);
        BigDecimal available = holdingInventory.get().subtract(activeSellCoveredQty.get());
        return available.signum() < 0 ? BigDecimal.ZERO : available;
    }

    private void reserveActiveSellQuantity(BigDecimal quantity) {
        activeSellCoveredQty.set(positiveOrZero(quantity));
        subtractKnownFreeBase(quantity);
    }

    private void releaseActiveSellReservation() {
        BigDecimal covered = activeSellCoveredQty.getAndSet(BigDecimal.ZERO);
        if (covered.signum() > 0) addKnownFreeBase(covered);
    }

    private void addKnownFreeBase(BigDecimal quantity) {
        BigDecimal qty = positiveOrZero(quantity);
        lastKnownFreeBaseBalance.updateAndGet(current -> current == null ? qty : current.add(qty));
    }

    private void subtractKnownFreeBase(BigDecimal quantity) {
        BigDecimal qty = positiveOrZero(quantity);
        lastKnownFreeBaseBalance.updateAndGet(current -> {
            if (current == null) return null;
            BigDecimal updated = current.subtract(qty);
            return updated.signum() < 0 ? BigDecimal.ZERO : updated;
        });
    }

    private BigDecimal positiveOrZero(BigDecimal value) {
        return value == null || value.signum() < 0 ? BigDecimal.ZERO : value;
    }

    private BigDecimal lastBestAskOrZero() {
        BigDecimal ask = lastBestAsk.get();
        return ask == null ? BigDecimal.ZERO : ask;
    }

    private BigDecimal entryAverageFloorPrice(SymbolRuleManager.SymbolRule rule) {
        BigDecimal filledQuantity = filledEntryQuantity.get();
        BigDecimal filledQuote = filledEntryQuoteQuantity.get();
        if (rule == null || filledQuantity.signum() <= 0 || filledQuote.signum() <= 0) return BigDecimal.ZERO;
        return PrecisionUtil.roundUpToStep(filledQuote.divide(filledQuantity,
                java.math.MathContext.DECIMAL64), rule.tickSize());
    }

    /** Exact weighted execution average used only for dashboard SELL cost display. */
    private BigDecimal currentEntryAverageExecutionPrice() {
        BigDecimal filledQuantity = filledEntryQuantity.get();
        BigDecimal filledQuote = filledEntryQuoteQuantity.get();
        if (filledQuantity.signum() > 0 && filledQuote.signum() > 0) {
            return filledQuote.divide(filledQuantity, java.math.MathContext.DECIMAL64);
        }
        TradingRiskGuard.RiskSnapshot risk = riskGuard.snapshot();
        if (risk.positionQty() != null && risk.positionQty().signum() > 0
                && risk.positionCostUsdt() != null && risk.positionCostUsdt().signum() > 0) {
            return risk.positionCostUsdt().divide(risk.positionQty(), java.math.MathContext.DECIMAL64);
        }
        return null;
    }

    private BigDecimal entryAverageStrictlyHigherPrice(SymbolRuleManager.SymbolRule rule) {
        BigDecimal average = entryAveragePrice(rule);
        if (average.signum() <= 0) return BigDecimal.ZERO;
        BigDecimal markup = rule.tickSize().multiply(BigDecimal.valueOf(bidAskInitialSellMarkupTicks()));
        return PrecisionUtil.roundUpToStep(average.add(markup), rule.tickSize());
    }

    private BigDecimal entryAveragePrice(SymbolRuleManager.SymbolRule rule) {
        BigDecimal filledQuantity = filledEntryQuantity.get();
        BigDecimal filledQuote = filledEntryQuoteQuantity.get();
        if (rule == null) return BigDecimal.ZERO;
        BigDecimal average = BigDecimal.ZERO;
        if (filledQuantity.signum() > 0 && filledQuote.signum() > 0) {
            average = filledQuote.divide(filledQuantity, java.math.MathContext.DECIMAL64);
        } else {
            TradingRiskGuard.RiskSnapshot risk = riskGuard.snapshot();
            if (risk.positionQty() != null && risk.positionQty().signum() > 0
                    && risk.positionCostUsdt() != null && risk.positionCostUsdt().signum() > 0) {
                average = risk.positionCostUsdt().divide(risk.positionQty(), java.math.MathContext.DECIMAL64);
            }
        }
        if (average.signum() <= 0) return BigDecimal.ZERO;
        return PrecisionUtil.roundUpToStep(average, rule.tickSize());
    }

    private BigDecimal bidAskMakerExitPrice(SymbolRuleManager.SymbolRule rule, BigDecimal bestAsk) {
        BigDecimal ask = positiveOrZero(bestAsk).signum() > 0
                ? PrecisionUtil.roundDownToStep(bestAsk, rule.tickSize()) : BigDecimal.ZERO;
        BigDecimal aboveEntry = entryAverageStrictlyHigherPrice(rule);
        BigDecimal price = aboveEntry.signum() > 0 ? ask.max(aboveEntry) : ask;
        return price.signum() > 0 ? price : aboveEntry;
    }

    private BigDecimal timedOutExitPrice(SymbolRuleManager.SymbolRule rule, BigDecimal bestAsk) {
        BigDecimal ask = positiveOrZero(bestAsk);
        return ask.signum() > 0 ? PrecisionUtil.roundDownToStep(ask, rule.tickSize()) : BigDecimal.ZERO;
    }

    private BigDecimal exitReferencePrice(SymbolRuleManager.SymbolRule rule) {
        return exitReferencePrice(rule, lastBestAskOrZero());
    }

    private BigDecimal exitReferencePrice(SymbolRuleManager.SymbolRule rule, BigDecimal fallbackPrice) {
        if (usesBidAskMakerStrategy()) return bidAskMakerExitPrice(rule, fallbackPrice);
        BigDecimal entryFloor = entryAverageFloorPrice(rule);
        return entryFloor.signum() > 0 ? entryFloor : positiveOrZero(fallbackPrice);
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
        BigDecimal estimatedExitFeeRate = makerSellFeeRate();
        BigDecimal feeDenominator = BigDecimal.ONE.subtract(estimatedExitFeeRate);
        if (feeDenominator.signum() <= 0) throw new IllegalStateException("预计卖出费率配置无效");
        BigDecimal target = unitCost.divide(feeDenominator, java.math.MathContext.DECIMAL64);
        return PrecisionUtil.roundUpToStep(target, rule.tickSize());
    }

    private BigDecimal feeProtectedExitPrice(SymbolRuleManager.SymbolRule rule, BigDecimal bestAsk) {
        BigDecimal target = feeAwareTargetPrice(rule);
        BigDecimal ask = positiveOrZero(bestAsk);
        return PrecisionUtil.roundUpToStep(target.max(ask), rule.tickSize());
    }

    private BigDecimal makerSellFeeRate() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        if (profile != null && profile.getMakerFeeBps() != null) {
            return profile.getMakerFeeBps().movePointLeft(4);
        }
        String symbol = properties.getStrategy().getSymbol().toUpperCase();
        long now = System.currentTimeMillis();
        CachedCommissionRate cached = makerSellFeeRateCache.get(symbol);
        if (cached != null && now - cached.loadedAtMs() < COMMISSION_RATE_CACHE_MS) return cached.rate();

        BigDecimal exchangeRate = parseMakerSellFeeRate(tradeService.getAccountCommissionRates(symbol));
        if (exchangeRate != null) {
            makerSellFeeRateCache.put(symbol, new CachedCommissionRate(exchangeRate, now));
            return exchangeRate;
        }
        BigDecimal fallback = properties.getStrategy().getAssumedMakerFeeBps().movePointLeft(4);
        log.warn("[accountId={} alias={}] 无法读取 {} 实际 Maker 卖出费率，使用保守配置 {} bps",
                accountId, accountAlias, symbol, properties.getStrategy().getAssumedMakerFeeBps());
        return fallback;
    }

    private BigDecimal parseMakerSellFeeRate(JsonNode response) {
        if (response == null || response.has("code") || !response.path("standardCommission").isObject()) return null;
        BigDecimal standard = commissionSideRate(response.path("standardCommission"));
        // Do not reduce the estimate for the advertised BNB discount. The exchange can charge
        // the full standard rate when the BNB balance is insufficient at fill time; keeping the
        // undiscounted rate makes the exit floor safe in both cases.
        return standard.add(commissionSideRate(response.path("specialCommission")))
                .add(commissionSideRate(response.path("taxCommission")));
    }

    private BigDecimal commissionSideRate(JsonNode section) {
        if (section == null || !section.isObject()) return BigDecimal.ZERO;
        BigDecimal maker = decimalOrNull(section.path("maker"));
        BigDecimal seller = decimalOrNull(section.path("seller"));
        return positiveOrZero(maker).add(positiveOrZero(seller));
    }

    private BigDecimal decimalOrNull(JsonNode value) {
        if (value == null || value.isMissingNode() || value.isNull()) return null;
        try {
            return new BigDecimal(value.asText());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private BigDecimal buyQuantity(BigDecimal bid, SymbolRuleManager.SymbolRule rule) {
        BigDecimal base = orderAmountUsdt().divide(bid, 8, RoundingMode.DOWN);
        return PrecisionUtil.roundDownToStep(applyJitter(base), rule.stepSize());
    }
    private BigDecimal sumDepth(JsonNode levels) {
        BigDecimal total = BigDecimal.ZERO;
        for (JsonNode level : levels) {
            if (level.isArray() && level.size() > 1) total = total.add(new BigDecimal(level.get(1).asText()));
        }
        return total;
    }

    private List<BigDecimal> depthPrices(JsonNode levels) {
        if (levels == null || !levels.isArray()) return List.of();
        List<BigDecimal> prices = new ArrayList<>(5);
        for (JsonNode level : levels) {
            if (!level.isArray() || level.isEmpty()) continue;
            BigDecimal price = new BigDecimal(level.get(0).asText());
            if (price.signum() > 0) prices.add(price);
            if (prices.size() == 5) break;
        }
        return List.copyOf(prices);
    }
    private boolean isValidOrder(BigDecimal qty, BigDecimal price, SymbolRuleManager.SymbolRule rule) {
        return qty != null && qty.compareTo(rule.stepSize()) >= 0 && qty.compareTo(rule.minQty()) >= 0
                && price != null && price.signum() > 0 && qty.multiply(price).compareTo(rule.minNotional()) >= 0;
    }

    private void submitMakerOrder(String symbol, String side, BigDecimal price, BigDecimal qty,
                                  Long cancelOrderId, ChurnStatus status) {
        boolean buy = "BUY".equalsIgnoreCase(side);
        AccountRiskCoordinator.EntryReservation reservation = buy
                ? accountRiskCoordinator.reserveEntry(accountEngineKey, price.multiply(qty),
                properties.getStrategy().getMaxInventoryUsdt(),
                properties.getStrategy().getMaxDailyDrawdownUsdt())
                : null;
        if (buy && !reservation.accepted()) {
            nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
            statusReason.set(reservation.reason());
            return;
        }
        String clientOrderId = nextClientOrderId(side);
        pendingClientOrderIds.add(clientOrderId);
        activeClientOrderId.set(clientOrderId);
        replacingOrderId.set(cancelOrderId);
        try {
            JsonNode response = buy
                    ? accountRiskCoordinator.submitBuy(() -> tradeService.cancelAndReplaceOrder(
                            symbol, side, price, qty, cancelOrderId, clientOrderId))
                    : tradeService.cancelAndReplaceOrder(symbol, side, price, qty, cancelOrderId, clientOrderId);
            handleMakerResponse(response, cancelOrderId, clientOrderId, status);
            JsonNode acceptedOrder = cancelOrderId == null ? response
                    : response == null ? null : response.path("newOrderResponse");
            if (acceptedOrder != null && acceptedOrder.has("orderId")) {
                long acceptedOrderId = acceptedOrder.get("orderId").asLong();
                if (Long.valueOf(acceptedOrderId).equals(activeOrderId.get())) {
                    activeOrderPrice.set(price);
                    persistRuntimeState(true);
                }
                log.info("[accountId={} alias={}] Maker 报单已接受: ID={} side={} qty={} price={} clientOrderId={}",
                        accountId, accountAlias, acceptedOrderId, side, qty, price, clientOrderId);
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
        if (response.path("localRateLimited").asBoolean(false)) {
            if (status == ChurnStatus.BUYING) accountRiskCoordinator.releaseEntry(accountEngineKey);
            pendingClientOrderIds.remove(clientOrderId);
            activeClientOrderId.compareAndSet(clientOrderId, null);
            if (oldOrderId == null && status == ChurnStatus.BUYING) resetEntryTarget();
            if (oldOrderId == null && status == ChurnStatus.SELLING) releaseActiveSellReservation();
            long retryAfterMs = Math.max(1_000, response.path("retryAfterMs").asLong(1_000));
            nextOrderAttemptAt.set(System.currentTimeMillis() + retryAfterMs);
            statusReason.set("共享 IP API 权重达到入场安全线，暂缓新报单约 "
                    + Math.max(1, (retryAfterMs + 999) / 1_000) + " 秒");
            log.warn("[accountId={} alias={}] 本地入场节流，不按报单结果未知处理: used={}/{}",
                    accountId, accountAlias, response.path("usedWeight1m").asInt(),
                    response.path("safeRequestWeightLimit1m").asInt());
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
                log.info("[accountId={} alias={}] 撤换单的原订单终态包含成交 {}，等待账户成交流对账",
                        accountId, accountAlias, exchangeExecuted);
                return;
            }
            knownOrderIds.remove(oldOrderId);
            clearActiveOrder();
            nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
            var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
            if (status == ChurnStatus.BUYING) {
                accountRiskCoordinator.releaseEntry(accountEngineKey);
                if (rule != null) transitionAfterInventoryChange(rule, "旧买单已撤销但新买单未创建，重新评估持仓");
                else { currentStatus.set(ChurnStatus.IDLE); resetEntryTarget(); }
            } else {
                releaseActiveSellReservation();
                if (rule != null) transitionAfterInventoryChange(rule, "旧卖单已撤销但新卖单未创建，重新评估持仓");
                else currentStatus.set(ChurnStatus.SELLING);
            }
            log.warn("[accountId={} alias={}] 旧订单已撤销但新订单未创建，已退避 1 秒: code={}, msg={}",
                    accountId, accountAlias, response.path("code").asText("unknown"),
                    response.path("msg").asText("unknown"));
            return;
        }
        if (oldOrderId != null && "FAILURE".equals(result.path("cancelResult").asText())) {
            activeClientOrderId.set(null);
            nextOrderAttemptAt.set(System.currentTimeMillis() + 1_000);
            log.warn("[accountId={} alias={}] 撤换单中的撤单失败；保留旧订单 {} 并退避",
                    accountId, accountAlias, oldOrderId);
            return;
        }
        if (oldOrderId == null) {
            if (status == ChurnStatus.BUYING) accountRiskCoordinator.releaseEntry(accountEngineKey);
            activeClientOrderId.compareAndSet(clientOrderId, null);
            boolean insufficientBalance = response.path("code").asInt() == -2010
                    && response.path("msg").asText("").toLowerCase().contains("insufficient balance");
            nextOrderAttemptAt.set(System.currentTimeMillis() + (insufficientBalance ? 30_000 : 1_000));
            if (status == ChurnStatus.BUYING) resetEntryTarget();
            if (status == ChurnStatus.SELLING) releaseActiveSellReservation();
            if (insufficientBalance) statusReason.set("账户可用余额不足，30 秒后重试");
            log.warn("[accountId={} alias={}] 新订单未被交易所接受，已退避 {} 秒: code={}, msg={}",
                    accountId, accountAlias, insufficientBalance ? 30 : 1,
                    response.path("code").asText("unknown"),
                    response.path("msg").asText("unknown"));
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
            accountRiskCoordinator.releaseEntry(accountEngineKey);
            var rule = ruleManager.getRule(properties.getStrategy().getSymbol());
            if (rule != null) transitionAfterInventoryChange(rule, "BUY 撤单包含成交，立即退出已成交持仓");
            else currentStatus.set(ChurnStatus.SELLING);
            log.info("已撤销 Maker 买单的成交已由账户成交流对账，转入持仓退出管理");
            return;
        }
        halt("Maker 买单撤销后的成交回报超时，需人工对账");
    }

    private void reconcileAmbiguousSubmission(String clientOrderId, ChurnStatus status, String reason) {
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (openOrders == null) {
            halt(reason + "，且无法查询活动订单");
            return;
        }
        JsonNode matched = null;
        for (JsonNode order : openOrders) {
            if (clientOrderId.equals(order.path("clientOrderId").asText())) {
                if (matched != null) {
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
            if (status == ChurnStatus.SELLING && activeSellCoveredQty.get().signum() <= 0) {
                BigDecimal originalQty = new BigDecimal(matched.path("origQty").asText("0"));
                BigDecimal executedQty = new BigDecimal(matched.path("executedQty").asText("0"));
                BigDecimal remainingQty = originalQty.subtract(executedQty);
                activeSellCoveredQty.set(remainingQty.signum() < 0 ? BigDecimal.ZERO : remainingQty);
            }
            persistRuntimeState(true);
            return;
        }
        pendingClientOrderIds.remove(clientOrderId);
        activeClientOrderId.compareAndSet(clientOrderId, null);
        if (status == ChurnStatus.BUYING) accountRiskCoordinator.releaseEntry(accountEngineKey);
        if (status == ChurnStatus.SELLING) releaseActiveSellReservation();
        halt(reason + "；交易所未发现对应活动订单，需核对成交历史");
    }

    private void cancelActiveEntryOrder(String reason) {
        Long orderId = activeOrderId.get();
        if (orderId == null || !entryCancellationPending.compareAndSet(false, true)) return;
        JsonNode cancel = tradeService.cancelOrder(properties.getStrategy().getSymbol(), orderId);
        if (cancel != null) {
            log.info("[accountId={} alias={}] 撤销活动买单 {}: {}", accountId, accountAlias, orderId, reason);
            CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS)
                    .execute(() -> reconcileCancelledEntry(orderId));
        } else {
            entryCancellationPending.set(false);
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
            accountRiskCoordinator.releaseEntry(accountEngineKey);
            transitionAfterInventoryChange(rule, "BUY 撤单包含成交，立即退出已成交持仓");
            log.info("[accountId={} alias={}] 撤单 {} 已确认，状态机恢复为 {}",
                    accountId, accountAlias, orderId, currentStatus.get());
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
        persistRuntimeState(true);
        if (currentStatus.get() != ChurnStatus.HALTED) scheduleOrderReconciliation(orderId);
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
            halt("订单 " + orderId + " 连续三次成交明细不一致，且无法确认安全空仓");
            return;
        }
        if (!reconcileInventory(rule, "订单 " + orderId + " REST 对账")) return;
        tradeReconcileFailures.remove(orderId);
        BigDecimal reconciledInventory = holdingInventory.get();
        markRestReconciled(orderId);
        if ("BUY".equalsIgnoreCase(side)) accountRiskCoordinator.releaseEntry(accountEngineKey);
        clearActiveOrder();
        if ("BUY".equalsIgnoreCase(side)) {
            transitionAfterInventoryChange(rule, "BUY REST 对账完成，重新评估持仓");
        } else if (reconciledInventory.compareTo(rule.stepSize()) < 0) {
            completeFlatExit(true);
        } else {
            transitionAfterInventoryChange(rule, "SELL REST 对账完成，重新评估剩余持仓");
        }
    }

    private void completeFlatExit(boolean restReconciled) {
        holdingInventory.set(BigDecimal.ZERO);
        activeSellCoveredQty.set(BigDecimal.ZERO);
        lastKnownFreeBaseBalance.set(BigDecimal.ZERO);
        lastDustStateSignature.set("");
        riskGuard.reconcileExchangeFlat(System.currentTimeMillis(), properties.getStrategy());
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null || !dailyStatsStore.reconcileFlatDust(accountId,
                properties.getStrategy().getSymbol(), rule.stepSize())) {
            halt("交易所已空仓，但每日账本无法安全归零");
            return;
        }
        syncDailyCounters();
        currentStatus.set(ChurnStatus.IDLE);
        applyPendingStrategyIfSafe();
        rememberFeeAwareEntryPriceCeiling(rule);
        resetEntryTarget();
        persistRuntimeState(false);
        if (dailyVolumeLimitReached()) {
            dailyVolumeStopPending.set(false);
            isRunning.set(false);
            postSellNextEntryAllowedAtMs.set(0);
            statusReason.set(dailyVolumeLimitMessage("已确认空仓并自动停止当前币种策略"));
            log.warn("[accountId={} alias={}] {}", accountId, accountAlias, statusReason.get());
            return;
        }
        BnbBalanceSnapshot bnb = bnbBalanceSnapshot.get();
        if (bnbBalanceStopPending.get() && bnb != null && bnb.belowMinimum()) {
            bnbBalanceStopPending.set(false);
            isRunning.set(false);
            postSellNextEntryAllowedAtMs.set(0);
            statusReason.set(bnbBalanceMessage(bnb, "已确认空仓并自动停止当前币种策略"));
            log.warn("[accountId={} alias={}] {}", accountId, accountAlias, statusReason.get());
            return;
        }
        long postSellDelayMs = schedulePostSellEntryDelay();
        if (isRunning.get() && postSellDelayMs > 0) {
            statusReason.set(getStrategyMode() + " 卖单已成交，等待 "
                    + durationLabel(postSellDelayMs) + " 后再挂下一笔买单");
        } else {
            statusReason.set(isRunning.get() ? "运行中，等待入场信号"
                    : "人工授权市价清仓已完成" + (restReconciled ? "（REST 对账）" : ""));
        }
    }

    private boolean continueAfterConfirmedFlatSell(long orderId, String side,
                                                    SymbolRuleManager.SymbolRule rule) {
        if (!"SELL".equalsIgnoreCase(side)) return false;
        BinanceAccountTradeClient.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (balance == null) {
            BigDecimal free = tradeService.getFreeAssetBalance(baseAsset());
            if (free != null) balance = new BinanceAccountTradeClient.AssetBalance(
                    baseAsset(), free, BigDecimal.ZERO, free);
        }
        JsonNode openOrders = tradeService.getOpenOrders(properties.getStrategy().getSymbol());
        if (balance == null || openOrders == null || !openOrders.isEmpty()
                || balance.total().compareTo(rule.stepSize()) >= 0) return false;
        holdingInventory.set(balance.total());
        lastKnownFreeBaseBalance.set(balance.free());
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
        activeSellCoveredQty.set(BigDecimal.ZERO);
        orderPlacedTimestamp.set(0);
        persistRuntimeState(false);
    }

    private void clearTrackedOrders() {
        accountRiskCoordinator.releaseEntry(accountEngineKey);
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
        filledEntryMaxPrice.set(BigDecimal.ZERO);
        feeAwareEntryCeilingBlockedSince.set(0);
    }

    private String nextClientOrderId(String side) {
        String sideTag = side != null && side.toUpperCase().startsWith("B") ? "B" : "S";
        return "ta-" + accountTag + "-" + sideTag + "-" + Long.toUnsignedString(clientOrderSequence.incrementAndGet(), 36)
                + "-" + Long.toUnsignedString(System.currentTimeMillis(), 36);
    }

    private boolean isTerminal(String status) {
        return "FILLED".equals(status) || "CANCELED".equals(status) || "EXPIRED".equals(status)
                || "EXPIRED_IN_MATCH".equals(status) || "REJECTED".equals(status);
    }

    private BigDecimal decimal(String value) {
        return value == null || value.isBlank() ? BigDecimal.ZERO : new BigDecimal(value);
    }


    public record LiquidationResult(boolean accepted, Long orderId, BigDecimal quantity, String message) {
        private static LiquidationResult rejected(String message) {
            return new LiquidationResult(false, null, BigDecimal.ZERO, message);
        }
    }
    private void halt(String reason) { isRunning.set(false); currentStatus.set(ChurnStatus.HALTED); statusReason.set(reason); log.error("[accountId={} alias={}] 引擎进入保护停机: {}", accountId, accountAlias, reason); }
    private BigDecimal applyJitter(BigDecimal qty) { double j = properties.getStrategy().getRandomSizeJitter(); return j <= 0 ? qty : qty.multiply(BigDecimal.valueOf(1 + ThreadLocalRandom.current().nextDouble(-j, j))); }
    private boolean calibrateHoldings() {
        BinanceAccountTradeClient.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        if (balance == null) {
            BigDecimal free = tradeService.getFreeAssetBalance(baseAsset());
            if (free == null) return false;
            balance = new BinanceAccountTradeClient.AssetBalance(baseAsset(), free, BigDecimal.ZERO, free);
        }
        holdingInventory.set(balance.total());
        lastKnownFreeBaseBalance.set(balance.free());
        accountRiskReconciled.set(balanceMatchesRiskLedger(balance.total()));
        return true;
    }

    /** Validates this symbol against a single shared account snapshot without REST fan-out. */
    public boolean reconcileAccountRiskSnapshot(JsonNode accountInfo) {
        BinanceAccountTradeClient.AssetBalance balance = accountAssetBalance(accountInfo);
        if (balance == null) {
            accountRiskReconciled.set(false);
            return false;
        }
        boolean matches = balanceMatchesRiskLedger(balance.total());
        accountRiskReconciled.set(matches);
        return matches;
    }

    /**
     * Explicit-start recovery for a position that cannot be matched from in-memory state. The
     * exchange balance stays authoritative and historical fills are queried backwards in bounded
     * 24-hour windows. Older fills are used only to reconstruct the open lot; today's PnL and fee
     * statistics keep their UTC-day-only scope and no historical position snapshot is persisted.
     */
    public boolean recoverAccountRiskSnapshotForStart(JsonNode accountInfo) {
        BinanceAccountTradeClient.AssetBalance balance = accountAssetBalance(accountInfo);
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (balance == null || rule == null) {
            accountRiskReconciled.set(false);
            return false;
        }
        BigDecimal remoteTotal = balance.total();
        holdingInventory.set(remoteTotal);
        lastKnownFreeBaseBalance.set(balance.free());
        if (isEffectivelyFlat(remoteTotal, rule.stepSize())) {
            riskGuard.reconcileExchangeFlat(System.currentTimeMillis(), properties.getStrategy());
            accountRiskReconciled.set(true);
            return true;
        }

        RecoveredOpenPosition recovered = recoverRemoteOpenPosition(remoteTotal, rule);
        if (recovered == null) {
            accountRiskReconciled.set(false);
            return false;
        }
        riskGuard.restoreOpenPosition(remoteTotal, recovered.costQuote(), lastBestAskOrZero(),
                recovered.openedAtMs(), properties.getStrategy());
        boolean matches = balanceMatchesRiskLedger(remoteTotal);
        accountRiskReconciled.set(matches);
        if (matches) {
            log.warn("[accountId={} alias={}] 启动前已从交易所历史成交恢复跨日持仓: symbol={} qty={} cost={} openedAt={}",
                    accountId, accountAlias, properties.getStrategy().getSymbol(), remoteTotal,
                    recovered.costQuote(), recovered.openedAtMs());
        }
        return matches;
    }

    private BinanceAccountTradeClient.AssetBalance accountAssetBalance(JsonNode accountInfo) {
        if (accountInfo == null || !accountInfo.path("balances").isArray()) return null;
        for (JsonNode balance : accountInfo.path("balances")) {
            if (!baseAsset().equalsIgnoreCase(balance.path("asset").asText())) continue;
            try {
                BigDecimal free = new BigDecimal(balance.path("free").asText("0"));
                BigDecimal locked = new BigDecimal(balance.path("locked").asText("0"));
                return new BinanceAccountTradeClient.AssetBalance(baseAsset(), free, locked, free.add(locked));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return new BinanceAccountTradeClient.AssetBalance(baseAsset(), BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO);
    }

    private boolean balanceMatchesRiskLedger(BigDecimal remoteTotal) {
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        if (rule == null || remoteTotal == null) return false;
        BigDecimal localQty = riskGuard.snapshot().positionQty();
        return remoteTotal.subtract(localQty == null ? BigDecimal.ZERO : localQty)
                .abs().compareTo(rule.stepSize()) < 0;
    }

    public void markAccountRiskUnconfirmed(String reason) {
        accountRiskReconciled.set(false);
        statusReason.set(reason);
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
        if (isRunning.get() || activeOrderId.get() != null) {
            return SymbolSwitchResult.rejected(current, "请先停止策略，确认没有活动订单后再切换");
        }

        SymbolRuleManager.SymbolRule targetRule = ruleManager.refreshRule(target);
        if (targetRule == null) {
            return SymbolSwitchResult.rejected(current, "目标交易对不可交易或无法加载交易规则: " + target);
        }
        BigDecimal orderNotional = orderAmountUsdt(target);
        if (orderNotional == null || orderNotional.compareTo(targetRule.minNotional()) < 0) {
            return SymbolSwitchResult.rejected(current, "当前单笔金额低于 " + target + " 的最小名义额 "
                    + targetRule.minNotional().toPlainString());
        }

            JsonNode currentOrders = tradeService.getOpenOrders(current);
            JsonNode targetOrders = tradeService.getOpenOrders(target);
            if (currentOrders == null || targetOrders == null) {
                return SymbolSwitchResult.rejected(current, "无法确认旧/新交易对的活动订单，已拒绝切换");
            }
            if (!currentOrders.isEmpty() || !targetOrders.isEmpty()) {
                return SymbolSwitchResult.rejected(current, "旧或新交易对仍有活动订单，已拒绝切换");
            }
            SymbolRuleManager.SymbolRule currentRule = ruleManager.getRule(current);
            BinanceAccountTradeClient.AssetBalance currentBalance = tradeService.getAssetBalance(baseAsset(current));
            BinanceAccountTradeClient.AssetBalance targetBalance = tradeService.getAssetBalance(baseAsset(target));
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

        try {
            dailyStatsStore.clearRuntimeState(accountId, current);
            dailyStatsStore.saveActiveSymbol(accountId, target);
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
        activeSellCoveredQty.set(BigDecimal.ZERO);
        lastKnownFreeBaseBalance.set(null);
        lastDustStateSignature.set("");
        feeAwareEntryPriceCeiling.set(null);
        feeAwareInitialEntryAnchorPrice.set(null);
        synchronized (feeAwareRecentBuyPrices) {
            feeAwareRecentBuyPrices.clear();
        }
        feeAwareEntryCeilingBlockedSince.set(0);
        postSellNextEntryAllowedAtMs.set(0);
        commissionPriceCache.clear();
        makerSellFeeRateCache.clear();
        lastBestBid.set(null);
        lastBestAsk.set(null);
        lastMidPrice.set(null);
        lastMarketDataTimestamp.set(0);
        activeEntryContext.set(null);
        activeEntrySignalReason.set("UNKNOWN");
        synchronized (inboundMarketMessage) { inboundMarketMessage.setLength(0); }
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set("已切换到 " + target + "，等待新行情后可启动");
        syncDailyCounters();
        restoreDailyRisk();
        forceMarketReconnect("交易对已切换到 " + target);
        log.warn("[accountId={} alias={}] 交易对已由 {} 切换为 {}；策略保持停止",
                accountId, accountAlias, current, target);
        return new SymbolSwitchResult(true, target, statusReason.get());
    }

    private void syncDailyCounters() {
        try {
            DailyTradeStatsStore.DailyStatsSnapshot today = dailyStatsStore.today(
                    accountId, accountAlias, properties.getStrategy().getSymbol());
            totalVolumeUsdt.set(today.totalVolumeQuote());
            roundTripsCompleted.set(today.roundTrips());
            dailyVolumeCounterDate.set(today.date());
        } catch (RuntimeException e) {
            log.error("读取今日统计计数失败；成交写入结果不受影响", e);
        }
    }

    private void refreshDailyVolumeCounterDate() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        if (!today.equals(dailyVolumeCounterDate.get())) {
            syncDailyCounters();
            if (!dailyVolumeLimitReached()) dailyVolumeStopPending.set(false);
        }
    }

    private BigDecimal dailyVolumeLimitUsdt() {
        BinanceProperties.SymbolStrategyProfile profile = symbolStrategy(properties.getStrategy().getSymbol());
        BigDecimal limit = profile == null ? null : profile.getDailyVolumeLimitUsdt();
        return limit == null || limit.signum() <= 0 ? new BigDecimal("510") : limit;
    }

    private boolean dailyVolumeLimitReached() {
        return totalVolumeUsdt.get().compareTo(dailyVolumeLimitUsdt()) >= 0;
    }

    private boolean enforceDailyVolumeLimit(SymbolRuleManager.SymbolRule rule) {
        if (!dailyVolumeLimitReached()) return false;
        dailyVolumeStopPending.set(true);
        if (currentStatus.get() == ChurnStatus.SELLING) {
            statusReason.set(dailyVolumeLimitMessage("停止新买入，等待当前卖单完成后自动停止"));
            return false;
        }
        if (currentStatus.get() == ChurnStatus.BUYING && activeOrderId.get() != null) {
            cancelActiveEntryOrder(dailyVolumeLimitMessage("撤销当前买单并对账"));
            return true;
        }
        return stopAtDailyVolumeLimitIfFlat(rule);
    }

    private boolean stopAtDailyVolumeLimitIfFlat(SymbolRuleManager.SymbolRule rule) {
        SellabilityResult sellability = currentSellability(rule, exitReferencePrice(rule));
        if (holdingInventory.get().signum() > 0 && sellability.sellable()) {
            currentStatus.set(ChurnStatus.SELLING);
            statusReason.set(dailyVolumeLimitMessage("停止新买入，先卖出当前持仓"));
            submitImmediateExit(rule);
            return true;
        }
        isRunning.set(false);
        dailyVolumeStopPending.set(false);
        postSellNextEntryAllowedAtMs.set(0);
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set(dailyVolumeLimitMessage(holdingInventory.get().signum() > 0
                ? "仅剩不可交易粉尘，已自动停止当前币种策略"
                : "已自动停止当前币种策略"));
        persistRuntimeState(false);
        log.warn("[accountId={} alias={}] {}", accountId, accountAlias, statusReason.get());
        return true;
    }

    private String dailyVolumeLimitMessage(String action) {
        return "今日真实成交量 " + totalVolumeUsdt.get().stripTrailingZeros().toPlainString()
                + " USDT 已达到每日上限 "
                + dailyVolumeLimitUsdt().stripTrailingZeros().toPlainString() + " USDT；" + action;
    }

    private synchronized BnbBalanceSnapshot refreshBnbBalanceSnapshot(boolean force) {
        AccountRiskCoordinator.BnbBalanceSnapshot shared =
                accountRiskCoordinator.refreshBnbBalance(force, tradeService);
        if (shared == null) {
            log.warn("[accountId={} alias={}] 暂时无法刷新 BNB 余额价值，沿用上一份检查结果",
                    accountId, accountAlias);
            return bnbBalanceSnapshot.get();
        }
        BnbBalanceSnapshot snapshot = new BnbBalanceSnapshot(shared.quantity(), shared.priceUsdt(),
                shared.valueUsdt(), shared.valueUsdt().compareTo(MIN_BNB_BALANCE_USDT) < 0,
                shared.checkedAtMs());
        bnbBalanceSnapshot.set(snapshot);
        return snapshot;
    }

    private boolean enforceBnbBalanceMinimum(SymbolRuleManager.SymbolRule rule) {
        BnbBalanceSnapshot bnb = refreshBnbBalanceSnapshot(false);
        if (bnb == null || !bnb.belowMinimum()) return false;
        if (bnbBalanceStopPending.compareAndSet(false, true)) {
            log.warn("[accountId={} alias={}] BNB 余额价值 {} USDT 低于最低要求 {} USDT；停止新买入，空仓后自动停止",
                    accountId, accountAlias, bnb.valueUsdt(), MIN_BNB_BALANCE_USDT);
        }
        if (currentStatus.get() == ChurnStatus.SELLING) {
            statusReason.set(bnbBalanceMessage(bnb, "停止新买入，等待当前卖单完成后自动停止"));
            return false;
        }
        if (currentStatus.get() == ChurnStatus.BUYING && activeOrderId.get() != null) {
            cancelActiveEntryOrder(bnbBalanceMessage(bnb, "撤销当前买单并对账"));
            return true;
        }
        SellabilityResult sellability = currentSellability(rule, exitReferencePrice(rule));
        if (holdingInventory.get().signum() > 0 && sellability.sellable()) {
            currentStatus.set(ChurnStatus.SELLING);
            statusReason.set(bnbBalanceMessage(bnb, "停止新买入，先卖出当前持仓"));
            submitImmediateExit(rule);
            return true;
        }
        isRunning.set(false);
        bnbBalanceStopPending.set(false);
        postSellNextEntryAllowedAtMs.set(0);
        currentStatus.set(ChurnStatus.IDLE);
        statusReason.set(bnbBalanceMessage(bnb, holdingInventory.get().signum() > 0
                ? "仅剩不可交易粉尘，已自动停止当前币种策略"
                : "已自动停止当前币种策略"));
        persistRuntimeState(false);
        log.warn("[accountId={} alias={}] {}", accountId, accountAlias, statusReason.get());
        return true;
    }

    private boolean enforceAccountRiskLimit() {
        String reason = accountRiskCoordinator.accountEntryBlockReason(
                properties.getStrategy().getMaxDailyDrawdownUsdt());
        if (reason == null) return false;
        if (currentStatus.get() == ChurnStatus.SELLING) return false;
        if (currentStatus.get() == ChurnStatus.BUYING && activeOrderId.get() != null) {
            cancelActiveEntryOrder(reason + "，撤销当前买单并对账");
        } else {
            statusReason.set(reason);
        }
        return true;
    }

    private String bnbBalanceMessage(BnbBalanceSnapshot bnb, String action) {
        return "BNB 余额价值 " + bnb.valueUsdt().setScale(4, RoundingMode.HALF_UP).toPlainString()
                + " USDT 低于 1 USDT；" + action;
    }

    private void restoreDailyRisk() {
        DailyTradeStatsStore.DailyStatsSnapshot today = dailyStatsStore.today(
                accountId, accountAlias, properties.getStrategy().getSymbol());
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
    public boolean hasActiveOrder() { return activeOrderId.get() != null; }
    public BigDecimal getOrderAmountUsdt() {
        BigDecimal amount = orderAmountUsdt();
        return amount == null ? BigDecimal.ZERO : amount;
    }
    public BigDecimal getTickSize() {
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        return rule == null || rule.tickSize() == null ? BigDecimal.ZERO : rule.tickSize();
    }
    public String getStrategyMode() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile == null || profile.getMode() == null || profile.getMode().isBlank()
                ? "FEE_AWARE_MAKER" : normalizeStrategyMode(profile.getMode());
    }

    public BinanceProperties.SymbolStrategyProfile getStrategyProfile() {
        BinanceProperties.SymbolStrategyProfile profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile == null ? copyStrategyProfile(new BinanceProperties.SymbolStrategyProfile())
                : copyStrategyProfile(profile);
    }

    public BigDecimal getFeeAwareRecommendedEntryAnchorPrice() {
        if (!usesFeeAwareMakerStrategy()) return BigDecimal.ZERO;
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(properties.getStrategy().getSymbol());
        BinanceProperties.SymbolStrategyProfile profile = symbolStrategy(properties.getStrategy().getSymbol());
        BigDecimal manual = configuredManualEntryAnchorPrice(profile, rule);
        BigDecimal existing = feeAwareInitialEntryAnchorPrice.get();
        if ((manual == null || manual.signum() <= 0) && existing != null && existing.signum() > 0) {
            return existing;
        }
        BigDecimal average = recentFeeAwareBuyAverageFloorPrice(rule);
        if (average != null && average.signum() > 0) return average;
        BigDecimal ceiling = feeAwareEntryPriceCeiling.get();
        if (ceiling != null && ceiling.signum() > 0) {
            return rule == null ? ceiling : PrecisionUtil.roundUpToStep(ceiling, rule.tickSize());
        }
        return existing != null && existing.signum() > 0 && manual != null && existing.compareTo(manual) != 0
                ? existing : BigDecimal.ZERO;
    }

    public boolean hasPendingStrategyChange() {
        return pendingStrategyProfiles.containsKey(normalizeStrategySymbol(properties.getStrategy().getSymbol()));
    }

    /**
     * Applies a strategy profile without restarting the account runtime.  If the current symbol has an
     * active order, the replacement is queued and applied only after the state machine returns to IDLE.
     */
    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, null, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                null, null, null, null, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                requestedEntryAnchorWaitMs, requestedMaxEntryAnchorDriftBps, null, null, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps,
                                                              BigDecimal requestedMaxCumulativeEntryAnchorDriftBps) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                requestedEntryAnchorWaitMs, requestedMaxEntryAnchorDriftBps,
                requestedMaxCumulativeEntryAnchorDriftBps, null, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps,
                                                              BigDecimal requestedMaxCumulativeEntryAnchorDriftBps,
                                                              BigDecimal requestedManualEntryAnchorPrice) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                requestedEntryAnchorWaitMs, requestedMaxEntryAnchorDriftBps,
                requestedMaxCumulativeEntryAnchorDriftBps, requestedManualEntryAnchorPrice, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps,
                                                              BigDecimal requestedMaxCumulativeEntryAnchorDriftBps,
                                                              BigDecimal requestedManualEntryAnchorPrice,
                                                              Long requestedPostSellEntryDelayMs) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                requestedEntryAnchorWaitMs, requestedMaxEntryAnchorDriftBps,
                requestedMaxCumulativeEntryAnchorDriftBps, requestedManualEntryAnchorPrice,
                requestedPostSellEntryDelayMs, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps,
                                                              BigDecimal requestedMaxCumulativeEntryAnchorDriftBps,
                                                              BigDecimal requestedManualEntryAnchorPrice,
                                                              Long requestedPostSellEntryDelayMs,
                                                              BigDecimal requestedDailyVolumeLimitUsdt) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                requestedEntryAnchorWaitMs, requestedMaxEntryAnchorDriftBps,
                requestedMaxCumulativeEntryAnchorDriftBps, requestedManualEntryAnchorPrice,
                requestedPostSellEntryDelayMs, requestedDailyVolumeLimitUsdt, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps,
                                                              BigDecimal requestedMaxCumulativeEntryAnchorDriftBps,
                                                              BigDecimal requestedManualEntryAnchorPrice,
                                                              Long requestedPostSellEntryDelayMs,
                                                              BigDecimal requestedDailyVolumeLimitUsdt,
                                                              Integer requestedBidAskInitialSellMarkupTicks) {
        return switchStrategy(requestedSymbol, requestedMode, requestedAmount, requestedEntryTimeoutMs,
                requestedExitTimeoutMs, requestedMakerFeeBps, requestedTargetNetProfitBps,
                requestedEntryAnchorWaitMs, requestedMaxEntryAnchorDriftBps,
                requestedMaxCumulativeEntryAnchorDriftBps, requestedManualEntryAnchorPrice,
                requestedPostSellEntryDelayMs, requestedDailyVolumeLimitUsdt,
                requestedBidAskInitialSellMarkupTicks, null);
    }

    public synchronized StrategySwitchResult switchStrategy(String requestedSymbol, String requestedMode,
                                                              BigDecimal requestedAmount, Long requestedEntryTimeoutMs,
                                                              Long requestedExitTimeoutMs,
                                                              BigDecimal requestedMakerFeeBps,
                                                              BigDecimal requestedTargetNetProfitBps,
                                                              Long requestedEntryAnchorWaitMs,
                                                              BigDecimal requestedMaxEntryAnchorDriftBps,
                                                              BigDecimal requestedMaxCumulativeEntryAnchorDriftBps,
                                                              BigDecimal requestedManualEntryAnchorPrice,
                                                              Long requestedPostSellEntryDelayMs,
                                                              BigDecimal requestedDailyVolumeLimitUsdt,
                                                              Integer requestedBidAskInitialSellMarkupTicks,
                                                              Integer requestedBidAskEntryBookLevel) {
        String symbol = normalizeStrategySymbol(requestedSymbol);
        if (symbol.isBlank() || !symbol.endsWith("USDT")) {
            return StrategySwitchResult.rejected(properties.getStrategy().getSymbol(),
                    "交易对格式无效；当前策略仅支持 USDT 现货交易对");
        }
        BinanceProperties.SymbolStrategyProfile existing = strategyProfiles.get(symbol);
        String mode = requestedMode == null || requestedMode.isBlank()
                ? existing == null ? "FEE_AWARE_MAKER" : normalizeStrategyMode(existing.getMode())
                : requestedMode.trim().toUpperCase();
        if (!"BID_ASK_MAKER".equals(mode) && !"BUY_PRICE_MAKER".equals(mode)
                && !"FEE_AWARE_MAKER".equals(mode)) {
            return StrategySwitchResult.rejected(symbol, "不支持的策略类型: " + mode);
        }
        BigDecimal amount = requestedAmount == null && existing != null
                ? existing.getOrderAmountUsdt() : requestedAmount;
        if (amount != null && (amount.signum() <= 0
                || amount.compareTo(properties.getStrategy().getMaxLiveOrderNotionalUsdt()) > 0)) {
            return StrategySwitchResult.rejected(symbol, "单笔金额必须大于 0 且不超过生产上限 "
                    + properties.getStrategy().getMaxLiveOrderNotionalUsdt().toPlainString() + " USDT");
        }
        Long entryTimeout = requestedEntryTimeoutMs == null && existing != null
                ? existing.getEntryTimeoutMs() : requestedEntryTimeoutMs;
        Long exitTimeout = requestedExitTimeoutMs == null && existing != null
                ? existing.getExitTimeoutMs() : requestedExitTimeoutMs;
        if (!validEntryTimeout(entryTimeout)) {
            return StrategySwitchResult.rejected(symbol, "买单超时时间必须在 1 秒到 30 分钟之间");
        }
        if (!validExitTimeout(exitTimeout)) {
            return StrategySwitchResult.rejected(symbol, "卖单检查时间必须在 1 秒到 24 小时之间");
        }
        Long postSellEntryDelayMs = requestedPostSellEntryDelayMs == null && existing != null
                ? existing.getPostSellEntryDelayMs() : requestedPostSellEntryDelayMs;
        if (postSellEntryDelayMs == null) postSellEntryDelayMs = 60_000L;
        if (!validPostSellEntryDelay(postSellEntryDelayMs)) {
            return StrategySwitchResult.rejected(symbol, "卖出后买入等待时间必须在 0 秒到 24 小时之间");
        }
        Integer bidAskInitialSellMarkupTicks = requestedBidAskInitialSellMarkupTicks == null && existing != null
                ? existing.getBidAskInitialSellMarkupTicks() : requestedBidAskInitialSellMarkupTicks;
        if (bidAskInitialSellMarkupTicks == null) bidAskInitialSellMarkupTicks = 1;
        if (bidAskInitialSellMarkupTicks < 1 || bidAskInitialSellMarkupTicks > 10_000) {
            return StrategySwitchResult.rejected(symbol, "BID_ASK_MAKER 初始卖价加价必须在 1 到 10,000 tick 之间");
        }
        Integer bidAskEntryBookLevel = requestedBidAskEntryBookLevel == null && existing != null
                ? existing.getBidAskEntryBookLevel() : requestedBidAskEntryBookLevel;
        if (bidAskEntryBookLevel == null) bidAskEntryBookLevel = 1;
        if (bidAskEntryBookLevel < 1 || bidAskEntryBookLevel > 5) {
            return StrategySwitchResult.rejected(symbol, "BID_ASK_MAKER 买入档位必须在 1 到 5 之间");
        }
        BigDecimal dailyVolumeLimitUsdt = requestedDailyVolumeLimitUsdt == null && existing != null
                ? existing.getDailyVolumeLimitUsdt() : requestedDailyVolumeLimitUsdt;
        if (dailyVolumeLimitUsdt == null) dailyVolumeLimitUsdt = new BigDecimal("510");
        if (dailyVolumeLimitUsdt.signum() <= 0
                || dailyVolumeLimitUsdt.compareTo(new BigDecimal("1000000000")) > 0) {
            return StrategySwitchResult.rejected(symbol,
                    "每日成交量上限必须大于 0 且不超过 1,000,000,000 USDT");
        }
        // Null deliberately clears a manual fee/profit override: the fee returns to automatic
        // account lookup and the profit target returns to the global default.
        BigDecimal makerFeeBps = requestedMakerFeeBps;
        BigDecimal targetNetProfitBps = requestedTargetNetProfitBps;
        if (!validBps(makerFeeBps, new BigDecimal("100"))) {
            return StrategySwitchResult.rejected(symbol, "Maker 费率必须在 0 到 100 bps 之间");
        }
        if (!validBps(targetNetProfitBps, new BigDecimal("1000"))) {
            return StrategySwitchResult.rejected(symbol, "目标净利润必须在 0 到 1000 bps 之间");
        }
        Long entryAnchorWaitMs = requestedEntryAnchorWaitMs == null && existing != null
                ? existing.getEntryAnchorWaitMs() : requestedEntryAnchorWaitMs;
        if (entryAnchorWaitMs == null) entryAnchorWaitMs = 1_800_000L;
        if (entryAnchorWaitMs < 0 || entryAnchorWaitMs > 86_400_000L) {
            return StrategySwitchResult.rejected(symbol, "买入锚点等待时间必须在 0 秒到 24 小时之间");
        }
        BigDecimal maxEntryAnchorDriftBps = requestedMaxEntryAnchorDriftBps == null && existing != null
                ? existing.getMaxEntryAnchorDriftBps() : requestedMaxEntryAnchorDriftBps;
        if (maxEntryAnchorDriftBps == null) maxEntryAnchorDriftBps = BigDecimal.ZERO;
        if (!validBps(maxEntryAnchorDriftBps, new BigDecimal("100"))) {
            return StrategySwitchResult.rejected(symbol, "最大追高必须在 0 到 100 bps 之间");
        }
        BigDecimal maxCumulativeEntryAnchorDriftBps = requestedMaxCumulativeEntryAnchorDriftBps == null
                && existing != null ? existing.getMaxCumulativeEntryAnchorDriftBps()
                : requestedMaxCumulativeEntryAnchorDriftBps;
        if (maxCumulativeEntryAnchorDriftBps == null) {
            maxCumulativeEntryAnchorDriftBps = maxEntryAnchorDriftBps;
        }
        if (!validBps(maxCumulativeEntryAnchorDriftBps, new BigDecimal("1000"))) {
            return StrategySwitchResult.rejected(symbol, "累计最大追高必须在 0 到 1000 bps（10%）之间");
        }
        BigDecimal manualEntryAnchorPrice = requestedManualEntryAnchorPrice;
        if (manualEntryAnchorPrice != null) {
            if (manualEntryAnchorPrice.signum() <= 0) {
                return StrategySwitchResult.rejected(symbol, "手动锚定价格必须大于 0，留空则自动生成");
            }
            SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
            if (rule != null) {
                manualEntryAnchorPrice = PrecisionUtil.roundUpToStep(manualEntryAnchorPrice, rule.tickSize());
            }
        }
        BinanceProperties.SymbolStrategyProfile profile = new BinanceProperties.SymbolStrategyProfile();
        profile.setMode(mode);
        profile.setOrderAmountUsdt(amount);
        profile.setEntryTimeoutMs(entryTimeout);
        profile.setExitTimeoutMs(exitTimeout);
        profile.setMakerFeeBps(makerFeeBps);
        profile.setTargetNetProfitBps(targetNetProfitBps);
        profile.setEntryAnchorWaitMs(entryAnchorWaitMs);
        profile.setMaxEntryAnchorDriftBps(maxEntryAnchorDriftBps);
        profile.setMaxCumulativeEntryAnchorDriftBps(maxCumulativeEntryAnchorDriftBps);
        profile.setManualEntryAnchorPrice(manualEntryAnchorPrice);
        profile.setPostSellEntryDelayMs(postSellEntryDelayMs);
        profile.setBidAskInitialSellMarkupTicks(bidAskInitialSellMarkupTicks);
        profile.setBidAskEntryBookLevel(bidAskEntryBookLevel);
        profile.setDailyVolumeLimitUsdt(dailyVolumeLimitUsdt);

        String currentSymbol = normalizeStrategySymbol(properties.getStrategy().getSymbol());
        if (symbol.equals(currentSymbol) && (activeOrderId.get() != null
                || currentStatus.get() == ChurnStatus.BUYING || currentStatus.get() == ChurnStatus.SELLING)) {
            pendingStrategyProfiles.put(symbol, profile);
            statusReason.set("策略切换已排队：当前 " + currentStatus.get() + " 完成后应用 " + mode);
            return new StrategySwitchResult(true, false, true, symbol, mode, statusReason.get());
        }
        if (!persistStrategyProfile(symbol, profile)) {
            return StrategySwitchResult.rejected(symbol, "策略配置持久化失败，未切换");
        }
        strategyProfiles.put(symbol, profile);
        applyFeeAwareManualAnchorIfCurrent(symbol, profile);
        statusReason.set("策略已切换为 " + mode + (symbol.equals(currentSymbol) ? "，等待下一次状态机周期" : ""));
        log.info("[accountId={} alias={}] 运行时策略已切换: symbol={} mode={}", accountId, accountAlias, symbol, mode);
        return new StrategySwitchResult(true, true, false, symbol, mode, statusReason.get());
    }

    private boolean persistStrategyProfile(String symbol, BinanceProperties.SymbolStrategyProfile profile) {
        try {
            dailyStatsStore.saveStrategyOverride(accountId, symbol, profile);
            return true;
        } catch (RuntimeException e) {
            log.error("[accountId={} alias={}] 持久化运行时策略失败: symbol={}", accountId, accountAlias, symbol, e);
            return false;
        }
    }

    private void applyPendingStrategyIfSafe() {
        if (currentStatus.get() != ChurnStatus.IDLE || activeOrderId.get() != null) return;
        String symbol = normalizeStrategySymbol(properties.getStrategy().getSymbol());
        BinanceProperties.SymbolStrategyProfile pending = pendingStrategyProfiles.remove(symbol);
        if (pending == null) return;
        if (!persistStrategyProfile(symbol, pending)) {
            pendingStrategyProfiles.put(symbol, pending);
            statusReason.set("策略切换等待持久化成功后应用");
            return;
        }
        strategyProfiles.put(symbol, pending);
        applyFeeAwareManualAnchorIfCurrent(symbol, pending);
        statusReason.set("策略切换已应用为 " + getStrategyMode());
        log.info("[accountId={} alias={}] 已在 IDLE 安全边界应用排队策略: symbol={} mode={}",
                accountId, accountAlias, symbol, getStrategyMode());
    }

    private void applyFeeAwareManualAnchorIfCurrent(String symbol, BinanceProperties.SymbolStrategyProfile profile) {
        if (!normalizeStrategySymbol(properties.getStrategy().getSymbol()).equals(symbol)) return;
        if (profile == null || !"FEE_AWARE_MAKER".equalsIgnoreCase(profile.getMode())) {
            feeAwareInitialEntryAnchorPrice.set(null);
            return;
        }
        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
        BigDecimal manual = configuredManualEntryAnchorPrice(profile, rule);
        if (manual != null && manual.signum() > 0) {
            feeAwareInitialEntryAnchorPrice.set(manual);
            feeAwareEntryPriceCeiling.set(manual);
        } else {
            feeAwareInitialEntryAnchorPrice.set(null);
        }
        feeAwareEntryCeilingBlockedSince.set(0);
    }

    private boolean validEntryTimeout(Long timeoutMs) {
        return timeoutMs == null || (timeoutMs >= 1_000 && timeoutMs <= 1_800_000);
    }

    private boolean validExitTimeout(Long timeoutMs) {
        return timeoutMs == null || (timeoutMs >= 1_000 && timeoutMs <= 86_400_000L);
    }

    private boolean validPostSellEntryDelay(Long delayMs) {
        return delayMs == null || (delayMs >= 0 && delayMs <= 86_400_000L);
    }

    private boolean validBps(BigDecimal value, BigDecimal maximum) {
        return value == null || (value.signum() >= 0 && value.compareTo(maximum) <= 0);
    }

    private String normalizeStrategyMode(String mode) {
        String normalized = mode == null ? "" : mode.trim().toUpperCase();
        if ("BID_ASK_MAKER".equals(normalized)) return "BID_ASK_MAKER";
        if ("BUY_PRICE_MAKER".equals(normalized)) return "BUY_PRICE_MAKER";
        return "FEE_AWARE_MAKER";
    }

    private String normalizeStrategySymbol(String symbol) {
        String normalized = symbol == null ? "" : symbol.trim().toUpperCase();
        return normalized.matches("[A-Z0-9]{5,20}") ? normalized : "";
    }

    private BinanceProperties.SymbolStrategyProfile copyStrategyProfile(
            BinanceProperties.SymbolStrategyProfile source) {
        BinanceProperties.SymbolStrategyProfile copy = new BinanceProperties.SymbolStrategyProfile();
        copy.setMode(normalizeStrategyMode(source.getMode()));
        copy.setOrderAmountUsdt(source.getOrderAmountUsdt());
        copy.setEntryTimeoutMs(source.getEntryTimeoutMs());
        copy.setExitTimeoutMs(source.getExitTimeoutMs());
        copy.setMakerFeeBps(source.getMakerFeeBps());
        copy.setTargetNetProfitBps(source.getTargetNetProfitBps());
        copy.setEntryAnchorWaitMs(source.getEntryAnchorWaitMs());
        copy.setMaxEntryAnchorDriftBps(source.getMaxEntryAnchorDriftBps());
        copy.setMaxCumulativeEntryAnchorDriftBps(source.getMaxCumulativeEntryAnchorDriftBps());
        copy.setManualEntryAnchorPrice(source.getManualEntryAnchorPrice());
        copy.setPostSellEntryDelayMs(source.getPostSellEntryDelayMs());
        copy.setBidAskInitialSellMarkupTicks(source.getBidAskInitialSellMarkupTicks());
        copy.setBidAskEntryBookLevel(source.getBidAskEntryBookLevel());
        copy.setDailyVolumeLimitUsdt(source.getDailyVolumeLimitUsdt());
        return copy;
    }

    private BinanceProperties.SymbolStrategyProfile symbolStrategy(String symbol) {
        return strategyProfiles.get(normalizeStrategySymbol(symbol));
    }

    private boolean usesBidAskMakerStrategy() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile != null && "BID_ASK_MAKER".equalsIgnoreCase(profile.getMode());
    }

    private boolean usesFeeAwareMakerStrategy() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile != null && "FEE_AWARE_MAKER".equalsIgnoreCase(profile.getMode());
    }

    private boolean usesBuyPriceMakerStrategy() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile != null && "BUY_PRICE_MAKER".equalsIgnoreCase(profile.getMode());
    }

    private boolean usesBestBidEntryStrategy() {
        return usesBidAskMakerStrategy() || usesBuyPriceMakerStrategy() || usesFeeAwareMakerStrategy();
    }

    private MarketSignalEvaluator.EntryDecision entryDecisionForStrategy(long now) {
        return marketSignalEvaluator.evaluateBestBidMaker(now, properties.getStrategy());
    }

    private long entryOrderTimeoutMs() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile != null && profile.getEntryTimeoutMs() != null && profile.getEntryTimeoutMs() > 0
                ? profile.getEntryTimeoutMs() : properties.getStrategy().getOrderTtlMs();
    }

    private long exitOrderTimeoutMs() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        return profile != null && profile.getExitTimeoutMs() != null && profile.getExitTimeoutMs() > 0
                ? profile.getExitTimeoutMs() : properties.getStrategy().getLimitSellTimeoutMs();
    }

    private long postSellEntryDelayMs() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        Long delayMs = profile == null ? null : profile.getPostSellEntryDelayMs();
        if (delayMs == null) delayMs = 60_000L;
        return Math.max(0, Math.min(86_400_000L, delayMs));
    }

    private int bidAskInitialSellMarkupTicks() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        Integer ticks = profile == null ? null : profile.getBidAskInitialSellMarkupTicks();
        return ticks == null ? 1 : Math.max(1, Math.min(10_000, ticks));
    }

    private int bidAskEntryBookLevel() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        Integer level = profile == null ? null : profile.getBidAskEntryBookLevel();
        return level == null ? 1 : Math.max(1, Math.min(5, level));
    }

    private String configuredEntryBookLevelLabel() {
        return usesBidAskMakerStrategy() ? "买" + bidAskEntryBookLevel() : "买一";
    }

    private BigDecimal configuredEntryBookPrice(BigDecimal bestBid, SymbolRuleManager.SymbolRule rule, long nowMs) {
        if (!usesBidAskMakerStrategy() || bidAskEntryBookLevel() == 1) {
            return bestBid == null || bestBid.signum() <= 0
                    ? null : PrecisionUtil.roundDownToStep(bestBid, rule.tickSize());
        }
        int level = bidAskEntryBookLevel();
        List<BigDecimal> prices = latestBidDepthPrices.get();
        long depthAgeMs = nowMs - lastDepthDataTimestamp.get();
        if (lastDepthDataTimestamp.get() <= 0 || depthAgeMs > properties.getStrategy().getDepthDataStaleMs()
                || prices.size() < level) {
            statusReason.set("等待" + configuredEntryBookLevelLabel() + "深度行情后再挂买单");
            return null;
        }
        BigDecimal price = prices.get(level - 1);
        return price == null || price.signum() <= 0
                ? null : PrecisionUtil.roundDownToStep(price, rule.tickSize());
    }

    private long schedulePostSellEntryDelay() {
        if (!usesBestBidEntryStrategy() || !isRunning.get()) {
            postSellNextEntryAllowedAtMs.set(0);
            return 0;
        }
        long delayMs = postSellEntryDelayMs();
        postSellNextEntryAllowedAtMs.set(delayMs > 0 ? System.currentTimeMillis() + delayMs : 0);
        return delayMs;
    }

    private boolean postSellEntryDelayActive(long nowMs) {
        if (!usesBestBidEntryStrategy()) {
            postSellNextEntryAllowedAtMs.set(0);
            return false;
        }
        long allowedAtMs = postSellNextEntryAllowedAtMs.get();
        if (allowedAtMs <= 0) return false;
        if (nowMs >= allowedAtMs) {
            postSellNextEntryAllowedAtMs.compareAndSet(allowedAtMs, 0);
            return false;
        }
        statusReason.set(getStrategyMode() + " 卖出后冷却中，剩余 "
                + durationLabel(allowedAtMs - nowMs) + " 再挂下一笔买单");
        return true;
    }

    private String durationLabel(long durationMs) {
        if (durationMs >= 60_000 && durationMs % 60_000 == 0) return (durationMs / 60_000) + " 分钟";
        return Math.max(1, durationMs / 1_000) + " 秒";
    }

    private BigDecimal initialStrategyExitPrice(SymbolRuleManager.SymbolRule rule) {
        if (usesFeeAwareMakerStrategy()) return feeProtectedExitPrice(rule, lastBestAskOrZero());
        if (usesBuyPriceMakerStrategy()) {
            BigDecimal price = entryAveragePrice(rule);
            if (price.signum() > 0) return price;
        }
        if (usesBidAskMakerStrategy()) {
            BigDecimal price = bidAskMakerExitPrice(rule, lastBestAskOrZero());
            if (price.signum() > 0) return price;
        }
        return exitReferencePrice(rule);
    }

    private BigDecimal entryPriceForStrategy(BigDecimal bestBid, SymbolRuleManager.SymbolRule rule, long nowMs) {
        BigDecimal configuredBookPrice = configuredEntryBookPrice(bestBid, rule, nowMs);
        if (configuredBookPrice == null || configuredBookPrice.signum() <= 0) return null;
        BigDecimal price = usesBestBidEntryStrategy()
                ? configuredBookPrice
                : PrecisionUtil.roundDownToStep(bestBid.subtract(rule.tickSize().multiply(BigDecimal.valueOf(
                properties.getStrategy().getBidDepthOffsetTicks()))), rule.tickSize());
        if (usesFeeAwareMakerStrategy() && !feeAwareEntryAllowedByAnchor(price, rule)) {
            statusReason.set(feeAwareAnchorWaitMessage(price, rule, "等待价格回落或等待时间满足后再挂买单"));
            return null;
        }
        return price;
    }

    private void rememberFeeAwareEntryPriceCeiling(SymbolRuleManager.SymbolRule rule) {
        if (!usesFeeAwareMakerStrategy()) return;
        BigDecimal entryAnchor = entryMaxFloorPrice(rule);
        if (entryAnchor.signum() <= 0) return;
        feeAwareEntryPriceCeiling.set(entryAnchor);
        ensureFeeAwareInitialEntryAnchor(rule);
        feeAwareEntryCeilingBlockedSince.set(0);
        log.info("[accountId={} alias={}] Fee-aware maker 下一轮买入价格上限更新为本轮最高买入价 {}",
                accountId, accountAlias, entryAnchor);
    }

    private BigDecimal entryMaxFloorPrice(SymbolRuleManager.SymbolRule rule) {
        BigDecimal maxPrice = filledEntryMaxPrice.get();
        if (rule == null || maxPrice == null || maxPrice.signum() <= 0) return BigDecimal.ZERO;
        return PrecisionUtil.roundUpToStep(maxPrice, rule.tickSize());
    }

    private boolean exceedsFeeAwareEntryCeiling(BigDecimal price) {
        BigDecimal ceiling = feeAwareEntryPriceCeiling.get();
        return price != null && ceiling != null && ceiling.signum() > 0 && price.compareTo(ceiling) > 0;
    }

    private boolean feeAwareEntryAllowedByAnchor(BigDecimal price, SymbolRuleManager.SymbolRule rule) {
        BigDecimal ceiling = feeAwareEntryPriceCeiling.get();
        if (price == null || ceiling == null || ceiling.signum() <= 0) {
            feeAwareEntryCeilingBlockedSince.set(0);
            return true;
        }
        BigDecimal maxAllowed = feeAwareAllowedEntryPrice(rule);
        if (maxAllowed == null || maxAllowed.signum() <= 0) {
            feeAwareEntryCeilingBlockedSince.set(0);
            return true;
        }
        if (price.compareTo(ceiling) <= 0 && price.compareTo(maxAllowed) <= 0) {
            feeAwareEntryCeilingBlockedSince.set(0);
            return true;
        }
        if (price.compareTo(ceiling) <= 0) return false;
        long now = System.currentTimeMillis();
        feeAwareEntryCeilingBlockedSince.compareAndSet(0, now);
        long waitMs = feeAwareAnchorWaitMs();
        if (now - feeAwareEntryCeilingBlockedSince.get() < waitMs) return false;
        return price.compareTo(maxAllowed) <= 0;
    }

    private String feeAwareAnchorWaitMessage(BigDecimal price, SymbolRuleManager.SymbolRule rule, String action) {
        BigDecimal ceiling = feeAwareEntryPriceCeiling.get();
        if (price == null || ceiling == null || ceiling.signum() <= 0) return action;
        long blockedSince = feeAwareEntryCeilingBlockedSince.get();
        long waitedMs = blockedSince <= 0 ? 0 : Math.max(0, System.currentTimeMillis() - blockedSince);
        long waitMs = feeAwareAnchorWaitMs();
        BigDecimal driftBps = price.subtract(ceiling).multiply(BigDecimal.valueOf(10_000))
                .divide(ceiling, 2, RoundingMode.HALF_UP);
        BigDecimal maxDriftBps = feeAwareMaxEntryAnchorDriftBps();
        BigDecimal initialAnchor = ensureFeeAwareInitialEntryAnchor(rule);
        BigDecimal cumulativeBps = feeAwareMaxCumulativeEntryAnchorDriftBps(maxDriftBps);
        BigDecimal maxAllowed = feeAwareAllowedEntryPrice(rule);
        String waitText = waitMs <= 0 ? "已允许检查追高上限"
                : "已等待 " + elapsedDurationLabel(waitedMs) + " / " + durationLabel(waitMs);
        String driftText = maxDriftBps.signum() <= 0
                ? "最大追高 0 bps，仍需回落到锚点或以下"
                : "最大追高 " + maxDriftBps.stripTrailingZeros().toPlainString() + " bps";
        BigDecimal manualAnchor = configuredManualEntryAnchorPrice(
                symbolStrategy(properties.getStrategy().getSymbol()), rule);
        String anchorLabel = manualAnchor != null && manualAnchor.signum() > 0
                ? "手动锚定价 " : "最近5笔均价锚点 ";
        String cumulativeText = initialAnchor == null || initialAnchor.signum() <= 0 ? ""
                : "；" + anchorLabel + initialAnchor.toPlainString()
                + "，累计最大追高 " + cumulativeBps.stripTrailingZeros().toPlainString() + " bps";
        String allowedText = maxAllowed == null || maxAllowed.signum() <= 0 ? ""
                : "，最终允许最高 " + maxAllowed.toPlainString();
        return "暂停新买入：当前买一 " + price.toPlainString()
                + " 高于买入锚点 " + ceiling.toPlainString()
                + "，高出 " + driftBps.stripTrailingZeros().toPlainString() + " bps；"
                + waitText + "；" + driftText + cumulativeText + allowedText + "，" + action;
    }

    private String elapsedDurationLabel(long durationMs) {
        if (durationMs < 1_000) return "0 秒";
        return durationLabel(durationMs);
    }

    private BigDecimal maxFeeAwareEntryPrice(BigDecimal ceiling, BigDecimal maxDriftBps) {
        return ceiling.multiply(BigDecimal.ONE.add(maxDriftBps.divide(BigDecimal.valueOf(10_000),
                java.math.MathContext.DECIMAL64)));
    }

    private BigDecimal feeAwareAllowedEntryPrice(SymbolRuleManager.SymbolRule rule) {
        BigDecimal ceiling = feeAwareEntryPriceCeiling.get();
        if (ceiling == null || ceiling.signum() <= 0) return null;
        BigDecimal maxDriftBps = feeAwareMaxEntryAnchorDriftBps();
        BigDecimal singleRoundCap = maxFeeAwareEntryPrice(ceiling, maxDriftBps);
        BigDecimal initialAnchor = ensureFeeAwareInitialEntryAnchor(rule);
        if (initialAnchor == null || initialAnchor.signum() <= 0) return singleRoundCap;
        BigDecimal cumulativeCap = maxFeeAwareEntryPrice(initialAnchor,
                feeAwareMaxCumulativeEntryAnchorDriftBps(maxDriftBps));
        return singleRoundCap.min(cumulativeCap);
    }

    private BigDecimal ensureFeeAwareInitialEntryAnchor(SymbolRuleManager.SymbolRule rule) {
        BigDecimal manual = configuredManualEntryAnchorPrice(symbolStrategy(properties.getStrategy().getSymbol()), rule);
        if (manual != null && manual.signum() > 0) {
            feeAwareInitialEntryAnchorPrice.set(manual);
            return manual;
        }
        BigDecimal existing = feeAwareInitialEntryAnchorPrice.get();
        if (existing != null && existing.signum() > 0) return existing;
        BigDecimal average = recentFeeAwareBuyAverageFloorPrice(rule);
        if (average.signum() <= 0) average = feeAwareEntryPriceCeiling.get();
        if (average == null || average.signum() <= 0) return BigDecimal.ZERO;
        feeAwareInitialEntryAnchorPrice.compareAndSet(null, average);
        return feeAwareInitialEntryAnchorPrice.get();
    }

    private BigDecimal configuredManualEntryAnchorPrice(BinanceProperties.SymbolStrategyProfile profile,
                                                        SymbolRuleManager.SymbolRule rule) {
        if (profile == null || profile.getManualEntryAnchorPrice() == null
                || profile.getManualEntryAnchorPrice().signum() <= 0) return null;
        return rule == null ? profile.getManualEntryAnchorPrice()
                : PrecisionUtil.roundUpToStep(profile.getManualEntryAnchorPrice(), rule.tickSize());
    }

    private void rememberFeeAwareRecentBuyPrice(BigDecimal price) {
        if (!usesFeeAwareMakerStrategy() || price == null || price.signum() <= 0) return;
        synchronized (feeAwareRecentBuyPrices) {
            feeAwareRecentBuyPrices.addLast(price);
            while (feeAwareRecentBuyPrices.size() > 5) feeAwareRecentBuyPrices.removeFirst();
        }
    }

    private BigDecimal recentFeeAwareBuyAverageFloorPrice(SymbolRuleManager.SymbolRule rule) {
        if (rule == null) return BigDecimal.ZERO;
        synchronized (feeAwareRecentBuyPrices) {
            if (feeAwareRecentBuyPrices.isEmpty()) return BigDecimal.ZERO;
            BigDecimal sum = BigDecimal.ZERO;
            for (BigDecimal price : feeAwareRecentBuyPrices) sum = sum.add(price);
            return PrecisionUtil.roundUpToStep(sum.divide(BigDecimal.valueOf(feeAwareRecentBuyPrices.size()),
                    java.math.MathContext.DECIMAL64), rule.tickSize());
        }
    }

    private long feeAwareAnchorWaitMs() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        Long waitMs = profile == null ? null : profile.getEntryAnchorWaitMs();
        if (waitMs == null) waitMs = 1_800_000L;
        return Math.max(0, Math.min(86_400_000L, waitMs));
    }

    private BigDecimal feeAwareMaxEntryAnchorDriftBps() {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        BigDecimal bps = profile == null ? null : profile.getMaxEntryAnchorDriftBps();
        if (bps == null || bps.signum() < 0) return BigDecimal.ZERO;
        return bps.min(new BigDecimal("100"));
    }

    private BigDecimal feeAwareMaxCumulativeEntryAnchorDriftBps(BigDecimal fallbackBps) {
        var profile = symbolStrategy(properties.getStrategy().getSymbol());
        BigDecimal bps = profile == null ? null : profile.getMaxCumulativeEntryAnchorDriftBps();
        if (bps == null) bps = fallbackBps;
        if (bps == null || bps.signum() < 0) return BigDecimal.ZERO;
        return bps.min(new BigDecimal("100"));
    }

    private BigDecimal orderAmountUsdt() {
        return orderAmountUsdt(properties.getStrategy().getSymbol());
    }

    private BigDecimal orderAmountUsdt(String symbol) {
        String normalizedSymbol = symbol == null ? "" : symbol.trim().toUpperCase();
        var profile = symbolStrategy(normalizedSymbol);
        if (profile != null && profile.getOrderAmountUsdt() != null
                && profile.getOrderAmountUsdt().signum() > 0) return profile.getOrderAmountUsdt();
        BigDecimal override = properties.getStrategy().getOrderAmountsUsdt().get(normalizedSymbol);
        return override != null && override.signum() > 0
                ? override : properties.getStrategy().getOrderAmountUsdt();
    }

    public int getUsedApiWeight() { return tradeService.getUsedWeight1m(); }
    public int getApiWeightLimit() { return tradeService.getRequestWeightLimit1m(); }
    public int getApiWeightEntrySafeLimit() { return tradeService.getSafeRequestWeightLimit1m(); }
    public BnbBalanceSnapshot getBnbBalanceSnapshot() { return refreshBnbBalanceSnapshot(false); }
    /** Monitoring must not make Binance REST calls for a stopped strategy. */
    public BnbBalanceSnapshot getBnbBalanceSnapshotForMonitoring() {
        return isRunning.get() ? refreshBnbBalanceSnapshot(false) : bnbBalanceSnapshot.get();
    }
    public MarketSignalEvaluator.EntryDecision getLastEntryDecision() { return marketSignalEvaluator.getLastDecision(); }
    public TradingRiskGuard.RiskSnapshot getRiskSnapshot() { return riskGuard.snapshot(); }
    public TradingRiskGuard getRiskGuard() { return riskGuard; }
    public AccountRiskCoordinator.ExposureSnapshot getAccountRiskSnapshot() {
        return accountRiskCoordinator.snapshot();
    }
    public TradeAccountingLedger.AccountingSnapshot getAccountingSnapshot() { return accountingLedger.snapshot(); }
    public DailyTradeStatsStore.DailyStatsSnapshot getDailyStatsSnapshot() {
        return dailyStatsStore.today(accountId, accountAlias, properties.getStrategy().getSymbol());
    }

    public RemoteTodayStatusSnapshot getRemoteTodayStatusSnapshot() {
        long now = System.currentTimeMillis();
        RemoteTodayStatusSnapshot cached = remoteTodayAccountingCache.get();
        if (cached != null && now - cached.updatedAtMs() < REMOTE_TODAY_ACCOUNTING_CACHE_MS) return cached;
        synchronized (remoteTodayAccountingLock) {
            cached = remoteTodayAccountingCache.get();
            if (cached != null && now - cached.updatedAtMs() < REMOTE_TODAY_ACCOUNTING_CACHE_MS) return cached;
            RemoteTodayStatusSnapshot refreshed = fetchRemoteTodayStatusSnapshot(now);
            if (refreshed != null) {
                remoteTodayAccountingCache.set(refreshed);
                return refreshed;
            }
            return cached == null ? localTodayStatusSnapshot(now) : cached;
        }
    }

    private RemoteTodayStatusSnapshot refreshRemoteTodayStatusSnapshot() {
        synchronized (remoteTodayAccountingLock) {
            long now = System.currentTimeMillis();
            RemoteTodayStatusSnapshot refreshed = fetchRemoteTodayStatusSnapshot(now);
            if (refreshed != null) remoteTodayAccountingCache.set(refreshed);
            return refreshed;
        }
    }

    /** Dashboard polling is cache-only; exchange reconciliation happens at start and stream recovery. */
    public RemoteTodayStatusSnapshot getRemoteTodayStatusSnapshotForMonitoring() {
        return localTodayStatusSnapshot(System.currentTimeMillis());
    }

    private RemoteTodayStatusSnapshot fetchRemoteTodayStatusSnapshot(long nowMs) {
        String symbol = properties.getStrategy().getSymbol();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        long startMs = today.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        RemoteTradePage remoteTrades = fetchAllRemoteTrades(symbol, startMs, nowMs);
        BinanceAccountTradeClient.AssetBalance balance = tradeService.getAssetBalance(baseAsset());
        JsonNode openOrders = tradeService.getOpenOrders(symbol);
        if (remoteTrades == null || balance == null || openOrders == null || !openOrders.isArray()) {
            log.warn("[accountId={} alias={}] 今日远程成交/余额快照暂不可用，继续使用上一份状态缓存", accountId, accountAlias);
            return null;
        }
        RemoteTodayAccumulator accumulator = new RemoteTodayAccumulator(today, accountId, accountAlias, symbol);
        List<JsonNode> sortedTrades = new ArrayList<>(remoteTrades.trades());
        sortedTrades.sort(Comparator
                .comparingLong((JsonNode trade) -> trade.path("time").asLong(0))
                .thenComparingLong(trade -> trade.path("id").asLong(0)));
        for (JsonNode trade : sortedTrades) {
            long tradeTime = trade.path("time").asLong(0);
            if (tradeTime < startMs || tradeTime > nowMs) continue;
            BigDecimal quantity = decimalOrZero(trade.path("qty").asText("0"));
            BigDecimal price = decimalOrZero(trade.path("price").asText("0"));
            BigDecimal quote = decimalOrZero(trade.path("quoteQty").asText(quantity.multiply(price).toPlainString()));
            BigDecimal commission = decimalOrZero(trade.path("commission").asText("0"));
            String commissionAsset = trade.path("commissionAsset").asText("");
            BigDecimal commissionQuote = commissionQuoteEquivalent(commission, commissionAsset, price);
            String side = trade.path("isBuyer").asBoolean(false) ? "BUY" : "SELL";
            boolean baseCommission = baseAsset().equalsIgnoreCase(commissionAsset);
            BigDecimal inventoryQuantity = quantity;
            if (baseCommission) {
                inventoryQuantity = "BUY".equals(side)
                        ? inventoryQuantity.subtract(commission).max(BigDecimal.ZERO)
                        : inventoryQuantity.add(commission);
            }
            BigDecimal economicFeeQuote = baseCommission ? BigDecimal.ZERO : commissionQuote;
            accumulator.record(side, trade.path("orderId").asLong(-1), trade.path("id").asLong(-1),
                    inventoryQuantity, quantity, price, quote, commission, commissionAsset,
                    commissionQuote, economicFeeQuote, tradeTime);
            DailyTradeStatsStore.RecordResult stored = dailyStatsStore.recordTrade(
                    accountId, accountAlias, symbol, trade.path("orderId").asLong(-1),
                    trade.path("id").asLong(-1), side, inventoryQuantity, quantity, price, quote,
                    commission, commissionAsset, commissionQuote, economicFeeQuote, tradeTime);
            if (stored == DailyTradeStatsStore.RecordResult.FAILED) return null;
        }

        SymbolRuleManager.SymbolRule rule = ruleManager.getRule(symbol);
        DailyTradeStatsStore.DailyStatsSnapshot daily = accumulator.dailySnapshot();
        DailyTradeStatsStore.DailyStatsSnapshot balanceAlignedDaily = alignRemoteDailyWithBalance(daily, balance, rule);
        applyRemoteTodayToRuntime(balanceAlignedDaily, balance, rule, noOpenOrders(openOrders),
                accumulator.positionOpenedAtMs(), nowMs);
        TradingRiskGuard.RiskSnapshot risk = remoteRiskSnapshot(balanceAlignedDaily, balance, rule,
                accumulator.positionOpenedAtMs());
        TradeAccountingLedger.AccountingSnapshot accounting = accumulator.accountingSnapshot();
        totalVolumeUsdt.set(balanceAlignedDaily.totalVolumeQuote());
        roundTripsCompleted.set(balanceAlignedDaily.roundTrips());
        dailyVolumeCounterDate.set(balanceAlignedDaily.date());
        dailyVolumeStopPending.set(dailyVolumeLimitReached());
        return new RemoteTodayStatusSnapshot(risk, accounting, balanceAlignedDaily, true,
                remoteTrades.truncated(), nowMs);
    }

    private RemoteTradePage fetchAllRemoteTrades(String symbol, long startMs, long endMs) {
        JsonNode first = tradeService.getMyTrades(symbol, startMs, endMs, 1000);
        if (first == null || !first.isArray()) return null;
        List<JsonNode> trades = new ArrayList<>();
        for (JsonNode trade : first) {
            long tradeTime = trade.path("time").asLong(0);
            if (tradeTime >= startMs && tradeTime <= endMs) trades.add(trade);
        }
        JsonNode page = first;
        int pages = 1;
        while (page.size() >= 1000) {
            if (pages++ >= 250) {
                log.error("[accountId={} alias={}] 今日成交分页超过安全上限，拒绝使用不完整统计", accountId, accountAlias);
                return null;
            }
            long lastId = page.get(page.size() - 1).path("id").asLong(-1);
            if (lastId < 0) return null;
            page = tradeService.getMyTradesFromId(symbol, lastId + 1, 1000);
            if (page == null || !page.isArray()) return null;
            long previousId = lastId;
            boolean passedWindowEnd = false;
            for (JsonNode trade : page) {
                long tradeId = trade.path("id").asLong(-1);
                long tradeTime = trade.path("time").asLong(0);
                if (tradeTime > endMs) {
                    passedWindowEnd = true;
                    break;
                }
                if (tradeId <= previousId || tradeTime < startMs || tradeTime > endMs) continue;
                trades.add(trade);
            }
            if (passedWindowEnd) break;
            if (page.size() > 0 && page.get(page.size() - 1).path("id").asLong(-1) <= previousId) {
                log.error("[accountId={} alias={}] 今日成交分页未向前推进，拒绝使用不完整统计", accountId, accountAlias);
                return null;
            }
        }
        return new RemoteTradePage(List.copyOf(trades), false);
    }

    private RecoveredOpenPosition recoverRemoteOpenPosition(BigDecimal remoteQuantity,
                                                            SymbolRuleManager.SymbolRule rule) {
        String symbol = properties.getStrategy().getSymbol();
        long windowEndMs = System.currentTimeMillis();
        Map<Long, JsonNode> tradesById = new LinkedHashMap<>();
        for (int window = 0; window < POSITION_RECOVERY_MAX_WINDOWS; window++) {
            long windowStartMs = Math.max(0, windowEndMs - POSITION_RECOVERY_WINDOW_MS + 1);
            RemoteTradePage page = fetchAllRemoteTrades(symbol, windowStartMs, windowEndMs);
            if (page == null) return null;
            for (JsonNode trade : page.trades()) {
                long tradeId = trade.path("id").asLong(-1);
                if (tradeId >= 0) tradesById.putIfAbsent(tradeId, trade);
            }
            RecoveredOpenPosition recovered = reconstructOpenPositionFromRemoteTrades(
                    remoteQuantity, rule, new ArrayList<>(tradesById.values()));
            if (recovered != null) return recovered;
            if (windowStartMs == 0) break;
            windowEndMs = windowStartMs - 1;
        }
        log.warn("[accountId={} alias={}] 最近 {} 天交易所成交仍无法还原持仓: symbol={} remoteQty={}",
                accountId, accountAlias, POSITION_RECOVERY_MAX_WINDOWS,
                properties.getStrategy().getSymbol(), remoteQuantity);
        return null;
    }

    private RecoveredOpenPosition reconstructOpenPositionFromRemoteTrades(
            BigDecimal remoteQuantity, SymbolRuleManager.SymbolRule rule, List<JsonNode> remoteTrades) {
        if (remoteQuantity == null || remoteQuantity.signum() <= 0 || remoteTrades == null || remoteTrades.isEmpty()) {
            return null;
        }
        List<JsonNode> descending = new ArrayList<>(remoteTrades);
        descending.sort(Comparator
                .comparingLong((JsonNode trade) -> trade.path("time").asLong(0))
                .thenComparingLong(trade -> trade.path("id").asLong(0))
                .reversed());
        BigDecimal quantityBeforeCandidate = remoteQuantity;
        List<JsonNode> candidate = new ArrayList<>();
        for (JsonNode trade : descending) {
            BigDecimal inventoryQuantity = remoteInventoryQuantity(trade);
            if (inventoryQuantity.signum() <= 0) continue;
            candidate.add(trade);
            quantityBeforeCandidate = trade.path("isBuyer").asBoolean(false)
                    ? quantityBeforeCandidate.subtract(inventoryQuantity)
                    : quantityBeforeCandidate.add(inventoryQuantity);
            if (!isEffectivelyFlat(quantityBeforeCandidate.abs(), rule.stepSize())) continue;

            RemoteTodayAccumulator accumulator = new RemoteTodayAccumulator(
                    LocalDate.now(ZoneOffset.UTC), accountId, accountAlias,
                    properties.getStrategy().getSymbol());
            candidate.stream().sorted(Comparator
                            .comparingLong((JsonNode value) -> value.path("time").asLong(0))
                            .thenComparingLong(value -> value.path("id").asLong(0)))
                    .forEach(value -> recordRemoteTradeForRecovery(accumulator, value));
            DailyTradeStatsStore.DailyStatsSnapshot reconstructed = accumulator.dailySnapshot();
            if (reconstructed.positionCostQuote().signum() > 0
                    && reconstructed.positionQty().subtract(remoteQuantity).abs()
                    .compareTo(rule.stepSize()) < 0) {
                return new RecoveredOpenPosition(remoteQuantity, reconstructed.positionCostQuote(),
                        accumulator.positionOpenedAtMs());
            }
        }
        return null;
    }

    private void recordRemoteTradeForRecovery(RemoteTodayAccumulator accumulator, JsonNode trade) {
        BigDecimal quantity = decimalOrZero(trade.path("qty").asText("0"));
        BigDecimal price = decimalOrZero(trade.path("price").asText("0"));
        BigDecimal quote = decimalOrZero(trade.path("quoteQty").asText(quantity.multiply(price).toPlainString()));
        BigDecimal commission = decimalOrZero(trade.path("commission").asText("0"));
        String commissionAsset = trade.path("commissionAsset").asText("");
        BigDecimal commissionQuote = commissionQuoteEquivalent(commission, commissionAsset, price);
        BigDecimal inventoryQuantity = remoteInventoryQuantity(trade);
        boolean baseCommission = baseAsset().equalsIgnoreCase(commissionAsset);
        accumulator.record(trade.path("isBuyer").asBoolean(false) ? "BUY" : "SELL",
                trade.path("orderId").asLong(-1), trade.path("id").asLong(-1), inventoryQuantity,
                quantity, price, quote, commission, commissionAsset, commissionQuote,
                baseCommission ? BigDecimal.ZERO : commissionQuote, trade.path("time").asLong(0));
    }

    private BigDecimal remoteInventoryQuantity(JsonNode trade) {
        BigDecimal quantity = decimalOrZero(trade.path("qty").asText("0"));
        BigDecimal commission = decimalOrZero(trade.path("commission").asText("0"));
        String commissionAsset = trade.path("commissionAsset").asText("");
        if (!baseAsset().equalsIgnoreCase(commissionAsset)) return quantity;
        return trade.path("isBuyer").asBoolean(false)
                ? quantity.subtract(commission).max(BigDecimal.ZERO)
                : quantity.add(commission);
    }

    private void reconcileRemoteTodayTradesIncrementally() {
        String symbol = properties.getStrategy().getSymbol();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        java.util.OptionalLong latest = dailyStatsStore.latestTradeId(accountId, symbol, today);
        if (latest.isEmpty()) {
            refreshRemoteTodayStatusSnapshot();
            syncDailyCounters();
            return;
        }
        long startMs = today.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long nowMs = System.currentTimeMillis();
        long nextId = latest.getAsLong() + 1;
        for (int pageNumber = 0; pageNumber < 250; pageNumber++) {
            JsonNode page = tradeService.getMyTradesFromId(symbol, nextId, 1000);
            if (page == null || !page.isArray()) return;
            long previousId = nextId - 1;
            for (JsonNode trade : page) {
                long tradeId = trade.path("id").asLong(-1);
                long tradeTime = trade.path("time").asLong(0);
                if (tradeId < nextId || tradeTime < startMs || tradeTime > nowMs) continue;
                persistRemoteTrade(symbol, trade);
                previousId = Math.max(previousId, tradeId);
            }
            if (page.size() < 1000) break;
            if (previousId < nextId) return;
            nextId = previousId + 1;
        }
        syncDailyCounters();
    }

    private void persistRemoteTrade(String symbol, JsonNode trade) {
        long tradeTime = trade.path("time").asLong(System.currentTimeMillis());
        BigDecimal quantity = decimalOrZero(trade.path("qty").asText("0"));
        BigDecimal price = decimalOrZero(trade.path("price").asText("0"));
        BigDecimal quote = decimalOrZero(trade.path("quoteQty").asText(quantity.multiply(price).toPlainString()));
        BigDecimal commission = decimalOrZero(trade.path("commission").asText("0"));
        String commissionAsset = trade.path("commissionAsset").asText("");
        BigDecimal commissionQuote = commissionQuoteEquivalent(commission, commissionAsset, price);
        String side = trade.path("isBuyer").asBoolean(false) ? "BUY" : "SELL";
        boolean baseCommission = baseAsset().equalsIgnoreCase(commissionAsset);
        BigDecimal inventoryQuantity = baseCommission
                ? ("BUY".equals(side) ? quantity.subtract(commission).max(BigDecimal.ZERO) : quantity.add(commission))
                : quantity;
        BigDecimal economicFeeQuote = baseCommission ? BigDecimal.ZERO : commissionQuote;
        dailyStatsStore.recordTrade(accountId, accountAlias, symbol,
                trade.path("orderId").asLong(-1), trade.path("id").asLong(-1), side,
                inventoryQuantity, quantity, price, quote, commission, commissionAsset,
                commissionQuote, economicFeeQuote, tradeTime);
    }

    private RemoteTodayStatusSnapshot localTodayStatusSnapshot(long nowMs) {
        return new RemoteTodayStatusSnapshot(getRiskSnapshot(), getAccountingSnapshot(), getDailyStatsSnapshot(),
                false, false, nowMs);
    }

    private DailyTradeStatsStore.DailyStatsSnapshot alignRemoteDailyWithBalance(
            DailyTradeStatsStore.DailyStatsSnapshot daily,
            BinanceAccountTradeClient.AssetBalance balance,
            SymbolRuleManager.SymbolRule rule) {
        if (daily == null || balance == null) return daily;
        BigDecimal remoteQty = balance.total() == null ? BigDecimal.ZERO : balance.total();
        BigDecimal step = rule == null ? BigDecimal.ZERO : rule.stepSize();
        if (isEffectivelyFlat(remoteQty, step) && daily.positionQty().signum() > 0) {
            return new DailyTradeStatsStore.DailyStatsSnapshot(daily.date(), daily.accountId(), daily.accountAlias(),
                    daily.symbol(), daily.buyVolumeQuote(), daily.sellVolumeQuote(), daily.totalVolumeQuote(),
                    daily.totalCommissionQuoteEquivalent(), daily.costPerMillionVolume(),
                    daily.realizedGrossPnlQuote(), daily.netRealizedPnlQuote(), BigDecimal.ZERO,
                    BigDecimal.ZERO, daily.tradeCount(), daily.roundTrips(), daily.commissionConversionComplete());
        }
        return daily;
    }

    private void applyRemoteTodayToRuntime(DailyTradeStatsStore.DailyStatsSnapshot daily,
                                           BinanceAccountTradeClient.AssetBalance balance,
                                           SymbolRuleManager.SymbolRule rule,
                                           boolean noOpenOrders,
                                           long positionOpenedAtMs,
                                           long nowMs) {
        BigDecimal remoteQty = balance.total() == null ? BigDecimal.ZERO : balance.total();
        holdingInventory.set(remoteQty);
        lastKnownFreeBaseBalance.set(balance.free());
        BigDecimal step = rule == null ? BigDecimal.ZERO : rule.stepSize();
        if (isEffectivelyFlat(remoteQty, step) && noOpenOrders) {
            activeSellCoveredQty.set(BigDecimal.ZERO);
            riskGuard.restoreFlatDaily(daily.netRealizedPnlQuote(), daily.totalCommissionQuoteEquivalent(),
                    daily.date(), properties.getStrategy());
            if ("MAX_INVENTORY_AGE".equals(getRiskBlockReason())) {
                riskGuard.reconcileExchangeFlat(nowMs, properties.getStrategy());
            }
            return;
        }
        if (daily.positionQty().signum() > 0 && daily.positionCostQuote().signum() > 0
                && remoteQty.subtract(daily.positionQty()).abs().compareTo(step.max(BigDecimal.ZERO)) <= 0) {
            riskGuard.restoreFlatDaily(daily.netRealizedPnlQuote(), daily.totalCommissionQuoteEquivalent(),
                    daily.date(), properties.getStrategy());
            riskGuard.restoreOpenPosition(daily.positionQty(), daily.positionCostQuote(), lastBestAskOrZero(),
                    positionOpenedAtMs, properties.getStrategy());
            return;
        }
        TradingRiskGuard.RiskSnapshot recovered = riskGuard.snapshot();
        if (accountRiskReconciled.get() && recovered.positionCostUsdt().signum() > 0
                && recovered.positionQty().subtract(remoteQty).abs()
                .compareTo(step.max(BigDecimal.ZERO)) <= 0) {
            // Explicit-start reconciliation may have reconstructed an open lot from fills before
            // today's UTC boundary. Keep that open-lot cost while resetting daily-only counters.
            riskGuard.restoreFlatDaily(daily.netRealizedPnlQuote(), daily.totalCommissionQuoteEquivalent(),
                    daily.date(), properties.getStrategy());
            riskGuard.restoreOpenPosition(remoteQty, recovered.positionCostUsdt(), lastBestAskOrZero(),
                    recovered.positionOpenedAtMs(), properties.getStrategy());
            return;
        }
        if (!isEffectivelyFlat(remoteQty, step) && activeOrderId.get() == null) {
            riskGuard.trip("POSITION_NOT_FLAT");
            if (isRunning.get()) {
                halt("交易所发现持仓，但交易所历史成交无法还原成本；需人工对账");
            } else {
                statusReason.set("交易所发现持仓，但交易所历史成交无法还原成本；需人工对账");
            }
        }
    }

    private TradingRiskGuard.RiskSnapshot remoteRiskSnapshot(DailyTradeStatsStore.DailyStatsSnapshot daily,
                                                            BinanceAccountTradeClient.AssetBalance balance,
                                                            SymbolRuleManager.SymbolRule rule,
                                                            long positionOpenedAtMs) {
        BigDecimal mark = lastBestAsk.get();
        if (mark == null || mark.signum() <= 0) mark = lastMidPrice.get();
        BigDecimal remoteQty = balance == null || balance.total() == null ? BigDecimal.ZERO : balance.total();
        BigDecimal step = rule == null ? BigDecimal.ZERO : rule.stepSize();
        boolean remoteFlat = isEffectivelyFlat(remoteQty, step);
        boolean reconstructedOpenPosition = !remoteFlat
                && daily.positionQty().subtract(remoteQty).abs().compareTo(step.max(BigDecimal.ZERO)) <= 0
                && daily.positionCostQuote().signum() > 0;
        TradingRiskGuard.RiskSnapshot recovered = riskGuard.snapshot();
        boolean recoveredOpenPosition = !remoteFlat && !reconstructedOpenPosition
                && accountRiskReconciled.get()
                && recovered.positionCostUsdt().signum() > 0
                && recovered.positionQty().subtract(remoteQty).abs()
                .compareTo(step.max(BigDecimal.ZERO)) <= 0;
        BigDecimal positionQty = remoteFlat ? BigDecimal.ZERO
                : (reconstructedOpenPosition ? daily.positionQty()
                : (recoveredOpenPosition ? recovered.positionQty() : remoteQty));
        BigDecimal positionCost = reconstructedOpenPosition ? daily.positionCostQuote()
                : (recoveredOpenPosition ? recovered.positionCostUsdt() : BigDecimal.ZERO);
        long effectivePositionOpenedAtMs = reconstructedOpenPosition ? positionOpenedAtMs
                : (recoveredOpenPosition ? recovered.positionOpenedAtMs() : positionOpenedAtMs);
        BigDecimal unrealized = positionQty.signum() > 0 && positionCost.signum() > 0
                && mark != null && mark.signum() > 0
                ? positionQty.multiply(mark).subtract(positionCost)
                : BigDecimal.ZERO;
        String blockReason = getRiskBlockReason();
        if (remoteFlat && "MAX_INVENTORY_AGE".equals(blockReason)) blockReason = null;
        if (!remoteFlat && !reconstructedOpenPosition && !recoveredOpenPosition && blockReason == null) {
            blockReason = "POSITION_NOT_FLAT";
        }
        return new TradingRiskGuard.RiskSnapshot(blockReason, positionQty, positionCost,
                mark, daily.netRealizedPnlQuote(), unrealized, daily.netRealizedPnlQuote().add(unrealized),
                daily.totalCommissionQuoteEquivalent(), positionQty.signum() > 0 ? effectivePositionOpenedAtMs : -1,
                daily.date());
    }

    private boolean noOpenOrders(JsonNode openOrders) {
        return openOrders != null && openOrders.isArray() && openOrders.isEmpty();
    }

    private boolean isEffectivelyFlat(BigDecimal quantity, BigDecimal stepSize) {
        BigDecimal safeQuantity = quantity == null ? BigDecimal.ZERO : quantity;
        BigDecimal safeStep = stepSize == null ? BigDecimal.ZERO : stepSize;
        return safeQuantity.signum() == 0 || (safeStep.signum() > 0 && safeQuantity.compareTo(safeStep) < 0);
    }

    private BigDecimal decimalOrZero(String value) {
        try {
            if (value == null || value.isBlank()) return BigDecimal.ZERO;
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    private final class RemoteTodayAccumulator {
        private static final BigDecimal ONE_MILLION = new BigDecimal("1000000");
        private final LocalDate date;
        private final String accountId;
        private final String accountAlias;
        private final String symbol;
        private final Set<String> processedTradeIds = new LinkedHashSet<>();
        private final Map<String, BigDecimal> commissionByAsset = new LinkedHashMap<>();
        private final Set<String> unconvertedCommissionAssets = new LinkedHashSet<>();
        private BigDecimal buyVolume = BigDecimal.ZERO;
        private BigDecimal sellVolume = BigDecimal.ZERO;
        private BigDecimal totalVolume = BigDecimal.ZERO;
        private BigDecimal commissionQuote = BigDecimal.ZERO;
        private BigDecimal economicFeeQuote = BigDecimal.ZERO;
        private BigDecimal realizedGrossPnl = BigDecimal.ZERO;
        private BigDecimal positionQty = BigDecimal.ZERO;
        private BigDecimal positionCostQuote = BigDecimal.ZERO;
        private int tradeCount;
        private int roundTrips;
        private long positionOpenedAtMs = -1;
        private boolean commissionComplete = true;

        private RemoteTodayAccumulator(LocalDate date, String accountId, String accountAlias, String symbol) {
            this.date = date;
            this.accountId = accountId;
            this.accountAlias = accountAlias;
            this.symbol = symbol;
        }

        private void record(String side, long orderId, long tradeId, BigDecimal inventoryQuantity,
                            BigDecimal rawQuantity, BigDecimal price, BigDecimal quoteQuantity,
                            BigDecimal commission, String commissionAsset, BigDecimal commissionQuoteEquivalent,
                            BigDecimal economicFeeQuote, long tradeTimeMs) {
            if (inventoryQuantity == null || inventoryQuantity.signum() <= 0
                    || quoteQuantity == null || quoteQuantity.signum() <= 0) return;
            String identity = orderId + ":" + (tradeId >= 0 ? tradeId
                    : side + ":" + inventoryQuantity.toPlainString() + ":" + quoteQuantity.toPlainString());
            if (!processedTradeIds.add(identity)) return;
            tradeCount++;
            totalVolume = totalVolume.add(quoteQuantity);
            if ("BUY".equalsIgnoreCase(side)) buyVolume = buyVolume.add(quoteQuantity);
            else sellVolume = sellVolume.add(quoteQuantity);

            BigDecimal normalizedCommission = commission == null ? BigDecimal.ZERO : commission.max(BigDecimal.ZERO);
            String normalizedCommissionAsset = normalizeAsset(commissionAsset);
            if (normalizedCommission.signum() > 0) {
                commissionByAsset.merge(normalizedCommissionAsset, normalizedCommission, BigDecimal::add);
                if (commissionQuoteEquivalent == null) {
                    commissionComplete = false;
                    unconvertedCommissionAssets.add(normalizedCommissionAsset);
                } else {
                    commissionQuote = commissionQuote.add(commissionQuoteEquivalent.max(BigDecimal.ZERO));
                }
            }
            if (economicFeeQuote == null) commissionComplete = false;
            else this.economicFeeQuote = this.economicFeeQuote.add(economicFeeQuote.max(BigDecimal.ZERO));

            if ("BUY".equalsIgnoreCase(side)) {
                if (positionQty.signum() == 0) positionOpenedAtMs = tradeTimeMs;
                positionQty = positionQty.add(inventoryQuantity);
                positionCostQuote = positionCostQuote.add(quoteQuantity);
                return;
            }
            if (positionQty.signum() <= 0) {
                commissionComplete = false;
                return;
            }
            BigDecimal closedQty = inventoryQuantity.min(positionQty);
            BigDecimal averageCost = positionCostQuote.divide(positionQty, java.math.MathContext.DECIMAL64);
            BigDecimal allocatedCost = averageCost.multiply(closedQty);
            BigDecimal proceedsShare = inventoryQuantity.compareTo(closedQty) == 0 ? quoteQuantity
                    : quoteQuantity.multiply(closedQty).divide(inventoryQuantity, java.math.MathContext.DECIMAL64);
            realizedGrossPnl = realizedGrossPnl.add(proceedsShare.subtract(allocatedCost));
            positionQty = positionQty.subtract(closedQty);
            positionCostQuote = positionCostQuote.subtract(allocatedCost);
            if (positionQty.signum() == 0) {
                positionQty = BigDecimal.ZERO;
                positionCostQuote = BigDecimal.ZERO;
                positionOpenedAtMs = -1;
                roundTrips++;
            }
        }

        private DailyTradeStatsStore.DailyStatsSnapshot dailySnapshot() {
            BigDecimal costPerMillion = commissionComplete && totalVolume.signum() > 0
                    ? commissionQuote.multiply(ONE_MILLION).divide(totalVolume, java.math.MathContext.DECIMAL64)
                    : null;
            return new DailyTradeStatsStore.DailyStatsSnapshot(date, accountId, accountAlias, symbol,
                    buyVolume, sellVolume, totalVolume, commissionQuote, costPerMillion, realizedGrossPnl,
                    realizedGrossPnl.subtract(economicFeeQuote), positionQty, positionCostQuote,
                    tradeCount, roundTrips, commissionComplete);
        }

        private TradeAccountingLedger.AccountingSnapshot accountingSnapshot() {
            BigDecimal costPerMillion = commissionComplete && totalVolume.signum() > 0
                    ? commissionQuote.multiply(ONE_MILLION).divide(totalVolume, java.math.MathContext.DECIMAL64)
                    : null;
            return new TradeAccountingLedger.AccountingSnapshot(totalVolume, Map.copyOf(commissionByAsset),
                    commissionQuote, costPerMillion, commissionComplete, Set.copyOf(unconvertedCommissionAssets),
                    processedTradeIds.size());
        }

        private long positionOpenedAtMs() { return positionOpenedAtMs; }

        private String normalizeAsset(String asset) {
            return asset == null || asset.isBlank() ? "UNKNOWN" : asset.toUpperCase();
        }
    }

    public DailyTradeStatsStore.AccountVolumeSummary getAccountVolumeSummary(int days) {
        return dailyStatsStore.accountVolumeSummary(accountId, accountAlias, days);
    }
    public java.util.List<DailyTradeStatsStore.AccountSymbolVolumeSummary> getAccountSymbolVolumeSummaries(int days) {
        return dailyStatsStore.accountSymbolVolumeSummaries(accountId, accountAlias, days);
    }
    public java.util.List<DailyTradeStatsStore.DailyStatsSnapshot> getRecentDailyStats(int limit) {
        return dailyStatsStore.recentCalendar(accountId, accountAlias, properties.getStrategy().getSymbol(), limit);
    }
    public String getAccountId() { return accountId; }
    public String getApiKeyAlias() { return accountAlias; }
    public String getRiskBlockReason() { return riskGuard.getEntryBlockReason(); }
    public String getExecutionMode() { return "LIVE"; }
    public boolean isAccountStreamReady() { return accountStreamReady.getAsBoolean(); }
    public MarketDataSnapshot getMarketDataSnapshot() {
        return new MarketDataSnapshot(lastBestBid.get(), lastBestAsk.get(), lastMidPrice.get(),
                latestBidDepthPrices.get(), lastMarketDataTimestamp.get(), lastDepthDataTimestamp.get(),
                lastMarketFrameTimestamp.get());
    }

    public record MarketDataSnapshot(BigDecimal bestBid, BigDecimal bestAsk, BigDecimal midPrice,
                                     List<BigDecimal> bidPrices, long updatedAtMs, long depthUpdatedAtMs,
                                     long lastFrameAtMs) { }
    public record RemoteTodayStatusSnapshot(TradingRiskGuard.RiskSnapshot risk,
                                            TradeAccountingLedger.AccountingSnapshot accounting,
                                            DailyTradeStatsStore.DailyStatsSnapshot dailyStats,
                                            boolean remote,
                                            boolean truncated,
                                            long updatedAtMs) { }
    private record RemoteTradePage(List<JsonNode> trades, boolean truncated) { }
    private record RecoveredOpenPosition(BigDecimal quantity, BigDecimal costQuote, long openedAtMs) { }
    public record BnbBalanceSnapshot(BigDecimal quantity, BigDecimal priceUsdt, BigDecimal valueUsdt,
                                     boolean belowMinimum, long updatedAtMs) { }
    public enum DustReason {
        BELOW_MIN_QTY,
        BELOW_MIN_NOTIONAL,
        BELOW_STEP_SIZE,
        INSUFFICIENT_FREE_BALANCE,
        NONE
    }
    public record SellabilityResult(
            BigDecimal rawAvailableQty,
            BigDecimal normalizedQty,
            BigDecimal notional,
            boolean sellable,
            DustReason dustReason
    ) { }
    public record SymbolSwitchResult(boolean accepted, String symbol, String message) {
        private static SymbolSwitchResult rejected(String current, String message) {
            return new SymbolSwitchResult(false, current, message);
        }
    }
    public record StrategySwitchResult(boolean accepted, boolean applied, boolean pending, String symbol,
                                       String mode, String message) {
        private static StrategySwitchResult rejected(String symbol, String message) {
            return new StrategySwitchResult(false, false, false, symbol, "", message);
        }
    }
    private record CommissionPrice(BigDecimal price, long updatedAtMs) { }
    private record CachedCommissionRate(BigDecimal rate, long loadedAtMs) { }
}
