package com.binance.bot.account;

import com.binance.bot.service.AccountUserDataStream;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.PostFillOutcomeTracker;
import com.binance.bot.strategy.TradingRiskGuard;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/** Owns one Binance account session and one independently stateful engine per configured symbol. */
public class AccountTradingRuntime {
    private final AccountCredentials credentials;
    private final BinanceAccountTradeClient tradeClient;
    private final AccountUserDataStream userDataStream;
    private final LinkedHashMap<String, HighFrequencyVolumeChurnEngine> engines = new LinkedHashMap<>();
    private final LinkedHashMap<String, TradingRiskGuard> riskGuards = new LinkedHashMap<>();
    private final LinkedHashMap<String, PostFillOutcomeTracker> outcomeTrackers = new LinkedHashMap<>();
    private final AccountRiskCoordinator accountRiskCoordinator;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicLong lastDashboardOpenOrderSnapshotAtMs = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();

    public AccountTradingRuntime(AccountCredentials credentials,
                                 BinanceAccountTradeClient tradeClient,
                                 AccountUserDataStream userDataStream,
                                 HighFrequencyVolumeChurnEngine engine,
                                 TradingRiskGuard riskGuard,
                                 PostFillOutcomeTracker outcomeTracker) {
        this(credentials, tradeClient, userDataStream, List.of(engine),
                Map.of(safeEngineSymbol(engine), riskGuard), Map.of(safeEngineSymbol(engine), outcomeTracker), null);
    }

    public AccountTradingRuntime(AccountCredentials credentials,
                                 BinanceAccountTradeClient tradeClient,
                                 AccountUserDataStream userDataStream,
                                 Collection<HighFrequencyVolumeChurnEngine> engines,
                                 Map<String, TradingRiskGuard> riskGuards,
                                 Map<String, PostFillOutcomeTracker> outcomeTrackers) {
        this(credentials, tradeClient, userDataStream, engines, riskGuards, outcomeTrackers, null);
    }

    public AccountTradingRuntime(AccountCredentials credentials,
                                 BinanceAccountTradeClient tradeClient,
                                 AccountUserDataStream userDataStream,
                                 Collection<HighFrequencyVolumeChurnEngine> engines,
                                 Map<String, TradingRiskGuard> riskGuards,
                                 Map<String, PostFillOutcomeTracker> outcomeTrackers,
                                 AccountRiskCoordinator accountRiskCoordinator) {
        this.credentials = credentials;
        this.tradeClient = tradeClient;
        this.userDataStream = userDataStream;
        if (engines == null || engines.isEmpty()) throw new IllegalArgumentException("at least one symbol engine is required");
        for (HighFrequencyVolumeChurnEngine engine : engines) {
            this.engines.put(safeEngineSymbol(engine), engine);
        }
        if (riskGuards != null) {
            riskGuards.forEach((symbol, riskGuard) -> {
                if (riskGuard != null) this.riskGuards.put(normalizeSymbol(symbol), riskGuard);
            });
        }
        if (outcomeTrackers != null) {
            outcomeTrackers.forEach((symbol, tracker) -> {
                if (tracker != null) this.outcomeTrackers.put(normalizeSymbol(symbol), tracker);
            });
        }
        this.accountRiskCoordinator = accountRiskCoordinator;
    }

    public void initialize() {
        lock.lock();
        try {
            if (!initialized.compareAndSet(false, true)) return;
            try {
                engines.values().forEach(HighFrequencyVolumeChurnEngine::initialize);
                userDataStream.start();
            } catch (RuntimeException e) {
                for (HighFrequencyVolumeChurnEngine engine : engines.values()) {
                    try { engine.shutdown(); } catch (RuntimeException cleanupFailure) { e.addSuppressed(cleanupFailure); }
                }
                try { userDataStream.shutdown(); } catch (RuntimeException cleanupFailure) { e.addSuppressed(cleanupFailure); }
                initialized.set(false);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean start() {
        if (symbolCount() > 1) return false;
        return start(engine().getSymbol());
    }

    public boolean start(String symbol) {
        HighFrequencyVolumeChurnEngine engine = engine(symbol).orElse(null);
        if (engine == null) return false;
        if (accountRiskCoordinator != null && !reconcileAccountRisk(engine)) return false;
        boolean started = engine.startTrading();
        if (started) refreshDashboardOpenOrderSnapshot(false);
        return started;
    }

    public boolean stop() {
        boolean stopped = true;
        for (HighFrequencyVolumeChurnEngine engine : engines()) {
            try {
                stopped &= engine.stopTrading();
            } catch (RuntimeException ignored) {
                stopped = false;
            }
        }
        return stopped;
    }

    public boolean stop(String symbol) {
        return engine(symbol).map(HighFrequencyVolumeChurnEngine::stopTrading).orElse(false);
    }

    public void onOrderUpdate(AccountExecutionEvent update) {
        Optional<HighFrequencyVolumeChurnEngine> target = engine(update.symbol());
        if (target.isPresent()) {
            target.get().onOrderUpdate(update);
            return;
        }
        if ("TRADE".equalsIgnoreCase(update.executionType()) && update.lastExecutedQty().signum() > 0) {
            engines().stream().filter(value -> value.getIsRunning().get()).forEach(value ->
                    value.handleUserStreamLoss("收到未配置交易对 " + update.symbol() + " 的账户成交事件"));
        }
    }

    public void handleUserStreamLoss(String reason) {
        engines().forEach(engine -> engine.handleUserStreamLoss(reason));
    }

    public void handleUserStreamReady() {
        boolean snapshotRequired = false;
        for (HighFrequencyVolumeChurnEngine engine : engines()) {
            snapshotRequired |= engine.handleUserStreamReady(false);
        }
        if (snapshotRequired) refreshDashboardOpenOrderSnapshot(true);
    }

    public void shutdown() {
        lock.lock();
        try {
            if (!initialized.get()) return;
            RuntimeException failure = null;
            try {
                for (HighFrequencyVolumeChurnEngine engine : engines()) {
                    try { engine.shutdown(); }
                    catch (RuntimeException e) { if (failure == null) failure = e; }
                }
            } finally {
                try {
                    userDataStream.shutdown();
                } finally {
                    initialized.set(false);
                }
            }
            if (failure != null) throw failure;
        } finally {
            lock.unlock();
        }
    }

    public String accountId() { return credentials.accountId(); }
    public String alias() { return credentials.alias(); }
    public AccountCredentials credentials() { return credentials; }
    public BinanceAccountTradeClient tradeClient() { return tradeClient; }
    public AccountUserDataStream userDataStream() { return userDataStream; }
    public AccountRiskCoordinator accountRiskCoordinator() { return accountRiskCoordinator; }
    public HighFrequencyVolumeChurnEngine engine() {
        lock.lock();
        try {
            return engines.values().iterator().next();
        } finally {
            lock.unlock();
        }
    }
    public Optional<HighFrequencyVolumeChurnEngine> engine(String symbol) {
        lock.lock();
        try {
            if (symbol == null || symbol.isBlank()) return Optional.of(engine());
            return Optional.ofNullable(engines.get(normalizeSymbol(symbol)));
        } finally {
            lock.unlock();
        }
    }
    public List<HighFrequencyVolumeChurnEngine> engines() {
        lock.lock();
        try {
            return List.copyOf(engines.values());
        } finally {
            lock.unlock();
        }
    }
    public TradingRiskGuard riskGuard() { return riskGuards.getOrDefault(engine().getSymbol(), engine().getRiskGuard()); }
    public Optional<TradingRiskGuard> riskGuard(String symbol) {
        lock.lock();
        try {
            if (symbol == null) return Optional.empty();
            return Optional.ofNullable(riskGuards.get(normalizeSymbol(symbol)));
        } finally {
            lock.unlock();
        }
    }
    public PostFillOutcomeTracker outcomeTracker() { return outcomeTrackers.get(engine().getSymbol()); }
    public int symbolCount() {
        lock.lock();
        try {
            return engines.size();
        } finally {
            lock.unlock();
        }
    }
    public boolean initialized() { return initialized.get(); }
    public boolean canChangeConfiguredSymbols() {
        lock.lock();
        try {
            return engines.values().stream().allMatch(engine -> !engine.getIsRunning().get() && !engine.hasActiveOrder());
        } finally {
            lock.unlock();
        }
    }

    public ApplySymbolsResult applyConfiguredSymbols(
            List<String> targetSymbols,
            Map<String, AccountTradingRuntimeFactory.AccountSymbolRuntime> additions) {
        lock.lock();
        try {
            List<String> normalizedTargets = normalizeSymbols(targetSymbols);
            if (normalizedTargets.isEmpty()) return new ApplySymbolsResult(false, "至少需要保留 1 个交易对");
            if (normalizedTargets.size() > 5) return new ApplySymbolsResult(false, "一个账户最多支持 5 个交易对");
            if (!canChangeConfiguredSymbols()) {
                return new ApplySymbolsResult(false, "请先停止该账户的全部币种，并确认没有活动订单");
            }
            Map<String, AccountTradingRuntimeFactory.AccountSymbolRuntime> normalizedAdditions = new LinkedHashMap<>();
            if (additions != null) {
                additions.forEach((symbol, addition) -> {
                    if (addition != null) normalizedAdditions.put(normalizeSymbol(symbol), addition);
                });
            }
            for (String symbol : normalizedTargets) {
                if (!engines.containsKey(symbol) && !normalizedAdditions.containsKey(symbol)) {
                    return new ApplySymbolsResult(false, "缺少新增交易对运行实例: " + symbol);
                }
            }
            for (String symbol : List.copyOf(engines.keySet())) {
                if (!normalizedTargets.contains(symbol)) {
                    HighFrequencyVolumeChurnEngine removed = engines.get(symbol);
                    if (removed != null) removed.shutdown();
                }
            }
            LinkedHashMap<String, HighFrequencyVolumeChurnEngine> nextEngines = new LinkedHashMap<>();
            LinkedHashMap<String, TradingRiskGuard> nextRiskGuards = new LinkedHashMap<>();
            LinkedHashMap<String, PostFillOutcomeTracker> nextOutcomeTrackers = new LinkedHashMap<>();
            for (String symbol : normalizedTargets) {
                AccountTradingRuntimeFactory.AccountSymbolRuntime addition = normalizedAdditions.get(symbol);
                if (addition != null) {
                    nextEngines.put(symbol, addition.engine());
                    nextRiskGuards.put(symbol, addition.riskGuard());
                    nextOutcomeTrackers.put(symbol, addition.outcomeTracker());
                } else {
                    nextEngines.put(symbol, engines.get(symbol));
                    TradingRiskGuard riskGuard = riskGuards.get(symbol);
                    if (riskGuard != null) nextRiskGuards.put(symbol, riskGuard);
                    PostFillOutcomeTracker tracker = outcomeTrackers.get(symbol);
                    if (tracker != null) nextOutcomeTrackers.put(symbol, tracker);
                }
            }
            engines.clear();
            engines.putAll(nextEngines);
            riskGuards.clear();
            riskGuards.putAll(nextRiskGuards);
            outcomeTrackers.clear();
            outcomeTrackers.putAll(nextOutcomeTrackers);
            return new ApplySymbolsResult(true, "交易对配置已热应用");
        } finally {
            lock.unlock();
        }
    }

    private boolean reconcileAccountRisk(HighFrequencyVolumeChurnEngine target) {
        var accountInfo = tradeClient.getAccountInfo();
        if (accountInfo == null) {
            target.markAccountRiskUnconfirmed("无法读取账户余额，拒绝启动");
            return false;
        }
        boolean reconciled = true;
        for (HighFrequencyVolumeChurnEngine engine : engines()) {
            boolean engineReconciled = engine.reconcileAccountRiskSnapshot(accountInfo);
            if (!engineReconciled) {
                engineReconciled = engine.recoverAccountRiskSnapshotForStart(accountInfo);
            }
            reconciled &= engineReconciled;
        }
        if (accountRiskCoordinator != null) {
            reconciled &= accountRiskCoordinator.updateQuoteBalance(accountInfo, "USDT");
        }
        if (!reconciled) {
            target.markAccountRiskUnconfirmed("账户币种持仓无法从交易所历史成交安全恢复，拒绝启动");
        }
        return reconciled;
    }

    private void refreshDashboardOpenOrderSnapshot(boolean force) {
        long now = System.currentTimeMillis();
        long previous = lastDashboardOpenOrderSnapshotAtMs.get();
        if (!force && previous > 0 && now - previous < 1_000) return;
        if (!force && !lastDashboardOpenOrderSnapshotAtMs.compareAndSet(previous, now)) return;
        if (force) lastDashboardOpenOrderSnapshotAtMs.set(now);
        engine().refreshDashboardOpenOrderSnapshot();
    }

    private static String safeEngineSymbol(HighFrequencyVolumeChurnEngine engine) {
        String symbol = engine == null ? null : engine.getSymbol();
        return normalizeSymbol(symbol);
    }

    private static List<String> normalizeSymbols(List<String> symbols) {
        if (symbols == null) return List.of();
        return symbols.stream().map(AccountTradingRuntime::normalizeSymbol)
                .filter(symbol -> !symbol.isBlank()).distinct().toList();
    }

    private static String normalizeSymbol(String symbol) {
        return symbol == null || symbol.isBlank() ? "" : symbol.trim().toUpperCase();
    }

    public record ApplySymbolsResult(boolean applied, String message) { }
}
