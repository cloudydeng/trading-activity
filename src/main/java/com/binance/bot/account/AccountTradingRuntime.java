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

/** Owns one Binance account session and one independently stateful engine per configured symbol. */
public class AccountTradingRuntime {
    private final AccountCredentials credentials;
    private final BinanceAccountTradeClient tradeClient;
    private final AccountUserDataStream userDataStream;
    private final List<HighFrequencyVolumeChurnEngine> engines;
    private final Map<String, TradingRiskGuard> riskGuards;
    private final Map<String, PostFillOutcomeTracker> outcomeTrackers;
    private final AccountRiskCoordinator accountRiskCoordinator;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicLong lastDashboardOpenOrderSnapshotAtMs = new AtomicLong();

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
        this.engines = List.copyOf(engines);
        this.riskGuards = Map.copyOf(riskGuards == null ? Map.of() : new LinkedHashMap<>(riskGuards));
        this.outcomeTrackers = Map.copyOf(outcomeTrackers == null ? Map.of() : new LinkedHashMap<>(outcomeTrackers));
        this.accountRiskCoordinator = accountRiskCoordinator;
    }

    public synchronized void initialize() {
        if (!initialized.compareAndSet(false, true)) return;
        try {
            engines.forEach(HighFrequencyVolumeChurnEngine::initialize);
            userDataStream.start();
        } catch (RuntimeException e) {
            for (HighFrequencyVolumeChurnEngine engine : engines) {
                try { engine.shutdown(); } catch (RuntimeException cleanupFailure) { e.addSuppressed(cleanupFailure); }
            }
            try { userDataStream.shutdown(); } catch (RuntimeException cleanupFailure) { e.addSuppressed(cleanupFailure); }
            initialized.set(false);
            throw e;
        }
    }

    public boolean start() {
        if (engines.size() > 1) return false;
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
        for (HighFrequencyVolumeChurnEngine engine : engines) {
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
            engines.stream().filter(value -> value.getIsRunning().get()).forEach(value ->
                    value.handleUserStreamLoss("收到未配置交易对 " + update.symbol() + " 的账户成交事件"));
        }
    }

    public void handleUserStreamLoss(String reason) {
        engines.forEach(engine -> engine.handleUserStreamLoss(reason));
    }

    public void handleUserStreamReady() {
        boolean snapshotRequired = false;
        for (HighFrequencyVolumeChurnEngine engine : engines) {
            snapshotRequired |= engine.handleUserStreamReady(false);
        }
        if (snapshotRequired) refreshDashboardOpenOrderSnapshot(true);
    }

    public synchronized void shutdown() {
        if (!initialized.get()) return;
        RuntimeException failure = null;
        try {
            for (HighFrequencyVolumeChurnEngine engine : engines) {
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
    }

    public String accountId() { return credentials.accountId(); }
    public String alias() { return credentials.alias(); }
    public BinanceAccountTradeClient tradeClient() { return tradeClient; }
    public AccountUserDataStream userDataStream() { return userDataStream; }
    public HighFrequencyVolumeChurnEngine engine() { return engines.get(0); }
    public Optional<HighFrequencyVolumeChurnEngine> engine(String symbol) {
        if (symbol == null || symbol.isBlank()) return Optional.of(engine());
        return engines.stream().filter(value -> symbol.equalsIgnoreCase(value.getSymbol())).findFirst();
    }
    public List<HighFrequencyVolumeChurnEngine> engines() { return engines; }
    public TradingRiskGuard riskGuard() { return riskGuards.getOrDefault(engine().getSymbol(), engine().getRiskGuard()); }
    public Optional<TradingRiskGuard> riskGuard(String symbol) {
        if (symbol == null) return Optional.empty();
        return riskGuards.entrySet().stream().filter(entry -> symbol.equalsIgnoreCase(entry.getKey()))
                .map(Map.Entry::getValue).findFirst();
    }
    public PostFillOutcomeTracker outcomeTracker() { return outcomeTrackers.get(engine().getSymbol()); }
    public int symbolCount() { return engines.size(); }
    public boolean initialized() { return initialized.get(); }
    public boolean canChangeConfiguredSymbols() {
        return engines.stream().allMatch(engine -> !engine.getIsRunning().get() && !engine.hasActiveOrder());
    }

    private boolean reconcileAccountRisk(HighFrequencyVolumeChurnEngine target) {
        var accountInfo = tradeClient.getAccountInfo();
        if (accountInfo == null) {
            target.markAccountRiskUnconfirmed("无法读取账户余额，拒绝启动");
            return false;
        }
        boolean reconciled = true;
        for (HighFrequencyVolumeChurnEngine engine : engines) {
            reconciled &= engine.reconcileAccountRiskSnapshot(accountInfo);
        }
        if (accountRiskCoordinator != null) {
            reconciled &= accountRiskCoordinator.updateQuoteBalance(accountInfo, "USDT");
        }
        if (!reconciled) target.markAccountRiskUnconfirmed("账户其他币种持仓与本地账本不一致，拒绝启动");
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
        return symbol == null || symbol.isBlank() ? "DEFAULT" : symbol.toUpperCase();
    }
}
