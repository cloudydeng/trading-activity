package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.AccountUserDataStream;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.service.BinanceIpRateLimitCoordinator;
import com.binance.bot.service.BinanceSigner;
import com.binance.bot.strategy.DailyTradeStatsStore;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.MarketSignalEvaluator;
import com.binance.bot.strategy.ObservationJournal;
import com.binance.bot.strategy.PostFillOutcomeTracker;
import com.binance.bot.strategy.TradingRiskGuard;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** Builds fresh stateful objects for each account; only public symbol rules and durable storage are shared. */
@Component
public class AccountTradingRuntimeFactory {
    private final BinanceProperties applicationProperties;
    private final BinanceSigner signer;
    private final SymbolRuleManager ruleManager;
    private final BinanceIpRateLimitCoordinator rateLimitCoordinator;
    private final DailyTradeStatsStore dailyStatsStore;
    private final TradeNotificationService notificationService;
    private final SymbolTradeCoordinator symbolTradeCoordinator;

    public AccountTradingRuntimeFactory(BinanceProperties applicationProperties, BinanceSigner signer,
                                        SymbolRuleManager ruleManager,
                                        BinanceIpRateLimitCoordinator rateLimitCoordinator,
                                        DailyTradeStatsStore dailyStatsStore,
                                        TradeNotificationService notificationService) {
        this(applicationProperties, signer, ruleManager, rateLimitCoordinator, dailyStatsStore,
                notificationService, new SymbolTradeCoordinator());
    }

    @Autowired
    public AccountTradingRuntimeFactory(BinanceProperties applicationProperties, BinanceSigner signer,
                                        SymbolRuleManager ruleManager,
                                        BinanceIpRateLimitCoordinator rateLimitCoordinator,
                                        DailyTradeStatsStore dailyStatsStore,
                                        TradeNotificationService notificationService,
                                        SymbolTradeCoordinator symbolTradeCoordinator) {
        this.applicationProperties = applicationProperties;
        this.signer = signer;
        this.ruleManager = ruleManager;
        this.rateLimitCoordinator = rateLimitCoordinator;
        this.dailyStatsStore = dailyStatsStore;
        this.notificationService = notificationService;
        this.symbolTradeCoordinator = symbolTradeCoordinator;
    }

    public AccountTradingRuntime create(AccountCredentials credentials) {
        applyTradingRuntimeSettings();
        Map<String, BinanceProperties.SymbolStrategyProfile> strategies = new LinkedHashMap<>(
                credentials.symbolStrategies());
        Map<String, BinanceProperties.SymbolStrategyProfile> persistedStrategies =
                dailyStatsStore.loadStrategyOverrides(credentials.accountId());
        if (persistedStrategies != null) strategies.putAll(persistedStrategies);
        List<String> symbols = configuredSymbols(credentials);
        BinanceProperties streamProperties = copyProperties();
        BinanceAccountTradeClient tradeClient =
                new BinanceAccountTradeClient(streamProperties, signer, credentials, rateLimitCoordinator);
        AccountRiskCoordinator accountRiskCoordinator = new AccountRiskCoordinator();
        AtomicReference<AccountUserDataStream> streamRef = new AtomicReference<>();
        List<HighFrequencyVolumeChurnEngine> engines = new ArrayList<>();
        Map<String, TradingRiskGuard> riskGuards = new LinkedHashMap<>();
        Map<String, PostFillOutcomeTracker> outcomeTrackers = new LinkedHashMap<>();
        BooleanSupplier streamReady = () -> {
            AccountUserDataStream stream = streamRef.get();
            return stream != null && stream.isReady();
        };
        for (String symbol : symbols) {
            AccountSymbolRuntime symbolRuntime = createSymbolRuntime(
                    credentials, tradeClient, streamReady, accountRiskCoordinator, symbol, strategies);
            engines.add(symbolRuntime.engine());
            riskGuards.put(symbol, symbolRuntime.riskGuard());
            outcomeTrackers.put(symbol, symbolRuntime.outcomeTracker());
        }
        AtomicReference<AccountTradingRuntime> runtimeRef = new AtomicReference<>();
        AccountUserDataStream stream = new AccountUserDataStream(streamProperties, signer, credentials,
                update -> {
                    AccountTradingRuntime runtime = runtimeRef.get();
                    if (runtime != null) runtime.onOrderUpdate(update);
                }, reason -> {
                    AccountTradingRuntime runtime = runtimeRef.get();
                    if (runtime != null) runtime.handleUserStreamLoss(reason);
                }, () -> {
                    AccountTradingRuntime runtime = runtimeRef.get();
                    if (runtime != null) runtime.handleUserStreamReady();
                }, update -> {
                    AccountTradingRuntime runtime = runtimeRef.get();
                    notificationService.notifyOrderUpdate(runtime == null
                            ? update : runtime.enrichDashboardOpenOrder(update));
                });
        streamRef.set(stream);
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                credentials, tradeClient, stream, engines, riskGuards, outcomeTrackers, accountRiskCoordinator);
        runtimeRef.set(runtime);
        return runtime;
    }

    public AccountSymbolRuntime createSymbolRuntime(AccountCredentials credentials,
                                                    BinanceAccountTradeClient tradeClient,
                                                    AccountUserDataStream userDataStream,
                                                    AccountRiskCoordinator accountRiskCoordinator,
                                                    String symbol) {
        applyTradingRuntimeSettings();
        return createSymbolRuntime(credentials, tradeClient, userDataStream::isReady,
                accountRiskCoordinator, symbol, mergedStrategies(credentials));
    }

    private void applyTradingRuntimeSettings() {
        java.util.Optional<DailyTradeStatsStore.TradingRuntimeSettings> persisted =
                dailyStatsStore.loadTradingRuntimeSettings();
        int maxConcurrent = (persisted == null ? java.util.Optional.<DailyTradeStatsStore.TradingRuntimeSettings>empty()
                : persisted)
                .map(DailyTradeStatsStore.TradingRuntimeSettings::maxConcurrentEntriesPerSymbol)
                .orElse(applicationProperties.getStrategy().getMaxConcurrentEntriesPerSymbol());
        maxConcurrent = Math.max(1, maxConcurrent);
        applicationProperties.getStrategy().setMaxConcurrentEntriesPerSymbol(maxConcurrent);
        symbolTradeCoordinator.configureMaxConcurrentEntriesPerSymbol(maxConcurrent);
    }

    private AccountSymbolRuntime createSymbolRuntime(AccountCredentials credentials,
                                                     BinanceAccountTradeClient tradeClient,
                                                     BooleanSupplier accountStreamReady,
                                                     AccountRiskCoordinator accountRiskCoordinator,
                                                     String symbol,
                                                     Map<String, BinanceProperties.SymbolStrategyProfile> strategies) {
        BinanceProperties accountProperties = copyProperties();
        String normalizedSymbol = symbol == null ? "" : symbol.trim().toUpperCase();
        accountProperties.getStrategy().setSymbol(normalizedSymbol);
        accountProperties.getStrategy().setOrderAmountsUsdt(credentials.orderAmountsUsdt());
        accountProperties.getStrategy().setSymbolStrategies(new LinkedHashMap<>(strategies));
        MarketSignalEvaluator signalEvaluator = new MarketSignalEvaluator();
        TradingRiskGuard riskGuard = new TradingRiskGuard();
        String journalPath = accountObservationPath(
                accountProperties.getStrategy().getObservationOutputFile(), credentials.accountId(), normalizedSymbol);
        PostFillOutcomeTracker outcomeTracker = new PostFillOutcomeTracker(new ObservationJournal(journalPath));
        HighFrequencyVolumeChurnEngine engine = new HighFrequencyVolumeChurnEngine(
                credentials.accountId(), credentials.alias(), credentials, accountProperties, tradeClient,
                ruleManager, accountStreamReady, signalEvaluator, outcomeTracker, riskGuard,
                dailyStatsStore, notificationService, accountRiskCoordinator, symbolTradeCoordinator);
        return new AccountSymbolRuntime(engine, riskGuard, outcomeTracker);
    }

    private Map<String, BinanceProperties.SymbolStrategyProfile> mergedStrategies(AccountCredentials credentials) {
        Map<String, BinanceProperties.SymbolStrategyProfile> strategies = new LinkedHashMap<>(
                credentials.symbolStrategies());
        Map<String, BinanceProperties.SymbolStrategyProfile> persistedStrategies =
                dailyStatsStore.loadStrategyOverrides(credentials.accountId());
        if (persistedStrategies != null) strategies.putAll(persistedStrategies);
        return strategies;
    }

    private List<String> configuredSymbols(AccountCredentials credentials) {
        LinkedHashSet<String> symbols = new LinkedHashSet<>(
                dailyStatsStore.loadAccountSymbols(credentials.accountId()).orElse(credentials.symbols()));
        if (symbols.isEmpty()) {
            dailyStatsStore.loadActiveSymbol(credentials.accountId()).ifPresent(symbols::add);
        }
        if (symbols.isEmpty() && applicationProperties.getStrategy().getSymbol() != null) {
            symbols.add(applicationProperties.getStrategy().getSymbol().trim().toUpperCase());
        }
        if (symbols.isEmpty()) throw new IllegalArgumentException("account has no configured symbol");
        if (symbols.size() > 5) throw new IllegalArgumentException("an account supports at most 5 concurrent symbols");
        return List.copyOf(symbols);
    }

    public record AccountSymbolRuntime(HighFrequencyVolumeChurnEngine engine,
                                       TradingRiskGuard riskGuard,
                                       PostFillOutcomeTracker outcomeTracker) { }

    private BinanceProperties copyProperties() {
        BinanceProperties copy = new BinanceProperties();
        BeanUtils.copyProperties(applicationProperties.getApi(), copy.getApi());
        copy.getApi().setProfiles(new java.util.LinkedHashMap<>());
        BeanUtils.copyProperties(applicationProperties.getStrategy(), copy.getStrategy());
        BeanUtils.copyProperties(applicationProperties.getSecurity(), copy.getSecurity());
        BeanUtils.copyProperties(applicationProperties.getStorage(), copy.getStorage());
        return copy;
    }

    private String accountObservationPath(String configured, String accountId, String symbol) {
        Path path = Path.of(configured);
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String scopedName = dot < 0
                ? name + "-" + accountId + "-" + symbol.toLowerCase()
                : name.substring(0, dot) + "-" + accountId + "-" + symbol.toLowerCase() + name.substring(dot);
        Path parent = path.getParent();
        return (parent == null ? Path.of(scopedName) : parent.resolve(scopedName)).toString();
    }
}
