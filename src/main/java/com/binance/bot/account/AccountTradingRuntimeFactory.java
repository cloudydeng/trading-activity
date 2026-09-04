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
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

/** Builds fresh stateful objects for each account; only public symbol rules and durable storage are shared. */
@Component
public class AccountTradingRuntimeFactory {
    private final BinanceProperties applicationProperties;
    private final BinanceSigner signer;
    private final SymbolRuleManager ruleManager;
    private final BinanceIpRateLimitCoordinator rateLimitCoordinator;
    private final DailyTradeStatsStore dailyStatsStore;
    private final TradeNotificationService notificationService;

    public AccountTradingRuntimeFactory(BinanceProperties applicationProperties, BinanceSigner signer,
                                        SymbolRuleManager ruleManager,
                                        BinanceIpRateLimitCoordinator rateLimitCoordinator,
                                        DailyTradeStatsStore dailyStatsStore,
                                        TradeNotificationService notificationService) {
        this.applicationProperties = applicationProperties;
        this.signer = signer;
        this.ruleManager = ruleManager;
        this.rateLimitCoordinator = rateLimitCoordinator;
        this.dailyStatsStore = dailyStatsStore;
        this.notificationService = notificationService;
    }

    public AccountTradingRuntime create(AccountCredentials credentials) {
        BinanceProperties accountProperties = copyProperties();
        accountProperties.getStrategy().setOrderAmountsUsdt(credentials.orderAmountsUsdt());
        accountProperties.getStrategy().setSymbolStrategies(credentials.symbolStrategies());
        dailyStatsStore.loadActiveSymbol(credentials.accountId())
                .ifPresent(accountProperties.getStrategy()::setSymbol);

        BinanceAccountTradeClient tradeClient =
                new BinanceAccountTradeClient(accountProperties, signer, credentials, rateLimitCoordinator);
        MarketSignalEvaluator signalEvaluator = new MarketSignalEvaluator();
        TradingRiskGuard riskGuard = new TradingRiskGuard();
        String journalPath = accountObservationPath(accountProperties.getStrategy().getObservationOutputFile(),
                credentials.accountId());
        PostFillOutcomeTracker outcomeTracker = new PostFillOutcomeTracker(new ObservationJournal(journalPath));
        AtomicReference<AccountUserDataStream> streamRef = new AtomicReference<>();
        HighFrequencyVolumeChurnEngine engine = new HighFrequencyVolumeChurnEngine(
                credentials.accountId(), credentials.alias(), credentials, accountProperties, tradeClient,
                ruleManager, () -> {
                    AccountUserDataStream stream = streamRef.get();
                    return stream != null && stream.isReady();
                }, signalEvaluator, outcomeTracker, riskGuard, dailyStatsStore, notificationService);
        AccountUserDataStream stream = new AccountUserDataStream(accountProperties, signer, credentials,
                engine::onOrderUpdate, engine::handleUserStreamLoss);
        streamRef.set(stream);
        return new AccountTradingRuntime(credentials, tradeClient, stream, engine, riskGuard, outcomeTracker);
    }

    private BinanceProperties copyProperties() {
        BinanceProperties copy = new BinanceProperties();
        BeanUtils.copyProperties(applicationProperties.getApi(), copy.getApi());
        copy.getApi().setProfiles(new java.util.LinkedHashMap<>());
        BeanUtils.copyProperties(applicationProperties.getStrategy(), copy.getStrategy());
        BeanUtils.copyProperties(applicationProperties.getSecurity(), copy.getSecurity());
        BeanUtils.copyProperties(applicationProperties.getStorage(), copy.getStorage());
        return copy;
    }

    private String accountObservationPath(String configured, String accountId) {
        Path path = Path.of(configured);
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String scopedName = dot < 0
                ? name + "-" + accountId
                : name.substring(0, dot) + "-" + accountId + name.substring(dot);
        Path parent = path.getParent();
        return (parent == null ? Path.of(scopedName) : parent.resolve(scopedName)).toString();
    }
}
