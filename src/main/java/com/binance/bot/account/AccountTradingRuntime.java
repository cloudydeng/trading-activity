package com.binance.bot.account;

import com.binance.bot.service.AccountUserDataStream;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.PostFillOutcomeTracker;
import com.binance.bot.strategy.TradingRiskGuard;

import java.util.concurrent.atomic.AtomicBoolean;

/** Owns every private, mutable component used by exactly one Binance account. */
public class AccountTradingRuntime {
    private final AccountCredentials credentials;
    private final BinanceAccountTradeClient tradeClient;
    private final AccountUserDataStream userDataStream;
    private final HighFrequencyVolumeChurnEngine engine;
    private final TradingRiskGuard riskGuard;
    private final PostFillOutcomeTracker outcomeTracker;
    private final AtomicBoolean initialized = new AtomicBoolean();

    public AccountTradingRuntime(AccountCredentials credentials,
                                 BinanceAccountTradeClient tradeClient,
                                 AccountUserDataStream userDataStream,
                                 HighFrequencyVolumeChurnEngine engine,
                                 TradingRiskGuard riskGuard,
                                 PostFillOutcomeTracker outcomeTracker) {
        this.credentials = credentials;
        this.tradeClient = tradeClient;
        this.userDataStream = userDataStream;
        this.engine = engine;
        this.riskGuard = riskGuard;
        this.outcomeTracker = outcomeTracker;
    }

    public synchronized void initialize() {
        if (!initialized.compareAndSet(false, true)) return;
        engine.initialize();
        userDataStream.start();
    }

    public boolean start() { return engine.startTrading(); }
    public boolean stop() { return engine.disarmLiveTrading(); }
    public boolean arm() { return engine.armLiveTrading(); }
    public boolean disarm() { return engine.disarmLiveTrading(); }

    public synchronized void shutdown() {
        if (!initialized.compareAndSet(true, false)) return;
        engine.shutdown();
        userDataStream.shutdown();
    }

    public String accountId() { return credentials.accountId(); }
    public String alias() { return credentials.alias(); }
    public BinanceAccountTradeClient tradeClient() { return tradeClient; }
    public AccountUserDataStream userDataStream() { return userDataStream; }
    public HighFrequencyVolumeChurnEngine engine() { return engine; }
    public TradingRiskGuard riskGuard() { return riskGuard; }
    public PostFillOutcomeTracker outcomeTracker() { return outcomeTracker; }
    public boolean initialized() { return initialized.get(); }
}
