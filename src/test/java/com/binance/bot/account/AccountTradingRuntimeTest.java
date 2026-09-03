package com.binance.bot.account;

import com.binance.bot.service.AccountUserDataStream;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.PostFillOutcomeTracker;
import com.binance.bot.strategy.TradingRiskGuard;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AccountTradingRuntimeTest {
    @Test
    void runtimesStartConcurrentlyAndStoppingOneDoesNotStopTheOther() {
        HighFrequencyVolumeChurnEngine engineA = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine engineB = mock(HighFrequencyVolumeChurnEngine.class);
        when(engineA.startTrading()).thenReturn(true);
        when(engineB.startTrading()).thenReturn(true);
        when(engineA.disarmLiveTrading()).thenReturn(true);
        AccountTradingRuntime accountA = runtime("account-a", engineA);
        AccountTradingRuntime accountB = runtime("account-b", engineB);

        CompletableFuture<Boolean> startA = CompletableFuture.supplyAsync(accountA::start);
        CompletableFuture<Boolean> startB = CompletableFuture.supplyAsync(accountB::start);
        assertTrue(startA.join());
        assertTrue(startB.join());
        assertTrue(accountA.stop());

        verify(engineA).startTrading();
        verify(engineB).startTrading();
        verify(engineA).disarmLiveTrading();
        verify(engineB, never()).disarmLiveTrading();
    }

    private AccountTradingRuntime runtime(String accountId, HighFrequencyVolumeChurnEngine engine) {
        return new AccountTradingRuntime(new AccountCredentials(accountId, accountId, "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class), engine,
                mock(TradingRiskGuard.class), mock(PostFillOutcomeTracker.class));
    }
}
