package com.binance.bot.account;

import com.binance.bot.service.AccountUserDataStream;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.strategy.PostFillOutcomeTracker;
import com.binance.bot.strategy.TradingRiskGuard;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class AccountTradingRuntimeTest {
    @Test
    void runtimesStartConcurrentlyAndStoppingOneDoesNotStopTheOther() {
        HighFrequencyVolumeChurnEngine engineA = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine engineB = mock(HighFrequencyVolumeChurnEngine.class);
        when(engineA.startTrading()).thenReturn(true);
        when(engineB.startTrading()).thenReturn(true);
        when(engineA.stopTrading()).thenReturn(true);
        AccountTradingRuntime accountA = runtime("account-a", engineA);
        AccountTradingRuntime accountB = runtime("account-b", engineB);

        CompletableFuture<Boolean> startA = CompletableFuture.supplyAsync(accountA::start);
        CompletableFuture<Boolean> startB = CompletableFuture.supplyAsync(accountB::start);
        assertTrue(startA.join());
        assertTrue(startB.join());
        assertTrue(accountA.stop());

        verify(engineA).startTrading();
        verify(engineB).startTrading();
        verify(engineA).refreshDashboardOpenOrderSnapshot();
        verify(engineB).refreshDashboardOpenOrderSnapshot();
        verify(engineA).stopTrading();
        verify(engineB, never()).stopTrading();
    }

    @Test
    void shutdownAlwaysClosesUserStreamWhenEngineCleanupFails() {
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        AccountUserDataStream stream = mock(AccountUserDataStream.class);
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"),
                mock(BinanceAccountTradeClient.class), stream, engine,
                mock(TradingRiskGuard.class), mock(PostFillOutcomeTracker.class));
        runtime.initialize();
        doThrow(new IllegalStateException("engine cleanup failed")).when(engine).shutdown();

        assertThrows(IllegalStateException.class, runtime::shutdown);

        verify(stream).shutdown();
        assertFalse(runtime.initialized());
    }

    @Test
    void oneAccountRoutesEventsAndControlsEachSymbolIndependently() {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.startTrading()).thenReturn(true);
        when(btc.stopTrading()).thenReturn(true);
        AccountUserDataStream stream = mock(AccountUserDataStream.class);
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret", Map.of(), Map.of(),
                        List.of("ENSOUSDT", "BTCUSDT")),
                mock(BinanceAccountTradeClient.class), stream, List.of(enso, btc), Map.of(), Map.of());
        AccountExecutionEvent btcFill = new AccountExecutionEvent(
                "account-a", "BTCUSDT", 42, 7, "ta-btc-B-1", "BUY", "TRADE", "FILLED",
                BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.TEN,
                BigDecimal.ZERO, "USDT", true, 123);

        runtime.initialize();
        runtime.onOrderUpdate(btcFill);

        assertTrue(runtime.start("ENSOUSDT"));
        assertTrue(runtime.stop("BTCUSDT"));
        verify(btc).onOrderUpdate(btcFill);
        verify(enso, never()).onOrderUpdate(any());
        verify(enso).startTrading();
        verify(enso).refreshDashboardOpenOrderSnapshot();
        verify(btc).stopTrading();
        verify(stream).start();
    }

    @Test
    void accountLevelStopStopsEveryConfiguredSymbol() {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.stopTrading()).thenReturn(true);
        when(btc.stopTrading()).thenReturn(true);
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class),
                List.of(enso, btc), Map.of(), Map.of());

        assertTrue(runtime.stop());

        verify(enso).stopTrading();
        verify(btc).stopTrading();
    }

    @Test
    void legacyAccountLevelStartRejectsAmbiguousMultiSymbolRuntime() {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class),
                List.of(enso, btc), Map.of(), Map.of());

        assertFalse(runtime.start());

        verify(enso, never()).startTrading();
        verify(btc, never()).startTrading();
    }

    @Test
    void symbolStartReconcilesEveryConfiguredAssetFromOneAccountSnapshot() throws Exception {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.reconcileAccountRiskSnapshot(any())).thenReturn(true);
        when(btc.reconcileAccountRiskSnapshot(any())).thenReturn(true);
        when(enso.startTrading()).thenReturn(true);
        BinanceAccountTradeClient client = mock(BinanceAccountTradeClient.class);
        when(client.getAccountInfo()).thenReturn(new ObjectMapper().readTree(
                "{\"balances\":[{\"asset\":\"USDT\",\"free\":\"30\",\"locked\":\"0\"}]}"));
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"), client,
                mock(AccountUserDataStream.class), List.of(enso, btc), Map.of(), Map.of(),
                new AccountRiskCoordinator());

        assertTrue(runtime.start("ENSOUSDT"));

        verify(client, times(1)).getAccountInfo();
        verify(enso).reconcileAccountRiskSnapshot(any());
        verify(btc).reconcileAccountRiskSnapshot(any());
        verify(enso).startTrading();
    }

    @Test
    void symbolStartFailsClosedWhenAnotherConfiguredAssetDoesNotReconcile() throws Exception {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.reconcileAccountRiskSnapshot(any())).thenReturn(true);
        when(btc.reconcileAccountRiskSnapshot(any())).thenReturn(false);
        BinanceAccountTradeClient client = mock(BinanceAccountTradeClient.class);
        when(client.getAccountInfo()).thenReturn(new ObjectMapper().readTree(
                "{\"balances\":[{\"asset\":\"USDT\",\"free\":\"30\",\"locked\":\"0\"}]}"));
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"), client,
                mock(AccountUserDataStream.class), List.of(enso, btc), Map.of(), Map.of(),
                new AccountRiskCoordinator());

        assertFalse(runtime.start("ENSOUSDT"));

        verify(enso, never()).startTrading();
        verify(enso).markAccountRiskUnconfirmed(anyString());
    }

    @Test
    void symbolStartRecoversAnotherConfiguredAssetBeforeFailingAccountRisk() throws Exception {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.reconcileAccountRiskSnapshot(any())).thenReturn(true);
        when(btc.reconcileAccountRiskSnapshot(any())).thenReturn(false);
        when(btc.recoverAccountRiskSnapshotForStart(any())).thenReturn(true);
        when(enso.startTrading()).thenReturn(true);
        BinanceAccountTradeClient client = mock(BinanceAccountTradeClient.class);
        when(client.getAccountInfo()).thenReturn(new ObjectMapper().readTree(
                "{\"balances\":[{\"asset\":\"USDT\",\"free\":\"30\",\"locked\":\"0\"}]}"));
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"), client,
                mock(AccountUserDataStream.class), List.of(enso, btc), Map.of(), Map.of(),
                new AccountRiskCoordinator());

        assertTrue(runtime.start("ENSOUSDT"));

        verify(client, times(1)).getAccountInfo();
        verify(btc).recoverAccountRiskSnapshotForStart(any());
        verify(enso).startTrading();
        verify(enso, never()).markAccountRiskUnconfirmed(anyString());
    }

    @Test
    void sharedAccountStreamLifecycleIsAppliedToEverySymbolButSnapshotRunsOnce() {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.handleUserStreamReady(false)).thenReturn(true);
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class),
                List.of(enso, btc), Map.of(), Map.of());

        runtime.handleUserStreamLoss("socket lost");
        runtime.handleUserStreamReady();

        verify(enso).handleUserStreamLoss("socket lost");
        verify(btc).handleUserStreamLoss("socket lost");
        verify(enso).handleUserStreamReady(false);
        verify(btc).handleUserStreamReady(false);
        verify(enso).refreshDashboardOpenOrderSnapshot();
        verify(btc, never()).refreshDashboardOpenOrderSnapshot();
    }

    @Test
    void initialAccountStreamReadyDoesNotQueryOrdersWhileEverySymbolIsStopped() {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class),
                List.of(enso, btc), Map.of(), Map.of());

        runtime.handleUserStreamReady();

        verify(enso).handleUserStreamReady(false);
        verify(btc).handleUserStreamReady(false);
        verify(enso, never()).refreshDashboardOpenOrderSnapshot();
        verify(btc, never()).refreshDashboardOpenOrderSnapshot();
    }

    @Test
    void configuredSymbolsCanChangeOnlyWhenEveryEngineIsStoppedAndOrderFree() {
        HighFrequencyVolumeChurnEngine enso = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(enso.getSymbol()).thenReturn("ENSOUSDT");
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(enso.getIsRunning()).thenReturn(new java.util.concurrent.atomic.AtomicBoolean(false));
        when(btc.getIsRunning()).thenReturn(new java.util.concurrent.atomic.AtomicBoolean(false));
        AccountTradingRuntime runtime = new AccountTradingRuntime(
                new AccountCredentials("account-a", "A", "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class),
                List.of(enso, btc), Map.of(), Map.of());

        assertTrue(runtime.canChangeConfiguredSymbols());
        when(btc.hasActiveOrder()).thenReturn(true);
        assertFalse(runtime.canChangeConfiguredSymbols());
        when(btc.hasActiveOrder()).thenReturn(false);
        when(enso.getIsRunning()).thenReturn(new java.util.concurrent.atomic.AtomicBoolean(true));
        assertFalse(runtime.canChangeConfiguredSymbols());
    }

    private AccountTradingRuntime runtime(String accountId, HighFrequencyVolumeChurnEngine engine) {
        return new AccountTradingRuntime(new AccountCredentials(accountId, accountId, "key", "secret"),
                mock(BinanceAccountTradeClient.class), mock(AccountUserDataStream.class), engine,
                mock(TradingRiskGuard.class), mock(PostFillOutcomeTracker.class));
    }
}
