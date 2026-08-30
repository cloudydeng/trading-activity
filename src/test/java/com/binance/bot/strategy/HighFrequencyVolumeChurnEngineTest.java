package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.service.BinanceOptimizedTradeService;
import com.binance.bot.service.UserDataStreamService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HighFrequencyVolumeChurnEngineTest {
    private BinanceProperties properties;
    private BinanceOptimizedTradeService tradeService;
    private SymbolRuleManager ruleManager;
    private UserDataStreamService userDataStreamService;
    private HighFrequencyVolumeChurnEngine engine;

    @BeforeEach
    void setUp() {
        properties = new BinanceProperties();
        properties.getApi().setApiKey("test-api-key");
        properties.getApi().setSecretKey("test-secret-key");
        properties.getStrategy().setExecutionMode("LIVE");
        properties.getStrategy().setLiveTradingEnabled(true);
        properties.getStrategy().setSymbol("ENSOUSDT");
        properties.getStrategy().setOrderAmountUsdt(new BigDecimal("6"));
        properties.getStrategy().setMaxLiveOrderNotionalUsdt(new BigDecimal("6"));

        tradeService = mock(BinanceOptimizedTradeService.class);
        ruleManager = mock(SymbolRuleManager.class);
        userDataStreamService = mock(UserDataStreamService.class);
        when(ruleManager.getRule("ENSOUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "ENSOUSDT", new BigDecimal("0.0001"), new BigDecimal("0.1"), new BigDecimal("5")));
        engine = new HighFrequencyVolumeChurnEngine(properties, tradeService, ruleManager, userDataStreamService,
                mock(MarketSignalEvaluator.class), mock(PostFillOutcomeTracker.class), new TradingRiskGuard());
    }

    @Test
    void refusesToStartWhenAccountStreamIsNotReady() {
        engine.getLiveArmed().set(true);
        when(userDataStreamService.isReady()).thenReturn(false);

        assertFalse(engine.startTrading());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
        assertFalse(engine.getLiveArmed().get());
        verify(tradeService, never()).getFreeAssetBalance(anyString());
    }

    @Test
    void streamLossStopsAndDisarmsWithoutAutomaticResume() {
        engine.getIsRunning().set(true);
        engine.getLiveArmed().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(77L);

        ReflectionTestUtils.invokeMethod(engine, "handleUserStreamLoss", "socket closed");

        assertFalse(engine.getIsRunning().get());
        assertFalse(engine.getLiveArmed().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
        verify(tradeService).cancelOrder("ENSOUSDT", 77L);
    }

    @Test
    void unknownTradeEventFailsClosed() {
        engine.getIsRunning().set(true);
        engine.getLiveArmed().set(true);

        orderUpdate(999L, "external-order", "BUY", "TRADE", "FILLED",
                "10", "0.60", "0", "USDT");

        assertFalse(engine.getIsRunning().get());
        assertFalse(engine.getLiveArmed().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
    }

    @Test
    void baseAssetCommissionReducesSellableInventory() {
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-1");

        orderUpdate(42L, "churn-BUY-1", "BUY", "TRADE", "PARTIALLY_FILLED",
                "10", "0.60", "0.01", "ENSO");

        assertEquals(0, new BigDecimal("9.99").compareTo(atomic("holdingInventory", BigDecimal.class).get()));
    }

    @Test
    void stopOnlyReportsSuccessAfterExchangeConfirmsNoOrderAndNoPosition() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        atomic("activeOrderId", Long.class).set(42L);
        when(tradeService.cancelOrder("ENSOUSDT", 42L)).thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L)).thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(BigDecimal.ZERO);

        assertTrue(engine.stopTrading());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
    }

    @Test
    void stopLossUsesEmergencyMarketSellInsteadOfPostOnlyMaker() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"), 1, properties.getStrategy());
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("10"));
        when(tradeService.placeMarketSell(eq("ENSOUSDT"), eq(new BigDecimal("10.0")), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine", new BigDecimal("0.59"), new BigDecimal("0.591"));

        verify(tradeService).placeMarketSell(eq("ENSOUSDT"), eq(new BigDecimal("10.0")), anyString());
        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(), anyString());
    }

    @SuppressWarnings("unchecked")
    private <T> AtomicReference<T> atomic(String field, Class<T> ignored) {
        return (AtomicReference<T>) ReflectionTestUtils.getField(engine, field);
    }

    private void orderUpdate(long orderId, String clientOrderId, String side, String executionType,
                             String status, String qty, String price, String commission, String commissionAsset) {
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", orderId, clientOrderId, side, executionType,
                status, new BigDecimal(qty), new BigDecimal(price), new BigDecimal(commission), commissionAsset);
    }
}
