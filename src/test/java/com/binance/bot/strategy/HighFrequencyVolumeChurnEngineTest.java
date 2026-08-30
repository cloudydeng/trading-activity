package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.service.BinanceOptimizedTradeService;
import com.binance.bot.service.UserDataStreamService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.net.http.WebSocket;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HighFrequencyVolumeChurnEngineTest {
    private BinanceProperties properties;
    private BinanceOptimizedTradeService tradeService;
    private SymbolRuleManager ruleManager;
    private UserDataStreamService userDataStreamService;
    private MarketSignalEvaluator marketSignalEvaluator;
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
        properties.getStrategy().setOrderTtlMs(1_200);
        properties.getStrategy().setMinEntryOrderRestMs(800);
        properties.getStrategy().setMakerEntryFallbackMs(2_000);
        properties.getStrategy().setEntryIocMaxSlippageTicks(1);

        tradeService = mock(BinanceOptimizedTradeService.class);
        ruleManager = mock(SymbolRuleManager.class);
        userDataStreamService = mock(UserDataStreamService.class);
        marketSignalEvaluator = mock(MarketSignalEvaluator.class);
        when(ruleManager.getRule("ENSOUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "ENSOUSDT", new BigDecimal("0.0001"), new BigDecimal("0.1"), new BigDecimal("5")));
        engine = new HighFrequencyVolumeChurnEngine(properties, tradeService, ruleManager, userDataStreamService,
                marketSignalEvaluator, mock(PostFillOutcomeTracker.class), new TradingRiskGuard());
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

    @Test
    void operatorLiquidationSellsVerifiedFreeBalanceWithoutStartingEntryEngine() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(userDataStreamService.isReady()).thenReturn(true);
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("7.08"));
        when(tradeService.placeMarketSell(eq("ENSOUSDT"), decimalEquals("7.0"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":90}"));

        HighFrequencyVolumeChurnEngine.LiquidationResult result = engine.liquidateExistingPosition();

        assertTrue(result.accepted());
        assertEquals(90L, result.orderId());
        assertEquals(0, new BigDecimal("7.0").compareTo(result.quantity()));
        assertFalse(engine.getIsRunning().get());
        assertFalse(engine.getLiveArmed().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        verify(tradeService).placeMarketSell(eq("ENSOUSDT"), decimalEquals("7.0"), anyString());
    }

    @Test
    void restFallbackRepairsMissedMarketSellExecutionReport() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("7.08"));
        ReflectionTestUtils.invokeMethod(engine, "trackOrder", 90L, "churn-SELLM-1",
                HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        when(tradeService.getOrder("ENSOUSDT", 90L)).thenReturn(mapper.readTree("""
                {"orderId":90,"status":"FILLED","side":"SELL","executedQty":"7.08",
                 "cummulativeQuoteQty":"6.018","price":"0"}
                """));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(BigDecimal.ZERO);

        ReflectionTestUtils.invokeMethod(engine, "reconcileTrackedOrder", 90L);

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("REST 对账"));
    }

    @Test
    void restFallbackRepairsMissedBuyExecutionReportAndStartsExitManagement() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        atomic("targetEntryQuantity", BigDecimal.class).set(new BigDecimal("7.08"));
        ReflectionTestUtils.invokeMethod(engine, "trackOrder", 91L, "churn-BUYI-1",
                HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        when(tradeService.getOrder("ENSOUSDT", 91L)).thenReturn(mapper.readTree("""
                {"orderId":91,"status":"FILLED","side":"BUY","executedQty":"7.08",
                 "cummulativeQuoteQty":"6.018","price":"0"}
                """));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("7.08"));

        ReflectionTestUtils.invokeMethod(engine, "reconcileTrackedOrder", 91L);

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
        assertEquals(0, new BigDecimal("7.08").compareTo(engine.getRiskSnapshot().positionQty()));

        orderUpdate(91L, "churn-BUYI-1", "BUY", "TRADE", "FILLED",
                "7.08", "0.85", "0.006", "USDT");
        assertEquals(0, new BigDecimal("7.08").compareTo(engine.getRiskSnapshot().positionQty()));
    }

    @Test
    void staleMarketWatchdogAbortsHalfOpenSocket() {
        WebSocket socket = mock(WebSocket.class);
        atomic("activeMarketWebSocket", WebSocket.class).set(socket);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(System.currentTimeMillis() - 6_000);
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "reconnectScheduled"))
                .set(true);

        ReflectionTestUtils.invokeMethod(engine, "checkMarketStreamHealth");

        verify(socket).abort();
        assertNull(atomic("activeMarketWebSocket", WebSocket.class).get());
    }

    @Test
    void staleMarketOnStartReconnectsWithoutDiscardingOperatorArm() {
        WebSocket socket = mock(WebSocket.class);
        engine.getLiveArmed().set(true);
        when(userDataStreamService.isReady()).thenReturn(true);
        atomic("activeMarketWebSocket", WebSocket.class).set(socket);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(System.currentTimeMillis() - 6_000);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(System.currentTimeMillis() - 6_000);
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "reconnectScheduled"))
                .set(true);

        assertFalse(engine.startTrading());

        verify(socket).abort();
        assertTrue(engine.getLiveArmed().get());
        assertTrue(engine.getStatusReason().get().contains("正在重连"));
    }

    @Test
    void classifiesOnlyImmediateSafetyConditionsAsHardEntryRisk() {
        assertTrue(HighFrequencyVolumeChurnEngine.isHardEntryRisk("SELL_TAKER_PRESSURE"));
        assertTrue(HighFrequencyVolumeChurnEngine.isHardEntryRisk("SHORT_TERM_DOWNMOVE"));
        assertTrue(HighFrequencyVolumeChurnEngine.isHardEntryRisk("STALE_MARKET_DATA"));
        assertFalse(HighFrequencyVolumeChurnEngine.isHardEntryRisk("WEAK_TOP_OF_BOOK"));
        assertFalse(HighFrequencyVolumeChurnEngine.isHardEntryRisk("WEAK_MULTI_LEVEL_BIDS"));
        assertFalse(HighFrequencyVolumeChurnEngine.isHardEntryRisk("MICROPRICE_NOT_SUPPORTIVE"));
    }

    @Test
    void initialMakerBuyIsPlacedAtBestBid() throws Exception {
        engine.getIsRunning().set(true);
        when(marketSignalEvaluator.evaluate(any(Long.class), eq(properties.getStrategy())))
                .thenReturn(MarketSignalEvaluator.EntryDecision.allow(
                        new BigDecimal("0.2"), new BigDecimal("0.1"), new BigDecimal("0.1"),
                        BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":101}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine", new BigDecimal("0.862"), new BigDecimal("0.863"));

        ArgumentCaptor<BigDecimal> price = ArgumentCaptor.forClass(BigDecimal.class);
        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), price.capture(), any(), isNull(), anyString());
        assertEquals(0, new BigDecimal("0.862").compareTo(price.getValue()));
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void softSignalNoiseDoesNotImmediatelyCancelFreshBuy() {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-soft");
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis());
        when(marketSignalEvaluator.evaluate(any(Long.class), eq(properties.getStrategy())))
                .thenReturn(MarketSignalEvaluator.EntryDecision.block("WEAK_TOP_OF_BOOK",
                        new BigDecimal("-0.01"), BigDecimal.ZERO, BigDecimal.ZERO,
                        BigDecimal.ZERO, BigDecimal.ZERO));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine", new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).cancelOrder("ENSOUSDT", 42L);
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void stillValidSignalFallsBackFromMakerToPriceCappedIoc() throws Exception {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-maker");
        atomic("targetEntryQuantity", BigDecimal.class).set(new BigDecimal("7.00"));
        atomic("filledEntryQuantity", BigDecimal.class).set(BigDecimal.ZERO);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 2_500);
        when(marketSignalEvaluator.evaluate(any(Long.class), eq(properties.getStrategy())))
                .thenReturn(MarketSignalEvaluator.EntryDecision.allow(
                        new BigDecimal("0.2"), new BigDecimal("0.1"), new BigDecimal("0.1"),
                        BigDecimal.ZERO, BigDecimal.ZERO));
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.cancelOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\",\"executedQty\":\"0\"}"));
        when(tradeService.placeLimitIocBuy(eq("ENSOUSDT"), decimalEquals("6.9"),
                decimalEquals("0.8631"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService).cancelOrder("ENSOUSDT", 42L);
        verify(tradeService).placeLimitIocBuy(eq("ENSOUSDT"), decimalEquals("6.9"),
                decimalEquals("0.8631"), anyString());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void riskGuardIsRecheckedBeforeIocFallback() throws Exception {
        prepareRestingMakerOrder();
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.cancelOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\",\"executedQty\":\"0\"}"));
        ((TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard")).trip("TEST_RISK_BLOCK");

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).placeLimitIocBuy(anyString(), any(), any(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
    }

    @Test
    void deterministicIocRejectionReturnsToIdleWithoutFailClosedHalt() throws Exception {
        prepareRestingMakerOrder();
        engine.getLiveArmed().set(true);
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.cancelOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\",\"executedQty\":\"0\"}"));
        when(tradeService.placeLimitIocBuy(eq("ENSOUSDT"), any(), any(), anyString()))
                .thenReturn(mapper.readTree("{\"code\":-1013,\"msg\":\"Filter failure\"}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        assertTrue(engine.getIsRunning().get());
        assertTrue(engine.getLiveArmed().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
    }

    @Test
    void makerFillDiscoveredDuringCancelWaitsForUserStreamInsteadOfSubmittingIoc() throws Exception {
        prepareRestingMakerOrder();
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.cancelOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\",\"executedQty\":\"1.0\"}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).placeLimitIocBuy(anyString(), any(), any(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
        assertEquals(42L, atomic("activeOrderId", Long.class).get());

        orderUpdate(42L, "churn-BUY-maker", "BUY", "TRADE", "PARTIALLY_FILLED",
                "1.0", "0.862", "0", "USDT");
        orderUpdate(42L, "churn-BUY-maker", "BUY", "CANCELED", "CANCELED",
                "0", "0", "0", "USDT");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
    }

    @Test
    void nestedCancelReplaceFailureIsInterpretedWithoutAmbiguousHalt() throws Exception {
        prepareRestingMakerOrder();
        atomic("activeClientOrderId", String.class).set("churn-BUY-replacement");
        JsonNode response = new ObjectMapper().readTree("""
                {"code":-2022,"msg":"Order cancel-replace failed.","data":{
                  "cancelResult":"SUCCESS","newOrderResult":"FAILURE",
                  "cancelResponse":{"orderId":42,"status":"CANCELED","executedQty":"0"},
                  "newOrderResponse":{"code":-1013,"msg":"Filter failure"}}}
                """);

        ReflectionTestUtils.invokeMethod(engine, "handleMakerResponse", response, 42L,
                "churn-BUY-replacement", HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
    }

    @Test
    void nestedCancelFailureKeepsOriginalOrderTracked() throws Exception {
        prepareRestingMakerOrder();
        atomic("activeClientOrderId", String.class).set("churn-BUY-replacement");
        JsonNode response = new ObjectMapper().readTree("""
                {"code":-2022,"msg":"Order cancel-replace failed.","data":{
                  "cancelResult":"FAILURE","newOrderResult":"NOT_ATTEMPTED",
                  "cancelResponse":{"code":-2011,"msg":"Unknown order sent."},
                  "newOrderResponse":null}}
                """);

        ReflectionTestUtils.invokeMethod(engine, "handleMakerResponse", response, 42L,
                "churn-BUY-replacement", HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
        assertEquals(42L, atomic("activeOrderId", Long.class).get());
        assertNull(atomic("activeClientOrderId", String.class).get());
    }

    private void prepareRestingMakerOrder() {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-maker");
        atomic("targetEntryQuantity", BigDecimal.class).set(new BigDecimal("7.00"));
        atomic("filledEntryQuantity", BigDecimal.class).set(BigDecimal.ZERO);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 2_500);
        when(marketSignalEvaluator.evaluate(any(Long.class), eq(properties.getStrategy())))
                .thenReturn(MarketSignalEvaluator.EntryDecision.allow(
                        new BigDecimal("0.2"), new BigDecimal("0.1"), new BigDecimal("0.1"),
                        BigDecimal.ZERO, BigDecimal.ZERO));
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

    private BigDecimal decimalEquals(String expected) {
        BigDecimal value = new BigDecimal(expected);
        return argThat(actual -> actual != null && actual.compareTo(value) == 0);
    }
}
