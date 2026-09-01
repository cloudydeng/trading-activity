package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.config.BinanceCredentialManager;
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
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HighFrequencyVolumeChurnEngineTest {
    private BinanceProperties properties;
    private BinanceOptimizedTradeService tradeService;
    private SymbolRuleManager ruleManager;
    private UserDataStreamService userDataStreamService;
    private MarketSignalEvaluator marketSignalEvaluator;
    private DailyTradeStatsStore dailyStatsStore;
    private BinanceCredentialManager credentialManager;
    private HighFrequencyVolumeChurnEngine engine;
    private final AtomicLong testTradeId = new AtomicLong(7_000);
    private final Map<Long, BigDecimal> cumulativeQuantity = new HashMap<>();
    private final Map<Long, BigDecimal> cumulativeQuote = new HashMap<>();

    @BeforeEach
    void setUp() {
        testTradeId.set(7_000);
        cumulativeQuantity.clear();
        cumulativeQuote.clear();
        properties = new BinanceProperties();
        properties.getApi().setApiKey("test-api-key");
        properties.getApi().setSecretKey("test-secret-key");
        properties.getApi().setApiKeyAlias("test-bot");
        BinanceProperties.CredentialProfile secondary = new BinanceProperties.CredentialProfile();
        secondary.setAlias("second-bot");
        secondary.setApiKey("second-api-key");
        secondary.setSecretKey("second-secret-key");
        properties.getApi().getProfiles().put("secondary", secondary);
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
        dailyStatsStore = mock(DailyTradeStatsStore.class);
        when(dailyStatsStore.loadActiveApiAlias()).thenReturn(Optional.empty());
        credentialManager = new BinanceCredentialManager(properties, dailyStatsStore);
        when(dailyStatsStore.recordTrade(anyString(), anyString(), anyLong(), anyLong(), anyString(),
                any(), any(), any(), any(), any(), anyLong()))
                .thenReturn(DailyTradeStatsStore.RecordResult.APPLIED);
        when(dailyStatsStore.today(anyString(), anyString())).thenReturn(new DailyTradeStatsStore.DailyStatsSnapshot(
                LocalDate.now(), "unlabeled", "ENSOUSDT", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, 0, 0, true));
        when(ruleManager.getRule("ENSOUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "ENSOUSDT", new BigDecimal("0.0001"), new BigDecimal("0.1"), new BigDecimal("5")));
        engine = new HighFrequencyVolumeChurnEngine(properties, tradeService, ruleManager, userDataStreamService,
                marketSignalEvaluator, mock(PostFillOutcomeTracker.class), new TradingRiskGuard(), dailyStatsStore,
                credentialManager);
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
    void symbolSwitchRequiresStoppedFlatAccountAndPersistsSelection() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(ruleManager.refreshRule("BTCUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "BTCUSDT", new BigDecimal("0.01"), new BigDecimal("0.00001"), new BigDecimal("5")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getOpenOrders("BTCUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceOptimizedTradeService.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getAssetBalance("BTC")).thenReturn(new BinanceOptimizedTradeService.AssetBalance(
                "BTC", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "acceptingMarketConnections"))
                .set(false);

        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = engine.switchSymbol("btcusdt");

        assertTrue(result.accepted());
        assertEquals("BTCUSDT", engine.getSymbol());
        assertFalse(engine.getIsRunning().get());
        assertFalse(engine.getLiveArmed().get());
        verify(dailyStatsStore).saveActiveSymbol("BTCUSDT");
        verify(tradeService).getOpenOrders("ENSOUSDT");
        verify(tradeService).getOpenOrders("BTCUSDT");
    }

    @Test
    void symbolSwitchIsRejectedBeforeExchangeCallsWhileEngineRuns() {
        engine.getIsRunning().set(true);

        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = engine.switchSymbol("BTCUSDT");

        assertFalse(result.accepted());
        assertEquals("ENSOUSDT", engine.getSymbol());
        verify(tradeService, never()).getOpenOrders(anyString());
    }

    @Test
    void apiProfileSwitchValidatesBothAccountsAndKeepsTradingStopped() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.getAllOpenOrders()).thenReturn(mapper.readTree("[]"));
        when(tradeService.getAccountInfo()).thenReturn(mapper.readTree("{\"canTrade\":true,\"balances\":[]}"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceOptimizedTradeService.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(userDataStreamService.reconnectForCredentialSwitch(12_000)).thenReturn(true);

        HighFrequencyVolumeChurnEngine.ApiProfileSwitchResult result = engine.switchApiProfile("second-bot");

        assertTrue(result.accepted());
        assertEquals("second-bot", engine.getApiKeyAlias());
        assertFalse(engine.getIsRunning().get());
        assertFalse(engine.getLiveArmed().get());
        verify(dailyStatsStore).saveActiveApiAlias("second-bot");
        verify(userDataStreamService).reconnectForCredentialSwitch(12_000);
    }

    @Test
    void apiProfileSwitchIsRejectedWhileEngineRuns() {
        engine.getIsRunning().set(true);

        HighFrequencyVolumeChurnEngine.ApiProfileSwitchResult result = engine.switchApiProfile("second-bot");

        assertFalse(result.accepted());
        assertEquals("test-bot", engine.getApiKeyAlias());
        verify(tradeService, never()).getAllOpenOrders();
    }

    @Test
    void failedApiProfileValidationRollsBackToOriginalCredential() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.getAllOpenOrders()).thenReturn(mapper.readTree("[]"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceOptimizedTradeService.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getAccountInfo()).thenReturn(mapper.readTree("{\"code\":-2015}"));
        when(userDataStreamService.reconnectForCredentialSwitch(12_000)).thenReturn(true);

        HighFrequencyVolumeChurnEngine.ApiProfileSwitchResult result = engine.switchApiProfile("second-bot");

        assertFalse(result.accepted());
        assertEquals("test-bot", engine.getApiKeyAlias());
        assertTrue(engine.getStatusReason().get().contains("已回滚"));
        verify(dailyStatsStore, never()).saveActiveApiAlias("second-bot");
        verify(userDataStreamService).reconnectForCredentialSwitch(12_000);
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
    void partialBuyBelowMinimumExitNotionalKeepsAccumulatingInsteadOfCreatingDust() {
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-1");
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.601"));

        orderUpdate(42L, "churn-BUY-1", "BUY", "TRADE", "PARTIALLY_FILLED",
                "1", "0.60", "0", "USDT");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
        assertEquals(42L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("尚未达到最小可卖额"));
        verify(tradeService, never()).cancelOrder("ENSOUSDT", 42L);
        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                any(), any(), any(), anyString());
    }

    @Test
    void actualCommissionAndCostPerMillionAreRecordedOnceFromExecutionReport() {
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-1");
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("10"));
        UserDataStreamService.ExecutionUpdate fill = new UserDataStreamService.ExecutionUpdate(
                42L, 7001L, "churn-BUY-1", "BUY", "TRADE", "FILLED",
                new BigDecimal("10"), new BigDecimal("0.60"), new BigDecimal("10"),
                new BigDecimal("6.00"), new BigDecimal("0.006"), "USDT", System.currentTimeMillis());

        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", fill);
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", fill);

        TradeAccountingLedger.AccountingSnapshot accounting = engine.getAccountingSnapshot();
        assertEquals(0, new BigDecimal("6.00").compareTo(accounting.totalVolumeQuote()));
        assertEquals(0, new BigDecimal("0.006").compareTo(accounting.totalCommissionQuoteEquivalent()));
        assertEquals(0, new BigDecimal("1000").compareTo(accounting.costPerMillionVolume()));
        assertEquals(1, accounting.processedTradeCount());
        assertEquals(0, new BigDecimal("6.006").compareTo(engine.getRiskSnapshot().positionCostUsdt()));
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
    void sellingImmediatelyRestsOneFeeAwareMakerWithoutTtlReplacements() throws Exception {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"),
                System.currentTimeMillis(), properties.getStrategy());
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":77}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 10_000);
        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6002"), new BigDecimal("0.6003"));

        ArgumentCaptor<BigDecimal> price = ArgumentCaptor.forClass(BigDecimal.class);
        verify(tradeService, times(1)).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                price.capture(), decimalEquals("10"), isNull(), anyString());
        assertEquals(0, new BigDecimal("0.6019").compareTo(price.getValue()));
        assertEquals(77L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("Maker 卖单已挂出"));
    }

    @Test
    void staleMakerExitOnlyRepricesDownTowardBestAsk() throws Exception {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeClientOrderId", String.class).set("churn-SELL-old");
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.6030"));
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 6_000);
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"),
                System.currentTimeMillis() - 61_000, properties.getStrategy());
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), decimalEquals("0.6019"),
                decimalEquals("10"), eq(77L), anyString())).thenReturn(new ObjectMapper().readTree("""
                {"cancelResult":"SUCCESS","newOrderResult":"SUCCESS",
                 "cancelResponse":{"orderId":77,"status":"CANCELED","executedQty":"0"},
                 "newOrderResponse":{"orderId":78,"price":"0.6019"}}
                """));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), decimalEquals("0.6019"),
                decimalEquals("10"), eq(77L), anyString());
        assertEquals(78L, atomic("activeOrderId", Long.class).get());
        assertEquals(0, new BigDecimal("0.6019").compareTo(
                atomic("activeOrderPrice", BigDecimal.class).get()));
    }

    @Test
    void longHeldPositionKeepsFeeAwareMakerThenContinuesAfterFlat() throws Exception {
        engine.getIsRunning().set(true);
        engine.getLiveArmed().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"),
                System.currentTimeMillis() - 61_000, properties.getStrategy());
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), decimalEquals("0.6019"),
                decimalEquals("10"), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":88}"));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(BigDecimal.ZERO);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), decimalEquals("0.6019"),
                decimalEquals("10"), isNull(), anyString());
        verify(tradeService, never()).placeLimitIocSell(anyString(), any(), any(), anyString());
        verify(tradeService, never()).placeMarketSell(anyString(), any(), anyString());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());

        orderUpdate(88L, atomic("activeClientOrderId", String.class).get(), "SELL", "TRADE", "FILLED",
                "10", "0.6019", "0.006019", "USDT");

        assertTrue(engine.getIsRunning().get());
        assertTrue(engine.getLiveArmed().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertEquals("运行中，等待入场信号", engine.getStatusReason().get());
    }

    @Test
    void emergencyExitReconcilesAlreadyFilledMakerBeforeLateAccountEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("7.02"));
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordFill("BUY", new BigDecimal("7.02"), new BigDecimal("0.854"), 1, properties.getStrategy());
        atomic("activeOrderId", Long.class).set(215889731L);
        atomic("activeClientOrderId", String.class).set("churn-SELL-1");
        ((java.util.Set<Long>) ReflectionTestUtils.getField(engine, "knownOrderIds")).add(215889731L);
        when(tradeService.cancelOrder("ENSOUSDT", 215889731L))
                .thenReturn(mapper.readTree("{\"code\":-2011,\"msg\":\"Unknown order sent.\"}"));
        when(tradeService.getOrder("ENSOUSDT", 215889731L)).thenReturn(mapper.readTree("""
                {"orderId":215889731,"status":"FILLED","side":"SELL","executedQty":"7.02",
                 "cummulativeQuoteQty":"5.97546","price":"0.851"}
                """));
        when(tradeService.getMyTrades("ENSOUSDT", 215889731L)).thenReturn(mapper.readTree("""
                [{"id":7001,"orderId":215889731,"price":"0.851","qty":"7.02",
                  "quoteQty":"5.97546","commission":"0","commissionAsset":"USDT","isBuyer":false}]
                """));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(BigDecimal.ZERO);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.850"), new BigDecimal("0.851"));

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertEquals(0, engine.getRiskSnapshot().positionQty().signum());
        verify(tradeService, never()).placeMarketSell(anyString(), any(), anyString());

        // The delayed account-stream reports for the same reconciled order are harmless.
        orderUpdate(215889731L, "churn-SELL-1", "SELL", "TRADE", "FILLED",
                "7.02", "0.851", "0", "USDT");
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
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
        when(tradeService.getMyTrades("ENSOUSDT", 90L)).thenReturn(mapper.readTree("""
                [{"id":7001,"orderId":90,"price":"0.85","qty":"7.08",
                  "quoteQty":"6.018","commission":"0.006018","commissionAsset":"USDT","isBuyer":false}]
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
        when(tradeService.getMyTrades("ENSOUSDT", 91L)).thenReturn(mapper.readTree("""
                [{"id":7001,"orderId":91,"price":"0.85","qty":"7.08",
                  "quoteQty":"6.018","commission":"0.006","commissionAsset":"USDT","isBuyer":true}]
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
                .set(System.currentTimeMillis() - 46_000);
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "reconnectScheduled"))
                .set(true);

        ReflectionTestUtils.invokeMethod(engine, "checkMarketStreamHealth");

        verify(socket).abort();
        assertNull(atomic("activeMarketWebSocket", WebSocket.class).get());
    }

    @Test
    void quietMarketForSixSecondsDoesNotAbortHealthySocket() {
        WebSocket socket = mock(WebSocket.class);
        atomic("activeMarketWebSocket", WebSocket.class).set(socket);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(System.currentTimeMillis() - 6_000);

        ReflectionTestUtils.invokeMethod(engine, "checkMarketStreamHealth");

        verify(socket, never()).abort();
        assertEquals(socket, atomic("activeMarketWebSocket", WebSocket.class).get());
    }

    @Test
    void staleMarketOnStartReconnectsWithoutDiscardingOperatorArm() {
        WebSocket socket = mock(WebSocket.class);
        engine.getLiveArmed().set(true);
        when(userDataStreamService.isReady()).thenReturn(true);
        atomic("activeMarketWebSocket", WebSocket.class).set(socket);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(System.currentTimeMillis() - 46_000);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(System.currentTimeMillis() - 46_000);
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
    void partialMakerBuyCancelsRemainderAndImmediatelySubmitsOneSell() throws Exception {
        prepareRestingMakerOrder();
        ObjectMapper mapper = new ObjectMapper();
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.863"));
        when(tradeService.cancelOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("""
                        {"orderId":42,"status":"CANCELED","executedQty":"7.0",
                         "cummulativeQuoteQty":"6.034"}
                        """));
        when(tradeService.getMyTrades("ENSOUSDT", 42L)).thenReturn(mapper.readTree("""
                [{"id":7001,"orderId":42,"price":"0.862","qty":"7.0",
                  "quoteQty":"6.034","commission":"0","commissionAsset":"USDT","isBuyer":true}]
                """));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("7.0"));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), any(), decimalEquals("7.0"),
                isNull(), anyString())).thenReturn(mapper.readTree("{\"orderId\":99}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).placeLimitIocBuy(anyString(), any(), any(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
        assertEquals(42L, atomic("activeOrderId", Long.class).get());

        UserDataStreamService.ExecutionUpdate partialFill = new UserDataStreamService.ExecutionUpdate(
                42L, 7001L, "churn-BUY-maker", "BUY", "TRADE", "PARTIALLY_FILLED",
                new BigDecimal("7.0"), new BigDecimal("0.862"), new BigDecimal("7.0"),
                new BigDecimal("6.034"), BigDecimal.ZERO, "USDT", System.currentTimeMillis());
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", partialFill);
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", partialFill);
        orderUpdate(42L, "churn-BUY-maker", "BUY", "CANCELED", "CANCELED",
                "0", "0", "0", "USDT");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(99L, atomic("activeOrderId", Long.class).get());
        verify(tradeService, times(1)).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), any(),
                decimalEquals("7.0"), isNull(), anyString());
        assertEquals(1, engine.getAccountingSnapshot().processedTradeCount());
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
        BigDecimal executedQuantity = new BigDecimal(qty);
        BigDecimal executedPrice = new BigDecimal(price);
        long tradeId = -1;
        if ("TRADE".equals(executionType) && executedQuantity.signum() > 0) {
            tradeId = testTradeId.incrementAndGet();
            cumulativeQuantity.merge(orderId, executedQuantity, BigDecimal::add);
            cumulativeQuote.merge(orderId, executedQuantity.multiply(executedPrice), BigDecimal::add);
        }
        UserDataStreamService.ExecutionUpdate update = new UserDataStreamService.ExecutionUpdate(
                orderId, tradeId, clientOrderId, side, executionType, status,
                executedQuantity, executedPrice,
                cumulativeQuantity.getOrDefault(orderId, BigDecimal.ZERO),
                cumulativeQuote.getOrDefault(orderId, BigDecimal.ZERO),
                new BigDecimal(commission), commissionAsset, System.currentTimeMillis());
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", update);
    }

    private BigDecimal decimalEquals(String expected) {
        BigDecimal value = new BigDecimal(expected);
        return argThat(actual -> actual != null && actual.compareTo(value) == 0);
    }
}
