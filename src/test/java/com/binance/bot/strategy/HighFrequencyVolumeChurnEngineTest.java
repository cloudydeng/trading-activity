package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.account.AccountCredentials;
import com.binance.bot.account.AccountExecutionEvent;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.net.http.WebSocket;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
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
import static org.mockito.Mockito.atLeast;

class HighFrequencyVolumeChurnEngineTest {
    private BinanceProperties properties;
    private BinanceAccountTradeClient tradeService;
    private SymbolRuleManager ruleManager;
    private AtomicBoolean userDataStreamReady;
    private MarketSignalEvaluator marketSignalEvaluator;
    private DailyTradeStatsStore dailyStatsStore;
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
        properties.getStrategy().setLiveTradingEnabled(true);
        properties.getStrategy().setSymbol("ENSOUSDT");
        properties.getStrategy().setOrderAmountUsdt(new BigDecimal("6"));
        properties.getStrategy().setMaxLiveOrderNotionalUsdt(new BigDecimal("6"));
        properties.getStrategy().setOrderTtlMs(1_200);
        properties.getStrategy().setMinEntryOrderRestMs(800);

        tradeService = mock(BinanceAccountTradeClient.class);
        ruleManager = mock(SymbolRuleManager.class);
        userDataStreamReady = new AtomicBoolean(true);
        marketSignalEvaluator = mock(MarketSignalEvaluator.class);
        dailyStatsStore = mock(DailyTradeStatsStore.class);
        when(dailyStatsStore.recordTrade(anyString(), anyString(), anyString(), anyLong(), anyLong(), anyString(),
                any(), any(), any(), any(), any(), anyLong()))
                .thenReturn(DailyTradeStatsStore.RecordResult.APPLIED);
        when(dailyStatsStore.reconcileFlatDust(anyString(), anyString(), any())).thenReturn(true);
        when(dailyStatsStore.loadRuntimeState(anyString(), anyString())).thenReturn(Optional.empty());
        when(dailyStatsStore.today(anyString(), anyString(), anyString())).thenReturn(new DailyTradeStatsStore.DailyStatsSnapshot(
                LocalDate.now(), "test-account", "test-bot", "ENSOUSDT", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, 0, 0, true));
        when(ruleManager.getRule("ENSOUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "ENSOUSDT", new BigDecimal("0.0001"), new BigDecimal("0.1"),
                new BigDecimal("0.1"), new BigDecimal("5")));
        when(tradeService.getAssetBalance("BNB")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "BNB", new BigDecimal("0.01"), BigDecimal.ZERO, new BigDecimal("0.01")));
        when(tradeService.getTickerPrice("BNBUSDT")).thenReturn(new BigDecimal("1000"));
        when(marketSignalEvaluator.markBestBidMakerReady()).thenReturn(
                new MarketSignalEvaluator.EntryDecision(true, "BEST_BID_MAKER", BigDecimal.ZERO,
                        BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(marketSignalEvaluator.evaluateBestBidMaker(anyLong(), eq(properties.getStrategy()))).thenReturn(
                new MarketSignalEvaluator.EntryDecision(true, "BEST_BID_MAKER", BigDecimal.ZERO,
                        BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy()))).thenReturn(
                MarketSignalEvaluator.EntryDecision.allow(new BigDecimal("0.2"), new BigDecimal("0.2"),
                        new BigDecimal("0.2"), BigDecimal.ZERO, BigDecimal.ZERO));
        AccountCredentials credentials = new AccountCredentials("test-account", "test-bot", "test-api-key", "test-secret-key");
        engine = new HighFrequencyVolumeChurnEngine("test-account", "test-bot", credentials,
                properties, tradeService, ruleManager, userDataStreamReady::get, marketSignalEvaluator,
                mock(PostFillOutcomeTracker.class), new TradingRiskGuard(), dailyStatsStore,
                mock(TradeNotificationService.class));
    }

    @Test
    void refusesToStartWhenAccountStreamIsNotReady() {
        userDataStreamReady.set(false);

        assertFalse(engine.startTrading());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
        verify(tradeService, never()).getFreeAssetBalance(anyString());
    }

    @Test
    void perSymbolOrderAmountOverridesGlobalAmount() {
        properties.getStrategy().setOrderAmountsUsdt(Map.of(
                "ENSOUSDT", new BigDecimal("6"), "BTCUSDT", new BigDecimal("9")));

        assertEquals(0, new BigDecimal("6").compareTo(engine.getOrderAmountUsdt()));
        properties.getStrategy().setSymbol("BTCUSDT");
        assertEquals(0, new BigDecimal("9").compareTo(engine.getOrderAmountUsdt()));
        properties.getStrategy().setSymbol("PROMUSDT");
        assertEquals(0, new BigDecimal("6").compareTo(engine.getOrderAmountUsdt()));
    }

    @Test
    void streamLossStopsAndDisarmsWithoutAutomaticResume() {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(77L);

        ReflectionTestUtils.invokeMethod(engine, "handleUserStreamLoss", "socket closed");

        assertFalse(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
        verify(tradeService).cancelOrder("ENSOUSDT", 77L);
    }

    @Test
    void accountAStreamLossLeavesAccountBRunningAndArmed() {
        HighFrequencyVolumeChurnEngine engineB = new HighFrequencyVolumeChurnEngine(
                "account-b", "B", new AccountCredentials("account-b", "B", "key-b", "secret-b"),
                properties, mock(BinanceAccountTradeClient.class), ruleManager, () -> true,
                new MarketSignalEvaluator(), new PostFillOutcomeTracker(), new TradingRiskGuard(),
                dailyStatsStore, mock(TradeNotificationService.class));
        engine.getIsRunning().set(true);
        engineB.getIsRunning().set(true);

        engine.handleUserStreamLoss("account A disconnected");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
        assertFalse(engine.getIsRunning().get());
        assertTrue(engineB.getIsRunning().get());
    }

    @Test
    void executionFromAnotherAccountCannotMutateThisEngineEvenWithSameOrderId() {
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("ta-test-B-1");
        AccountExecutionEvent foreign = new AccountExecutionEvent("account-a", "ENSOUSDT", 42L, 7L,
                "ta-test-B-1", "BUY", "TRADE", "FILLED", BigDecimal.TEN, new BigDecimal("0.6"),
                BigDecimal.TEN, new BigDecimal("6"), BigDecimal.ZERO, "USDT", true,
                System.currentTimeMillis());

        engine.onOrderUpdate(foreign);

        assertEquals(0, engine.getRiskSnapshot().positionQty().signum());
        assertEquals(0, atomic("holdingInventory", BigDecimal.class).get().signum());
        verify(dailyStatsStore, never()).recordTrade(anyString(), anyString(), anyString(), anyLong(),
                anyLong(), anyString(), any(), any(), any(), any(), any(), anyLong());
    }

    @Test
    void symbolSwitchRequiresStoppedFlatAccountAndPersistsSelection() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(ruleManager.refreshRule("BTCUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "BTCUSDT", new BigDecimal("0.01"), new BigDecimal("0.00001"),
                new BigDecimal("0.00001"), new BigDecimal("5")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getOpenOrders("BTCUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getAssetBalance("BTC")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "BTC", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "acceptingMarketConnections"))
                .set(false);

        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = engine.switchSymbol("btcusdt");

        assertTrue(result.accepted());
        assertEquals("BTCUSDT", engine.getSymbol());
        assertFalse(engine.getIsRunning().get());
        verify(dailyStatsStore).saveActiveSymbol("test-account", "BTCUSDT");
        verify(tradeService).getOpenOrders("ENSOUSDT");
        verify(tradeService).getOpenOrders("BTCUSDT");
    }

    @Test
    void startTradingIgnoresDurableLedgerPositionWhenExchangeIsFlat() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long now = System.currentTimeMillis();
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(now);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(now);
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(dailyStatsStore.today(eq("test-account"), eq("test-bot"), eq("ENSOUSDT")))
                .thenReturn(dailyStatsWithPosition("ENSOUSDT", "10", "6"));

        assertTrue(engine.startTrading());

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertEquals("运行中，等待入场信号", engine.getStatusReason().get());
    }

    @Test
    void symbolSwitchIgnoresDurableLedgerPositionsAfterExchangeFlatCheck() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(ruleManager.refreshRule("BTCUSDT")).thenReturn(new SymbolRuleManager.SymbolRule(
                "BTCUSDT", new BigDecimal("0.01"), new BigDecimal("0.00001"),
                new BigDecimal("0.00001"), new BigDecimal("5")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getOpenOrders("BTCUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getAssetBalance("BTC")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "BTC", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(dailyStatsStore.today(eq("test-account"), eq("test-bot"), eq("ENSOUSDT")))
                .thenReturn(dailyStatsWithPosition("ENSOUSDT", "10", "6"));
        when(dailyStatsStore.today(eq("test-account"), eq("test-bot"), eq("BTCUSDT")))
                .thenReturn(dailyStatsWithPosition("BTCUSDT", "0.01", "1200"));
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "acceptingMarketConnections"))
                .set(false);

        HighFrequencyVolumeChurnEngine.SymbolSwitchResult result = engine.switchSymbol("BTCUSDT");

        assertTrue(result.accepted());
        assertEquals("BTCUSDT", engine.getSymbol());
        verify(dailyStatsStore).saveActiveSymbol("test-account", "BTCUSDT");
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
    void runtimeStrategySwitchAppliesImmediatelyWhenIdleAndPersists() {
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                "ensousdt", "BID_ASK_MAKER", new BigDecimal("6"), 20_000L, 120_000L,
                null, null, 1_800_000L, new BigDecimal("8"));

        assertTrue(result.accepted());
        assertTrue(result.applied());
        assertFalse(result.pending());
        assertEquals("BID_ASK_MAKER", engine.getStrategyMode());
        assertEquals(0, new BigDecimal("6").compareTo(engine.getOrderAmountUsdt()));
        ArgumentCaptor<BinanceProperties.SymbolStrategyProfile> profile =
                ArgumentCaptor.forClass(BinanceProperties.SymbolStrategyProfile.class);
        verify(dailyStatsStore).saveStrategyOverride(eq("test-account"), eq("ENSOUSDT"), profile.capture());
        assertEquals(1_800_000L, profile.getValue().getEntryAnchorWaitMs());
        assertEquals(0, new BigDecimal("8").compareTo(profile.getValue().getMaxEntryAnchorDriftBps()));
        assertEquals(0, new BigDecimal("8").compareTo(profile.getValue().getMaxCumulativeEntryAnchorDriftBps()));
        assertEquals(60_000L, profile.getValue().getPostSellEntryDelayMs());
        assertEquals(0, new BigDecimal("510").compareTo(profile.getValue().getDailyVolumeLimitUsdt()));
    }

    @Test
    void dailyVolumeLimitCanBeConfiguredPerAccountAndSymbol() {
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                "ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"), 20_000L, 120_000L,
                null, null, null, null, null, null, 60_000L, new BigDecimal("750"));

        assertTrue(result.accepted());
        assertEquals(0, new BigDecimal("750").compareTo(engine.getStrategyProfile().getDailyVolumeLimitUsdt()));
    }

    @Test
    void flatAccountStopsBeforeAnotherBuyWhenDailyVolumeLimitIsReached() {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"), 20_000L, 120_000L);
        when(dailyStatsStore.today(eq("test-account"), eq("test-bot"), eq("ENSOUSDT")))
                .thenReturn(dailyStatsWithVolume("510"));
        engine.getIsRunning().set(true);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        assertFalse(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("每日上限 510 USDT"));
        verify(tradeService, never()).cancelAndReplaceOrder(anyString(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void accountStopsOnlyAfterCurrentSellHasFlattenedAtDailyVolumeLimit() {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"), 20_000L, 120_000L);
        when(dailyStatsStore.today(eq("test-account"), eq("test-bot"), eq("ENSOUSDT")))
                .thenReturn(dailyStatsWithVolume("522"));
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);

        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", true);

        assertFalse(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("已确认空仓并自动停止当前账户"));
    }

    @Test
    void flatAccountStopsWhenBnbValueFallsBelowOneUsdt() {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"), 20_000L, 120_000L);
        when(tradeService.getAssetBalance("BNB")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "BNB", new BigDecimal("0.0008"), BigDecimal.ZERO, new BigDecimal("0.0008")));
        when(tradeService.getTickerPrice("BNBUSDT")).thenReturn(new BigDecimal("1000"));
        engine.getIsRunning().set(true);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        assertFalse(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("BNB 余额价值 0.8000 USDT 低于 1 USDT"));
        verify(tradeService, never()).cancelAndReplaceOrder(anyString(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void lowBnbDoesNotCancelAnExistingSellOrder() {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"), 20_000L, 120_000L);
        when(tradeService.getAssetBalance("BNB")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "BNB", new BigDecimal("0.0008"), BigDecimal.ZERO, new BigDecimal("0.0008")));
        when(tradeService.getTickerPrice("BNBUSDT")).thenReturn(new BigDecimal("1000"));
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.6001"));
        ((AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis());

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(42L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("等待当前卖单完成后自动停止"));
        verify(tradeService, never()).cancelOrder("ENSOUSDT", 42L);
    }

    @Test
    void defaultsToFeeAwareMakerAndRejectsRemovedCurrentMode() {
        assertEquals("FEE_AWARE_MAKER", engine.getStrategyMode());
        assertEquals("FEE_AWARE_MAKER", engine.getStrategyProfile().getMode());

        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                "ENSOUSDT", "CURRENT", new BigDecimal("6"), null, null);

        assertFalse(result.accepted());
        assertEquals("FEE_AWARE_MAKER", engine.getStrategyMode());
    }

    @Test
    void feeAwareMakerIgnoresSellPressureSignalGate() throws Exception {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO);
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy()))).thenReturn(
                MarketSignalEvaluator.EntryDecision.block("SELL_TAKER_PRESSURE",
                        new BigDecimal("-0.1"), BigDecimal.ZERO, new BigDecimal("-0.8"),
                        BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":301}"));
        engine.getIsRunning().set(true);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), any(), isNull(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void bidAskMakerIgnoresSellPressureSignalGate() throws Exception {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy()))).thenReturn(
                MarketSignalEvaluator.EntryDecision.block("SELL_TAKER_PRESSURE",
                        new BigDecimal("-0.1"), BigDecimal.ZERO, new BigDecimal("-0.8"),
                        BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":302}"));
        engine.getIsRunning().set(true);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), any(), isNull(), anyString());
        verify(marketSignalEvaluator, never()).evaluate(anyLong(), eq(properties.getStrategy()));
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void bidAskMakerInitialSellMustStayAboveActualBuyAverageEvenBelowBestAsk() throws Exception {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.6001"), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":77}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.5990"), new BigDecimal("0.5991"));

        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.6001"), anyString());
        assertEquals(77L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("买入价上方"));
    }

    @Test
    void feeAwareMakerUsesProtectedInitialExitThenLatestAskAfterTimeout() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                "ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"), 20_000L, 120_000L,
                new BigDecimal("10"), new BigDecimal("10"));
        assertTrue(result.accepted());
        assertEquals("FEE_AWARE_MAKER", engine.getStrategyMode());

        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordActualFill("BUY", new BigDecimal("10"), new BigDecimal("6"),
                new BigDecimal("0.006"), System.currentTimeMillis(), properties.getStrategy());
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        atomic("filledEntryMaxPrice", BigDecimal.class).set(new BigDecimal("0.6000"));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), decimalEquals("0.6013"),
                decimalEquals("10"), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":77}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        assertEquals(77L, atomic("activeOrderId", Long.class).get());
        assertEquals(0, new BigDecimal("0.6013").compareTo(atomic("activeOrderPrice", BigDecimal.class).get()));
        verify(tradeService, never()).placeLimitGtcSell(anyString(), any(), any(), anyString());
        ArgumentCaptor<DailyTradeStatsStore.RuntimeState> runtimeState =
                ArgumentCaptor.forClass(DailyTradeStatsStore.RuntimeState.class);
        verify(dailyStatsStore, atLeast(1)).saveRuntimeState(eq("test-account"), eq("ENSOUSDT"), runtimeState.capture());
        assertEquals(77L, runtimeState.getValue().orderId());
        assertEquals(0, new BigDecimal("0.6000").compareTo(runtimeState.getValue().feeAwareEntryPriceCeiling()));

        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 121_000);
        when(tradeService.cancelOrder("ENSOUSDT", 77L))
                .thenReturn(mapper.readTree("{\"orderId\":77,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 77L)).thenReturn(mapper.readTree(
                "{\"orderId\":77,\"status\":\"CANCELED\",\"side\":\"SELL\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\"}"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(
                new BinanceAccountTradeClient.AssetBalance("ENSO", new BigDecimal("10"), BigDecimal.ZERO,
                        new BigDecimal("10")));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), decimalEquals("0.6020"),
                decimalEquals("10"), isNull(), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.5990"), new BigDecimal("0.6020"));

        verify(tradeService).cancelOrder("ENSOUSDT", 77L);
        verify(tradeService, times(1)).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                decimalEquals("0.6013"), decimalEquals("10"), isNull(), anyString());
        verify(tradeService, times(1)).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                decimalEquals("0.6020"), decimalEquals("10"), isNull(), anyString());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("最新卖一价快速退出"));
    }

    @Test
    void timedOutSellAtLatestAskIsKeptAndTimerIsRearmed() throws Exception {
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.602000"));
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        AtomicLong placedAt = (AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp");
        long expiredAt = System.currentTimeMillis() - 121_000L;
        placedAt.set(expiredAt);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.601900"), new BigDecimal("0.602000"));

        verify(tradeService, never()).cancelOrder("ENSOUSDT", 77L);
        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                any(), any(), isNull(), anyString());
        assertTrue(placedAt.get() > expiredAt);
        assertTrue(engine.getStatusReason().get().contains("仍在卖一，保留当前 LIMIT 卖单"));
    }

    @Test
    void timedOutBidAskSellDropsEntryFloorAndRepricesToLatestAsk() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.600100"));
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        AtomicLong placedAt = (AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp");
        long expiredAt = System.currentTimeMillis() - 121_000L;
        placedAt.set(expiredAt);
        when(tradeService.cancelOrder("ENSOUSDT", 77L))
                .thenReturn(mapper.readTree("{\"orderId\":77,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 77L)).thenReturn(mapper.readTree(
                "{\"orderId\":77,\"status\":\"CANCELED\",\"side\":\"SELL\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\"}"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(
                new BinanceAccountTradeClient.AssetBalance("ENSO", new BigDecimal("10"), BigDecimal.ZERO,
                        new BigDecimal("10")));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.5990"), anyString())).thenReturn(mapper.readTree("{\"orderId\":88}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.598900"), new BigDecimal("0.599000"));

        verify(tradeService).cancelOrder("ENSOUSDT", 77L);
        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.5990"), anyString());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
    }

    @Test
    void feeAwareMakerWaitsWhenNextEntryWouldExceedFirstBuyAnchor() throws Exception {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO);
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        atomic("filledEntryMaxPrice", BigDecimal.class).set(new BigDecimal("0.6000"));
        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", false);
        engine.getIsRunning().set(true);
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":101}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6001"), new BigDecimal("0.6002"));

        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                any(), any(), isNull(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("高于买入锚点"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), any(), isNull(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void feeAwareMakerAllowsConfiguredSmallChaseAfterAnchorWait() throws Exception {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO,
                1_800_000L, new BigDecimal("10"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        atomic("filledEntryMaxPrice", BigDecimal.class).set(new BigDecimal("0.6000"));
        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", false);
        engine.getIsRunning().set(true);
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":102}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6005"), new BigDecimal("0.6006"));

        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                any(), any(), isNull(), anyString());
        assertTrue(engine.getStatusReason().get().contains("已等待"));

        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(
                engine, "feeAwareEntryCeilingBlockedSince")).set(System.currentTimeMillis() - 1_801_000L);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6005"), new BigDecimal("0.6006"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6005"), any(), isNull(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void feeAwareMakerCapsChaseByRecentFiveBuyAverageAnchor() throws Exception {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO,
                0L, new BigDecimal("100"), new BigDecimal("10"));
        atomic("feeAwareEntryPriceCeiling", BigDecimal.class).set(new BigDecimal("0.6200"));
        atomic("feeAwareInitialEntryAnchorPrice", BigDecimal.class).set(new BigDecimal("0.6000"));
        engine.getIsRunning().set(true);
        BigDecimal allowed = ReflectionTestUtils.invokeMethod(engine, "feeAwareAllowedEntryPrice",
                ruleManager.getRule("ENSOUSDT"));
        assertEquals(0, new BigDecimal("0.60060000").compareTo(allowed));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":103}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6010"), new BigDecimal("0.6011"));

        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                any(), any(), isNull(), anyString());
        assertTrue(engine.getStatusReason().get().contains("最终允许最高"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6006"), new BigDecimal("0.6007"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6006"), any(), isNull(), anyString());
    }

    @Test
    void feeAwareMakerInitialAnchorUsesRecentFiveBuyAverageAndStaysFixed() {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO,
                0L, new BigDecimal("100"), new BigDecimal("10"));
        atomic("feeAwareEntryPriceCeiling", BigDecimal.class).set(new BigDecimal("0.6200"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.5900"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6000"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6100"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6200"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6300"));

        BigDecimal allowed = ReflectionTestUtils.invokeMethod(engine, "feeAwareAllowedEntryPrice",
                ruleManager.getRule("ENSOUSDT"));

        assertEquals(0, new BigDecimal("0.6106100").compareTo(allowed));
        assertEquals(0, new BigDecimal("0.6100").compareTo(
                atomic("feeAwareInitialEntryAnchorPrice", BigDecimal.class).get()));

        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.9000"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.9100"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.9200"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.9300"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.9400"));

        BigDecimal stillAllowed = ReflectionTestUtils.invokeMethod(engine, "feeAwareAllowedEntryPrice",
                ruleManager.getRule("ENSOUSDT"));

        assertEquals(0, new BigDecimal("0.6106100").compareTo(stillAllowed));
        assertEquals(0, new BigDecimal("0.6100").compareTo(
                atomic("feeAwareInitialEntryAnchorPrice", BigDecimal.class).get()));
    }

    @Test
    void feeAwareMakerManualAnchorOverridesGeneratedRecentBuyAnchor() {
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                "ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO,
                0L, new BigDecimal("100"), new BigDecimal("10"), new BigDecimal("0.5900"));
        assertTrue(result.accepted());
        assertEquals(0, new BigDecimal("0.5900").compareTo(
                atomic("feeAwareEntryPriceCeiling", BigDecimal.class).get()));
        atomic("feeAwareEntryPriceCeiling", BigDecimal.class).set(new BigDecimal("0.6200"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6100"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6200"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6300"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6400"));
        ReflectionTestUtils.invokeMethod(engine, "rememberFeeAwareRecentBuyPrice", new BigDecimal("0.6500"));

        BigDecimal allowed = ReflectionTestUtils.invokeMethod(engine, "feeAwareAllowedEntryPrice",
                ruleManager.getRule("ENSOUSDT"));

        assertEquals(0, new BigDecimal("0.5905900").compareTo(allowed));
        assertEquals(0, new BigDecimal("0.5900").compareTo(
                atomic("feeAwareInitialEntryAnchorPrice", BigDecimal.class).get()));
        assertEquals(0, new BigDecimal("0.5900").compareTo(
                engine.getStrategyProfile().getManualEntryAnchorPrice()));
    }

    @Test
    void feeAwareMakerRejectsInvalidManualAnchorPrice() {
        HighFrequencyVolumeChurnEngine.StrategySwitchResult result = engine.switchStrategy(
                "ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO,
                0L, new BigDecimal("100"), new BigDecimal("10"), BigDecimal.ZERO);

        assertFalse(result.accepted());
        assertTrue(result.message().contains("手动锚定价格必须大于 0"));
        verify(dailyStatsStore, never()).saveStrategyOverride(anyString(), anyString(), any());
    }

    @Test
    void feeAwareMakerKeepsLowerRestingBuyWhenBestBidExceedsAllowedAnchorPrice() throws Exception {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                1_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO);
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        atomic("filledEntryMaxPrice", BigDecimal.class).set(new BigDecimal("0.6000"));
        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", false);
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-maker");
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.6000"));
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 5_000);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6001"), new BigDecimal("0.6002"));

        verify(tradeService, never()).cancelOrder("ENSOUSDT", 42L);
        assertEquals(42L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("保留较低 Maker 买单"));
    }

    @Test
    void feeAwareMakerPersistsHighestBuyPriceAfterFlatExit() {
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO);
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("12"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("7.2100"));
        atomic("filledEntryMaxPrice", BigDecimal.class).set(new BigDecimal("0.6010"));

        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", false);

        ArgumentCaptor<DailyTradeStatsStore.RuntimeState> state =
                ArgumentCaptor.forClass(DailyTradeStatsStore.RuntimeState.class);
        verify(dailyStatsStore).saveRuntimeState(eq("test-account"), eq("ENSOUSDT"), state.capture());
        assertNull(state.getValue().orderId());
        assertEquals(0, new BigDecimal("0.6010").compareTo(state.getValue().feeAwareEntryPriceCeiling()));
    }

    @Test
    void startTradingRestoresMatchingDurableSellOrderAfterRestart() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long now = System.currentTimeMillis();
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(now);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(now);
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.6001"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("10"), BigDecimal.ZERO, new BigDecimal("10")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("""
                [{"orderId":77,"clientOrderId":"ta-restore-S-1","side":"SELL","price":"0.6013",
                  "origQty":"10","executedQty":"0","status":"NEW"}]
                """));
        when(dailyStatsStore.today(eq("test-account"), eq("test-bot"), eq("ENSOUSDT")))
                .thenReturn(dailyStatsWithPosition("ENSOUSDT", "10", "6.006"));
        when(dailyStatsStore.loadRuntimeState("test-account", "ENSOUSDT")).thenReturn(Optional.of(
                new DailyTradeStatsStore.RuntimeState("test-account", "ENSOUSDT", "SELLING", 77L,
                        "ta-restore-S-1", "SELL", new BigDecimal("0.6013"), new BigDecimal("10"),
                        new BigDecimal("0.6000"), now - 10_000, now - 10_000,
                        new BigDecimal("0.6000"), List.of(new BigDecimal("0.6000")))));

        assertTrue(engine.startTrading());

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(77L, atomic("activeOrderId", Long.class).get());
        assertEquals(0, new BigDecimal("6.006").compareTo(engine.getRiskSnapshot().positionCostUsdt()));
        assertEquals(0, new BigDecimal("0.6000").compareTo(
                atomic("feeAwareEntryPriceCeiling", BigDecimal.class).get()));
        assertTrue(engine.getStatusReason().get().contains("重启后已恢复本进程卖单"));
    }

    @Test
    void startTradingRestoresMatchingRemoteTodayInventoryWithoutOpenOrder() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long now = System.currentTimeMillis();
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(now);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(now);
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.6001"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("10"), BigDecimal.ZERO, new BigDecimal("10")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getMyTrades(eq("ENSOUSDT"), anyLong(), anyLong(), eq(1000)))
                .thenReturn(mapper.readTree("""
                        [{"id":11,"orderId":101,"price":"0.6000","qty":"10","quoteQty":"6.0000",
                          "commission":"0","commissionAsset":"USDT","isBuyer":true,"time":%d}]
                        """.formatted(now - 1000)));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.6001"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        assertTrue(engine.startTrading());

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
        assertEquals(0, new BigDecimal("10").compareTo(engine.getRiskSnapshot().positionQty()));
        assertEquals(0, new BigDecimal("6.0000").compareTo(engine.getRiskSnapshot().positionCostUsdt()));
        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.6001"), anyString());
    }

    @Test
    void startTradingRestoresFeeAwareRemoteTodayInventoryWithoutOpenOrder() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long now = System.currentTimeMillis();
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(now);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(now);
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.6001"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("10"), BigDecimal.ZERO, new BigDecimal("10")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getMyTrades(eq("ENSOUSDT"), anyLong(), anyLong(), eq(1000)))
                .thenReturn(mapper.readTree("""
                        [{"id":11,"orderId":101,"price":"0.6000","qty":"10","quoteQty":"6.0000",
                          "commission":"0","commissionAsset":"USDT","isBuyer":true,"time":%d}]
                        """.formatted(now - 1000)));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                decimalEquals("0.6007"), decimalEquals("10"), isNull(), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        assertTrue(engine.startTrading());

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
        assertEquals(0, new BigDecimal("10").compareTo(engine.getRiskSnapshot().positionQty()));
        assertEquals(0, new BigDecimal("6.0000").compareTo(engine.getRiskSnapshot().positionCostUsdt()));
        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"),
                decimalEquals("0.6007"), decimalEquals("10"), isNull(), anyString());
        verify(tradeService, never()).placeLimitGtcSell(anyString(), any(), any(), anyString());
    }

    @Test
    void startTradingRejectsRemoteTodayInventoryWhenExchangeBalanceDoesNotMatchTrades() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long now = System.currentTimeMillis();
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(now);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(now);
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.6001"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("11"), BigDecimal.ZERO, new BigDecimal("11")));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getMyTrades(eq("ENSOUSDT"), anyLong(), anyLong(), eq(1000)))
                .thenReturn(mapper.readTree("""
                        [{"id":11,"orderId":101,"price":"0.6000","qty":"10","quoteQty":"6.0000",
                          "commission":"0","commissionAsset":"USDT","isBuyer":true,"time":%d}]
                        """.formatted(now - 1000)));

        assertFalse(engine.startTrading());

        assertFalse(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.HALTED, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("今日远程成交无法还原成本或与交易所余额不一致"));
        verify(tradeService, never()).placeLimitGtcSell(anyString(), any(), any(), anyString());
        verify(tradeService, never()).cancelAndReplaceOrder(anyString(), eq("SELL"), any(), any(), any(), anyString());
    }

    @Test
    void remoteTodayStatusClearsStaleLocalInventoryWhenExchangeIsFlat() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.restoreOpenPosition(new BigDecimal("1298"), new BigDecimal("12.0025"),
                new BigDecimal("0.0092"), System.currentTimeMillis(), properties.getStrategy());
        guard.trip("MAX_INVENTORY_AGE");
        when(tradeService.getMyTrades(eq("ENSOUSDT"), anyLong(), anyLong(), eq(1000)))
                .thenReturn(mapper.readTree("[]"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));

        HighFrequencyVolumeChurnEngine.RemoteTodayStatusSnapshot snapshot = engine.getRemoteTodayStatusSnapshot();

        assertTrue(snapshot.remote());
        assertEquals(0, snapshot.risk().positionQty().signum());
        assertEquals(0, snapshot.dailyStats().positionQty().signum());
        assertNull(snapshot.risk().entryBlockReason());
        assertEquals(0, engine.getRiskSnapshot().positionQty().signum());
        assertNull(engine.getRiskSnapshot().entryBlockReason());
    }

    @Test
    void remoteTodayStatusComputesTodayFeeAndProfitFromExchangeTrades() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long now = System.currentTimeMillis();
        when(tradeService.getMyTrades(eq("ENSOUSDT"), anyLong(), anyLong(), eq(1000)))
                .thenReturn(mapper.readTree("""
                        [
                          {"id":11,"orderId":101,"price":"0.6000","qty":"10","quoteQty":"6.0000",
                           "commission":"0.0060","commissionAsset":"USDT","isBuyer":true,"time":%d},
                          {"id":12,"orderId":102,"price":"0.6010","qty":"10","quoteQty":"6.0100",
                           "commission":"0.00601","commissionAsset":"USDT","isBuyer":false,"time":%d}
                        ]
                        """.formatted(now - 2000, now - 1000)));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));

        HighFrequencyVolumeChurnEngine.RemoteTodayStatusSnapshot snapshot = engine.getRemoteTodayStatusSnapshot();

        assertTrue(snapshot.remote());
        assertEquals(0, new BigDecimal("12.0100").compareTo(snapshot.dailyStats().totalVolumeQuote()));
        assertEquals(0, new BigDecimal("0.01201").compareTo(snapshot.dailyStats().totalCommissionQuoteEquivalent()));
        assertEquals(0, new BigDecimal("0.0100").compareTo(snapshot.dailyStats().realizedGrossPnlQuote()));
        assertEquals(0, new BigDecimal("-0.00201").compareTo(snapshot.dailyStats().netRealizedPnlQuote()));
        assertEquals(0, new BigDecimal("-0.00201").compareTo(snapshot.risk().realizedPnlUsdt()));
        assertEquals(0, new BigDecimal("0.01201").compareTo(snapshot.risk().estimatedFeesUsdt()));
    }

    @Test
    void parsesConservativeMakerSellCommissionWithoutAssumingBnbBalance() throws Exception {
        JsonNode response = new ObjectMapper().readTree("""
                {"standardCommission":{"maker":"0.001","seller":"0.0001"},
                 "specialCommission":{"maker":"0.0002","seller":"0.0001"},
                 "taxCommission":{"maker":"0.00001","seller":"0.00002"},
                 "discount":{"enabledForAccount":true,"enabledForSymbol":true,"discount":"0.75"}}
                """);

        BigDecimal rate = ReflectionTestUtils.invokeMethod(engine, "parseMakerSellFeeRate", response);

        assertEquals(0, new BigDecimal("0.00143").compareTo(rate));
    }

    @Test
    void runtimeStrategySwitchQueuesBehindActiveOrderAndAppliesAtIdleBoundary() {
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("activeOrderId", Long.class).set(42L);

        HighFrequencyVolumeChurnEngine.StrategySwitchResult queued = engine.switchStrategy(
                "ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"), null, null);

        assertTrue(queued.accepted());
        assertFalse(queued.applied());
        assertTrue(queued.pending());
        assertEquals("FEE_AWARE_MAKER", engine.getStrategyMode());
        verify(dailyStatsStore, never()).saveStrategyOverride(anyString(), anyString(), any());

        atomic("activeOrderId", Long.class).set(null);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE);
        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.60"), new BigDecimal("0.61"));

        assertEquals("BID_ASK_MAKER", engine.getStrategyMode());
        verify(dailyStatsStore).saveStrategyOverride(eq("test-account"), eq("ENSOUSDT"), any());
    }

    @Test
    void unknownTradeEventFailsClosed() {
        engine.getIsRunning().set(true);

        orderUpdate(999L, "external-order", "BUY", "TRADE", "FILLED",
                "10", "0.60", "0", "USDT");

        assertFalse(engine.getIsRunning().get());
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
    void partialBuyDustDoesNotBlockNextMakerBuy() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-1");
        atomic("lastBestAsk", BigDecimal.class).set(new BigDecimal("0.60"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("5"), BigDecimal.ZERO, new BigDecimal("5")));
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":43}"));

        orderUpdate(42L, "churn-BUY-1", "BUY", "TRADE", "PARTIALLY_FILLED",
                "5", "0.60", "0", "USDT");
        orderUpdate(42L, "churn-BUY-1", "BUY", "CANCELED", "CANCELED",
                "0", "0", "0", "USDT");
        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
        assertTrue(engine.getSellabilitySnapshot().dustReason()
                == HighFrequencyVolumeChurnEngine.DustReason.BELOW_MIN_NOTIONAL);
        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(),
                isNull(), anyString());
        verify(tradeService, never()).placeLimitGtcSell(eq("ENSOUSDT"), any(), any(), anyString());
    }

    @Test
    void dustMergedWithNewBuyCreatesOneSellForCombinedInventory() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(43L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-2");
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("5"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("5"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("3.00"));
        ((AtomicReference<BigDecimal>) ReflectionTestUtils.getField(engine, "lastKnownFreeBaseBalance"))
                .set(new BigDecimal("5"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("10"), BigDecimal.ZERO, new BigDecimal("10")));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10.0"),
                decimalEquals("0.70"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":99}"));

        orderUpdate(43L, "churn-BUY-2", "BUY", "TRADE", "FILLED",
                "5", "0.80", "0", "USDT");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(99L, atomic("activeOrderId", Long.class).get());
        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10.0"),
                decimalEquals("0.70"), anyString());
    }

    @Test
    void partialSellLeavesDustWithoutCreatingReplacementSell() throws Exception {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeClientOrderId", String.class).set("churn-SELL-1");
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.00"));
        ((AtomicReference<BigDecimal>) ReflectionTestUtils.getField(engine, "activeSellCoveredQty"))
                .set(new BigDecimal("10"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(new BinanceAccountTradeClient.AssetBalance(
                "ENSO", new BigDecimal("3"), BigDecimal.ZERO, new BigDecimal("3")));

        orderUpdate(77L, "churn-SELL-1", "SELL", "TRADE", "PARTIALLY_FILLED",
                "7", "0.60", "0", "USDT");
        orderUpdate(77L, "churn-SELL-1", "SELL", "CANCELED", "CANCELED",
                "0", "0", "0", "USDT");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertFalse(engine.getSellabilitySnapshot().sellable());
        assertEquals(HighFrequencyVolumeChurnEngine.DustReason.BELOW_MIN_NOTIONAL,
                engine.getSellabilitySnapshot().dustReason());
        verify(tradeService, never()).placeLimitGtcSell(eq("ENSOUSDT"), any(), any(), anyString());
    }

    @Test
    void activeSellCoveredQuantityPreventsDuplicateSell() {
        HighFrequencyVolumeChurnEngine.SellabilityResult result = engine.evaluateSellability(
                new BigDecimal("10"), new BigDecimal("7"), new BigDecimal("3"),
                new BigDecimal("0.60"), rule("0.1", "0.1", "5"));

        assertFalse(result.sellable());
        assertEquals(0, new BigDecimal("3.0").compareTo(result.normalizedQty()));
        assertEquals(HighFrequencyVolumeChurnEngine.DustReason.BELOW_MIN_NOTIONAL, result.dustReason());
    }

    @Test
    void sellabilityRoundsDownToStepSize() {
        HighFrequencyVolumeChurnEngine.SellabilityResult result = engine.evaluateSellability(
                new BigDecimal("1.23456"), BigDecimal.ZERO, new BigDecimal("1.23456"),
                BigDecimal.ONE, rule("0.01", "0.01", "0.1"));

        assertTrue(result.sellable());
        assertEquals(0, new BigDecimal("1.23").compareTo(result.normalizedQty()));
    }

    @Test
    void sellabilityReportsBelowMinQty() {
        HighFrequencyVolumeChurnEngine.SellabilityResult result = engine.evaluateSellability(
                new BigDecimal("0.50"), BigDecimal.ZERO, new BigDecimal("0.50"),
                BigDecimal.ONE, rule("0.01", "1", "0.1"));

        assertFalse(result.sellable());
        assertEquals(HighFrequencyVolumeChurnEngine.DustReason.BELOW_MIN_QTY, result.dustReason());
    }

    @Test
    void sellabilityAppliesMinNotionalBuffer() {
        HighFrequencyVolumeChurnEngine.SellabilityResult belowBuffer = engine.evaluateSellability(
                new BigDecimal("5.10"), BigDecimal.ZERO, new BigDecimal("5.10"),
                BigDecimal.ONE, rule("0.01", "0.01", "5"));
        HighFrequencyVolumeChurnEngine.SellabilityResult atBuffer = engine.evaluateSellability(
                new BigDecimal("5.25"), BigDecimal.ZERO, new BigDecimal("5.25"),
                BigDecimal.ONE, rule("0.01", "0.01", "5"));

        assertFalse(belowBuffer.sellable());
        assertEquals(HighFrequencyVolumeChurnEngine.DustReason.BELOW_MIN_NOTIONAL,
                belowBuffer.dustReason());
        assertTrue(atBuffer.sellable());
    }

    @Test
    void actualCommissionAndCostPerMillionAreRecordedOnceFromExecutionReport() {
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-1");
        atomic("lastBestBid", BigDecimal.class).set(new BigDecimal("0.60"));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("10"));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10.0"),
                decimalEquals("0.60"), anyString()))
                .thenReturn(new ObjectMapper().createObjectNode().put("orderId", 99L));
        AccountExecutionEvent fill = new AccountExecutionEvent(
                "test-account", "ENSOUSDT", 42L, 7001L, "churn-BUY-1", "BUY", "TRADE", "FILLED",
                new BigDecimal("10"), new BigDecimal("0.60"), new BigDecimal("10"),
                new BigDecimal("6.00"), new BigDecimal("0.006"), "USDT", true, System.currentTimeMillis());

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
    void priceDropNeverTriggersAutomaticMarketSell() {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeClientOrderId", String.class).set("churn-SELLG-active");
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis());

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine", new BigDecimal("0.59"), new BigDecimal("0.591"));

        verify(tradeService, never()).placeMarketSell(anyString(), any(), anyString());
        verify(tradeService, never()).cancelOrder("ENSOUSDT", 77L);
        assertEquals(77L, atomic("activeOrderId", Long.class).get());
    }

    @Test
    void sellingImmediatelyRestsAtActualBuyAverage() throws Exception {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6"));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.60"), anyString()))
                .thenReturn(new ObjectMapper().readTree("{\"orderId\":77}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.60"), anyString());
        assertEquals(77L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("买入均价下限"));
    }

    @Test
    void bidAskMakerEverySellTimeoutRollsRemainingPositionToBestAsk() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuantity", BigDecimal.class).set(new BigDecimal("10"));
        atomic("filledEntryQuoteQuantity", BigDecimal.class).set(new BigDecimal("6.0000"));
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeClientOrderId", String.class).set("churn-SELLG-old");
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 121_000);
        TradingRiskGuard guard = (TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard");
        guard.recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"),
                System.currentTimeMillis(), properties.getStrategy());
        when(tradeService.cancelOrder("ENSOUSDT", 77L))
                .thenReturn(mapper.readTree("{\"orderId\":77,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 77L)).thenReturn(mapper.readTree(
                "{\"orderId\":77,\"status\":\"CANCELED\",\"side\":\"SELL\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\"}"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(
                new BinanceAccountTradeClient.AssetBalance("ENSO", new BigDecimal("10"), BigDecimal.ZERO,
                        new BigDecimal("10")));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.6001"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelOrder("ENSOUSDT", 77L);
        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.6001"), anyString());
        verify(tradeService, never()).placeMarketSell(anyString(), any(), anyString());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("最新卖一"));

        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 121_000);
        when(tradeService.cancelOrder("ENSOUSDT", 88L))
                .thenReturn(mapper.readTree("{\"orderId\":88,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 88L)).thenReturn(mapper.readTree(
                "{\"orderId\":88,\"status\":\"CANCELED\",\"side\":\"SELL\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\"}"));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.5991"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":99}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.5990"), new BigDecimal("0.5991"));

        verify(tradeService).cancelOrder("ENSOUSDT", 88L);
        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.5991"), anyString());
        assertEquals(99L, atomic("activeOrderId", Long.class).get());
    }

    @Test
    void bidAskMakerWaitsAfterFlatSellBeforeOpeningNextBuy() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, null, null, null, null, null, null, 90_000L);
        engine.getIsRunning().set(true);

        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", false);

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("等待 90 秒"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                any(), any(), isNull(), anyString());
        assertTrue(engine.getStatusReason().get().contains("卖出后冷却中"));

        ((AtomicLong) ReflectionTestUtils.getField(engine, "postSellNextEntryAllowedAtMs"))
                .set(System.currentTimeMillis() - 1);
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), decimalEquals("10"), isNull(), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":101}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), decimalEquals("10"), isNull(), anyString());
        assertEquals(101L, atomic("activeOrderId", Long.class).get());
    }

    @Test
    void feeAwareMakerAlsoWaitsAfterFlatSellBeforeOpeningNextBuy() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.switchStrategy("ENSOUSDT", "FEE_AWARE_MAKER", new BigDecimal("6"),
                20_000L, 120_000L, new BigDecimal("10"), BigDecimal.ZERO,
                1_800_000L, BigDecimal.ZERO, BigDecimal.ZERO, null, 75_000L);
        engine.getIsRunning().set(true);

        ReflectionTestUtils.invokeMethod(engine, "completeFlatExit", false);

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("等待 75 秒"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                any(), any(), isNull(), anyString());
        assertTrue(engine.getStatusReason().get().contains("卖出后冷却中"));

        ((AtomicLong) ReflectionTestUtils.getField(engine, "postSellNextEntryAllowedAtMs"))
                .set(System.currentTimeMillis() - 1);
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), decimalEquals("10"), isNull(), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":101}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.6000"), new BigDecimal("0.6001"));

        verify(tradeService).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"),
                decimalEquals("0.6000"), decimalEquals("10"), isNull(), anyString());
        assertEquals(101L, atomic("activeOrderId", Long.class).get());
    }

    @Test
    void bidAskMakerTimedSellRollUsesBestAskBelowRestoredBuyAverage() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.switchStrategy("ENSOUSDT", "BID_ASK_MAKER", new BigDecimal("6"),
                20_000L, 120_000L);
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        atomic("activeOrderId", Long.class).set(77L);
        atomic("activeClientOrderId", String.class).set("churn-SELLG-old");
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 121_000);
        ((TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard"))
                .recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"),
                        System.currentTimeMillis(), properties.getStrategy());
        when(tradeService.cancelOrder("ENSOUSDT", 77L))
                .thenReturn(mapper.readTree("{\"orderId\":77,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 77L)).thenReturn(mapper.readTree(
                "{\"orderId\":77,\"status\":\"CANCELED\",\"side\":\"SELL\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\"}"));
        when(tradeService.getAssetBalance("ENSO")).thenReturn(
                new BinanceAccountTradeClient.AssetBalance("ENSO", new BigDecimal("10"), BigDecimal.ZERO,
                        new BigDecimal("10")));
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.5991"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":88}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.5990"), new BigDecimal("0.5991"));

        verify(tradeService).cancelOrder("ENSOUSDT", 77L);
        verify(tradeService).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("10"),
                decimalEquals("0.5991"), anyString());
        assertEquals(88L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("最新卖一"));
    }

    @Test
    void timedRollReconcilesAlreadyFilledLimitBeforeLateAccountEvent() throws Exception {
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
        userDataStreamReady.set(true);
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(new BigDecimal("7.08"));
        when(tradeService.placeMarketSell(eq("ENSOUSDT"), decimalEquals("7.0"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":90}"));

        HighFrequencyVolumeChurnEngine.LiquidationResult result = engine.liquidateExistingPosition();

        assertTrue(result.accepted());
        assertEquals(90L, result.orderId());
        assertEquals(0, new BigDecimal("7.0").compareTo(result.quantity()));
        assertFalse(engine.getIsRunning().get());
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
    void delayedSellTradesContinueRunningAfterThreeRetriesWhenExchangeIsFlat() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        atomic("holdingInventory", BigDecimal.class).set(new BigDecimal("10"));
        ((TradingRiskGuard) ReflectionTestUtils.getField(engine, "riskGuard"))
                .recordFill("BUY", new BigDecimal("10"), new BigDecimal("0.60"), 1, properties.getStrategy());
        ReflectionTestUtils.invokeMethod(engine, "trackOrder", 91L, "churn-SELLM-2",
                HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING);
        when(tradeService.getOrder("ENSOUSDT", 91L)).thenReturn(mapper.readTree("""
                {"orderId":91,"status":"FILLED","side":"SELL","executedQty":"10",
                 "cummulativeQuoteQty":"6.02","price":"0"}
                """));
        when(tradeService.getMyTrades("ENSOUSDT", 91L)).thenReturn(mapper.readTree("[]"));
        when(tradeService.getFreeAssetBalance("ENSO")).thenReturn(BigDecimal.ZERO);
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));

        ReflectionTestUtils.invokeMethod(engine, "reconcileTrackedOrder", 91L);
        ReflectionTestUtils.invokeMethod(engine, "reconcileTrackedOrder", 91L);
        assertEquals(91L, atomic("activeOrderId", Long.class).get());
        ReflectionTestUtils.invokeMethod(engine, "reconcileTrackedOrder", 91L);

        assertNull(atomic("activeOrderId", Long.class).get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getIsRunning().get());
        assertEquals(0, engine.getRiskSnapshot().positionQty().signum());
        assertEquals("运行中，等待入场信号", engine.getStatusReason().get());
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
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("7.0"),
                decimalEquals("0.85"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":92}"));

        ReflectionTestUtils.invokeMethod(engine, "reconcileTrackedOrder", 91L);

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(92L, atomic("activeOrderId", Long.class).get());
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
    void staleMarketOnStartReconnectsWithoutAutomaticStart() {
        WebSocket socket = mock(WebSocket.class);
        userDataStreamReady.set(true);
        atomic("activeMarketWebSocket", WebSocket.class).set(socket);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketDataTimestamp"))
                .set(System.currentTimeMillis() - 46_000);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "lastMarketFrameTimestamp"))
                .set(System.currentTimeMillis() - 46_000);
        ((java.util.concurrent.atomic.AtomicBoolean) ReflectionTestUtils.getField(engine, "reconnectScheduled"))
                .set(true);

        assertFalse(engine.startTrading());

        verify(socket).abort();
        assertTrue(engine.getStatusReason().get().contains("正在重连"));
    }

    @Test
    void initialMakerBuyIsPlacedAtBestBid() throws Exception {
        engine.getIsRunning().set(true);
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy())))
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
    void localIpWeightThrottleDefersEntryWithoutHaltingEngine() throws Exception {
        engine.getIsRunning().set(true);
        when(tradeService.cancelAndReplaceOrder(eq("ENSOUSDT"), eq("BUY"), any(), any(), isNull(), anyString()))
                .thenReturn(new ObjectMapper().readTree("""
                        {"localRateLimited":true,"usedWeight1m":4800,
                         "safeRequestWeightLimit1m":4800,"retryAfterMs":5000}
                        """));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        assertTrue(engine.getIsRunning().get());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertTrue(engine.getStatusReason().get().contains("暂缓新报单"));
        verify(tradeService, never()).getOpenOrders(anyString());
    }

    @Test
    void softSignalNoiseDoesNotImmediatelyCancelFreshBuy() {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-soft");
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis());
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy())))
                .thenReturn(MarketSignalEvaluator.EntryDecision.block("WEAK_TOP_OF_BOOK",
                        new BigDecimal("-0.01"), BigDecimal.ZERO, BigDecimal.ZERO,
                        BigDecimal.ZERO, BigDecimal.ZERO));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine", new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).cancelOrder("ENSOUSDT", 42L);
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
    }

    @Test
    void makerEntryTimeoutCancelsWithoutIocFallback() throws Exception {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-maker");
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.861"));
        atomic("targetEntryQuantity", BigDecimal.class).set(new BigDecimal("7.00"));
        atomic("filledEntryQuantity", BigDecimal.class).set(BigDecimal.ZERO);
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 5_500);
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy())))
                .thenReturn(MarketSignalEvaluator.EntryDecision.allow(
                        new BigDecimal("0.2"), new BigDecimal("0.1"), new BigDecimal("0.1"),
                        BigDecimal.ZERO, BigDecimal.ZERO));
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.cancelOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\"}"));
        when(tradeService.getOrder("ENSOUSDT", 42L))
                .thenReturn(mapper.readTree("{\"orderId\":42,\"status\":\"CANCELED\",\"executedQty\":\"0\"}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService).cancelOrder("ENSOUSDT", 42L);
        verify(tradeService, never()).placeLimitIocBuy(anyString(), any(), any(), anyString());
        ReflectionTestUtils.invokeMethod(engine, "reconcileCancelledEntry", 42L);
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.IDLE, engine.getCurrentStatus().get());
        assertNull(atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getIsRunning().get());
    }

    @Test
    void makerEntryPastTimeoutRemainsWhenItIsStillBestBid() {
        engine.getIsRunning().set(true);
        engine.getCurrentStatus().set(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING);
        atomic("activeOrderId", Long.class).set(42L);
        atomic("activeClientOrderId", String.class).set("churn-BUY-maker");
        atomic("activeOrderPrice", BigDecimal.class).set(new BigDecimal("0.862"));
        ((java.util.concurrent.atomic.AtomicLong) ReflectionTestUtils.getField(engine, "orderPlacedTimestamp"))
                .set(System.currentTimeMillis() - 25_000);

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).cancelOrder("ENSOUSDT", 42L);
        assertEquals(42L, atomic("activeOrderId", Long.class).get());
        assertTrue(engine.getStatusReason().get().contains("仍处于买一"));
    }

    @Test
    void partialMakerBuyCancelsRemainderAndSubmitsOneEntryPriceGtcSell() throws Exception {
        prepareRestingMakerOrder();
        ObjectMapper mapper = new ObjectMapper();
        atomic("lastBestBid", BigDecimal.class).set(new BigDecimal("0.862"));
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
        when(tradeService.placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("7.0"),
                decimalEquals("0.862"), anyString()))
                .thenReturn(mapper.readTree("{\"orderId\":99}"));

        ReflectionTestUtils.invokeMethod(engine, "driveChurnStateMachine",
                new BigDecimal("0.862"), new BigDecimal("0.863"));

        verify(tradeService, never()).placeLimitIocBuy(anyString(), any(), any(), anyString());
        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.BUYING, engine.getCurrentStatus().get());
        assertEquals(42L, atomic("activeOrderId", Long.class).get());

        AccountExecutionEvent partialFill = new AccountExecutionEvent(
                "test-account", "ENSOUSDT", 42L, 7001L, "churn-BUY-maker", "BUY", "TRADE", "PARTIALLY_FILLED",
                new BigDecimal("7.0"), new BigDecimal("0.862"), new BigDecimal("7.0"),
                new BigDecimal("6.034"), BigDecimal.ZERO, "USDT", true, System.currentTimeMillis());
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", partialFill);
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", partialFill);
        orderUpdate(42L, "churn-BUY-maker", "BUY", "CANCELED", "CANCELED",
                "0", "0", "0", "USDT");

        assertEquals(HighFrequencyVolumeChurnEngine.ChurnStatus.SELLING, engine.getCurrentStatus().get());
        assertEquals(99L, atomic("activeOrderId", Long.class).get());
        verify(tradeService, times(1)).placeLimitGtcSell(eq("ENSOUSDT"), decimalEquals("7.0"),
                decimalEquals("0.862"), anyString());
        verify(tradeService, never()).placeMarketSell(anyString(), any(), anyString());
        verify(tradeService, never()).cancelAndReplaceOrder(eq("ENSOUSDT"), eq("SELL"), any(),
                any(), any(), anyString());
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
        when(marketSignalEvaluator.evaluate(anyLong(), eq(properties.getStrategy())))
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
        AccountExecutionEvent update = new AccountExecutionEvent(
                "test-account", "ENSOUSDT", orderId, tradeId, clientOrderId, side, executionType, status,
                executedQuantity, executedPrice,
                cumulativeQuantity.getOrDefault(orderId, BigDecimal.ZERO),
                cumulativeQuote.getOrDefault(orderId, BigDecimal.ZERO),
                new BigDecimal(commission), commissionAsset, true, System.currentTimeMillis());
        ReflectionTestUtils.invokeMethod(engine, "onOrderUpdate", update);
    }

    private BigDecimal decimalEquals(String expected) {
        BigDecimal value = new BigDecimal(expected);
        return argThat(actual -> actual != null && actual.compareTo(value) == 0);
    }

    private SymbolRuleManager.SymbolRule rule(String stepSize, String minQty, String minNotional) {
        return new SymbolRuleManager.SymbolRule("ENSOUSDT", new BigDecimal("0.0001"),
                new BigDecimal(stepSize), new BigDecimal(minQty), new BigDecimal(minNotional));
    }

    private DailyTradeStatsStore.DailyStatsSnapshot dailyStatsWithPosition(String symbol, String quantity,
                                                                           String cost) {
        return new DailyTradeStatsStore.DailyStatsSnapshot(
                LocalDate.now(), "test-account", "test-bot", symbol,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                new BigDecimal(quantity), new BigDecimal(cost), 0, 0, true);
    }

    private DailyTradeStatsStore.DailyStatsSnapshot dailyStatsWithVolume(String totalVolume) {
        return new DailyTradeStatsStore.DailyStatsSnapshot(
                LocalDate.now(ZoneOffset.UTC), "test-account", "test-bot", "ENSOUSDT",
                BigDecimal.ZERO, BigDecimal.ZERO, new BigDecimal(totalVolume),
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, 0, 0, true);
    }

}
