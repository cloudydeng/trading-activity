package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketSignalEvaluatorTest {
    private final BinanceProperties.Strategy config = new BinanceProperties.Strategy();

    @Test
    void minuteAveragesRequireConsecutiveFreshHistoryAndUseCurrentMinuteClose() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        long firstMinute = 60_000L;
        for (int i = 0; i < 24; i++) {
            evaluator.recordMinuteClose(firstMinute + i * 60_000L,
                    decimal(i < 18 ? "100" : "90"), firstMinute + i * 60_000L);
        }
        assertNull(evaluator.minuteMovingAverages(firstMinute + 24 * 60_000L + 1_000L, 5_000L));

        long currentMinute = firstMinute + 24 * 60_000L;
        evaluator.recordMinuteClose(currentMinute, decimal("90"), currentMinute + 1_000L);
        MarketSignalEvaluator.MinuteMovingAverages averages = evaluator.minuteMovingAverages(
                currentMinute + 2_000L, 5_000L);
        assertEquals(0, decimal("90").compareTo(averages.ma7()));
        assertEquals(0, decimal("97.2").compareTo(averages.ma25()));

        evaluator.recordMinuteClose(currentMinute, decimal("80"), currentMinute + 3_000L);
        averages = evaluator.minuteMovingAverages(currentMinute + 4_000L, 5_000L);
        assertEquals(0, decimal("88.57142857142857").compareTo(averages.ma7()));
        assertEquals(0, decimal("96.8").compareTo(averages.ma25()));
        assertNull(evaluator.minuteMovingAverages(currentMinute + 10_000L, 5_000L));
        evaluator.reset();
        assertNull(evaluator.minuteMovingAverages(currentMinute + 4_000L, 5_000L));
    }

    @Test
    void minuteAveragesRejectGaps() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        for (int i = 0; i < 25; i++) {
            long minute = (i + (i > 12 ? 1 : 0)) * 60_000L;
            evaluator.recordMinuteClose(minute, decimal("100"), minute + 1_000L);
        }
        assertNull(evaluator.minuteMovingAverages(26 * 60_000L + 2_000L, 5_000L));
    }

    @Test
    void permitsBalancedMarketWhenBidDepthSupportsIt() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_500);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluate(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("ALLOWED", decision.reason());
    }

    @Test
    void blocksWhenTopBookNotionalIsTooThinForOrderSize() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setOrderAmountUsdt(decimal("12"));
        config.setMinTopBookNotionalMultiplier(1.0);
        evaluator.recordQuote(decimal("1"), decimal("8"), decimal("1.01"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("1"), decimal("8"), decimal("1.01"), decimal("100"), 1_500, config);
        evaluator.recordDepth(decimal("1000"), decimal("1000"), 1_500);

        assertEquals("THIN_TOP_OF_BOOK", evaluator.evaluate(1_500, config).reason());
    }

    @Test
    void blocksWhenFiveLevelDepthNotionalIsTooThinForOrderSize() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setOrderAmountUsdt(decimal("12"));
        config.setMinDepthNotionalMultiplier(1.0);
        evaluator.recordQuote(decimal("1"), decimal("100"), decimal("1.01"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("1"), decimal("100"), decimal("1.01"), decimal("100"), 1_500, config);
        evaluator.recordDepth(decimal("8"), decimal("1000"), 1_500);

        assertEquals("THIN_DEPTH_BOOK", evaluator.evaluate(1_500, config).reason());
    }

    @Test
    void permitsMildTopBookAskImbalanceRegardlessOfDepthDirection() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setMinBookImbalance(-0.35);
        evaluator.recordQuote(decimal("100"), decimal("80"), decimal("101"), decimal("120"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("80"), decimal("101"), decimal("120"), 1_500, config);
        evaluator.recordDepth(decimal("600"), decimal("1400"), 1_500);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluate(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("ALLOWED", decision.reason());
        assertEquals(0, decimal("-0.4").compareTo(decision.depthImbalance()));
    }

    @Test
    void ignoresShortTermDownwardMoveForEntry() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setMaxShortTermVolatilityBps(200);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_500);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluate(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("ALLOWED", decision.reason());
        assertTrue(decision.returnBps().signum() < 0);
    }

    @Test
    void ignoresAggressiveSellFlowForEntry() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_500);
        evaluator.recordAggTrade(decimal("20"), true, 1_300, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_400, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_500, config);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluate(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("ALLOWED", decision.reason());
        assertTrue(decision.takerFlowImbalance().signum() < 0);
    }

    @Test
    void bestBidMakerIgnoresAggressiveSellFlowWithoutFullFeeAwareBookGate() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordQuote(decimal("100"), decimal("1"), decimal("101"), decimal("200"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("1"), decimal("101"), decimal("200"), 1_500, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_300, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_400, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_500, config);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluateBestBidMaker(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("BEST_BID_MAKER", decision.reason());
        assertTrue(decision.takerFlowImbalance().signum() < 0);
    }

    @Test
    void bestBidMakerAllowsThinBookWhenSellPressureGateIsClean() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setOrderAmountUsdt(decimal("12"));
        config.setMinTopBookNotionalMultiplier(1.0);
        config.setMinDepthNotionalMultiplier(1.0);
        evaluator.recordQuote(decimal("1"), decimal("1"), decimal("1.01"), decimal("1"), 1_000, config);
        evaluator.recordQuote(decimal("1"), decimal("1"), decimal("1.01"), decimal("1"), 1_500, config);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluateBestBidMaker(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("BEST_BID_MAKER", decision.reason());
    }

    @Test
    void treatsSparseSingleSellAsNeutralFlow() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setMinTakerFlowSamples(3);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("600"), decimal("1400"), 1_500);
        evaluator.recordAggTrade(decimal("20"), true, 1_500, config);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluate(1_500, config);

        assertTrue(decision.allowed());
        assertEquals(0, BigDecimal.ZERO.compareTo(decision.takerFlowImbalance()));
    }

    @Test
    void expiresTradeFlowWhenNoNewTradesArrive() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordAggTrade(decimal("20"), true, 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 4_000, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 4_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 4_500);

        assertEquals("ALLOWED", evaluator.evaluate(4_500, config).reason());
    }

    @Test
    void depthUsesIndependentFreshnessWindow() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setMarketDataStaleMs(1_000);
        config.setDepthDataStaleMs(2_500);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_000);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 2_900, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 3_000, config);

        assertEquals("ALLOWED", evaluator.evaluate(3_000, config).reason());
        assertEquals("STALE_DEPTH_DATA", evaluator.evaluate(3_501, config).reason());
    }

    @Test
    void ignoresSelloffReclaimBeforeAllowingAnotherEntry() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setSignalLookbackMs(1_000);
        config.setPostSelloffCooldownMs(1_000);
        config.setMinPostSelloffReclaimBps(3);
        config.setMaxShortTermVolatilityBps(200);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 0, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 500);
        assertEquals("ALLOWED", evaluator.evaluate(500, config).reason());

        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_200, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_600, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_600);
        assertEquals("ALLOWED", evaluator.evaluate(1_600, config).reason());

        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_800, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_800);
        assertEquals("ALLOWED", evaluator.evaluate(1_800, config).reason());
    }

    @Test
    void bestBidMakerIgnoresReclaimAfterShortTermDownmove() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setSignalLookbackMs(1_000);
        config.setPostSelloffCooldownMs(1_000);
        config.setMinPostSelloffReclaimBps(3);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 0, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 500, config);
        assertEquals("BEST_BID_MAKER", evaluator.evaluateBestBidMaker(500, config).reason());

        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_200, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_600, config);
        assertEquals("BEST_BID_MAKER", evaluator.evaluateBestBidMaker(1_600, config).reason());

        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_800, config);
        assertEquals("BEST_BID_MAKER", evaluator.evaluateBestBidMaker(1_800, config).reason());
    }

    private static BigDecimal decimal(String value) { return new BigDecimal(value); }
}
