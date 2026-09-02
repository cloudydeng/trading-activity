package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketSignalEvaluatorTest {
    private final BinanceProperties.Strategy config = new BinanceProperties.Strategy();

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
    void blocksNewEntryDuringShortTermDownwardMove() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_500);

        assertEquals("SHORT_TERM_DOWNMOVE", evaluator.evaluate(1_500, config).reason());
    }

    @Test
    void blocksNewEntryWhenAggressiveSellFlowDominates() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_500);
        evaluator.recordAggTrade(decimal("20"), true, 1_300, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_400, config);
        evaluator.recordAggTrade(decimal("20"), true, 1_500, config);

        assertEquals("SELL_TAKER_PRESSURE", evaluator.evaluate(1_500, config).reason());
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
    void requiresReclaimAfterSelloffBeforeAllowingAnotherEntry() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        config.setSignalLookbackMs(1_000);
        config.setPostSelloffCooldownMs(1_000);
        config.setMinPostSelloffReclaimBps(3);
        config.setMaxShortTermVolatilityBps(200);
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 0, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 500);
        assertEquals("SHORT_TERM_DOWNMOVE", evaluator.evaluate(500, config).reason());

        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_200, config);
        evaluator.recordQuote(decimal("99"), decimal("110"), decimal("100"), decimal("90"), 1_600, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_600);
        assertEquals("WAIT_FOR_PRICE_RECLAIM", evaluator.evaluate(1_600, config).reason());

        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_800, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_800);
        assertEquals("ALLOWED", evaluator.evaluate(1_800, config).reason());
    }

    private static BigDecimal decimal(String value) { return new BigDecimal(value); }
}
