package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketSignalEvaluatorTest {
    private final BinanceProperties.Strategy config = new BinanceProperties.Strategy();

    @Test
    void permitsBalancedMarketOnlyWhenBidDepthAndMicropriceSupportIt() {
        MarketSignalEvaluator evaluator = new MarketSignalEvaluator();
        evaluator.recordQuote(decimal("100"), decimal("100"), decimal("101"), decimal("100"), 1_000, config);
        evaluator.recordQuote(decimal("100"), decimal("110"), decimal("101"), decimal("90"), 1_500, config);
        evaluator.recordDepth(decimal("1100"), decimal("900"), 1_500);

        MarketSignalEvaluator.EntryDecision decision = evaluator.evaluate(1_500, config);

        assertTrue(decision.allowed());
        assertEquals("ALLOWED", decision.reason());
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
        evaluator.recordAggTrade(decimal("20"), true, 1_500, config);

        assertEquals("SELL_TAKER_PRESSURE", evaluator.evaluate(1_500, config).reason());
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

    private static BigDecimal decimal(String value) { return new BigDecimal(value); }
}
