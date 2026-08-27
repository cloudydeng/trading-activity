package com.binance.bot.strategy;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PostFillOutcomeTrackerTest {
    @Test
    void recordsHorizonReturnsAndRecoveryAfterABuyFill() {
        PostFillOutcomeTracker tracker = new PostFillOutcomeTracker();
        tracker.recordBuyFill(decimal("100"), "ALLOWED", 0);
        tracker.recordMarketPrice(decimal("99"), 1_000);
        tracker.recordMarketPrice(decimal("101"), 5_000);
        tracker.recordMarketPrice(decimal("102"), 15_000);
        tracker.recordMarketPrice(decimal("103"), 60_000);

        PostFillOutcomeTracker.OutcomeSummary summary = tracker.getSummary();

        assertEquals(1, summary.completedObservations());
        assertEquals(new BigDecimal("1"), summary.bounceProbability60s());
        assertEquals(5_000L, summary.medianRecoveryMs());
        assertNotNull(summary.lastOutcome());
        assertEquals(new BigDecimal("-100"), summary.lastOutcome().maxAdverseBps());
    }

    private static BigDecimal decimal(String value) { return new BigDecimal(value); }
}
