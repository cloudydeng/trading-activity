package com.binance.bot.strategy;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MarketActivityTrackerTest {
    private static final BigDecimal ORDER = new BigDecimal("12");

    @Test
    void distinguishesUnknownNormalActiveAndQuietWithoutUsingItAsEntryGate() {
        MarketActivityTracker tracker = new MarketActivityTracker();
        assertEquals("UNKNOWN", tracker.snapshot(1_000_000, ORDER, true).level());
        tracker.recordTrade(new BigDecimal("2"), new BigDecimal("10"), true, 1_000_000);
        MarketActivityTracker.Snapshot warming = tracker.snapshot(1_000_001, ORDER, true);
        assertEquals("UNKNOWN", warming.level());
        assertEquals(1, warming.aggregateTradeEvents1m());
        assertEquals(0, new BigDecimal("20").compareTo(warming.quoteVolume1m()));
        assertEquals(0, new BigDecimal("20").compareTo(warming.aggressiveSellQuote1m()));

        for (int i = 1; i < 20; i++) {
            tracker.recordTrade(new BigDecimal("2"), new BigDecimal("10"), false, 1_000_000 + i * 1_000L);
        }
        MarketActivityTracker.Snapshot active = tracker.snapshot(1_019_001, ORDER, true);
        assertEquals("ACTIVE", active.level());
        assertEquals(20, active.aggregateTradeEvents1m());
        assertEquals(0, new BigDecimal("400").compareTo(active.quoteVolume1m()));
        assertEquals(0, new BigDecimal("380").compareTo(active.aggressiveBuyQuote1m()));
        assertEquals("QUIET", tracker.snapshot(1_050_000, ORDER, true).level());
        tracker.recordTrade(new BigDecimal("2"), new BigDecimal("50"), false, 1_061_000);
        assertEquals("NORMAL", tracker.snapshot(1_061_001, ORDER, true).level());
        assertEquals("UNKNOWN", tracker.snapshot(1_061_001, ORDER, false).level());
    }

    @Test
    void bucketsAreBoundedAndResetClearsSamples() {
        MarketActivityTracker tracker = new MarketActivityTracker();
        tracker.recordTrade(BigDecimal.ONE, BigDecimal.TEN, true, 1_000_000);
        tracker.recordTrade(BigDecimal.ONE, BigDecimal.TEN, true, 1_300_000);
        MarketActivityTracker.Snapshot snapshot = tracker.snapshot(1_300_001, ORDER, true);
        assertEquals(1, snapshot.aggregateTradeEvents1m());
        assertEquals(1, snapshot.aggregateTradeEvents5m());
        tracker.reset();
        snapshot = tracker.snapshot(1_300_002, ORDER, true);
        assertEquals(0, snapshot.aggregateTradeEvents5m());
        assertNull(snapshot.lastTradeAgeMs());
    }

    @Test
    void quietNeedsEnoughObservationTimeWhenTradesAreSparse() {
        MarketActivityTracker tracker = new MarketActivityTracker();
        tracker.recordTrade(BigDecimal.ONE, BigDecimal.ONE, true, 1_000_000);
        tracker.recordTrade(BigDecimal.ONE, BigDecimal.ONE, true, 1_059_000);
        assertEquals("UNKNOWN", tracker.snapshot(1_059_001, ORDER, true).level());
        tracker.recordTrade(BigDecimal.ONE, BigDecimal.ONE, true, 1_060_000);
        assertEquals("QUIET", tracker.snapshot(1_060_001, ORDER, true).level());
    }
}
