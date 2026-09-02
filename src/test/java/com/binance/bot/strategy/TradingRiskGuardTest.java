package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradingRiskGuardTest {
    @Test
    void blocksNewEntriesWhenProjectedInventoryExceedsLimit() {
        BinanceProperties.Strategy config = defaults();
        config.setMaxInventoryUsdt(decimal("25"));
        TradingRiskGuard guard = new TradingRiskGuard();

        assertTrue(guard.permitsNewEntry(decimal("0.2"), decimal("100"), 0, config));
        assertFalse(guard.permitsNewEntry(decimal("0.3"), decimal("100"), 0, config));
        assertEquals("MAX_INVENTORY_USDT", guard.getEntryBlockReason());
    }

    @Test
    void includesEstimatedFeesAndTripsDrawdownOnAdverseMark() {
        BinanceProperties.Strategy config = defaults();
        config.setMaxDailyDrawdownUsdt(decimal("1"));
        config.setAssumedMakerFeeBps(decimal("10"));
        TradingRiskGuard guard = new TradingRiskGuard();

        guard.recordFill("BUY", decimal("1"), decimal("100"), 0, config);
        guard.recordMark(decimal("98"), 1_000, config);

        assertEquals("MAX_DAILY_DRAWDOWN", guard.getEntryBlockReason());
        assertEquals(0, decimal("0.100").compareTo(guard.snapshot().estimatedFeesUsdt()));
        assertTrue(guard.snapshot().estimatedNetPnlUsdt().compareTo(decimal("-2")) < 0);
    }

    @Test
    void tripsHoldingTimeLimitButKeepsPositionVisible() {
        BinanceProperties.Strategy config = defaults();
        config.setMaxInventoryAgeMs(1_000);
        TradingRiskGuard guard = new TradingRiskGuard();

        guard.recordFill("BUY", decimal("1"), decimal("100"), 0, config);
        guard.recordMark(decimal("100"), 1_000, config);

        assertEquals("MAX_INVENTORY_AGE", guard.getEntryBlockReason());
        assertEquals(decimal("1"), guard.snapshot().positionQty());
    }

    @Test
    void clearsInventoryAgeBlockAfterForcedExitAndAllowsNextEntry() {
        BinanceProperties.Strategy config = defaults();
        config.setMaxInventoryAgeMs(1_000);
        TradingRiskGuard guard = new TradingRiskGuard();

        guard.recordFill("BUY", decimal("1"), decimal("100"), 0, config);
        guard.recordMark(decimal("100"), 1_000, config);
        assertEquals("MAX_INVENTORY_AGE", guard.getEntryBlockReason());

        guard.recordFill("SELL", decimal("1"), decimal("100"), 1_001, config);

        assertEquals(null, guard.getEntryBlockReason());
        assertEquals(BigDecimal.ZERO, guard.snapshot().positionQty());
        assertTrue(guard.permitsNewEntry(decimal("0.1"), decimal("100"), 1_002, config));
    }

    @Test
    void exchangeFlatReconciliationClearsUntradeableLedgerDustAndAgeBlock() {
        BinanceProperties.Strategy config = defaults();
        config.setMaxInventoryAgeMs(1_000);
        TradingRiskGuard guard = new TradingRiskGuard();

        guard.recordFill("BUY", decimal("1"), decimal("100"), 0, config);
        guard.recordFill("SELL", decimal("0.999"), decimal("100"), 500, config);
        guard.recordMark(decimal("100"), 1_000, config);
        assertEquals("MAX_INVENTORY_AGE", guard.getEntryBlockReason());
        assertEquals(decimal("0.001"), guard.snapshot().positionQty());

        guard.reconcileExchangeFlat(1_001, config);

        assertEquals(null, guard.getEntryBlockReason());
        assertEquals(BigDecimal.ZERO, guard.snapshot().positionQty());
        assertEquals(BigDecimal.ZERO, guard.snapshot().positionCostUsdt());
        assertEquals(-1, guard.snapshot().positionOpenedAtMs());
        assertTrue(guard.permitsNewEntry(decimal("0.1"), decimal("100"), 1_002, config));
    }

    private static BinanceProperties.Strategy defaults() { return new BinanceProperties.Strategy(); }
    private static BigDecimal decimal(String value) { return new BigDecimal(value); }
}
