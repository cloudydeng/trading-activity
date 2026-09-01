package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DailyTradeStatsStoreTest {
    @TempDir Path tempDir;

    @Test
    void persistsDailyEconomicsAndDeduplicatesTradesAcrossRestart() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore first = new DailyTradeStatsStore(properties);
        long now = System.currentTimeMillis();

        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, first.recordTrade(
                "huaqin-bot", "ENSOUSDT", 10, 1001, "BUY", new BigDecimal("10"),
                new BigDecimal("6.00"), new BigDecimal("0.006"), new BigDecimal("0.006"),
                new BigDecimal("0.006"), now));
        assertEquals(DailyTradeStatsStore.RecordResult.DUPLICATE, first.recordTrade(
                "huaqin-bot", "ENSOUSDT", 10, 1001, "BUY", new BigDecimal("10"),
                new BigDecimal("6.00"), new BigDecimal("0.006"), new BigDecimal("0.006"),
                new BigDecimal("0.006"), now));
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, first.recordTrade(
                "huaqin-bot", "ENSOUSDT", 11, 1002, "SELL", new BigDecimal("10"),
                new BigDecimal("6.02"), new BigDecimal("0.00602"), new BigDecimal("0.00602"),
                new BigDecimal("0.00602"), now));
        first.saveActiveSymbol("BTCUSDT");
        first.close();

        BinanceProperties restartedProperties = properties();
        DailyTradeStatsStore restarted = new DailyTradeStatsStore(restartedProperties);
        DailyTradeStatsStore.DailyStatsSnapshot stats = restarted.today("huaqin-bot", "ENSOUSDT");

        assertEquals("BTCUSDT", restartedProperties.getStrategy().getSymbol());
        assertDecimal("12.02", stats.totalVolumeQuote());
        assertDecimal("0.01202", stats.totalCommissionQuoteEquivalent());
        assertDecimal("1000", stats.costPerMillionVolume());
        assertDecimal("0.02", stats.realizedGrossPnlQuote());
        assertDecimal("0.00798", stats.netRealizedPnlQuote());
        assertEquals(2, stats.tradeCount());
        assertEquals(1, stats.roundTrips());
        restarted.close();
    }

    private BinanceProperties properties() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStrategy().setSymbol("ENSOUSDT");
        properties.getStorage().setDailyStatsDb(tempDir.resolve("daily.db").toString());
        return properties;
    }

    private void assertDecimal(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
    }
}
