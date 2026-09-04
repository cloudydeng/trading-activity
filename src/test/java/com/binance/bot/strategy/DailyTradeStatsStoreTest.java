package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DailyTradeStatsStoreTest {
    @TempDir Path tempDir;

    @Test
    void persistsDailyEconomicsAndDeduplicatesTradesAcrossRestart() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore first = new DailyTradeStatsStore(properties);
        long now = System.currentTimeMillis();

        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, first.recordTrade(
                "account-a", "huaqin-bot", "ENSOUSDT", 10, 1001, "BUY", new BigDecimal("10"),
                new BigDecimal("6.00"), new BigDecimal("0.006"), new BigDecimal("0.006"),
                new BigDecimal("0.006"), now));
        assertEquals(DailyTradeStatsStore.RecordResult.DUPLICATE, first.recordTrade(
                "account-a", "huaqin-bot", "ENSOUSDT", 10, 1001, "BUY", new BigDecimal("10"),
                new BigDecimal("6.00"), new BigDecimal("0.006"), new BigDecimal("0.006"),
                new BigDecimal("0.006"), now));
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, first.recordTrade(
                "account-a", "huaqin-bot", "ENSOUSDT", 11, 1002, "SELL", new BigDecimal("10"),
                new BigDecimal("6.02"), new BigDecimal("0.00602"), new BigDecimal("0.00602"),
                new BigDecimal("0.00602"), now));
        first.saveActiveSymbol("account-a", "BTCUSDT");
        first.close();

        BinanceProperties restartedProperties = properties();
        DailyTradeStatsStore restarted = new DailyTradeStatsStore(restartedProperties);
        DailyTradeStatsStore.DailyStatsSnapshot stats = restarted.today("account-a", "huaqin-bot", "ENSOUSDT");

        assertEquals("BTCUSDT", restarted.loadActiveSymbol("account-a").orElseThrow());
        assertDecimal("12.02", stats.totalVolumeQuote());
        assertDecimal("0.01202", stats.totalCommissionQuoteEquivalent());
        assertDecimal("1000", stats.costPerMillionVolume());
        assertDecimal("0.02", stats.realizedGrossPnlQuote());
        assertDecimal("0.00798", stats.netRealizedPnlQuote());
        assertEquals(2, stats.tradeCount());
        assertEquals(1, stats.roundTrips());
        restarted.close();
    }

    @Test
    void exchangeFlatReconciliationNormalizesSubStepDust() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties);
        long now = System.currentTimeMillis();
        store.recordTrade("account-b", "lee", "ENSOUSDT", 20, 2001, "BUY", new BigDecimal("10"),
                new BigDecimal("8.50"), BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, now);
        store.recordTrade("account-b", "lee", "ENSOUSDT", 21, 2002, "SELL", new BigDecimal("9.999"),
                new BigDecimal("8.509149"), BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, now);

        assertEquals(true, store.reconcileFlatDust("account-b", "ENSOUSDT", new BigDecimal("0.01")));
        DailyTradeStatsStore.DailyStatsSnapshot stats = store.today("account-b", "lee", "ENSOUSDT");
        assertDecimal("0", stats.positionQty());
        assertDecimal("0", stats.positionCostQuote());
        assertEquals(1, stats.roundTrips());
        store.close();
    }

    @Test
    void exchangeFlatReconciliationNormalizesTradeableLedgerRemainder() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties);
        long now = System.currentTimeMillis();
        store.recordTrade("account-b", "lee", "ENSOUSDT", 20, 2001, "BUY", new BigDecimal("10"),
                new BigDecimal("8.50"), BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, now);

        assertEquals(true, store.reconcileFlatDust("account-b", "ENSOUSDT", new BigDecimal("0.01")));
        DailyTradeStatsStore.DailyStatsSnapshot stats = store.today("account-b", "lee", "ENSOUSDT");
        assertDecimal("0", stats.positionQty());
        assertDecimal("0", stats.positionCostQuote());
        assertDecimal("-8.50", stats.realizedGrossPnlQuote());
        assertEquals(1, stats.roundTrips());
        store.close();
    }

    @Test
    void identicalTradeIdIsDeduplicatedPerAccountNotGlobally() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        long now = System.currentTimeMillis();
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-a", "A", "ENSOUSDT", 42, 7, "BUY", BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, now));
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-b", "B", "ENSOUSDT", 42, 7, "BUY", BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, now));
        assertEquals(1, store.today("account-a", "A", "ENSOUSDT").tradeCount());
        assertEquals(1, store.today("account-b", "B", "ENSOUSDT").tradeCount());
        store.close();
    }

    @Test
    void recentCalendarIncludesZeroVolumeDays() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        long now = System.currentTimeMillis();
        store.recordTrade("account-a", "huaqin-bot", "ENSOUSDT", 42, 7001, "BUY",
                new BigDecimal("10"), new BigDecimal("6.00"), BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, now);

        List<DailyTradeStatsStore.DailyStatsSnapshot> days = store.recentCalendar(
                "account-a", "huaqin-bot", "ENSOUSDT", 10);

        assertEquals(10, days.size());
        assertEquals(LocalDate.now(ZoneOffset.UTC), days.get(0).date());
        assertEquals(0, days.get(0).totalVolumeQuote().compareTo(new BigDecimal("6.00")));
        assertEquals(0, days.get(1).totalVolumeQuote().signum());
        assertEquals(0, days.get(9).totalVolumeQuote().signum());
        store.close();
    }

    @Test
    void accountVolumeSummaryAggregatesOnlyTheRequestedWindowAcrossSymbols() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        long now = System.currentTimeMillis();
        store.recordTrade("account-a", "huaqin-bot", "ENSOUSDT", 51, 5101, "BUY",
                new BigDecimal("10"), new BigDecimal("6.00"), BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, now);
        store.recordTrade("account-a", "huaqin-bot", "BTCUSDT", 52, 5201, "SELL",
                new BigDecimal("1"), new BigDecimal("8.00"), BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, now);
        store.recordTrade("account-b", "other", "ENSOUSDT", 53, 5301, "BUY",
                new BigDecimal("10"), new BigDecimal("100.00"), BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, now);

        DailyTradeStatsStore.AccountVolumeSummary summary = store.accountVolumeSummary(
                "account-a", "huaqin-bot", 10);

        assertEquals(List.of("BTCUSDT", "ENSOUSDT"), summary.symbols());
        assertDecimal("14.00", summary.totalVolumeQuote());
        assertDecimal("6.00", summary.buyVolumeQuote());
        assertDecimal("8.00", summary.sellVolumeQuote());
        assertEquals(2, summary.tradeCount());
        store.close();
    }

    @Test
    void migratesLegacyAliasSchemaWithoutDeletingHistoricalData() throws Exception {
        Path database = tempDir.resolve("legacy.db");
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE daily_trade_stats (
                      api_alias TEXT NOT NULL, trade_date TEXT NOT NULL, symbol TEXT NOT NULL,
                      buy_volume TEXT NOT NULL, sell_volume TEXT NOT NULL, total_volume TEXT NOT NULL,
                      commission_quote TEXT NOT NULL, economic_fee_quote TEXT NOT NULL,
                      realized_gross_pnl TEXT NOT NULL, position_qty TEXT NOT NULL,
                      position_cost_quote TEXT NOT NULL, trade_count INTEGER NOT NULL,
                      round_trips INTEGER NOT NULL, commission_complete INTEGER NOT NULL,
                      processed_trade_ids TEXT NOT NULL, updated_at INTEGER NOT NULL,
                      PRIMARY KEY (api_alias, trade_date, symbol))
                    """);
            statement.execute("""
                    INSERT INTO daily_trade_stats VALUES(
                      'legacy-bot', date('now'), 'ENSOUSDT', '6', '6.02', '12.02',
                      '0.012', '0.012', '0.02', '0', '0', 2, 1, 1, '[\"1:1\"]', 1)
                    """);
        }
        BinanceProperties properties = new BinanceProperties();
        properties.getStorage().setDailyStatsDb(database.toString());
        properties.setAccountProfilesJson("""
                {"primary":{"alias":"legacy-bot","apiKey":"key","secretKey":"secret"}}
                """);

        DailyTradeStatsStore store = new DailyTradeStatsStore(properties);

        DailyTradeStatsStore.DailyStatsSnapshot stats = store.today("primary", "legacy-bot", "ENSOUSDT");
        assertEquals(2, stats.tradeCount());
        assertDecimal("12.02", stats.totalVolumeQuote());
        assertEquals(0, store.today("legacy-bot", "legacy-bot", "ENSOUSDT").tradeCount());
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var row = connection.createStatement().executeQuery(
                     "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='daily_trade_stats_legacy_v1'")) {
            assertEquals(1, row.getInt(1));
        }
        store.close();
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
