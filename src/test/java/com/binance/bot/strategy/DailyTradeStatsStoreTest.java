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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DailyTradeStatsStoreTest {
    @TempDir Path tempDir;

    @Test
    void remembersLatestBuyFillAcrossAccountsRestartAndOlderReconciliation() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        long now = System.currentTimeMillis();
        assertEquals(true, store.latestBuyFill("BABYUSDT").isEmpty());
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-a", "A", "BABYUSDT", 1, 11, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, new BigDecimal("0.01340"), new BigDecimal("0.01340"),
                BigDecimal.ZERO, "USDT", BigDecimal.ZERO, BigDecimal.ZERO, now - 1_000));
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-b", "B", "BABYUSDT", 2, 12, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, new BigDecimal("0.01333"), new BigDecimal("0.01333"),
                BigDecimal.ZERO, "USDT", BigDecimal.ZERO, BigDecimal.ZERO, now));
        store.saveBuyPriceGapWaitStartedAt("BABYUSDT", now);
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-c", "C", "BABYUSDT", 3, 13, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, new BigDecimal("0.01200"), new BigDecimal("0.01200"),
                BigDecimal.ZERO, "USDT", BigDecimal.ZERO, BigDecimal.ZERO, now - 500));
        assertDecimal("0.01333", store.latestBuyFill("BABYUSDT").orElseThrow().price());
        assertEquals(now, store.latestBuyFill("BABYUSDT").orElseThrow().tradeTimeMs());
        assertEquals(now, store.loadBuyPriceGapWaitStartedAt("BABYUSDT").orElseThrow());
        store.close();

        DailyTradeStatsStore restarted = new DailyTradeStatsStore(properties());
        assertDecimal("0.01333", restarted.latestBuyFill("babyusdt").orElseThrow().price());
        assertEquals(true, restarted.latestBuyFill("VTHOUSDT").isEmpty());
        assertEquals(now, restarted.loadBuyPriceGapWaitStartedAt("BABYUSDT").orElseThrow());
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, restarted.recordTrade(
                "account-d", "D", "BABYUSDT", 4, 14, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, new BigDecimal("0.01335"), new BigDecimal("0.01335"),
                BigDecimal.ZERO, "USDT", BigDecimal.ZERO, BigDecimal.ZERO, now + 1_000));
        assertEquals(true, restarted.loadBuyPriceGapWaitStartedAt("BABYUSDT").isEmpty());
        restarted.close();
    }

    @Test
    void upgradesExistingBuyFillRowsToDurablePriceReference() throws Exception {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        long now = System.currentTimeMillis();
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-a", "A", "BABYUSDT", 1, 11, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, new BigDecimal("0.01340"), new BigDecimal("0.01340"),
                BigDecimal.ZERO, "USDT", BigDecimal.ZERO, BigDecimal.ZERO, now));
        store.close();
        try (var connection = DriverManager.getConnection(
                "jdbc:sqlite:" + tempDir.resolve("daily.db"))) {
            connection.createStatement().executeUpdate(
                    "DELETE FROM runtime_setting WHERE setting_key='last_buy_fill:BABYUSDT'");
        }
        DailyTradeStatsStore upgraded = new DailyTradeStatsStore(properties());
        assertDecimal("0.01340", upgraded.latestBuyFill("BABYUSDT").orElseThrow().price());
        upgraded.close();
        try (var connection = DriverManager.getConnection(
                "jdbc:sqlite:" + tempDir.resolve("daily.db"));
             var rows = connection.createStatement().executeQuery(
                     "SELECT setting_value FROM runtime_setting WHERE setting_key='last_buy_fill:BABYUSDT'")) {
            assertEquals(true, rows.next());
            assertEquals("0.01340", rows.getString(1));
        }
    }

    @Test
    void persistsNormalizedAccountSymbolsAcrossRestart() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore first = new DailyTradeStatsStore(properties);
        first.saveAccountSymbols("account-a", List.of(" promusdt ", "SAHARAUSDT", "PROMUSDT"));
        first.close();

        DailyTradeStatsStore restarted = new DailyTradeStatsStore(properties());
        assertEquals(List.of("PROMUSDT", "SAHARAUSDT"),
                restarted.loadAccountSymbols("account-a").orElseThrow());
        assertEquals(true, restarted.loadAccountSymbols("account-b").isEmpty());
        restarted.close();
    }

    @Test
    void rejectsEmptyOrOversizedAccountSymbolLists() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());

        assertThrows(IllegalArgumentException.class, () -> store.saveAccountSymbols("account-a", List.of()));
        assertThrows(IllegalArgumentException.class, () -> store.saveAccountSymbols("account-a",
                List.of("AUSDT", "BUSDT", "CUSDT", "DUSDT", "EUSDT", "FUSDT")));
        store.close();
    }

    @Test
    void persistsTradingRuntimeSettingsAcrossRestart() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore first = new DailyTradeStatsStore(properties);
        first.saveTradingRuntimeSettings(new DailyTradeStatsStore.TradingRuntimeSettings(
                3, Map.of(" rezusdt ", 0, "THEUSDT", 2)));
        first.close();

        DailyTradeStatsStore restarted = new DailyTradeStatsStore(properties());

        DailyTradeStatsStore.TradingRuntimeSettings settings =
                restarted.loadTradingRuntimeSettings().orElseThrow();
        assertEquals(3, settings.maxConcurrentEntriesPerSymbol());
        assertEquals(Map.of("REZUSDT", 0, "THEUSDT", 2),
                settings.maxConcurrentEntriesBySymbol());
        assertThrows(IllegalArgumentException.class, () -> restarted.saveTradingRuntimeSettings(
                new DailyTradeStatsStore.TradingRuntimeSettings(-1)));
        assertThrows(IllegalArgumentException.class, () -> restarted.saveTradingRuntimeSettings(
                new DailyTradeStatsStore.TradingRuntimeSettings(21)));
        restarted.close();
    }

    @Test
    void persistsDailyEconomicsAndDeduplicatesTradesAcrossRestart() throws Exception {
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
        try (var connection = DriverManager.getConnection(
                "jdbc:sqlite:" + tempDir.resolve("daily.db"));
             var columns = connection.createStatement().executeQuery(
                     "PRAGMA table_info(daily_trade_stats)")) {
            boolean oldJsonColumnPresent = false;
            while (columns.next()) {
                if ("processed_trade_ids".equals(columns.getString("name"))) {
                    oldJsonColumnPresent = true;
                }
            }
            assertEquals(false, oldJsonColumnPresent);
        }
        try (var connection = DriverManager.getConnection(
                "jdbc:sqlite:" + tempDir.resolve("daily.db"));
             var count = connection.createStatement().executeQuery(
                     "SELECT COUNT(*) FROM processed_trade")) {
            assertEquals(2, count.getInt(1));
        }
        restarted.close();
    }

    @Test
    void persistsCompleteFillDetailsAndExposesLatestTradeId() throws Exception {
        BinanceProperties properties = properties();
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties);
        long now = System.currentTimeMillis();

        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-a", "yanzi", "PROMUSDT", 9001, 7001, "SELL",
                new BigDecimal("2.5"), new BigDecimal("2.5"), new BigDecimal("5.61"),
                new BigDecimal("14.025"), new BigDecimal("0.00001"), "BNB",
                new BigDecimal("0.006"), new BigDecimal("0.006"), now));
        assertEquals(7001L, store.latestTradeId("account-a", "PROMUSDT",
                LocalDate.now(ZoneOffset.UTC)).orElseThrow());

        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + tempDir.resolve("daily.db"));
             var row = connection.createStatement().executeQuery("""
                     SELECT order_id, trade_id, side, price, quantity, quote_quantity,
                            commission, commission_asset, commission_quote
                     FROM trade_fill
                     """)) {
            assertEquals(true, row.next());
            assertEquals(9001L, row.getLong("order_id"));
            assertEquals(7001L, row.getLong("trade_id"));
            assertEquals("SELL", row.getString("side"));
            assertEquals("5.61", row.getString("price"));
            assertEquals("2.5", row.getString("quantity"));
            assertEquals("14.025", row.getString("quote_quantity"));
            assertEquals("0.00001", row.getString("commission"));
            assertEquals("BNB", row.getString("commission_asset"));
            assertEquals("0.006", row.getString("commission_quote"));
        }
        store.close();
    }

    @Test
    void startupPurgesOnlyFillDetailsOutsideTheTenDayUtcWindow() throws Exception {
        BinanceProperties properties = properties();
        DailyTradeStatsStore first = new DailyTradeStatsStore(properties);
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        long expiredAt = today.minusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long boundaryAt = today.minusDays(9).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long todayAt = today.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

        first.recordTrade("account-a", "yanzi", "PROMUSDT", 9101, 7101, "BUY",
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN,
                new BigDecimal("0.00001"), "BNB", new BigDecimal("0.006"),
                new BigDecimal("0.006"), expiredAt);
        first.recordTrade("account-a", "yanzi", "PROMUSDT", 9102, 7102, "BUY",
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN,
                new BigDecimal("0.00002"), "BNB", new BigDecimal("0.012"),
                new BigDecimal("0.012"), boundaryAt);
        first.recordTrade("account-a", "yanzi", "PROMUSDT", 9103, 7103, "BUY",
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN,
                new BigDecimal("0.00003"), "BNB", new BigDecimal("0.018"),
                new BigDecimal("0.018"), todayAt);
        first.close();

        DailyTradeStatsStore restarted = new DailyTradeStatsStore(properties());
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + tempDir.resolve("daily.db"));
             var statement = connection.createStatement()) {
            try (var count = statement.executeQuery("SELECT COUNT(*) FROM trade_fill")) {
                assertEquals(2, count.getInt(1));
            }
            try (var count = statement.executeQuery("SELECT COUNT(*) FROM daily_trade_stats")) {
                assertEquals(3, count.getInt(1));
            }
            try (var count = statement.executeQuery("SELECT COUNT(*) FROM processed_trade")) {
                assertEquals(3, count.getInt(1));
            }
        }
        List<DailyTradeStatsStore.AccountSymbolVolumeSummary> rows =
                restarted.accountSymbolVolumeSummaries("account-a", "yanzi", 10);
        assertEquals(1, rows.size());
        assertDecimal("0.00005", rows.get(0).totalCommissionBnb());
        restarted.close();
    }

    @Test
    void symbolFillsReturnEveryAccountFillNewestFirstAndClampToRetainedWindow() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        long earlier = today.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long later = earlier + 60_000L;
        store.recordTrade("account-a", "yanzi", "PROMUSDT", 9101, 7101, "BUY",
                new BigDecimal("4"), new BigDecimal("4"), new BigDecimal("2.5"), new BigDecimal("10"),
                new BigDecimal("0.00001"), "BNB", new BigDecimal("0.006"), new BigDecimal("0.006"), earlier);
        store.recordTrade("account-b", "huaqin-bot", "PROMUSDT", 9102, 7102, "SELL",
                new BigDecimal("4"), new BigDecimal("4"), new BigDecimal("2.6"), new BigDecimal("10.4"),
                new BigDecimal("0.00002"), "BNB", new BigDecimal("0.012"), new BigDecimal("0.012"), later);
        store.recordTrade("account-b", "huaqin-bot", "ENSOUSDT", 9103, 7103, "BUY",
                new BigDecimal("1"), new BigDecimal("1"), new BigDecimal("0.96"), new BigDecimal("0.96"),
                new BigDecimal("0.00003"), "BNB", new BigDecimal("0.018"), new BigDecimal("0.018"), earlier);

        List<DailyTradeStatsStore.SymbolFill> prom = store.symbolFills("PROMUSDT", today, today);
        assertEquals(2, prom.size());
        assertEquals("account-b", prom.get(0).accountId());
        assertEquals("SELL", prom.get(0).side());
        assertDecimal("2.6", prom.get(0).price());
        assertEquals("account-a", prom.get(1).accountId());
        assertEquals("BUY", prom.get(1).side());
        assertDecimal("4", prom.get(1).quantity());
        assertEquals(1, store.symbolFills("ENSOUSDT", today, today).size());
        assertEquals(3, store.symbolFills("", today, today).size());
        assertEquals(0, store.symbolFills("PROMUSDT", today.minusDays(30), today.minusDays(20)).size());
        assertEquals(3, store.symbolFills(List.of("promusdt", "ENSOUSDT"), today, today).size());
        assertEquals(2, store.symbolFills(List.of("PROMUSDT", "promusdt"), today, today).size());
        assertEquals(3, store.symbolFills(java.util.Arrays.asList("", null), today, today).size());
        assertEquals(1, store.symbolFills(List.of("ENSOUSDT", "NOSUCHUSDT"), today, today).size());
        store.close();
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
    void accountSymbolVolumeSummariesReturnOneRowPerSymbol() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        long now = System.currentTimeMillis();
        store.recordTrade("account-a", "huaqin-bot", "ENSOUSDT", 61, 6101, "BUY",
                new BigDecimal("10"), new BigDecimal("10"), new BigDecimal("0.60"),
                new BigDecimal("6.00"), new BigDecimal("0.00001"), "BNB",
                new BigDecimal("0.006"), new BigDecimal("0.006"), now);
        store.recordTrade("account-a", "huaqin-bot", "BTCUSDT", 62, 6201, "BUY",
                new BigDecimal("1"), new BigDecimal("1"), new BigDecimal("8.00"),
                new BigDecimal("8.00"), new BigDecimal("0.00002"), "BNB",
                new BigDecimal("0.012"), new BigDecimal("0.012"), now);

        List<DailyTradeStatsStore.AccountSymbolVolumeSummary> rows = store.accountSymbolVolumeSummaries(
                "account-a", "huaqin-bot", 10);

        assertEquals(2, rows.size());
        assertEquals(List.of("BTCUSDT", "ENSOUSDT"), rows.stream()
                .map(DailyTradeStatsStore.AccountSymbolVolumeSummary::symbol).toList());
        assertDecimal("8.00", rows.get(0).totalVolumeQuote());
        assertDecimal("0.00002", rows.get(0).totalCommissionBnb());
        assertDecimal("6.00", rows.get(1).totalVolumeQuote());
        assertDecimal("0.00001", rows.get(1).totalCommissionBnb());
        DailyTradeStatsStore.AccountSymbolVolumeSummary clamped = store.accountSymbolVolumeSummaries(
                "account-a", "huaqin-bot", 90).get(0);
        assertEquals(LocalDate.now(ZoneOffset.UTC).minusDays(9), clamped.startDate());
        assertEquals(LocalDate.now(ZoneOffset.UTC), clamped.endDate());
        store.close();
    }

    @Test
    void persistsAndLoadsRuntimeStrategyOverridesWithoutSecrets() {
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties());
        BinanceProperties.SymbolStrategyProfile profile = new BinanceProperties.SymbolStrategyProfile();
        profile.setMode("FEE_AWARE_MAKER");
        profile.setOrderAmountUsdt(new BigDecimal("6"));
        profile.setEntryTimeoutMs(20_000L);
        profile.setExitTimeoutMs(120_000L);
        profile.setMakerFeeBps(new BigDecimal("7.5"));
        profile.setTargetNetProfitBps(new BigDecimal("10"));
        profile.setEntryAnchorWaitMs(1_800_000L);
        profile.setMaxEntryAnchorDriftBps(new BigDecimal("8"));
        profile.setMaxCumulativeEntryAnchorDriftBps(new BigDecimal("5"));
        profile.setManualEntryAnchorPrice(new BigDecimal("0.5900"));
        profile.setBidAskInitialSellMarkupTicks(3);
        profile.setBidAskEntryBookLevel(3);
        profile.setDailyVolumeLimitUsdt(new BigDecimal("750"));

        store.saveStrategyOverride("account-a", "ensousdt", profile);

        Map<String, BinanceProperties.SymbolStrategyProfile> loaded =
                store.loadStrategyOverrides("account-a");
        assertEquals("FEE_AWARE_MAKER", loaded.get("ENSOUSDT").getMode());
        assertDecimal("6", loaded.get("ENSOUSDT").getOrderAmountUsdt());
        assertEquals(20_000L, loaded.get("ENSOUSDT").getEntryTimeoutMs());
        assertEquals(120_000L, loaded.get("ENSOUSDT").getExitTimeoutMs());
        assertDecimal("7.5", loaded.get("ENSOUSDT").getMakerFeeBps());
        assertDecimal("10", loaded.get("ENSOUSDT").getTargetNetProfitBps());
        assertEquals(1_800_000L, loaded.get("ENSOUSDT").getEntryAnchorWaitMs());
        assertDecimal("8", loaded.get("ENSOUSDT").getMaxEntryAnchorDriftBps());
        assertDecimal("5", loaded.get("ENSOUSDT").getMaxCumulativeEntryAnchorDriftBps());
        assertDecimal("0.5900", loaded.get("ENSOUSDT").getManualEntryAnchorPrice());
        assertEquals(3, loaded.get("ENSOUSDT").getBidAskInitialSellMarkupTicks());
        assertEquals(3, loaded.get("ENSOUSDT").getBidAskEntryBookLevel());
        assertDecimal("750", loaded.get("ENSOUSDT").getDailyVolumeLimitUsdt());
        store.close();
    }

    @Test
    void persistsLoadsAndClearsRuntimeStateAcrossRestart() {
        BinanceProperties properties = properties();
        DailyTradeStatsStore first = new DailyTradeStatsStore(properties);
        first.saveRuntimeState("account-a", "ENSOUSDT", new DailyTradeStatsStore.RuntimeState(
                "account-a", "ENSOUSDT", "SELLING", 77L, "ta-a-S-1", "SELL",
                new BigDecimal("0.6013"), new BigDecimal("0.5995"),
                new BigDecimal("10"), new BigDecimal("0.6000"),
                1234L, 5678L, new BigDecimal("0.5990"),
                List.of(new BigDecimal("0.5980"), new BigDecimal("0.6000")), 150_000L));
        first.close();

        DailyTradeStatsStore restarted = new DailyTradeStatsStore(properties);
        DailyTradeStatsStore.RuntimeState state = restarted.loadRuntimeState("account-a", "ENSOUSDT").orElseThrow();
        assertEquals(77L, state.orderId());
        assertEquals("ta-a-S-1", state.clientOrderId());
        assertDecimal("0.6013", state.orderPrice());
        assertDecimal("0.5995", state.previousBuyOrderPrice());
        assertDecimal("0.6000", state.feeAwareEntryPriceCeiling());
        assertDecimal("0.5990", state.feeAwareInitialEntryAnchorPrice());
        assertEquals(2, state.feeAwareRecentBuyPrices().size());
        assertEquals(150_000L, state.orderTimeoutMs());

        restarted.clearRuntimeState("account-a", "ENSOUSDT");
        assertEquals(true, restarted.loadRuntimeState("account-a", "ENSOUSDT").isEmpty());
        restarted.close();
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
        assertEquals(DailyTradeStatsStore.RecordResult.DUPLICATE, store.recordTrade(
                "primary", "legacy-bot", "ENSOUSDT", 1, 1, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                System.currentTimeMillis()));
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var tables = connection.createStatement().executeQuery(
                     "SELECT name FROM sqlite_master WHERE type='table'")) {
            java.util.Set<String> names = new java.util.HashSet<>();
            while (tables.next()) names.add(tables.getString(1));
            assertEquals(true, names.contains("daily_trade_stats_legacy_v1"));
            assertEquals(true, names.contains("daily_trade_stats"));
            assertEquals(true, names.contains("processed_trade"));
        }
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             var columns = connection.createStatement().executeQuery(
                     "PRAGMA table_info(daily_trade_stats)")) {
            boolean oldJsonColumnPresent = false;
            while (columns.next()) {
                if ("processed_trade_ids".equals(columns.getString("name"))) {
                    oldJsonColumnPresent = true;
                }
            }
            assertEquals(false, oldJsonColumnPresent);
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
