package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.SymbolTradeCoordinator;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.DailyTradeStatsStore;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class BotDashboardControllerTest {
    @Test
    void lossSaleExportIncludesFilteredRowsAndBeijingTimes() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        long buy = Instant.parse("2026-10-01T00:00:00Z").toEpochMilli();
        long sell = buy + 90_000;
        when(store.lossSaleExportRows("account-a", "PUMPUSDC")).thenReturn(List.of(
                new DailyTradeStatsStore.LossSale("account-a", "=alias", "PUMPUSDC",
                        12, 34, buy, sell, 90_000L, new BigDecimal("2"),
                        new BigDecimal("3"), new BigDecimal("2"), new BigDecimal("2"))));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService, store,
                new SymbolTradeCoordinator());

        ResponseEntity<byte[]> response = controller.exportLossSales("account-a", "PUMPUSDC");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        String csv = new String(response.getBody(), StandardCharsets.UTF_8);
        assertEquals(true, csv.startsWith("\uFEFF账户别名,账户ID"));
        assertEquals(true, csv.contains("\"'=alias\",\"account-a\",\"PUMPUSDC\""));
        assertEquals(true, csv.contains("\"2026-10-01 08:00:00\",\"2026-10-01 08:01:30\",\"90\""));
        assertEquals(true, csv.contains("\"USDC\",\"34\",\"12\""));
        verify(store).lossSaleExportRows("account-a", "PUMPUSDC");
    }

    @Test
    void tradingSettingsCanBeSavedAndAppliedImmediately() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService, store, coordinator);

        ResponseEntity<?> response = controller.updateTradingSettings(
                new BotDashboardController.TradingSettingsRequest(2));

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(2, coordinator.maxConcurrentEntriesPerSymbol());
        verify(store).saveTradingRuntimeSettings(new DailyTradeStatsStore.TradingRuntimeSettings(2));
    }

    @Test
    void symbolTradingSettingPreservesDefaultAndOtherOverrides() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        when(store.loadTradingRuntimeSettings()).thenReturn(Optional.of(
                new DailyTradeStatsStore.TradingRuntimeSettings(
                        3, Map.of("THEUSDT", 2))));
        when(accountManager.configuredTradingSymbols()).thenReturn(List.of("REZUSDT", "THEUSDT"));
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService, store, coordinator);

        ResponseEntity<?> response = controller.updateTradingSettings(
                new BotDashboardController.TradingSettingsRequest(0, " rezusdt "));

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(3, coordinator.maxConcurrentEntriesPerSymbol());
        assertEquals(0, coordinator.maxConcurrentEntriesPerSymbol("REZUSDT"));
        assertEquals(2, coordinator.maxConcurrentEntriesPerSymbol("THEUSDT"));
        verify(store).saveTradingRuntimeSettings(new DailyTradeStatsStore.TradingRuntimeSettings(
                3, Map.of("REZUSDT", 0, "THEUSDT", 2)));
    }

    @Test
    void tradingSettingsRemoveOverridesForSymbolsNoAccountUses() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        when(accountManager.configuredTradingSymbols()).thenReturn(List.of("THEUSDT"));
        when(store.loadTradingRuntimeSettings()).thenReturn(Optional.of(
                new DailyTradeStatsStore.TradingRuntimeSettings(
                        2, Map.of("REZUSDT", 1, "THEUSDT", 3))));
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService, store, coordinator);

        BotDashboardController.TradingSettingsView result = controller.tradingSettings();

        assertEquals(Map.of("THEUSDT", 3), result.maxConcurrentEntriesBySymbol());
        assertEquals(2, coordinator.maxConcurrentEntriesPerSymbol("REZUSDT"));
        assertEquals(3, coordinator.maxConcurrentEntriesPerSymbol("THEUSDT"));
        verify(store).saveTradingRuntimeSettings(new DailyTradeStatsStore.TradingRuntimeSettings(
                2, Map.of("THEUSDT", 3)));
    }

    @Test
    void accountSymbolConfigurationCanBeReadAndUpdated() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        TradingAccountManager.AccountSymbolsConfiguration configuration =
                new TradingAccountManager.AccountSymbolsConfiguration(
                        "account-a", "A", List.of("PROMUSDT", "SAHARAUSDT"),
                        List.of("PROMUSDT"), true, true, "");
        when(accountManager.accountSymbolsConfiguration("account-a")).thenReturn(Optional.of(configuration));
        when(accountManager.updateAccountSymbols("account-a", List.of("PROMUSDT", "SAHARAUSDT")))
                .thenReturn(new TradingAccountManager.SymbolsUpdateResult(true, "已保存", configuration));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        ResponseEntity<?> read = controller.configuredSymbols("account-a");
        ResponseEntity<?> update = controller.updateConfiguredSymbols("account-a",
                new BotDashboardController.SymbolsConfigurationRequest(List.of("PROMUSDT", "SAHARAUSDT")));

        assertEquals(HttpStatus.OK, read.getStatusCode());
        assertEquals(configuration, read.getBody());
        assertEquals(HttpStatus.OK, update.getStatusCode());
        verify(accountManager).updateAccountSymbols("account-a", List.of("PROMUSDT", "SAHARAUSDT"));
    }

    @Test
    void globalRecentFillsComeOnlyFromWebSocketMemory() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        FillNotification fill = new FillNotification("account-a", "A", "ENSOUSDT", "BUY",
                101, 11, "ta-a-B-1", BigDecimal.TEN, new BigDecimal("0.6000"),
                new BigDecimal("6"), new BigDecimal("0.006"), "USDT", 100);
        when(notificationService.recentFills(10)).thenReturn(List.of(fill));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        List<FillNotification> result = controller.allNotifications(10);

        assertEquals(List.of(fill), result);
        verifyNoInteractions(accountManager);
    }

    @Test
    void allOpenOrdersComesOnlyFromWebSocketMemory() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        when(notificationService.currentOpenOrders()).thenReturn(List.of());
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        var result = controller.allOpenOrders();

        assertEquals(List.of(), result.get("orders"));
        verifyNoInteractions(accountManager);
    }

    @Test
    void accountDetailsQueryExchangeWhileStrategyIsStopped() throws Exception {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        BinanceAccountTradeClient tradeClient = mock(BinanceAccountTradeClient.class);
        ObjectMapper mapper = new ObjectMapper();
        when(accountManager.find("account-a")).thenReturn(Optional.of(runtime));
        when(runtime.engine()).thenReturn(engine);
        when(runtime.tradeClient()).thenReturn(tradeClient);
        when(runtime.accountId()).thenReturn("account-a");
        when(runtime.alias()).thenReturn("A");
        when(engine.getSymbol()).thenReturn("HOLOUSDT");
        when(tradeClient.getAccountInfo()).thenReturn(mapper.readTree(
                "{\"accountType\":\"SPOT\",\"canTrade\":true,\"balances\":[]}"));
        when(tradeClient.getAllOrders("HOLOUSDT", 100)).thenReturn(mapper.readTree("[]"));
        when(tradeClient.getOpenOrders("HOLOUSDT")).thenReturn(mapper.readTree("[]"));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        ResponseEntity<?> response = controller.account("account-a");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        verify(tradeClient).getAccountInfo();
        verify(tradeClient).getAllOrders("HOLOUSDT", 100);
        verify(tradeClient).getOpenOrders("HOLOUSDT");
    }

    @Test
    void allAssetsReadsEachAccountOnceAndKeepsPartialFailuresVisible() throws Exception {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime healthy = mock(AccountTradingRuntime.class);
        AccountTradingRuntime unavailable = mock(AccountTradingRuntime.class);
        BinanceAccountTradeClient healthyClient = mock(BinanceAccountTradeClient.class);
        BinanceAccountTradeClient unavailableClient = mock(BinanceAccountTradeClient.class);
        when(accountManager.runtimes()).thenReturn(List.of(healthy, unavailable));
        when(healthy.accountId()).thenReturn("account-a");
        when(healthy.alias()).thenReturn("Alpha");
        when(healthy.tradeClient()).thenReturn(healthyClient);
        when(unavailable.accountId()).thenReturn("account-b");
        when(unavailable.alias()).thenReturn("Beta");
        when(unavailable.tradeClient()).thenReturn(unavailableClient);
        when(healthyClient.getAccountInfo()).thenReturn(new ObjectMapper().readTree("""
                {"balances":[
                  {"asset":"USDT","free":"0.00000000","locked":"0.00000000"},
                  {"asset":"BABY","free":"1.20000000","locked":"0.30000000"},
                  {"asset":"BNB","free":"0.00000001","locked":"0.00000000"}
                ]}
                """));
        when(unavailableClient.getAccountInfo()).thenThrow(new IllegalStateException("unavailable"));
        when(accountManager.summaries()).thenReturn(List.of(new TradingAccountManager.AccountSummary(
                "account-c", "account-c", "Gamma", 0, false, false,
                "INITIALIZATION_FAILED", "unavailable", null, null, false, "unavailable")));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        BotDashboardController.AccountAssetsSnapshot result = controller.allAssets();

        assertEquals(3, result.accounts().size());
        assertEquals("Alpha", result.accounts().get(0).accountAlias());
        assertNull(result.accounts().get(0).error());
        assertEquals(List.of("BABY", "BNB"), result.accounts().get(0).balances().stream()
                .map(BotDashboardController.BalanceView::asset).toList());
        assertEquals("1.50000000", result.accounts().get(0).balances().get(0).total());
        assertEquals("账户余额暂时不可用", result.accounts().get(1).error());
        assertEquals("账户初始化失败", result.accounts().get(2).error());
        verify(healthyClient).getAccountInfo();
        verify(unavailableClient).getAccountInfo();
        verifyNoMoreInteractions(healthyClient, unavailableClient);
    }

    @Test
    void todaySummaryKeepsHealthyAccountsWhenOneAccountFails() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime healthy = mock(AccountTradingRuntime.class);
        AccountTradingRuntime broken = mock(AccountTradingRuntime.class);
        HighFrequencyVolumeChurnEngine healthyEngine = mock(HighFrequencyVolumeChurnEngine.class);
        HighFrequencyVolumeChurnEngine brokenEngine = mock(HighFrequencyVolumeChurnEngine.class);
        when(healthy.accountId()).thenReturn("account-a");
        when(healthy.alias()).thenReturn("healthy");
        when(healthy.engine()).thenReturn(healthyEngine);
        when(broken.accountId()).thenReturn("account-b");
        when(broken.alias()).thenReturn("broken");
        when(broken.engine()).thenReturn(brokenEngine);
        when(accountManager.runtimes()).thenReturn(List.of(healthy, broken));
        when(healthyEngine.getAccountSymbolVolumeSummaries(1)).thenReturn(List.of(
                new DailyTradeStatsStore.AccountSymbolVolumeSummary(
                        "account-a", "healthy", "SAHARAUSDT", java.time.LocalDate.now(),
                        java.time.LocalDate.now(), new BigDecimal("12"), new BigDecimal("11.9"),
                        new BigDecimal("23.9"), new BigDecimal("0.02"), new BigDecimal("0.00003"), null,
                        new BigDecimal("-0.08"), new BigDecimal("-0.10"), 2, 1, true)));
        when(brokenEngine.getAccountSymbolVolumeSummaries(1))
                .thenThrow(new IllegalStateException("damaged row"));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        BotDashboardController.TodayTradingSummary result = controller.todayTradingSummary();

        assertEquals(2, result.accounts().size());
        BotDashboardController.TodayAccountTradingSummary brokenResult = result.accounts().get(0);
        BotDashboardController.TodayAccountTradingSummary healthyResult = result.accounts().get(1);
        assertEquals("broken", brokenResult.accountAlias());
        assertEquals("今日统计暂时不可用", brokenResult.error());
        assertEquals(List.of(), brokenResult.symbols());
        assertEquals("healthy", healthyResult.accountAlias());
        assertNull(healthyResult.error());
        assertEquals(1, healthyResult.symbols().size());
        assertEquals(new BigDecimal("23.9"), result.totalVolumeQuote());
        assertEquals(new BigDecimal("0.02"), result.totalCommissionQuoteEquivalent());
        assertEquals(new BigDecimal("-0.10"), result.netRealizedPnlQuote());
        assertEquals(new BigDecimal("0.10"), result.totalLossQuote());
    }

    @Test
    void symbolStartControlsOnlyTheRequestedEngine() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        HighFrequencyVolumeChurnEngine btc = mock(HighFrequencyVolumeChurnEngine.class);
        when(accountManager.find("account-a")).thenReturn(Optional.of(runtime));
        when(runtime.engine("BTCUSDT")).thenReturn(Optional.of(btc));
        when(btc.getSymbol()).thenReturn("BTCUSDT");
        when(runtime.start("BTCUSDT")).thenReturn(true);
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        ResponseEntity<?> response = controller.startSymbol("account-a", "BTCUSDT");

        assertEquals(HttpStatus.OK, response.getStatusCode());
        verify(runtime).start("BTCUSDT");
    }
}
