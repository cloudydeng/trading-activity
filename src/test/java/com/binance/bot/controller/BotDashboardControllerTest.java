package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.DailyTradeStatsStore;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BotDashboardControllerTest {
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
    void accountDetailsDoNotQueryExchangeWhileStrategyIsStopped() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        BinanceAccountTradeClient tradeClient = mock(BinanceAccountTradeClient.class);
        when(accountManager.find("account-a")).thenReturn(Optional.of(runtime));
        when(runtime.engine()).thenReturn(engine);
        when(runtime.tradeClient()).thenReturn(tradeClient);
        when(engine.getIsRunning()).thenReturn(new AtomicBoolean(false));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        ResponseEntity<?> response = controller.account("account-a");

        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        verifyNoInteractions(tradeClient);
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
                        new BigDecimal("23.9"), new BigDecimal("0.02"), null,
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
}
