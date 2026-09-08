package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void allOpenOrdersSkipsStoppedStrategies() {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        BinanceAccountTradeClient tradeClient = mock(BinanceAccountTradeClient.class);
        when(accountManager.runtimes()).thenReturn(List.of(runtime));
        when(runtime.engine()).thenReturn(engine);
        when(runtime.tradeClient()).thenReturn(tradeClient);
        when(engine.getIsRunning()).thenReturn(new AtomicBoolean(false));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        var result = controller.allOpenOrders();

        assertEquals(List.of(), result.get("orders"));
        verifyNoInteractions(tradeClient);
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
}
