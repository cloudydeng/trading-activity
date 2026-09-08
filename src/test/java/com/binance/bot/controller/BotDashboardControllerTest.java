package com.binance.bot.controller;

import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

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
}
