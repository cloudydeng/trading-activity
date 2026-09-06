package com.binance.bot.notification;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryTradeNotificationServiceTest {
    @Test
    void fillNotificationIsVisibleOnlyToOwningAccount() {
        InMemoryTradeNotificationService service = new InMemoryTradeNotificationService();
        service.notifyFill(new FillNotification("account-a", "A", "ENSOUSDT", "BUY", 42, 7,
                "ta-a-B-1", BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ZERO, "USDT", 123));

        assertEquals(1, service.recentFills("account-a", 10).size());
        assertEquals(0, service.recentFills("account-b", 10).size());
    }

    @Test
    void globalRecentFillsMixesAccountsByNewestFirst() {
        InMemoryTradeNotificationService service = new InMemoryTradeNotificationService();
        service.notifyFill(new FillNotification("account-a", "A", "ENSOUSDT", "BUY", 42, 7,
                "ta-a-B-1", BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ZERO, "USDT", 123));
        service.notifyFill(new FillNotification("account-b", "B", "ZKCUSDT", "SELL", 43, 8,
                "ta-b-S-1", BigDecimal.TEN, BigDecimal.ONE, BigDecimal.TEN,
                BigDecimal.ZERO, "USDT", 456));

        var fills = service.recentFills(10);

        assertEquals(2, fills.size());
        assertEquals("account-b", fills.get(0).accountId());
        assertEquals("account-a", fills.get(1).accountId());
    }
}
