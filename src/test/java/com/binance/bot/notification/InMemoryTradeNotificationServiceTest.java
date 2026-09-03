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
}
