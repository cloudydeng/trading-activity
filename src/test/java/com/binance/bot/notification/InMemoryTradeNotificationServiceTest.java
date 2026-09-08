package com.binance.bot.notification;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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

    @Test
    void fillListenersReceiveNewWebSocketEventsAndCanUnsubscribe() throws Exception {
        InMemoryTradeNotificationService service = new InMemoryTradeNotificationService();
        List<FillNotification> received = new ArrayList<>();
        AutoCloseable registration = service.addFillListener(received::add);
        FillNotification first = new FillNotification("account-a", "A", "ENSOUSDT", "BUY", 42, 7,
                "ta-a-B-1", BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ZERO, "USDT", 123);
        FillNotification second = new FillNotification("account-a", "A", "ENSOUSDT", "SELL", 43, 8,
                "ta-a-S-1", BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ZERO, "USDT", 456);

        service.notifyFill(first);
        registration.close();
        service.notifyFill(second);

        assertEquals(List.of(first), received);
    }

    @Test
    void openOrderSnapshotAndEventsReplaceUpdateAndRemoveOrders() {
        InMemoryTradeNotificationService service = new InMemoryTradeNotificationService();
        OpenOrderNotification initial = new OpenOrderNotification(
                "account-a", "A", "ENSOUSDT", "BUY", "LIMIT_MAKER", "NEW",
                "0.600000", "10", "0", 42, 100);
        OpenOrderNotification partial = new OpenOrderNotification(
                "account-a", "A", "ENSOUSDT", "BUY", "LIMIT_MAKER", "PARTIALLY_FILLED",
                "0.600000", "10", "4", 42, 101);
        OpenOrderNotification filled = new OpenOrderNotification(
                "account-a", "A", "ENSOUSDT", "BUY", "LIMIT_MAKER", "FILLED",
                "0.600000", "10", "10", 42, 102);

        service.replaceOpenOrders("account-a", List.of(initial));
        service.notifyOrderUpdate(partial);
        assertEquals(List.of(partial), service.currentOpenOrders());

        service.notifyOrderUpdate(filled);
        assertEquals(List.of(), service.currentOpenOrders());
    }

    @Test
    void openOrderListenersReceiveWholeCurrentSnapshot() throws Exception {
        InMemoryTradeNotificationService service = new InMemoryTradeNotificationService();
        List<List<OpenOrderNotification>> received = new ArrayList<>();
        AutoCloseable registration = service.addOpenOrderListener(received::add);
        OpenOrderNotification order = new OpenOrderNotification(
                "account-a", "A", "ENSOUSDT", "SELL", "LIMIT", "NEW",
                "0.601000", "10", "0", 43, 200);

        service.notifyOrderUpdate(order);
        registration.close();
        service.notifyOrderUpdate(new OpenOrderNotification(
                "account-a", "A", "ENSOUSDT", "SELL", "LIMIT", "CANCELED",
                "0.601000", "10", "0", 43, 201));

        assertEquals(List.of(List.of(order)), received);
    }
}
