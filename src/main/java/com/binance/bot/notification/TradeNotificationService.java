package com.binance.bot.notification;

import java.util.List;
import java.util.function.Consumer;

public interface TradeNotificationService {
    void notifyFill(FillNotification notification);
    List<FillNotification> recentFills(String accountId, int limit);
    List<FillNotification> recentFills(int limit);
    AutoCloseable addFillListener(Consumer<FillNotification> listener);
    void replaceOpenOrders(String accountId, List<OpenOrderNotification> orders);
    void notifyOrderUpdate(OpenOrderNotification order);
    List<OpenOrderNotification> currentOpenOrders();
    AutoCloseable addOpenOrderListener(Consumer<List<OpenOrderNotification>> listener);
}
