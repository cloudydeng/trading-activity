package com.binance.bot.notification;

import java.util.List;

public interface TradeNotificationService {
    void notifyFill(FillNotification notification);
    List<FillNotification> recentFills(String accountId, int limit);
    List<FillNotification> recentFills(int limit);
}
