package com.binance.bot.notification;

import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Bounded per-account notification queues; there is intentionally no global broadcast channel. */
@Service
public class InMemoryTradeNotificationService implements TradeNotificationService {
    private static final int MAX_PER_ACCOUNT = 200;
    private final ConcurrentMap<String, Deque<FillNotification>> fillsByAccount = new ConcurrentHashMap<>();

    @Override
    public void notifyFill(FillNotification notification) {
        Deque<FillNotification> queue = fillsByAccount.computeIfAbsent(
                notification.accountId(), ignored -> new ArrayDeque<>());
        synchronized (queue) {
            queue.addFirst(notification);
            while (queue.size() > MAX_PER_ACCOUNT) queue.removeLast();
        }
    }

    @Override
    public List<FillNotification> recentFills(String accountId, int limit) {
        Deque<FillNotification> queue = fillsByAccount.get(accountId);
        if (queue == null) return List.of();
        int safeLimit = Math.max(1, Math.min(limit, MAX_PER_ACCOUNT));
        synchronized (queue) {
            List<FillNotification> result = new ArrayList<>(Math.min(queue.size(), safeLimit));
            for (FillNotification fill : queue) {
                if (result.size() == safeLimit) break;
                result.add(fill);
            }
            return List.copyOf(result);
        }
    }
}
