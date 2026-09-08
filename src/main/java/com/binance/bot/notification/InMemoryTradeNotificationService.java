package com.binance.bot.notification;

import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Bounded per-account notification queues; there is intentionally no global broadcast channel. */
@Service
public class InMemoryTradeNotificationService implements TradeNotificationService {
    private static final int MAX_PER_ACCOUNT = 200;
    private final ConcurrentMap<String, Deque<FillNotification>> fillsByAccount = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<FillNotification>> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void notifyFill(FillNotification notification) {
        Deque<FillNotification> queue = fillsByAccount.computeIfAbsent(
                notification.accountId(), ignored -> new ArrayDeque<>());
        synchronized (queue) {
            queue.addFirst(notification);
            while (queue.size() > MAX_PER_ACCOUNT) queue.removeLast();
        }
        for (Consumer<FillNotification> listener : listeners) {
            try {
                listener.accept(notification);
            } catch (RuntimeException ignored) {
                // A dashboard listener must never interfere with accounting or order handling.
            }
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

    @Override
    public List<FillNotification> recentFills(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, MAX_PER_ACCOUNT));
        List<FillNotification> result = new ArrayList<>();
        for (Deque<FillNotification> queue : fillsByAccount.values()) {
            synchronized (queue) {
                result.addAll(queue);
            }
        }
        result.sort(Comparator.comparingLong(FillNotification::eventTime).reversed());
        if (result.size() > safeLimit) result = result.subList(0, safeLimit);
        return List.copyOf(result);
    }

    @Override
    public AutoCloseable addFillListener(Consumer<FillNotification> listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }
}
