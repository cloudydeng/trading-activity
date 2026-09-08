package com.binance.bot.notification;

import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** In-memory dashboard state with account-isolated fill queues and order snapshots. */
@Service
public class InMemoryTradeNotificationService implements TradeNotificationService {
    private static final int MAX_PER_ACCOUNT = 200;
    private final ConcurrentMap<String, Deque<FillNotification>> fillsByAccount = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<FillNotification>> listeners = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, Map<Long, OpenOrderNotification>> openOrdersByAccount =
            new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<List<OpenOrderNotification>>> openOrderListeners =
            new CopyOnWriteArrayList<>();

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

    @Override
    public void replaceOpenOrders(String accountId, List<OpenOrderNotification> orders) {
        if (accountId == null || accountId.isBlank()) return;
        Map<Long, OpenOrderNotification> replacement = new HashMap<>();
        if (orders != null) {
            for (OpenOrderNotification order : orders) {
                if (order != null && order.valid() && accountId.equals(order.accountId()) && order.active()) {
                    replacement.put(order.orderId(), order);
                }
            }
        }
        if (orders != null && !orders.isEmpty() && replacement.isEmpty()) {
            // A non-empty exchange response in which every row is invalid is not a trustworthy
            // "no orders" snapshot. Keep this account's previous valid state.
            return;
        }
        // One atomic map replacement per account. Readers can see either the old or new account
        // snapshot, never an incomplete half-replaced snapshot.
        openOrdersByAccount.put(accountId, Map.copyOf(replacement));
        publishOpenOrders();
    }

    @Override
    public void notifyOrderUpdate(OpenOrderNotification order) {
        if (order == null || !order.valid()) return;
        openOrdersByAccount.compute(order.accountId(), (ignored, current) -> {
            Map<Long, OpenOrderNotification> updated = new HashMap<>(
                    current == null ? Map.of() : current);
            if (order.active()) updated.put(order.orderId(), order);
            else updated.remove(order.orderId());
            return Map.copyOf(updated);
        });
        publishOpenOrders();
    }

    @Override
    public List<OpenOrderNotification> currentOpenOrders() {
        List<OpenOrderNotification> snapshot = new ArrayList<>();
        for (Map<Long, OpenOrderNotification> accountOrders : openOrdersByAccount.values()) {
            if (accountOrders == null) continue;
            for (OpenOrderNotification order : accountOrders.values()) {
                if (order != null && order.valid()) snapshot.add(order);
            }
        }
        snapshot.sort(Comparator.comparingLong(OpenOrderNotification::timeMs).reversed()
                .thenComparing(OpenOrderNotification::accountAlias, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(OpenOrderNotification::symbol, String.CASE_INSENSITIVE_ORDER));
        return List.copyOf(snapshot);
    }

    @Override
    public AutoCloseable addOpenOrderListener(Consumer<List<OpenOrderNotification>> listener) {
        openOrderListeners.add(listener);
        return () -> openOrderListeners.remove(listener);
    }

    private void publishOpenOrders() {
        List<OpenOrderNotification> snapshot = currentOpenOrders();
        for (Consumer<List<OpenOrderNotification>> listener : openOrderListeners) {
            try {
                listener.accept(snapshot);
            } catch (RuntimeException ignored) {
                // Dashboard listeners must never interfere with order handling.
            }
        }
    }
}
