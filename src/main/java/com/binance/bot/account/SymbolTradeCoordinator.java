package com.binance.bot.account;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JVM-wide coordinator that limits how many account runtimes may manage the same
 * symbol at the same time. A slot is held for the whole entry/exit cycle, not
 * just for the HTTP BUY submission.
 */
@Component
public final class SymbolTradeCoordinator {
    /**
     * Fair lock defines the ordering of truly concurrent first requests; the per-symbol
     * waiting maps below preserve that order across later retries.
     */
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Map<String, LinkedHashMap<String, Holder>> holdersBySymbol = new LinkedHashMap<>();
    private final Map<String, LinkedHashMap<String, Waiter>> waitersBySymbol = new LinkedHashMap<>();
    private final AtomicInteger maxConcurrentEntriesPerSymbol = new AtomicInteger(1);

    public void configureMaxConcurrentEntriesPerSymbol(int value) {
        maxConcurrentEntriesPerSymbol.set(Math.max(1, Math.min(20, value)));
    }

    public int maxConcurrentEntriesPerSymbol() {
        return maxConcurrentEntriesPerSymbol.get();
    }

    public EntryPermit acquire(String symbol, String engineId, String accountAlias) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = engineId == null ? "" : engineId.trim();
        int limit = maxConcurrentEntriesPerSymbol.get();
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) {
            return new EntryPermit(false, "同交易对交易协调参数无效");
        }
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            if (holders.containsKey(normalizedEngineId)) {
                waiters.remove(normalizedEngineId);
                cleanupEmptyState(normalizedSymbol, holders, waiters);
                return new EntryPermit(true, "");
            }

            waiters.putIfAbsent(normalizedEngineId, new Waiter(normalizedEngineId,
                    accountAlias == null || accountAlias.isBlank() ? normalizedEngineId : accountAlias,
                    System.currentTimeMillis()));
            String firstWaitingEngineId = waiters.keySet().iterator().next();
            if (holders.size() >= limit || !normalizedEngineId.equals(firstWaitingEngineId)) {
                return new EntryPermit(false, waitingReason(normalizedSymbol, normalizedEngineId,
                        holders, waiters, limit));
            }

            Waiter waiter = waiters.remove(normalizedEngineId);
            holders.put(normalizedEngineId, new Holder(normalizedEngineId,
                    waiter.accountAlias(),
                    System.currentTimeMillis()));
            cleanupEmptyState(normalizedSymbol, holders, waiters);
            return new EntryPermit(true, "");
        } finally {
            lock.unlock();
        }
    }

    public void release(String symbol, String engineId) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = engineId == null ? "" : engineId.trim();
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) return;
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.get(normalizedSymbol);
            if (holders != null) holders.remove(normalizedEngineId);
            if (waiters != null) waiters.remove(normalizedEngineId);
            cleanupEmptyState(normalizedSymbol, holders, waiters);
        } finally {
            lock.unlock();
        }
    }

    /** Records an already-existing active cycle, for example a restored SELL after restart. */
    public void claimExisting(String symbol, String engineId, String accountAlias) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = engineId == null ? "" : engineId.trim();
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) return;
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.get(normalizedSymbol);
            if (waiters != null) waiters.remove(normalizedEngineId);
            holders.putIfAbsent(normalizedEngineId, new Holder(normalizedEngineId,
                    accountAlias == null || accountAlias.isBlank() ? normalizedEngineId : accountAlias,
                    System.currentTimeMillis()));
            cleanupEmptyState(normalizedSymbol, holders, waiters);
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot(String symbol) {
        String normalizedSymbol = normalizeSymbol(symbol);
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.get(normalizedSymbol);
            return new Snapshot(normalizedSymbol,
                    holders == null ? List.of() : List.copyOf(holders.values()),
                    waiters == null ? List.of() : List.copyOf(waiters.values()));
        } finally {
            lock.unlock();
        }
    }

    private String waitingReason(String symbol, String engineId, LinkedHashMap<String, Holder> holders,
                                 LinkedHashMap<String, Waiter> waiters, int limit) {
        int position = 1;
        for (String waitingEngineId : waiters.keySet()) {
            if (waitingEngineId.equals(engineId)) break;
            position++;
        }
        String occupied = describeHolders(holders);
        return symbol + " 同交易对已有 " + holders.size() + "/" + limit + " 个账户交易中"
                + (occupied.isBlank() ? "" : "：" + occupied)
                + "；FIFO 排队第 " + position + " 位";
    }

    private void cleanupEmptyState(String symbol, LinkedHashMap<String, Holder> holders,
                                   LinkedHashMap<String, Waiter> waiters) {
        if (holders == null || holders.isEmpty()) holdersBySymbol.remove(symbol);
        if (waiters == null || waiters.isEmpty()) waitersBySymbol.remove(symbol);
    }

    private String describeHolders(LinkedHashMap<String, Holder> holders) {
        List<String> labels = new ArrayList<>();
        for (Holder holder : holders.values()) labels.add(holder.accountAlias());
        return String.join("、", labels);
    }

    private String normalizeSymbol(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase();
    }

    public record EntryPermit(boolean accepted, String reason) { }
    public record Holder(String engineId, String accountAlias, long acquiredAtMs) { }
    public record Waiter(String engineId, String accountAlias, long enqueuedAtMs) { }
    public record Snapshot(String symbol, List<Holder> holders, List<Waiter> waiters) { }
}
