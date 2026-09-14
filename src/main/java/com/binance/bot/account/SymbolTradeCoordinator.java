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
 * just for the BUY or SELL order independently.
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

    /** Requests one of the symbol's complete-cycle slots while preserving FIFO order. */
    public EntryPermit acquire(String symbol, String engineId, String accountAlias) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
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
            Holder current = holders.get(normalizedEngineId);
            if (current != null && current.phase() == Phase.BUYING) {
                waiters.remove(normalizedEngineId);
                cleanupEmptyState(normalizedSymbol, holders, waiters);
                return new EntryPermit(true, "");
            }
            if (current != null && current.phase() == Phase.SELLING) {
                return new EntryPermit(false, normalizedSymbol + " 当前持仓正在卖出，不能再次买入");
            }
            if (current != null && current.phase() == Phase.HOLDING) {
                waiters.remove(normalizedEngineId);
                holders.put(normalizedEngineId, new Holder(current.engineId(), current.accountAlias(),
                        Phase.BUYING, current.acquiredAtMs(), System.currentTimeMillis()));
                cleanupEmptyState(normalizedSymbol, holders, waiters);
                return new EntryPermit(true, "");
            }

            waiters.putIfAbsent(normalizedEngineId, new Waiter(normalizedEngineId,
                    displayAlias(normalizedEngineId, accountAlias), System.currentTimeMillis()));
            String firstWaitingEngineId = waiters.keySet().iterator().next();
            boolean totalCapacityReached = current == null
                    ? holders.size() >= limit
                    : holders.size() > limit;
            if (totalCapacityReached || !normalizedEngineId.equals(firstWaitingEngineId)) {
                return new EntryPermit(false, waitingReason(normalizedSymbol, normalizedEngineId,
                        holders, waiters, limit));
            }

            Waiter waiter = waiters.remove(normalizedEngineId);
            long now = System.currentTimeMillis();
            long acquiredAt = current == null ? now : current.acquiredAtMs();
            holders.put(normalizedEngineId, new Holder(normalizedEngineId, waiter.accountAlias(),
                    Phase.BUYING, acquiredAt, now));
            cleanupEmptyState(normalizedSymbol, holders, waiters);
            return new EntryPermit(true, "");
        } finally {
            lock.unlock();
        }
    }

    /** Moves a completed BUY to SELLING while retaining the same complete-cycle slot. */
    public EntryPermit transitionToSell(String symbol, String engineId, String accountAlias) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) {
            return new EntryPermit(false, "同交易对卖出协调参数无效");
        }
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.get(normalizedSymbol);
            Holder current = holders.get(normalizedEngineId);
            long now = System.currentTimeMillis();
            if (current == null) {
                current = new Holder(normalizedEngineId, displayAlias(normalizedEngineId, accountAlias),
                        Phase.HOLDING, now, now);
                holders.put(normalizedEngineId, current);
            }
            if (waiters != null) waiters.remove(normalizedEngineId);
            holders.put(normalizedEngineId, new Holder(current.engineId(), current.accountAlias(),
                    Phase.SELLING, current.acquiredAtMs(), now));
            cleanupEmptyState(normalizedSymbol, holders, waiters);
            return new EntryPermit(true, "");
        } finally {
            lock.unlock();
        }
    }

    /** Marks retained dust/inventory while keeping the same complete-cycle slot. */
    public void markHolding(String symbol, String engineId, String accountAlias) {
        updateExistingPhase(symbol, engineId, Phase.HOLDING);
    }

    /** Records recovered inventory before it is transitioned to SELLING. */
    public void claimHolding(String symbol, String engineId, String accountAlias) {
        claimWithPhase(symbol, engineId, accountAlias, Phase.HOLDING);
    }

    /** Records an already-existing active SELL after restart without canceling it. */
    public void claimExistingSell(String symbol, String engineId, String accountAlias) {
        claimWithPhase(symbol, engineId, accountAlias, Phase.SELLING);
    }

    /** Backwards-compatible name for restored active cycles. */
    public void claimExisting(String symbol, String engineId, String accountAlias) {
        claimExistingSell(symbol, engineId, accountAlias);
    }

    public void release(String symbol, String engineId) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
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

    private void claimWithPhase(String symbol, String engineId, String accountAlias, Phase phase) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) return;
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.get(normalizedSymbol);
            if (waiters != null) waiters.remove(normalizedEngineId);
            Holder current = holders.get(normalizedEngineId);
            long now = System.currentTimeMillis();
            holders.put(normalizedEngineId, new Holder(normalizedEngineId,
                    current == null ? displayAlias(normalizedEngineId, accountAlias) : current.accountAlias(),
                    phase, current == null ? now : current.acquiredAtMs(), now));
            cleanupEmptyState(normalizedSymbol, holders, waiters);
        } finally {
            lock.unlock();
        }
    }

    private void updateExistingPhase(String symbol, String engineId, Phase phase) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) return;
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> waiters = waitersBySymbol.get(normalizedSymbol);
            Holder current = holders == null ? null : holders.get(normalizedEngineId);
            if (current == null) return;
            if (waiters != null) waiters.remove(normalizedEngineId);
            holders.put(normalizedEngineId, new Holder(current.engineId(), current.accountAlias(),
                    phase, current.acquiredAtMs(), System.currentTimeMillis()));
            cleanupEmptyState(normalizedSymbol, holders, waiters);
        } finally {
            lock.unlock();
        }
    }

    private String waitingReason(String symbol, String engineId, LinkedHashMap<String, Holder> holders,
                                 LinkedHashMap<String, Waiter> waiters, int limit) {
        int position = queuePosition(waiters, engineId);
        String occupied = describeHolders(holders);
        return symbol + " 同交易对已有 " + holders.size() + "/" + limit + " 个账户交易中"
                + (occupied.isBlank() ? "" : "：" + occupied)
                + "；FIFO 排队第 " + position + " 位";
    }

    private int queuePosition(LinkedHashMap<String, Waiter> waiters, String engineId) {
        int position = 1;
        for (String waitingEngineId : waiters.keySet()) {
            if (waitingEngineId.equals(engineId)) break;
            position++;
        }
        return position;
    }

    private void cleanupEmptyState(String symbol, LinkedHashMap<String, Holder> holders,
                                   LinkedHashMap<String, Waiter> waiters) {
        if (holders == null || holders.isEmpty()) holdersBySymbol.remove(symbol);
        if (waiters == null || waiters.isEmpty()) waitersBySymbol.remove(symbol);
    }

    private String describeHolders(LinkedHashMap<String, Holder> holders) {
        List<String> labels = new ArrayList<>();
        for (Holder holder : holders.values()) {
            labels.add(holder.accountAlias());
        }
        return String.join("、", labels);
    }

    private String displayAlias(String engineId, String accountAlias) {
        return accountAlias == null || accountAlias.isBlank() ? engineId : accountAlias;
    }

    private String normalizeSymbol(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase();
    }

    private String normalizeEngineId(String engineId) {
        return engineId == null ? "" : engineId.trim();
    }

    public enum Phase { BUYING, HOLDING, SELLING }
    public record EntryPermit(boolean accepted, String reason) { }
    public record Holder(String engineId, String accountAlias, Phase phase,
                         long acquiredAtMs, long phaseChangedAtMs) { }
    public record Waiter(String engineId, String accountAlias, long enqueuedAtMs) { }
    public record Snapshot(String symbol, List<Holder> holders, List<Waiter> waiters) { }
}
