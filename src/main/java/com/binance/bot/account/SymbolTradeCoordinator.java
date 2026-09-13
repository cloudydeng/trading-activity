package com.binance.bot.account;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JVM-wide, per-symbol pipeline shared by all account runtimes.
 *
 * <p>The configured concurrency is split as evenly as possible between BUY and
 * SELL lanes, with the odd extra lane assigned to SELL (and one shared cycle
 * lane when the configured concurrency is one).
 * Positions whose BUY has completed keep their total-concurrency slot while
 * waiting for a SELL lane in a strict per-symbol FIFO queue.</p>
 */
@Component
public final class SymbolTradeCoordinator {
    /**
     * Fair lock defines the ordering of truly concurrent first requests; the per-symbol
     * waiting maps below preserve that order across later retries.
     */
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Map<String, LinkedHashMap<String, Holder>> holdersBySymbol = new LinkedHashMap<>();
    private final Map<String, LinkedHashMap<String, Waiter>> buyWaitersBySymbol = new LinkedHashMap<>();
    private final Map<String, LinkedHashMap<String, Waiter>> sellWaitersBySymbol = new LinkedHashMap<>();
    private final AtomicInteger maxConcurrentEntriesPerSymbol = new AtomicInteger(1);

    public void configureMaxConcurrentEntriesPerSymbol(int value) {
        maxConcurrentEntriesPerSymbol.set(Math.max(1, Math.min(20, value)));
    }

    public int maxConcurrentEntriesPerSymbol() {
        return maxConcurrentEntriesPerSymbol.get();
    }

    /** Requests one of the symbol's BUY lanes while preserving BUY FIFO order. */
    public EntryPermit acquire(String symbol, String engineId, String accountAlias) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
        int limit = maxConcurrentEntriesPerSymbol.get();
        int buyLimit = buyLaneLimit(limit);
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) {
            return new EntryPermit(false, "同交易对交易协调参数无效");
        }
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> buyWaiters = buyWaitersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> sellWaiters = sellWaitersBySymbol.get(normalizedSymbol);
            Holder current = holders.get(normalizedEngineId);
            if (current != null && current.phase() == Phase.BUYING) {
                buyWaiters.remove(normalizedEngineId);
                cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
                return new EntryPermit(true, "");
            }
            if (current != null && (current.phase() == Phase.SELLING
                    || current.phase() == Phase.WAITING_TO_SELL)) {
                return new EntryPermit(false, normalizedSymbol + " 当前持仓正在卖出或等待卖出，不能再次买入");
            }

            buyWaiters.putIfAbsent(normalizedEngineId, new Waiter(normalizedEngineId,
                    displayAlias(normalizedEngineId, accountAlias), System.currentTimeMillis()));
            String firstWaitingEngineId = buyWaiters.keySet().iterator().next();
            boolean buyLaneOccupied = countPhase(holders, Phase.BUYING) >= buyLimit;
            boolean totalCapacityReached = current == null
                    ? holders.size() >= limit
                    : holders.size() > limit;
            if (buyLaneOccupied || totalCapacityReached || !normalizedEngineId.equals(firstWaitingEngineId)) {
                return new EntryPermit(false, buyWaitingReason(normalizedSymbol, normalizedEngineId,
                        holders, buyWaiters, buyLimit, limit));
            }

            Waiter waiter = buyWaiters.remove(normalizedEngineId);
            long now = System.currentTimeMillis();
            long acquiredAt = current == null ? now : current.acquiredAtMs();
            holders.put(normalizedEngineId, new Holder(normalizedEngineId, waiter.accountAlias(),
                    Phase.BUYING, acquiredAt, now));
            if (sellWaiters != null) sellWaiters.remove(normalizedEngineId);
            cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
            return new EntryPermit(true, "");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves a completed BUY into the symbol's SELL lanes. When all SELL lanes are
     * occupied, the position remains a holder and enters the strict SELL FIFO.
     */
    public EntryPermit acquireSell(String symbol, String engineId, String accountAlias) {
        String normalizedSymbol = normalizeSymbol(symbol);
        String normalizedEngineId = normalizeEngineId(engineId);
        int sellLimit = sellLaneLimit(maxConcurrentEntriesPerSymbol.get());
        if (normalizedSymbol.isBlank() || normalizedEngineId.isBlank()) {
            return new EntryPermit(false, "同交易对卖出协调参数无效");
        }
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            LinkedHashMap<String, Waiter> buyWaiters = buyWaitersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> sellWaiters = sellWaitersBySymbol.computeIfAbsent(
                    normalizedSymbol, ignored -> new LinkedHashMap<>());
            Holder current = holders.get(normalizedEngineId);
            long now = System.currentTimeMillis();
            if (current == null) {
                current = new Holder(normalizedEngineId, displayAlias(normalizedEngineId, accountAlias),
                        Phase.HOLDING, now, now);
                holders.put(normalizedEngineId, current);
            }
            if (current.phase() == Phase.SELLING) {
                sellWaiters.remove(normalizedEngineId);
                cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
                return new EntryPermit(true, "");
            }
            if (buyWaiters != null) buyWaiters.remove(normalizedEngineId);
            sellWaiters.putIfAbsent(normalizedEngineId, new Waiter(normalizedEngineId,
                    current.accountAlias(), now));
            holders.put(normalizedEngineId, new Holder(current.engineId(), current.accountAlias(),
                    Phase.WAITING_TO_SELL, current.acquiredAtMs(), now));

            String firstWaitingEngineId = sellWaiters.keySet().iterator().next();
            int activeSells = countPhase(holders, Phase.SELLING);
            if (activeSells >= sellLimit || !normalizedEngineId.equals(firstWaitingEngineId)) {
                return new EntryPermit(false, sellWaitingReason(normalizedSymbol, normalizedEngineId,
                        holders, sellWaiters, sellLimit));
            }

            sellWaiters.remove(normalizedEngineId);
            Holder waiting = holders.get(normalizedEngineId);
            holders.put(normalizedEngineId, new Holder(waiting.engineId(), waiting.accountAlias(),
                    Phase.SELLING, waiting.acquiredAtMs(), now));
            cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
            return new EntryPermit(true, "");
        } finally {
            lock.unlock();
        }
    }

    /** Marks retained dust/inventory as holding neither the BUY nor SELL lane. */
    public void markHolding(String symbol, String engineId, String accountAlias) {
        updateExistingPhase(symbol, engineId, Phase.HOLDING);
    }

    /** Records recovered inventory before it requests a SELL lane. */
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
            LinkedHashMap<String, Waiter> buyWaiters = buyWaitersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> sellWaiters = sellWaitersBySymbol.get(normalizedSymbol);
            if (holders != null) holders.remove(normalizedEngineId);
            if (buyWaiters != null) buyWaiters.remove(normalizedEngineId);
            if (sellWaiters != null) sellWaiters.remove(normalizedEngineId);
            cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot(String symbol) {
        String normalizedSymbol = normalizeSymbol(symbol);
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> buyWaiters = buyWaitersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> sellWaiters = sellWaitersBySymbol.get(normalizedSymbol);
            return new Snapshot(normalizedSymbol,
                    holders == null ? List.of() : List.copyOf(holders.values()),
                    buyWaiters == null ? List.of() : List.copyOf(buyWaiters.values()),
                    sellWaiters == null ? List.of() : List.copyOf(sellWaiters.values()));
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
            LinkedHashMap<String, Waiter> buyWaiters = buyWaitersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> sellWaiters = sellWaitersBySymbol.get(normalizedSymbol);
            if (buyWaiters != null) buyWaiters.remove(normalizedEngineId);
            if (sellWaiters != null) sellWaiters.remove(normalizedEngineId);
            Holder current = holders.get(normalizedEngineId);
            long now = System.currentTimeMillis();
            holders.put(normalizedEngineId, new Holder(normalizedEngineId,
                    current == null ? displayAlias(normalizedEngineId, accountAlias) : current.accountAlias(),
                    phase, current == null ? now : current.acquiredAtMs(), now));
            cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
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
            LinkedHashMap<String, Waiter> buyWaiters = buyWaitersBySymbol.get(normalizedSymbol);
            LinkedHashMap<String, Waiter> sellWaiters = sellWaitersBySymbol.get(normalizedSymbol);
            Holder current = holders == null ? null : holders.get(normalizedEngineId);
            if (current == null) return;
            if (buyWaiters != null) buyWaiters.remove(normalizedEngineId);
            if (sellWaiters != null) sellWaiters.remove(normalizedEngineId);
            holders.put(normalizedEngineId, new Holder(current.engineId(), current.accountAlias(),
                    phase, current.acquiredAtMs(), System.currentTimeMillis()));
            cleanupEmptyState(normalizedSymbol, holders, buyWaiters, sellWaiters);
        } finally {
            lock.unlock();
        }
    }

    private String buyWaitingReason(String symbol, String engineId, LinkedHashMap<String, Holder> holders,
                                    LinkedHashMap<String, Waiter> waiters, int buyLimit, int limit) {
        int position = queuePosition(waiters, engineId);
        String buying = describeHolders(holders, Phase.BUYING);
        return symbol + " 买入通道已有 " + countPhase(holders, Phase.BUYING) + "/" + buyLimit
                + " 个账户买入中"
                + (buying.isBlank() ? "" : "：" + buying)
                + "；总交易轮次 " + holders.size() + "/" + limit
                + "；BUY FIFO 排队第 " + position + " 位";
    }

    private String sellWaitingReason(String symbol, String engineId, LinkedHashMap<String, Holder> holders,
                                     LinkedHashMap<String, Waiter> waiters, int sellLimit) {
        int position = queuePosition(waiters, engineId);
        String selling = describeHolders(holders, Phase.SELLING);
        return symbol + " 卖出通道已有 " + countPhase(holders, Phase.SELLING) + "/" + sellLimit
                + " 个账户卖出中" + (selling.isBlank() ? "" : "：" + selling)
                + "；持仓待卖 FIFO 排队第 " + position + " 位";
    }

    private int queuePosition(LinkedHashMap<String, Waiter> waiters, String engineId) {
        int position = 1;
        for (String waitingEngineId : waiters.keySet()) {
            if (waitingEngineId.equals(engineId)) break;
            position++;
        }
        return position;
    }

    private int countPhase(LinkedHashMap<String, Holder> holders, Phase phase) {
        int count = 0;
        for (Holder holder : holders.values()) {
            if (holder.phase() == phase) count++;
        }
        return count;
    }

    private int buyLaneLimit(int totalLimit) {
        return Math.max(1, totalLimit / 2);
    }

    private int sellLaneLimit(int totalLimit) {
        return totalLimit == 1 ? 1 : totalLimit - buyLaneLimit(totalLimit);
    }

    private void cleanupEmptyState(String symbol, LinkedHashMap<String, Holder> holders,
                                   LinkedHashMap<String, Waiter> buyWaiters,
                                   LinkedHashMap<String, Waiter> sellWaiters) {
        if (holders == null || holders.isEmpty()) holdersBySymbol.remove(symbol);
        if (buyWaiters == null || buyWaiters.isEmpty()) buyWaitersBySymbol.remove(symbol);
        if (sellWaiters == null || sellWaiters.isEmpty()) sellWaitersBySymbol.remove(symbol);
    }

    private String describeHolders(LinkedHashMap<String, Holder> holders, Phase phase) {
        List<String> labels = new ArrayList<>();
        for (Holder holder : holders.values()) {
            if (holder.phase() == phase) labels.add(holder.accountAlias());
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

    public enum Phase { BUYING, HOLDING, WAITING_TO_SELL, SELLING }
    public record EntryPermit(boolean accepted, String reason) { }
    public record Holder(String engineId, String accountAlias, Phase phase,
                         long acquiredAtMs, long phaseChangedAtMs) { }
    public record Waiter(String engineId, String accountAlias, long enqueuedAtMs) { }
    public record Snapshot(String symbol, List<Holder> holders, List<Waiter> waiters,
                           List<Waiter> sellWaiters) { }
}
