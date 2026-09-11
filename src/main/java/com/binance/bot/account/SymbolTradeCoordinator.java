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
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, LinkedHashMap<String, Holder>> holdersBySymbol = new LinkedHashMap<>();
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
            if (holders.containsKey(normalizedEngineId)) return new EntryPermit(true, "");
            if (holders.size() >= limit) {
                String occupied = describeHolders(holders);
                return new EntryPermit(false, normalizedSymbol + " 同交易对已有 "
                        + holders.size() + "/" + limit + " 个账户交易中"
                        + (occupied.isBlank() ? "" : "：" + occupied));
            }
            holders.put(normalizedEngineId, new Holder(normalizedEngineId,
                    accountAlias == null || accountAlias.isBlank() ? normalizedEngineId : accountAlias,
                    System.currentTimeMillis()));
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
            if (holders == null) return;
            holders.remove(normalizedEngineId);
            if (holders.isEmpty()) holdersBySymbol.remove(normalizedSymbol);
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
            holders.putIfAbsent(normalizedEngineId, new Holder(normalizedEngineId,
                    accountAlias == null || accountAlias.isBlank() ? normalizedEngineId : accountAlias,
                    System.currentTimeMillis()));
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot(String symbol) {
        String normalizedSymbol = normalizeSymbol(symbol);
        lock.lock();
        try {
            LinkedHashMap<String, Holder> holders = holdersBySymbol.get(normalizedSymbol);
            return new Snapshot(normalizedSymbol, holders == null ? List.of() : List.copyOf(holders.values()));
        } finally {
            lock.unlock();
        }
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
    public record Snapshot(String symbol, List<Holder> holders) { }
}
