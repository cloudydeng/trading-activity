package com.binance.bot.strategy;


import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/** Records post-fill market outcomes for validation; it does not make trading decisions. */
public class PostFillOutcomeTracker {
    private static final long[] HORIZONS_MS = {1_000, 5_000, 15_000, 60_000};
    private static final MathContext MC = MathContext.DECIMAL64;
    private final Deque<Observation> active = new ArrayDeque<>();
    private final Deque<Outcome> completed = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final ObservationJournal observationJournal;

    /** Keeps lightweight unit tests independent from the filesystem. */
    public PostFillOutcomeTracker() {
        this.observationJournal = null;
    }

    public PostFillOutcomeTracker(ObservationJournal observationJournal) {
        this.observationJournal = observationJournal;
    }

    public void recordBuyFill(BigDecimal entryPrice, String entryReason, long timestampMs) {
        recordBuyFill(entryPrice, entryReason, null, timestampMs);
    }

    public void recordBuyFill(BigDecimal entryPrice, String entryReason,
                                           MarketSignalEvaluator.MarketContext entryContext, long timestampMs) {
        lock.lock();
        try {
        recordEntry(entryPrice, entryReason, entryContext, "FILLED", timestampMs);
        } finally {
            lock.unlock();
        }
    }

    private void recordEntry(BigDecimal entryPrice, String entryReason, MarketSignalEvaluator.MarketContext entryContext,
                             String entryType, long timestampMs) {
        if (entryPrice == null || entryPrice.signum() <= 0) return;
        active.addLast(new Observation(entryPrice, entryReason, entryContext, entryType, timestampMs));
    }

    public void recordMarketPrice(BigDecimal price, long timestampMs) {
        lock.lock();
        try {
        if (price == null || price.signum() <= 0) return;
        var iterator = active.iterator();
        while (iterator.hasNext()) {
            Observation observation = iterator.next();
            observation.update(price, timestampMs);
            if (observation.isComplete()) {
                Outcome outcome = observation.toOutcome();
                completed.addLast(outcome);
                if (observationJournal != null) observationJournal.append(outcome);
                iterator.remove();
            }
        }
        while (completed.size() > 500) completed.removeFirst();
        } finally {
            lock.unlock();
        }
    }

    public OutcomeSummary getSummary() {
        lock.lock();
        try {
            return summarize(null);
        } finally {
            lock.unlock();
        }
    }

    /** Prevents price observations from two symbols being combined after a hot switch. */
    public void reset() {
        lock.lock();
        try {
        active.clear();
        completed.clear();
        } finally {
            lock.unlock();
        }
    }

    private OutcomeSummary summarize(String entryType) {
        int activeCount = (int) active.stream().filter(o -> entryType == null || entryType.equals(o.entryType)).count();
        List<Outcome> outcomes = completed.stream().filter(o -> entryType == null || entryType.equals(o.entryType)).toList();
        if (outcomes.isEmpty()) return OutcomeSummary.empty(activeCount);
        long bounceCount = outcomes.stream().filter(o -> o.returnBps()[3] != null && o.returnBps()[3].signum() >= 0).count();
        Map<String, Integer> byReason = new TreeMap<>();
        for (Outcome outcome : outcomes) byReason.merge(outcome.entryReason(), 1, Integer::sum);
        return new OutcomeSummary(activeCount, outcomes.size(), BigDecimal.valueOf(bounceCount).divide(BigDecimal.valueOf(outcomes.size()), MC),
                median(outcomes.stream().map(Outcome::maxAdverseBps).toList()),
                median(outcomes.stream().map(Outcome::maxFavorableBps).toList()),
                median(outcomes.stream().map(Outcome::firstRecoveryMs).filter(v -> v != null).toList()), byReason,
                outcomes.get(outcomes.size() - 1));
    }

    private static <T extends Comparable<? super T>> T median(List<T> values) {
        if (values.isEmpty()) return null;
        List<T> sorted = new ArrayList<>(values);
        sorted.sort(null);
        return sorted.get(sorted.size() / 2);
    }

    private static final class Observation {
        private final BigDecimal entryPrice;
        private final String entryReason;
        private final MarketSignalEvaluator.MarketContext entryContext;
        private final String entryType;
        private final long entryTimestampMs;
        private final BigDecimal[] returnBps = new BigDecimal[HORIZONS_MS.length];
        private BigDecimal maxAdverseBps = BigDecimal.ZERO;
        private BigDecimal maxFavorableBps = BigDecimal.ZERO;
        private Long firstRecoveryMs;

        private Observation(BigDecimal entryPrice, String entryReason, MarketSignalEvaluator.MarketContext entryContext,
                            String entryType, long entryTimestampMs) {
            this.entryPrice = entryPrice;
            this.entryReason = entryReason == null ? "UNKNOWN" : entryReason;
            this.entryContext = entryContext;
            this.entryType = entryType;
            this.entryTimestampMs = entryTimestampMs;
        }

        private void update(BigDecimal price, long timestampMs) {
            long elapsedMs = Math.max(0, timestampMs - entryTimestampMs);
            BigDecimal changeBps = price.subtract(entryPrice).multiply(BigDecimal.valueOf(10_000)).divide(entryPrice, MC);
            maxAdverseBps = maxAdverseBps.min(changeBps);
            maxFavorableBps = maxFavorableBps.max(changeBps);
            if (firstRecoveryMs == null && changeBps.signum() >= 0) firstRecoveryMs = elapsedMs;
            for (int i = 0; i < HORIZONS_MS.length; i++) if (returnBps[i] == null && elapsedMs >= HORIZONS_MS[i]) returnBps[i] = changeBps;
        }

        private boolean isComplete() { return returnBps[HORIZONS_MS.length - 1] != null; }
        private Outcome toOutcome() { return new Outcome(entryReason, entryContext, entryType, Arrays.copyOf(returnBps, returnBps.length), maxAdverseBps, maxFavorableBps, firstRecoveryMs); }
    }

    public record Outcome(String entryReason, MarketSignalEvaluator.MarketContext entryContext, String entryType, BigDecimal[] returnBps,
                          BigDecimal maxAdverseBps, BigDecimal maxFavorableBps, Long firstRecoveryMs) { }
    public record OutcomeSummary(int activeObservations, int completedObservations, BigDecimal bounceProbability60s,
                                 BigDecimal medianMaxAdverseBps, BigDecimal medianMaxFavorableBps, Long medianRecoveryMs,
                                 Map<String, Integer> completedByEntryReason, Outcome lastOutcome) {
        static OutcomeSummary empty(int active) { return new OutcomeSummary(active, 0, null, null, null, null, Map.of(), null); }
    }
}
