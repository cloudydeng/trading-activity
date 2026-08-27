package com.binance.bot.strategy;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Records post-fill market outcomes for validation; it does not make trading decisions. */
@Component
public class PostFillOutcomeTracker {
    private static final long[] HORIZONS_MS = {1_000, 5_000, 15_000, 60_000};
    private static final MathContext MC = MathContext.DECIMAL64;
    private final Deque<Observation> active = new ArrayDeque<>();
    private final Deque<Outcome> completed = new ArrayDeque<>();

    public synchronized void recordBuyFill(BigDecimal entryPrice, String entryReason, long timestampMs) {
        if (entryPrice == null || entryPrice.signum() <= 0) return;
        active.addLast(new Observation(entryPrice, entryReason, timestampMs));
    }

    public synchronized void recordMarketPrice(BigDecimal price, long timestampMs) {
        if (price == null || price.signum() <= 0) return;
        var iterator = active.iterator();
        while (iterator.hasNext()) {
            Observation observation = iterator.next();
            observation.update(price, timestampMs);
            if (observation.isComplete()) {
                completed.addLast(observation.toOutcome());
                iterator.remove();
            }
        }
        while (completed.size() > 500) completed.removeFirst();
    }

    public synchronized OutcomeSummary getSummary() {
        if (completed.isEmpty()) return OutcomeSummary.empty(active.size());
        List<Outcome> outcomes = new ArrayList<>(completed);
        long bounceCount = outcomes.stream().filter(o -> o.returnBps()[3] != null && o.returnBps()[3].signum() >= 0).count();
        Map<String, Integer> byReason = new TreeMap<>();
        for (Outcome outcome : outcomes) byReason.merge(outcome.entryReason(), 1, Integer::sum);
        return new OutcomeSummary(active.size(), outcomes.size(), BigDecimal.valueOf(bounceCount).divide(BigDecimal.valueOf(outcomes.size()), MC),
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
        private final long entryTimestampMs;
        private final BigDecimal[] returnBps = new BigDecimal[HORIZONS_MS.length];
        private BigDecimal maxAdverseBps = BigDecimal.ZERO;
        private BigDecimal maxFavorableBps = BigDecimal.ZERO;
        private Long firstRecoveryMs;

        private Observation(BigDecimal entryPrice, String entryReason, long entryTimestampMs) {
            this.entryPrice = entryPrice;
            this.entryReason = entryReason == null ? "UNKNOWN" : entryReason;
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
        private Outcome toOutcome() { return new Outcome(entryReason, Arrays.copyOf(returnBps, returnBps.length), maxAdverseBps, maxFavorableBps, firstRecoveryMs); }
    }

    public record Outcome(String entryReason, BigDecimal[] returnBps, BigDecimal maxAdverseBps, BigDecimal maxFavorableBps, Long firstRecoveryMs) { }
    public record OutcomeSummary(int activeObservations, int completedObservations, BigDecimal bounceProbability60s,
                                 BigDecimal medianMaxAdverseBps, BigDecimal medianMaxFavorableBps, Long medianRecoveryMs,
                                 Map<String, Integer> completedByEntryReason, Outcome lastOutcome) {
        static OutcomeSummary empty(int active) { return new OutcomeSummary(active, 0, null, null, null, null, Map.of(), null); }
    }
}
