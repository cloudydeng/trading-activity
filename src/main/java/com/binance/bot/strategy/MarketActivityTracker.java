package com.binance.bot.strategy;

import java.math.BigDecimal;
import java.util.concurrent.locks.ReentrantLock;

/** Bounded, in-memory diagnostics from the existing aggregate-trade stream; never gates orders. */
public class MarketActivityTracker {
    private static final int WINDOW_SECONDS = 300;
    private static final BigDecimal ACTIVE_MIN_QUOTE = BigDecimal.valueOf(200);
    private static final BigDecimal QUIET_MIN_QUOTE = BigDecimal.valueOf(50);
    private final SecondBucket[] buckets = new SecondBucket[WINDOW_SECONDS];
    private final ReentrantLock lock = new ReentrantLock();
    private long firstTradeAtMs;
    private long lastTradeAtMs;

    public void recordTrade(BigDecimal price, BigDecimal quantity, boolean buyerIsMaker, long receivedAtMs) {
        if (price == null || quantity == null || price.signum() <= 0 || quantity.signum() <= 0
                || receivedAtMs <= 0) return;
        lock.lock();
        try {
            long second = receivedAtMs / 1_000;
            int index = (int) Math.floorMod(second, WINDOW_SECONDS);
            SecondBucket bucket = buckets[index];
            if (bucket == null || bucket.second != second) {
                bucket = new SecondBucket(second);
                buckets[index] = bucket;
            }
            BigDecimal quote = price.multiply(quantity);
            bucket.events++;
            bucket.quoteVolume = bucket.quoteVolume.add(quote);
            if (buyerIsMaker) bucket.aggressiveSellQuote = bucket.aggressiveSellQuote.add(quote);
            else bucket.aggressiveBuyQuote = bucket.aggressiveBuyQuote.add(quote);
            if (firstTradeAtMs == 0) firstTradeAtMs = receivedAtMs;
            lastTradeAtMs = Math.max(lastTradeAtMs, receivedAtMs);
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot(long nowMs, BigDecimal orderAmountUsdt, boolean marketBookFresh) {
        lock.lock();
        try {
            long nowSecond = nowMs / 1_000;
            int events1m = 0, events5m = 0;
            BigDecimal quote1m = BigDecimal.ZERO, quote5m = BigDecimal.ZERO;
            BigDecimal buy1m = BigDecimal.ZERO, sell1m = BigDecimal.ZERO;
            for (SecondBucket bucket : buckets) {
                if (bucket == null) continue;
                long ageSeconds = nowSecond - bucket.second;
                if (ageSeconds < 0 || ageSeconds >= WINDOW_SECONDS) continue;
                events5m += bucket.events;
                quote5m = quote5m.add(bucket.quoteVolume);
                if (ageSeconds < 60) {
                    events1m += bucket.events;
                    quote1m = quote1m.add(bucket.quoteVolume);
                    buy1m = buy1m.add(bucket.aggressiveBuyQuote);
                    sell1m = sell1m.add(bucket.aggressiveSellQuote);
                }
            }
            Long lastTradeAgeMs = lastTradeAtMs == 0 ? null : Math.max(0, nowMs - lastTradeAtMs);
            BigDecimal order = orderAmountUsdt == null || orderAmountUsdt.signum() <= 0
                    ? BigDecimal.valueOf(12) : orderAmountUsdt;
            BigDecimal activeQuote = order.multiply(BigDecimal.valueOf(20)).max(ACTIVE_MIN_QUOTE);
            BigDecimal quietQuote = order.multiply(BigDecimal.valueOf(5)).max(QUIET_MIN_QUOTE);
            String level;
            String reason;
            if (!marketBookFresh || lastTradeAgeMs == null) {
                level = "UNKNOWN";
                reason = marketBookFresh ? "等待实时成交流样本" : "盘口行情未就绪或已过期";
            } else if (lastTradeAgeMs >= 30_000) {
                level = "QUIET";
                reason = "最近 30 秒无成交";
            } else if (events1m >= 20 && quote1m.compareTo(activeQuote) >= 0 && lastTradeAgeMs <= 10_000) {
                level = "ACTIVE";
                reason = "近 1 分钟成交事件、金额及最近成交间隔均达观察阈值";
            } else if (nowMs - firstTradeAtMs < 60_000) {
                level = "UNKNOWN";
                reason = "实时成交样本尚未积累满 60 秒";
            } else if (events1m < 5 && quote1m.compareTo(quietQuote) < 0) {
                level = "QUIET";
                reason = "近 1 分钟成交事件和金额都偏少";
            } else {
                level = "NORMAL";
                reason = "有成交，但未同时满足活跃或清淡的观察条件";
            }
            return new Snapshot(level, reason, events1m, events5m, quote1m, quote5m,
                    buy1m, sell1m, lastTradeAgeMs, activeQuote, quietQuote);
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        lock.lock();
        try {
            java.util.Arrays.fill(buckets, null);
            firstTradeAtMs = 0;
            lastTradeAtMs = 0;
        } finally {
            lock.unlock();
        }
    }

    private static final class SecondBucket {
        private final long second;
        private int events;
        private BigDecimal quoteVolume = BigDecimal.ZERO;
        private BigDecimal aggressiveBuyQuote = BigDecimal.ZERO;
        private BigDecimal aggressiveSellQuote = BigDecimal.ZERO;

        private SecondBucket(long second) { this.second = second; }
    }

    public record Snapshot(String level, String reason, int aggregateTradeEvents1m,
                           int aggregateTradeEvents5m, BigDecimal quoteVolume1m,
                           BigDecimal quoteVolume5m, BigDecimal aggressiveBuyQuote1m,
                           BigDecimal aggressiveSellQuote1m, Long lastTradeAgeMs,
                           BigDecimal activeQuoteThreshold, BigDecimal quietQuoteThreshold) { }
}
