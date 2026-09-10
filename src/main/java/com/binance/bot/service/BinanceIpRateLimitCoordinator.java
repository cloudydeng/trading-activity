package com.binance.bot.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.util.function.LongSupplier;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates Binance REQUEST_WEIGHT for every API key using this JVM's public IP.
 * Binance reports X-MBX-USED-WEIGHT-1M per IP, not per API key.
 */
@Slf4j
@Component
public class BinanceIpRateLimitCoordinator {
    static final int DEFAULT_LIMIT_1M = 6_000;
    static final double ENTRY_SAFETY_RATIO = 0.80;

    private final ReentrantLock lock = new ReentrantLock();
    private final LongSupplier clock;
    private long minuteBucket = -1;
    private int usedWeight1m;
    private int requestWeightLimit1m = DEFAULT_LIMIT_1M;

    public BinanceIpRateLimitCoordinator() {
        this(System::currentTimeMillis);
    }

    BinanceIpRateLimitCoordinator(LongSupplier clock) {
        this.clock = clock;
    }

    /** New entries stop at 80% capacity, preserving headroom for exits and reconciliation. */
    public Permit tryAcquireEntryRequest(int weight) {
        int normalizedWeight = Math.max(1, weight);
        lock.lock();
        try {
            long now = clock.getAsLong();
            rotateWindow(now);
            int safeLimit = safeLimit(requestWeightLimit1m);
            if (usedWeight1m + normalizedWeight > safeLimit) {
                long retryAfterMs = Math.max(1_000, ((minuteBucket + 1) * 60_000L) - now + 250);
                return new Permit(false, usedWeight1m, requestWeightLimit1m, safeLimit, retryAfterMs);
            }
            usedWeight1m += normalizedWeight;
            return new Permit(true, usedWeight1m, requestWeightLimit1m, safeLimit, 0);
        } finally {
            lock.unlock();
        }
    }

    /** Safety-critical exits and reconciliation are never blocked by the entry reserve. */
    public void reserveSafetyRequest(int weight) {
        lock.lock();
        try {
            rotateWindow(clock.getAsLong());
            usedWeight1m += Math.max(1, weight);
        } finally {
            lock.unlock();
        }
    }

    public void updateFromHeaders(HttpHeaders headers) {
        String value = headers.getFirst("x-mbx-used-weight-1m");
        if (value == null) return;
        try {
            recordExchangeWeight(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            log.warn("忽略无效 Binance 请求权重响应头: {}", value);
        }
    }

    void recordExchangeWeight(int weight) {
        lock.lock();
        try {
            rotateWindow(clock.getAsLong());
            usedWeight1m = Math.max(usedWeight1m, Math.max(0, weight));
        } finally {
            lock.unlock();
        }
    }

    public void updateLimitFromExchangeInfo(JsonNode root) {
        if (root == null || !root.path("rateLimits").isArray()) return;
        for (JsonNode rateLimit : root.path("rateLimits")) {
            if (!"REQUEST_WEIGHT".equals(rateLimit.path("rateLimitType").asText())
                    || !"MINUTE".equals(rateLimit.path("interval").asText())
                    || rateLimit.path("intervalNum").asInt() != 1) continue;
            int discovered = rateLimit.path("limit").asInt(0);
            if (discovered <= 0) return;
            lock.lock();
            try {
                if (requestWeightLimit1m != discovered) {
                    log.info("Binance IP 请求权重上限更新: {}/min -> {}/min",
                            requestWeightLimit1m, discovered);
                    requestWeightLimit1m = discovered;
                }
            } finally {
                lock.unlock();
            }
            return;
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try {
            rotateWindow(clock.getAsLong());
            return new Snapshot(usedWeight1m, requestWeightLimit1m, safeLimit(requestWeightLimit1m));
        } finally {
            lock.unlock();
        }
    }

    private void rotateWindow(long now) {
        long currentBucket = Math.floorDiv(now, 60_000L);
        if (minuteBucket != currentBucket) {
            minuteBucket = currentBucket;
            usedWeight1m = 0;
        }
    }

    private int safeLimit(int limit) {
        return Math.max(1, (int) Math.floor(limit * ENTRY_SAFETY_RATIO));
    }

    public record Permit(boolean allowed, int usedWeight1m, int limit1m, int safeLimit1m,
                         long retryAfterMs) { }
    public record Snapshot(int usedWeight1m, int limit1m, int safeLimit1m) { }
}
