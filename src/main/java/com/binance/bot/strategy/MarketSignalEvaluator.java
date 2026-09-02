package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A conservative entry gate. It never asks the engine to buy; it only rejects
 * new entries when the best-book data indicates stale, weak, or unstable flow.
 */
@Component
public class MarketSignalEvaluator {
    private static final MathContext MC = MathContext.DECIMAL64;
    private final Deque<Quote> quotes = new ArrayDeque<>();
    private final Deque<TradeFlow> trades = new ArrayDeque<>();
    private final AtomicReference<EntryDecision> lastDecision = new AtomicReference<>(EntryDecision.block("AWAITING_MARKET_DATA"));
    private DepthSnapshot latestDepth;
    private Selloff selloff;

    public synchronized void recordQuote(BigDecimal bid, BigDecimal bidQty, BigDecimal ask, BigDecimal askQty, long timestampMs,
                                         BinanceProperties.Strategy config) {
        if (bid.signum() <= 0 || ask.signum() <= 0 || bidQty.signum() < 0 || askQty.signum() < 0) return;
        quotes.addLast(new Quote(bid, bidQty, ask, askQty, timestampMs));
        long cutoff = timestampMs - config.getSignalLookbackMs();
        while (!quotes.isEmpty() && quotes.peekFirst().timestampMs() < cutoff) quotes.removeFirst();
    }

    public synchronized void recordAggTrade(BigDecimal quantity, boolean buyerIsMaker, long timestampMs,
                                            BinanceProperties.Strategy config) {
        if (quantity.signum() <= 0) return;
        // buyerIsMaker=true means the aggressor sold into the bid.
        trades.addLast(new TradeFlow(buyerIsMaker ? quantity.negate() : quantity, quantity, timestampMs));
        long cutoff = timestampMs - config.getSignalLookbackMs();
        while (!trades.isEmpty() && trades.peekFirst().timestampMs() < cutoff) trades.removeFirst();
    }

    public synchronized void recordDepth(BigDecimal bidDepth, BigDecimal askDepth, long timestampMs) {
        if (bidDepth.signum() < 0 || askDepth.signum() < 0) return;
        latestDepth = new DepthSnapshot(bidDepth, askDepth, timestampMs);
    }

    public synchronized EntryDecision evaluate(long nowMs, BinanceProperties.Strategy config) {
        pruneTrades(nowMs - config.getSignalLookbackMs());
        Quote latest = quotes.peekLast();
        if (latest == null || nowMs - latest.timestampMs() > config.getMarketDataStaleMs()) return set(EntryDecision.block("STALE_MARKET_DATA"));
        if (quotes.size() < 2) return set(EntryDecision.block("INSUFFICIENT_SIGNAL_HISTORY"));
        if (latestDepth == null || nowMs - latestDepth.timestampMs() > config.getDepthDataStaleMs()) return set(EntryDecision.block("STALE_DEPTH_DATA"));

        BigDecimal mid = latest.bid().add(latest.ask()).divide(BigDecimal.valueOf(2), MC);
        BigDecimal totalQty = latest.bidQty().add(latest.askQty());
        if (totalQty.signum() == 0) return set(EntryDecision.block("EMPTY_TOP_OF_BOOK"));
        BigDecimal imbalance = latest.bidQty().subtract(latest.askQty()).divide(totalQty, MC);
        Quote first = quotes.peekFirst();
        BigDecimal firstMid = first.bid().add(first.ask()).divide(BigDecimal.valueOf(2), MC);
        BigDecimal returnBps = mid.subtract(firstMid).multiply(BigDecimal.valueOf(10_000)).divide(firstMid, MC);
        BigDecimal minMid = mid;
        BigDecimal maxMid = mid;
        for (Quote quote : quotes) {
            BigDecimal quoteMid = quote.bid().add(quote.ask()).divide(BigDecimal.valueOf(2), MC);
            minMid = minMid.min(quoteMid);
            maxMid = maxMid.max(quoteMid);
        }
        BigDecimal rangeBps = maxMid.subtract(minMid).multiply(BigDecimal.valueOf(10_000)).divide(mid, MC);
        BigDecimal depthTotal = latestDepth.bidDepth().add(latestDepth.askDepth());
        if (depthTotal.signum() == 0) return set(EntryDecision.block("EMPTY_DEPTH_BOOK"));
        BigDecimal depthImbalance = latestDepth.bidDepth().subtract(latestDepth.askDepth()).divide(depthTotal, MC);
        BigDecimal signedTradeQty = BigDecimal.ZERO;
        BigDecimal totalTradeQty = BigDecimal.ZERO;
        for (TradeFlow trade : trades) {
            signedTradeQty = signedTradeQty.add(trade.signedQuantity());
            totalTradeQty = totalTradeQty.add(trade.totalQuantity());
        }
        BigDecimal takerFlowImbalance = totalTradeQty.signum() == 0 ? BigDecimal.ZERO : signedTradeQty.divide(totalTradeQty, MC);

        if (imbalance.compareTo(BigDecimal.valueOf(config.getMinBookImbalance())) < 0) return set(EntryDecision.block("WEAK_TOP_OF_BOOK", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
        if (depthImbalance.compareTo(BigDecimal.valueOf(config.getMinDepthImbalance())) < 0) return set(EntryDecision.block("WEAK_MULTI_LEVEL_BIDS", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
        if (takerFlowImbalance.compareTo(BigDecimal.valueOf(config.getMinTakerFlowImbalance())) < 0) {
            recordSelloff(mid, nowMs);
            return set(EntryDecision.block("SELL_TAKER_PRESSURE", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
        }
        if (returnBps.compareTo(BigDecimal.valueOf(-config.getMaxDownwardMoveBps())) < 0) {
            recordSelloff(mid, nowMs);
            return set(EntryDecision.block("SHORT_TERM_DOWNMOVE", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
        }
        if (rangeBps.compareTo(BigDecimal.valueOf(config.getMaxShortTermVolatilityBps())) > 0) return set(EntryDecision.block("EXCESS_SHORT_TERM_VOLATILITY", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
        if (selloff != null) {
            if (nowMs - selloff.detectedAtMs() < config.getPostSelloffCooldownMs()) return set(EntryDecision.block("POST_SELLOFF_COOLDOWN", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
            BigDecimal reclaimBps = mid.subtract(selloff.lowMid()).multiply(BigDecimal.valueOf(10_000)).divide(selloff.lowMid(), MC);
            if (reclaimBps.compareTo(BigDecimal.valueOf(config.getMinPostSelloffReclaimBps())) < 0) return set(EntryDecision.block("WAIT_FOR_PRICE_RECLAIM", imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
            selloff = null;
        }
        return set(EntryDecision.allow(imbalance, depthImbalance, takerFlowImbalance, returnBps, rangeBps));
    }

    public EntryDecision getLastDecision() { return lastDecision.get(); }
    public synchronized void reset() {
        quotes.clear();
        trades.clear();
        latestDepth = null;
        selloff = null;
        lastDecision.set(EntryDecision.block("AWAITING_MARKET_DATA"));
    }

    public synchronized MarketContext getMarketContext(long nowMs) {
        EntryDecision decision = lastDecision.get();
        return new MarketContext(decision.reason(), decision.bookImbalance(), decision.depthImbalance(),
                decision.takerFlowImbalance(), decision.returnBps(), decision.rangeBps(),
                selloff == null ? null : nowMs - selloff.detectedAtMs(), selloff == null ? null : selloff.lowMid());
    }

    private void pruneTrades(long cutoff) {
        while (!trades.isEmpty() && trades.peekFirst().timestampMs() < cutoff) trades.removeFirst();
    }
    private EntryDecision set(EntryDecision value) { lastDecision.set(value); return value; }

    private record Quote(BigDecimal bid, BigDecimal bidQty, BigDecimal ask, BigDecimal askQty, long timestampMs) { }
    private record TradeFlow(BigDecimal signedQuantity, BigDecimal totalQuantity, long timestampMs) { }
    private record DepthSnapshot(BigDecimal bidDepth, BigDecimal askDepth, long timestampMs) { }
    private record Selloff(long detectedAtMs, BigDecimal lowMid) { }

    private void recordSelloff(BigDecimal mid, long nowMs) {
        selloff = selloff == null ? new Selloff(nowMs, mid) : new Selloff(nowMs, selloff.lowMid().min(mid));
    }

    public record EntryDecision(boolean allowed, String reason, BigDecimal bookImbalance, BigDecimal depthImbalance,
                                BigDecimal takerFlowImbalance, BigDecimal returnBps, BigDecimal rangeBps) {
        static EntryDecision allow(BigDecimal book, BigDecimal depth, BigDecimal flow, BigDecimal returnBps, BigDecimal rangeBps) { return new EntryDecision(true, "ALLOWED", book, depth, flow, returnBps, rangeBps); }
        static EntryDecision block(String reason) { return new EntryDecision(false, reason, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO); }
        static EntryDecision block(String reason, BigDecimal book, BigDecimal depth, BigDecimal flow, BigDecimal returnBps, BigDecimal rangeBps) { return new EntryDecision(false, reason, book, depth, flow, returnBps, rangeBps); }
    }

    public record MarketContext(String decisionReason, BigDecimal bookImbalance, BigDecimal depthImbalance,
                                BigDecimal takerFlowImbalance, BigDecimal returnBps, BigDecimal rangeBps,
                                Long selloffAgeMs, BigDecimal selloffLowMid) { }
}
