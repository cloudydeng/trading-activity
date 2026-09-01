package com.binance.bot.strategy;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Process-local, idempotent accounting for authoritative Binance trade fills.
 *
 * <p>The exchange trade id is the primary deduplication key.  Per-order quantity
 * and quote totals are retained independently so REST reconciliation can prove
 * that the account stream did not omit a fill before an order is forgotten.</p>
 */
public final class TradeAccountingLedger {
    private static final MathContext MC = MathContext.DECIMAL64;
    private static final BigDecimal ONE_MILLION = new BigDecimal("1000000");

    private final Set<TradeKey> processedTrades = new LinkedHashSet<>();
    private final Map<Long, BigDecimal> orderQuantity = new LinkedHashMap<>();
    private final Map<Long, BigDecimal> orderQuote = new LinkedHashMap<>();
    private final Map<String, BigDecimal> commissionByAsset = new LinkedHashMap<>();
    private final Set<String> unconvertedCommissionAssets = new LinkedHashSet<>();
    private BigDecimal totalVolumeQuote = BigDecimal.ZERO;
    private BigDecimal totalCommissionQuote = BigDecimal.ZERO;

    synchronized AppliedTrade record(long orderId, long tradeId, BigDecimal quantity, BigDecimal price,
                                     BigDecimal quoteQuantity, BigDecimal commission, String commissionAsset,
                                     BigDecimal commissionQuoteEquivalent) {
        if (quantity == null || quantity.signum() <= 0) return AppliedTrade.ignored();
        BigDecimal normalizedPrice = price == null ? BigDecimal.ZERO : price;
        BigDecimal normalizedQuote = quoteQuantity != null && quoteQuantity.signum() > 0
                ? quoteQuantity : quantity.multiply(normalizedPrice);
        TradeKey key = new TradeKey(orderId, tradeId >= 0 ? Long.toString(tradeId)
                : quantity.toPlainString() + ":" + normalizedQuote.toPlainString() + ":"
                + (commission == null ? "0" : commission.toPlainString()) + ":"
                + normalizeAsset(commissionAsset));
        if (!processedTrades.add(key)) return AppliedTrade.ignored();

        orderQuantity.merge(orderId, quantity, BigDecimal::add);
        orderQuote.merge(orderId, normalizedQuote, BigDecimal::add);
        totalVolumeQuote = totalVolumeQuote.add(normalizedQuote);

        BigDecimal normalizedCommission = commission == null ? BigDecimal.ZERO : commission.max(BigDecimal.ZERO);
        String normalizedAsset = normalizeAsset(commissionAsset);
        if (normalizedCommission.signum() > 0) {
            commissionByAsset.merge(normalizedAsset, normalizedCommission, BigDecimal::add);
            if (commissionQuoteEquivalent == null) {
                unconvertedCommissionAssets.add(normalizedAsset);
            } else {
                totalCommissionQuote = totalCommissionQuote.add(commissionQuoteEquivalent.max(BigDecimal.ZERO));
            }
        }
        return new AppliedTrade(true, quantity, normalizedQuote, normalizedCommission, normalizedAsset,
                commissionQuoteEquivalent);
    }

    synchronized BigDecimal accountedQuantity(long orderId) {
        return orderQuantity.getOrDefault(orderId, BigDecimal.ZERO);
    }

    synchronized BigDecimal accountedQuote(long orderId) {
        return orderQuote.getOrDefault(orderId, BigDecimal.ZERO);
    }

    synchronized AccountingSnapshot snapshot() {
        BigDecimal costPerMillion = totalVolumeQuote.signum() > 0 && unconvertedCommissionAssets.isEmpty()
                ? totalCommissionQuote.multiply(ONE_MILLION).divide(totalVolumeQuote, MC)
                : null;
        return new AccountingSnapshot(totalVolumeQuote, Map.copyOf(commissionByAsset), totalCommissionQuote,
                costPerMillion, unconvertedCommissionAssets.isEmpty(), Set.copyOf(unconvertedCommissionAssets),
                processedTrades.size());
    }

    synchronized void reset() {
        processedTrades.clear();
        orderQuantity.clear();
        orderQuote.clear();
        commissionByAsset.clear();
        unconvertedCommissionAssets.clear();
        totalVolumeQuote = BigDecimal.ZERO;
        totalCommissionQuote = BigDecimal.ZERO;
    }

    private String normalizeAsset(String asset) {
        return asset == null || asset.isBlank() ? "UNKNOWN" : asset.toUpperCase();
    }

    record AppliedTrade(boolean applied, BigDecimal quantity, BigDecimal quoteQuantity,
                        BigDecimal commission, String commissionAsset, BigDecimal commissionQuoteEquivalent) {
        static AppliedTrade ignored() {
            return new AppliedTrade(false, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, "", BigDecimal.ZERO);
        }
    }

    public record AccountingSnapshot(BigDecimal totalVolumeQuote, Map<String, BigDecimal> commissionByAsset,
                                     BigDecimal totalCommissionQuoteEquivalent, BigDecimal costPerMillionVolume,
                                     boolean commissionConversionComplete, Set<String> unconvertedCommissionAssets,
                                     int processedTradeCount) { }

    private record TradeKey(long orderId, String tradeIdentity) { }
}
