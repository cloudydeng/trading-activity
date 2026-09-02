package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A local, fee-aware risk ledger. It is deliberately fail-closed for new entries:
 * once a loss, drawdown, inventory or holding-time limit trips, only reducing an
 * existing position remains possible until an operator investigates and restarts.
 */
@Component
public class TradingRiskGuard {
    private static final MathContext MC = MathContext.DECIMAL64;
    private final AtomicReference<String> entryBlockReason = new AtomicReference<>();
    private BigDecimal positionQty = BigDecimal.ZERO;
    private BigDecimal positionCostUsdt = BigDecimal.ZERO;
    private BigDecimal realizedPnlUsdt = BigDecimal.ZERO;
    private BigDecimal estimatedFeesUsdt = BigDecimal.ZERO;
    private BigDecimal markPrice;
    private BigDecimal peakNetPnlUsdt = BigDecimal.ZERO;
    private long positionOpenedAtMs = -1;
    private LocalDate ledgerDate = LocalDate.now(ZoneOffset.UTC);

    public synchronized void recordFill(String side, BigDecimal quantity, BigDecimal price, long nowMs,
                                        BinanceProperties.Strategy config) {
        if (quantity == null || price == null || quantity.signum() <= 0 || price.signum() <= 0) return;
        BigDecimal notional = quantity.multiply(price);
        BigDecimal fee = notional.multiply(config.getAssumedMakerFeeBps()).divide(BigDecimal.valueOf(10_000), MC);
        recordActualFill(side, quantity, notional, fee, nowMs, config);
    }

    /** Records exchange-reported economics instead of applying the configured fee estimate. */
    public synchronized void recordActualFill(String side, BigDecimal inventoryQuantity, BigDecimal quoteNotional,
                                              BigDecimal cashCommissionQuote, long nowMs,
                                              BinanceProperties.Strategy config) {
        rollDayIfNeeded();
        if (inventoryQuantity == null || quoteNotional == null || inventoryQuantity.signum() <= 0
                || quoteNotional.signum() <= 0) return;
        BigDecimal fee = cashCommissionQuote == null ? BigDecimal.ZERO : cashCommissionQuote.max(BigDecimal.ZERO);
        estimatedFeesUsdt = estimatedFeesUsdt.add(fee);
        BigDecimal effectivePrice = quoteNotional.divide(inventoryQuantity, MC);
        markPrice = effectivePrice;
        if ("BUY".equalsIgnoreCase(side)) {
            if (positionQty.signum() == 0) positionOpenedAtMs = nowMs;
            positionQty = positionQty.add(inventoryQuantity);
            positionCostUsdt = positionCostUsdt.add(quoteNotional).add(fee);
        } else if ("SELL".equalsIgnoreCase(side) && positionQty.signum() > 0) {
            BigDecimal closedQty = inventoryQuantity.min(positionQty);
            BigDecimal averageCost = positionCostUsdt.divide(positionQty, MC);
            BigDecimal allocatedCost = averageCost.multiply(closedQty);
            BigDecimal proceedsShare = inventoryQuantity.compareTo(closedQty) == 0
                    ? quoteNotional : quoteNotional.multiply(closedQty).divide(inventoryQuantity, MC);
            BigDecimal netProceeds = proceedsShare.subtract(fee);
            realizedPnlUsdt = realizedPnlUsdt.add(netProceeds.subtract(allocatedCost));
            positionQty = positionQty.subtract(closedQty);
            positionCostUsdt = positionCostUsdt.subtract(allocatedCost);
            if (positionQty.signum() == 0) {
                positionQty = BigDecimal.ZERO;
                positionCostUsdt = BigDecimal.ZERO;
                positionOpenedAtMs = -1;
                // Inventory age is a position-scoped guard.  Once the forced
                // exit has fully flattened the ledger, do not leave a stale
                // MAX_INVENTORY_AGE latch blocking every future entry.  The
                // following evaluate() still re-checks daily loss/drawdown,
                // so a real account-level trip remains fail-closed.
                entryBlockReason.compareAndSet("MAX_INVENTORY_AGE", null);
            }
        }
        evaluate(nowMs, config);
    }

    public synchronized void recordMark(BigDecimal price, long nowMs, BinanceProperties.Strategy config) {
        rollDayIfNeeded();
        if (price == null || price.signum() <= 0) return;
        markPrice = price;
        evaluate(nowMs, config);
    }

    public synchronized boolean permitsNewEntry(BigDecimal newOrderQty, BigDecimal entryPrice, long nowMs,
                                                BinanceProperties.Strategy config) {
        evaluate(nowMs, config);
        if (entryBlockReason.get() != null) return false;
        BigDecimal projectedNotional = positionQty.add(newOrderQty).multiply(entryPrice);
        if (projectedNotional.compareTo(config.getMaxInventoryUsdt()) > 0) {
            trip("MAX_INVENTORY_USDT");
            return false;
        }
        return true;
    }

    public synchronized RiskSnapshot snapshot() {
        BigDecimal unrealized = unrealizedPnl();
        return new RiskSnapshot(entryBlockReason.get(), positionQty, positionCostUsdt, markPrice, realizedPnlUsdt,
                unrealized, realizedPnlUsdt.add(unrealized), estimatedFeesUsdt, positionOpenedAtMs, ledgerDate);
    }

    public void trip(String reason) { entryBlockReason.compareAndSet(null, reason); }
    public String getEntryBlockReason() { return entryBlockReason.get(); }

    /**
     * Normalizes an untradeable ledger remainder after exchange balance reconciliation has
     * proved the base-asset position is below the symbol step size. Any remaining cost is
     * conservatively realized as a loss instead of silently disappearing.
     */
    public synchronized void reconcileExchangeFlat(long nowMs, BinanceProperties.Strategy config) {
        if (positionQty.signum() > 0) realizedPnlUsdt = realizedPnlUsdt.subtract(positionCostUsdt);
        positionQty = BigDecimal.ZERO;
        positionCostUsdt = BigDecimal.ZERO;
        positionOpenedAtMs = -1;
        entryBlockReason.compareAndSet("MAX_INVENTORY_AGE", null);
        evaluate(nowMs, config);
    }

    /** Safe only after exchange reconciliation has proved the previous symbol is flat. */
    public synchronized void resetForFlatSymbol() {
        positionQty = BigDecimal.ZERO;
        positionCostUsdt = BigDecimal.ZERO;
        realizedPnlUsdt = BigDecimal.ZERO;
        estimatedFeesUsdt = BigDecimal.ZERO;
        markPrice = null;
        peakNetPnlUsdt = BigDecimal.ZERO;
        positionOpenedAtMs = -1;
        ledgerDate = LocalDate.now(ZoneOffset.UTC);
        entryBlockReason.set(null);
    }

    /** Restores today's durable realized result while intentionally refusing to reconstruct inventory. */
    public synchronized void restoreFlatDaily(BigDecimal realizedNetPnl, BigDecimal actualFees, LocalDate date,
                                              BinanceProperties.Strategy config) {
        resetForFlatSymbol();
        ledgerDate = date == null ? LocalDate.now(ZoneOffset.UTC) : date;
        realizedPnlUsdt = realizedNetPnl == null ? BigDecimal.ZERO : realizedNetPnl;
        estimatedFeesUsdt = actualFees == null ? BigDecimal.ZERO : actualFees.max(BigDecimal.ZERO);
        peakNetPnlUsdt = realizedPnlUsdt.max(BigDecimal.ZERO);
        evaluate(System.currentTimeMillis(), config);
    }

    private void evaluate(long nowMs, BinanceProperties.Strategy config) {
        BigDecimal netPnl = realizedPnlUsdt.add(unrealizedPnl());
        peakNetPnlUsdt = peakNetPnlUsdt.max(netPnl);
        if (peakNetPnlUsdt.subtract(netPnl).compareTo(config.getMaxDailyDrawdownUsdt()) >= 0) trip("MAX_DAILY_DRAWDOWN");
        if (positionOpenedAtMs >= 0 && nowMs - positionOpenedAtMs >= config.getMaxInventoryAgeMs()) trip("MAX_INVENTORY_AGE");
    }

    private BigDecimal unrealizedPnl() {
        if (positionQty.signum() == 0 || markPrice == null) return BigDecimal.ZERO;
        return positionQty.multiply(markPrice).subtract(positionCostUsdt);
    }

    private void rollDayIfNeeded() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        if (!today.equals(ledgerDate) && positionQty.signum() == 0) {
            ledgerDate = today;
            realizedPnlUsdt = BigDecimal.ZERO;
            estimatedFeesUsdt = BigDecimal.ZERO;
            peakNetPnlUsdt = BigDecimal.ZERO;
            entryBlockReason.set(null);
        }
    }

    public record RiskSnapshot(String entryBlockReason, BigDecimal positionQty, BigDecimal positionCostUsdt,
                               BigDecimal markPrice, BigDecimal realizedPnlUsdt, BigDecimal unrealizedPnlUsdt,
                               BigDecimal estimatedNetPnlUsdt, BigDecimal estimatedFeesUsdt,
                               long positionOpenedAtMs, LocalDate ledgerDate) { }
}
