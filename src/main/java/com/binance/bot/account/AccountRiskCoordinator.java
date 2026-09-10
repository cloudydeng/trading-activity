package com.binance.bot.account;

import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.TradingRiskGuard;
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** Serializes same-account BUY submissions and enforces account-wide capital safety. */
public final class AccountRiskCoordinator {
    private static final long BNB_CACHE_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long FORCED_BNB_DEDUP_MS = BNB_CACHE_MS;
    private final Map<String, Supplier<TradingRiskGuard.RiskSnapshot>> riskSuppliers = new ConcurrentHashMap<>();
    private final Map<String, Supplier<Boolean>> reconciliationSuppliers = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> pendingEntryNotional = new LinkedHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private LocalDate drawdownDate = LocalDate.now(ZoneOffset.UTC);
    private BigDecimal peakAccountNetPnl = BigDecimal.ZERO;
    private String latchedEntryBlockReason;
    private BnbBalanceSnapshot bnbBalanceSnapshot;
    private long lastBnbBalanceAttemptAtMs;
    private BigDecimal freeQuoteBalance;
    private BigDecimal quoteBalancePositionCostBaseline = BigDecimal.ZERO;

    public void register(String engineId, Supplier<TradingRiskGuard.RiskSnapshot> riskSupplier) {
        register(engineId, riskSupplier, () -> true);
    }

    public void register(String engineId, Supplier<TradingRiskGuard.RiskSnapshot> riskSupplier,
                         Supplier<Boolean> reconciliationSupplier) {
        if (engineId != null && riskSupplier != null) {
            riskSuppliers.put(engineId, riskSupplier);
            reconciliationSuppliers.put(engineId,
                    reconciliationSupplier == null ? () -> false : reconciliationSupplier);
        }
    }

    public void unregister(String engineId) {
        lock.lock();
        try {
            riskSuppliers.remove(engineId);
            reconciliationSuppliers.remove(engineId);
            pendingEntryNotional.remove(engineId);
        } finally {
            lock.unlock();
        }
    }

    /** Seeds the shared quote budget from one authoritative account snapshot. */
    public boolean updateQuoteBalance(JsonNode accountInfo, String quoteAsset) {
        lock.lock();
        try {
            if (accountInfo == null || !accountInfo.path("balances").isArray()) return false;
            for (JsonNode balance : accountInfo.path("balances")) {
                if (quoteAsset.equalsIgnoreCase(balance.path("asset").asText())) {
                    try {
                        freeQuoteBalance = new BigDecimal(balance.path("free").asText("0"));
                        quoteBalancePositionCostBaseline = accountRiskSnapshot().positionCost();
                        return true;
                    } catch (NumberFormatException ignored) {
                        return false;
                    }
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public EntryReservation reserveEntry(String engineId, BigDecimal requestedNotional,
                                                       BigDecimal accountExposureLimit,
                                                       BigDecimal accountDrawdownLimit) {
        lock.lock();
        try {
            if (engineId == null || requestedNotional == null || requestedNotional.signum() <= 0) {
                return new EntryReservation(false, "账户风险预留参数无效");
            }
            AccountRiskSnapshot accountRisk = accountRiskSnapshot();
            if (!accountRisk.complete()) {
                return new EntryReservation(false, "无法确认账户所有币种的风险状态");
            }
            String riskBlock = accountEntryBlockReason(accountRisk, accountDrawdownLimit);
            if (riskBlock != null) return new EntryReservation(false, riskBlock);
            BigDecimal total = accountRisk.positionCost();
            for (Map.Entry<String, BigDecimal> entry : pendingEntryNotional.entrySet()) {
                if (!engineId.equals(entry.getKey())) total = total.add(entry.getValue());
            }
            BigDecimal projected = total.add(requestedNotional);
            if (accountExposureLimit != null && accountExposureLimit.signum() > 0
                    && projected.compareTo(accountExposureLimit) > 0) {
                return new EntryReservation(false, "账户所有币种的持仓和活动买单已达到总风险上限 "
                        + accountExposureLimit.stripTrailingZeros().toPlainString() + " USDT");
            }
            if (freeQuoteBalance != null) {
                BigDecimal spentSinceSnapshot = accountRisk.positionCost()
                        .subtract(quoteBalancePositionCostBaseline).max(BigDecimal.ZERO);
                BigDecimal available = freeQuoteBalance.subtract(spentSinceSnapshot);
                for (Map.Entry<String, BigDecimal> entry : pendingEntryNotional.entrySet()) {
                    if (!engineId.equals(entry.getKey())) available = available.subtract(entry.getValue());
                }
                if (requestedNotional.compareTo(available.max(BigDecimal.ZERO)) > 0) {
                    return new EntryReservation(false, "账户可用 USDT 不足，暂缓新买单");
                }
            }
            pendingEntryNotional.put(engineId, requestedNotional);
            return new EntryReservation(true, "");
        } finally {
            lock.unlock();
        }
    }

    public void releaseEntry(String engineId) {
        lock.lock();
        try {
            if (engineId != null) pendingEntryNotional.remove(engineId);
        } finally {
            lock.unlock();
        }
    }

    /** The exchange request remains inside this lock, so two symbols cannot submit BUY concurrently. */
    public JsonNode submitBuy(Supplier<JsonNode> submission) {
        lock.lock();
        try {
            return submission.get();
        } finally {
            lock.unlock();
        }
    }

    public ExposureSnapshot snapshot() {
        lock.lock();
        try {
            AccountRiskSnapshot accountRisk = accountRiskSnapshot();
            BigDecimal positions = accountRisk.positionCost();
            BigDecimal pending = pendingEntryNotional.values().stream().reduce(BigDecimal.ZERO, BigDecimal::add);
            return new ExposureSnapshot(positions, pending, positions.add(pending),
                    accountRisk.todayNetPnl(), peakAccountNetPnl, accountRisk.complete(),
                    accountRisk.complete() ? latchedEntryBlockReason : "无法确认账户所有币种的风险状态");
        } finally {
            lock.unlock();
        }
    }

    /** Re-evaluated on every market tick so another symbol's working BUY is canceled promptly. */
    public String accountEntryBlockReason(BigDecimal accountDrawdownLimit) {
        lock.lock();
        try {
            AccountRiskSnapshot accountRisk = accountRiskSnapshot();
            if (!accountRisk.complete()) return "无法确认账户所有币种的风险状态";
            return accountEntryBlockReason(accountRisk, accountDrawdownLimit);
        } finally {
            lock.unlock();
        }
    }

    /** One account-level BNB read is shared by all symbol engines for 30 seconds. */
    public BnbBalanceSnapshot refreshBnbBalance(boolean force,
                                                              BinanceAccountTradeClient tradeClient) {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            long attemptAge = now - lastBnbBalanceAttemptAtMs;
            if (lastBnbBalanceAttemptAtMs > 0
                    && ((!force && attemptAge < BNB_CACHE_MS) || (force && attemptAge < FORCED_BNB_DEDUP_MS))) {
                return bnbBalanceSnapshot;
            }
            if (bnbBalanceSnapshot != null) {
                long age = now - bnbBalanceSnapshot.checkedAtMs();
                if ((!force && age < BNB_CACHE_MS) || (force && age < FORCED_BNB_DEDUP_MS)) {
                    return bnbBalanceSnapshot;
                }
            }
            lastBnbBalanceAttemptAtMs = now;
            BinanceAccountTradeClient.AssetBalance balance = tradeClient.getAssetBalance("BNB");
            if (balance == null || balance.total() == null) return bnbBalanceSnapshot;
            BigDecimal price = tradeClient.getTickerPrice("BNBUSDT");
            if (price == null || price.signum() <= 0) {
                return bnbBalanceSnapshot;
            }
            bnbBalanceSnapshot = new BnbBalanceSnapshot(balance.total(), price,
                    balance.total().multiply(price), now);
            return bnbBalanceSnapshot;
        } finally {
            lock.unlock();
        }
    }

    private AccountRiskSnapshot accountRiskSnapshot() {
        BigDecimal positionCost = BigDecimal.ZERO;
        BigDecimal todayNetPnl = BigDecimal.ZERO;
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (Supplier<TradingRiskGuard.RiskSnapshot> supplier : riskSuppliers.values()) {
            try {
                TradingRiskGuard.RiskSnapshot value = supplier.get();
                if (value == null) return new AccountRiskSnapshot(positionCost, todayNetPnl, false);
                if (value.positionCostUsdt() != null && value.positionCostUsdt().signum() > 0) {
                    positionCost = positionCost.add(value.positionCostUsdt());
                }
                if (today.equals(value.ledgerDate()) && value.estimatedNetPnlUsdt() != null) {
                    todayNetPnl = todayNetPnl.add(value.estimatedNetPnlUsdt());
                }
            } catch (RuntimeException ignored) {
                return new AccountRiskSnapshot(positionCost, todayNetPnl, false);
            }
        }
        for (Supplier<Boolean> supplier : reconciliationSuppliers.values()) {
            try {
                if (!Boolean.TRUE.equals(supplier.get())) {
                    return new AccountRiskSnapshot(positionCost, todayNetPnl, false);
                }
            } catch (RuntimeException ignored) {
                return new AccountRiskSnapshot(positionCost, todayNetPnl, false);
            }
        }
        return new AccountRiskSnapshot(positionCost, todayNetPnl, true);
    }

    private String accountEntryBlockReason(AccountRiskSnapshot accountRisk, BigDecimal accountDrawdownLimit) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        if (!today.equals(drawdownDate)) {
            drawdownDate = today;
            peakAccountNetPnl = accountRisk.todayNetPnl().max(BigDecimal.ZERO);
            latchedEntryBlockReason = null;
        } else {
            peakAccountNetPnl = peakAccountNetPnl.max(accountRisk.todayNetPnl());
        }
        if (latchedEntryBlockReason != null) return latchedEntryBlockReason;
        if (accountDrawdownLimit != null && accountDrawdownLimit.signum() > 0
                && peakAccountNetPnl.subtract(accountRisk.todayNetPnl()).compareTo(accountDrawdownLimit) >= 0) {
            latchedEntryBlockReason = "账户所有币种合计已达到今日最大回撤限制";
        }
        return latchedEntryBlockReason;
    }

    private record AccountRiskSnapshot(BigDecimal positionCost, BigDecimal todayNetPnl, boolean complete) { }
    public record EntryReservation(boolean accepted, String reason) { }
    public record ExposureSnapshot(BigDecimal positionCost, BigDecimal pendingEntryNotional,
                                   BigDecimal totalExposure, BigDecimal todayNetPnl,
                                   BigDecimal peakTodayNetPnl, boolean complete,
                                   String entryBlockReason) { }
    public record BnbBalanceSnapshot(BigDecimal quantity, BigDecimal priceUsdt,
                                     BigDecimal valueUsdt, long checkedAtMs) { }
}
