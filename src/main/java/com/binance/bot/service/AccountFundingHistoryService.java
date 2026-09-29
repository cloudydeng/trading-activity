package com.binance.bot.service;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Read-only Binance funding import. Balances remain authoritative and are not inferred from these events. */
@Service
public class AccountFundingHistoryService {
    static final int LOOKBACK_DAYS = 179; // Safely inside Binance's six-calendar-month transfer limit.
    private static final long DAY_MS = Duration.ofDays(1).toMillis();
    private static final long REFRESH_MS = Duration.ofMinutes(10).toMillis();
    private static final long FULL_REFRESH_MS = Duration.ofDays(1).toMillis();
    private static final int MAX_PAGES = 20;
    private static final List<String> INCOMING_SPOT_TYPES = List.of(
            "UMFUTURE_MAIN", "CMFUTURE_MAIN", "MARGIN_MAIN", "FUNDING_MAIN",
            "OPTION_MAIN", "PORTFOLIO_MARGIN_MAIN");

    private final TradingAccountManager accountManager;
    private final AccountFundingHistoryStore store;
    private final Map<String, FetchStatus> cache = new HashMap<>();

    public AccountFundingHistoryService(TradingAccountManager accountManager, AccountFundingHistoryStore store) {
        this.accountManager = accountManager;
        this.store = store;
    }

    public synchronized Snapshot snapshot() {
        long now = System.currentTimeMillis();
        long since = now - LOOKBACK_DAYS * DAY_MS;
        List<AccountView> accounts = new ArrayList<>();
        for (AccountTradingRuntime runtime : accountManager.runtimes()) {
            String id = runtime.accountId();
            FetchStatus status = cache.get(id);
            if (status == null || now - status.atMs() >= REFRESH_MS) {
                status = refresh(runtime, since, now);
                cache.put(id, status);
            }
            Map<String, AccountFundingHistoryStore.Totals> totals = store.totals(id, since);
            List<AssetView> assets = totals.entrySet().stream()
                    .map(entry -> new AssetView(entry.getKey(),
                            entry.getValue().deposits().toPlainString(),
                            entry.getValue().transfers().toPlainString()))
                    .sorted(Comparator.comparing(AssetView::asset)).toList();
            accounts.add(new AccountView(id, assets, status.depositComplete(),
                    status.transferComplete(), status.error()));
        }
        return new Snapshot(since, now, accounts);
    }

    private FetchStatus refresh(AccountTradingRuntime runtime, long since, long now) {
        List<String> errors = new ArrayList<>();
        String id = runtime.accountId();
        BinanceAccountTradeClient client = runtime.tradeClient();
        boolean depositComplete = true;
        boolean transferComplete = true;
        try {
            boolean full = now - store.lastFullSyncAt(id, "deposit") >= FULL_REFRESH_MS;
            importDeposits(id, client, full ? since : now - 7 * DAY_MS, now);
            if (full) store.markFullSync(id, "deposit", now);
        } catch (RuntimeException failure) {
            errors.add("充值记录读取不完整");
            depositComplete = false;
        }
        for (String type : INCOMING_SPOT_TYPES) {
            try {
                String syncKind = "transfer:" + type;
                boolean full = now - store.lastFullSyncAt(id, syncKind) >= FULL_REFRESH_MS;
                importTransfers(id, client, type, full ? since : now - 7 * DAY_MS, now);
                if (full) store.markFullSync(id, syncKind, now);
            } catch (RuntimeException failure) {
                transferComplete = false;
                if (!errors.contains("钱包划转记录读取不完整（可能缺少接口权限）")) {
                    errors.add("钱包划转记录读取不完整（可能缺少接口权限）");
                }
            }
        }
        return new FetchStatus(now, depositComplete, transferComplete,
                errors.isEmpty() ? null : String.join("；", errors));
    }

    private void importDeposits(String accountId, BinanceAccountTradeClient client, long since, long until) {
        // Binance requires each deposit-history request to cover strictly less than 90 days.
        for (long start = since; start <= until; start += 89 * DAY_MS) {
            long end = Math.min(until, start + 89 * DAY_MS - 1);
            boolean exhausted = false;
            for (int page = 0; page < MAX_PAGES; page++) {
                JsonNode result = client.getDepositHistory(start, end, page * 1000, 1000);
                if (result == null || !result.isArray()) throw new IllegalStateException("deposit history unavailable");
                for (JsonNode item : result) {
                    if (item.path("status").asInt(-1) != 1 || item.path("walletType").asInt(0) != 0) continue;
                    String id = item.path("id").asText("");
                    String asset = item.path("coin").asText("");
                    long at = item.path("insertTime").asLong(0);
                    store.save(new AccountFundingHistoryStore.Event(accountId, "deposit", id, asset,
                            positiveAmount(item.path("amount").asText("0")), at));
                }
                if (result.size() < 1000) {
                    exhausted = true;
                    break;
                }
            }
            if (!exhausted) throw new IllegalStateException("deposit history page limit reached");
            if (end == until) break;
        }
    }

    private void importTransfers(String accountId, BinanceAccountTradeClient client,
                                 String type, long since, long until) {
        boolean exhausted = false;
        for (int page = 1; page <= MAX_PAGES; page++) {
            JsonNode result = client.getUniversalTransferHistory(type, since, until, page, 100);
            JsonNode rows = result == null ? null : result.path("rows");
            if (rows == null || !rows.isArray()) throw new IllegalStateException("transfer history unavailable");
            for (JsonNode item : rows) {
                if (!"CONFIRMED".equals(item.path("status").asText())) continue;
                String id = item.path("tranId").asText("");
                String asset = item.path("asset").asText("");
                long at = item.path("timestamp").asLong(0);
                store.save(new AccountFundingHistoryStore.Event(accountId, "transfer", type + ":" + id,
                        asset, positiveAmount(item.path("amount").asText("0")), at));
            }
            int total = result.path("total").asInt(0);
            if (rows.size() < 100 || (total > 0 && page * 100 >= total)) {
                exhausted = true;
                break;
            }
        }
        if (!exhausted) throw new IllegalStateException("transfer history page limit reached");
    }

    private static BigDecimal positiveAmount(String text) {
        BigDecimal amount = new BigDecimal(text);
        if (amount.signum() <= 0) throw new IllegalArgumentException("invalid funding amount");
        return amount;
    }

    private record FetchStatus(long atMs, boolean depositComplete, boolean transferComplete, String error) { }
    public record Snapshot(long sinceMs, long updatedAtMs, List<AccountView> accounts) { }
    public record AccountView(String accountId, List<AssetView> assets,
                              boolean depositComplete, boolean transferComplete, String error) { }
    public record AssetView(String asset, String deposits, String transfers) { }
}
