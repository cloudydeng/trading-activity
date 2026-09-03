package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Durable, UTC-day aggregates for the small set of production metrics the bot needs.
 * Monetary values are stored as decimal strings so SQLite never rounds them through binary floating point.
 */
@Slf4j
@Component
public class DailyTradeStatsStore {
    private static final MathContext MC = MathContext.DECIMAL64;
    private static final BigDecimal ONE_MILLION = new BigDecimal("1000000");
    private static final TypeReference<LinkedHashSet<String>> STRING_SET = new TypeReference<>() { };

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Connection connection;

    public DailyTradeStatsStore(BinanceProperties properties) {
        try {
            Path dbPath = Path.of(properties.getStorage().getDailyStatsDb()).toAbsolutePath().normalize();
            Path parent = dbPath.getParent();
            if (parent != null) Files.createDirectories(parent);
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            initializeSchema(properties);
            log.info("每日交易统计已启用: {}", dbPath);
        } catch (Exception e) {
            throw new IllegalStateException("无法初始化每日交易统计数据库", e);
        }
    }

    private void initializeSchema(BinanceProperties properties) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=FULL");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS runtime_setting (
                      setting_key TEXT PRIMARY KEY,
                      setting_value TEXT NOT NULL,
                      updated_at INTEGER NOT NULL
                    )
                    """);
        }
        if (tableExists("daily_trade_stats") && !columnExists("daily_trade_stats", "account_id")) {
            migrateLegacyDailyStats();
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS daily_trade_stats (
                      account_id TEXT NOT NULL,
                      account_alias TEXT NOT NULL,
                      trade_date TEXT NOT NULL,
                      symbol TEXT NOT NULL,
                      buy_volume TEXT NOT NULL,
                      sell_volume TEXT NOT NULL,
                      total_volume TEXT NOT NULL,
                      commission_quote TEXT NOT NULL,
                      economic_fee_quote TEXT NOT NULL,
                      realized_gross_pnl TEXT NOT NULL,
                      position_qty TEXT NOT NULL,
                      position_cost_quote TEXT NOT NULL,
                      trade_count INTEGER NOT NULL,
                      round_trips INTEGER NOT NULL,
                      commission_complete INTEGER NOT NULL,
                      processed_trade_ids TEXT NOT NULL,
                      updated_at INTEGER NOT NULL,
                      PRIMARY KEY (account_id, trade_date, symbol)
                    )
                    """);
        }
        rekeyLegacyAccountIds(properties);
    }

    private boolean tableExists(String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")) {
            statement.setString(1, table);
            try (ResultSet row = statement.executeQuery()) { return row.next(); }
        }
    }

    private boolean columnExists(String table, String column) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rows.next()) if (column.equalsIgnoreCase(rows.getString("name"))) return true;
            return false;
        }
    }

    /** Keeps the v1 table as a recoverable backup after copying every aggregate into the v2 schema. */
    private void migrateLegacyDailyStats() throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE daily_trade_stats RENAME TO daily_trade_stats_legacy_v1");
            statement.execute("""
                    CREATE TABLE daily_trade_stats (
                      account_id TEXT NOT NULL, account_alias TEXT NOT NULL, trade_date TEXT NOT NULL,
                      symbol TEXT NOT NULL, buy_volume TEXT NOT NULL, sell_volume TEXT NOT NULL,
                      total_volume TEXT NOT NULL, commission_quote TEXT NOT NULL,
                      economic_fee_quote TEXT NOT NULL, realized_gross_pnl TEXT NOT NULL,
                      position_qty TEXT NOT NULL, position_cost_quote TEXT NOT NULL,
                      trade_count INTEGER NOT NULL, round_trips INTEGER NOT NULL,
                      commission_complete INTEGER NOT NULL, processed_trade_ids TEXT NOT NULL,
                      updated_at INTEGER NOT NULL, PRIMARY KEY (account_id, trade_date, symbol)
                    )
                    """);
            statement.execute("""
                    INSERT INTO daily_trade_stats
                    SELECT api_alias, api_alias, trade_date, symbol, buy_volume, sell_volume, total_volume,
                           commission_quote, economic_fee_quote, realized_gross_pnl, position_qty,
                           position_cost_quote, trade_count, round_trips, commission_complete,
                           processed_trade_ids, updated_at
                    FROM daily_trade_stats_legacy_v1
                    """);
            connection.commit();
            log.warn("每日统计数据库已迁移到 account_id 主键；旧表保留为 daily_trade_stats_legacy_v1");
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    /**
     * The v1 schema used the display alias as its primary identity. Multi-account runtimes use the
     * profile map key instead, so move those rows to the stable id before any engine restores risk.
     * Running this on every startup also repairs databases first opened by an early v2 build.
     */
    private void rekeyLegacyAccountIds(BinanceProperties properties) throws SQLException {
        Map<String, String> aliasToAccountId = legacyAliasMappings(properties);
        if (aliasToAccountId.isEmpty()) return;
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE daily_trade_stats SET account_id=? WHERE account_id=?")) {
            for (Map.Entry<String, String> mapping : aliasToAccountId.entrySet()) {
                if (mapping.getKey().equals(mapping.getValue())) continue;
                update.setString(1, mapping.getValue());
                update.setString(2, mapping.getKey());
                try {
                    int changed = update.executeUpdate();
                    if (changed > 0) {
                        log.warn("已将旧统计账号标识从 alias={} 迁移为 accountId={}，记录数={}",
                                mapping.getKey(), mapping.getValue(), changed);
                    }
                } catch (SQLException conflict) {
                    // Never merge aggregates implicitly. Keeping the legacy row is recoverable and
                    // safer than overwriting an existing account/date/symbol record.
                    log.error("旧统计账号标识迁移冲突，保留原记录: alias={} accountId={}",
                            mapping.getKey(), mapping.getValue(), conflict);
                }
            }
        }
    }

    private Map<String, String> legacyAliasMappings(BinanceProperties properties) {
        Map<String, String> unique = new HashMap<>();
        Set<String> ambiguous = new LinkedHashSet<>();
        Map<String, BinanceProperties.CredentialProfile> profiles = configuredProfiles(properties);
        profiles.forEach((accountId, profile) -> {
            if (profile == null || !profile.isEnabled() || profile.getAlias() == null
                    || profile.getAlias().isBlank() || accountId == null
                    || !accountId.trim().matches("[A-Za-z0-9._-]{1,64}")) return;
            String alias = profile.getAlias().trim();
            String previous = unique.putIfAbsent(alias, accountId.trim());
            if (previous != null && !previous.equals(accountId.trim())) ambiguous.add(alias);
        });
        ambiguous.forEach(alias -> {
            unique.remove(alias);
            log.error("多个账号配置使用相同 alias={}，无法自动迁移该别名的旧统计", alias);
        });
        if (profiles.isEmpty()
                && (properties.getAccountProfilesJson() == null
                    || properties.getAccountProfilesJson().isBlank())
                && properties.getApi().getApiKeyAlias() != null
                && !properties.getApi().getApiKeyAlias().isBlank()) {
            unique.put(properties.getApi().getApiKeyAlias().trim(), "default");
        }
        return unique;
    }

    private Map<String, BinanceProperties.CredentialProfile> configuredProfiles(BinanceProperties properties) {
        Map<String, BinanceProperties.CredentialProfile> result = new LinkedHashMap<>(
                properties.getApi().getProfiles());
        String profilesJson = properties.getAccountProfilesJson();
        if (profilesJson == null || profilesJson.isBlank()) return result;
        try {
            var root = objectMapper.readTree(profilesJson);
            if (root == null || !root.isObject()) throw new IllegalArgumentException("profiles must be an object");
            root.fields().forEachRemaining(entry -> result.put(entry.getKey(),
                    objectMapper.convertValue(entry.getValue(), BinanceProperties.CredentialProfile.class)));
        } catch (Exception e) {
            // Parser excerpts may contain credentials, so never log the exception message.
            log.error("BOT_ACCOUNT_PROFILES_JSON 无效，跳过旧统计别名迁移");
            result.clear();
        }
        return result;
    }

    public synchronized RecordResult recordTrade(String accountId, String accountAlias, String symbol,
                                                  long orderId, long tradeId,
                                                  String side, BigDecimal inventoryQuantity,
                                                  BigDecimal quoteQuantity, BigDecimal commission,
                                                  BigDecimal commissionQuoteEquivalent,
                                                  BigDecimal economicFeeQuote, long tradeTimeMs) {
        if (inventoryQuantity == null || inventoryQuantity.signum() <= 0
                || quoteQuantity == null || quoteQuantity.signum() <= 0) return RecordResult.IGNORED;
        LocalDate date = Instant.ofEpochMilli(tradeTimeMs > 0 ? tradeTimeMs : System.currentTimeMillis())
                .atZone(ZoneOffset.UTC).toLocalDate();
        String normalizedAccountId = normalizeAccountId(accountId);
        String alias = normalizeAlias(accountAlias);
        String normalizedSymbol = symbol.toUpperCase();
        String identity = orderId + ":" + (tradeId >= 0 ? Long.toString(tradeId)
                : side + ":" + inventoryQuantity.toPlainString() + ":" + quoteQuantity.toPlainString()
                + ":" + (commission == null ? "0" : commission.toPlainString()));
        try {
            connection.setAutoCommit(false);
            MutableStats stats = load(date, normalizedAccountId, normalizedSymbol);
            if (stats == null) stats = newStats(date, normalizedAccountId, alias, normalizedSymbol);
            stats.accountAlias = alias;
            if (!stats.processedTradeIds.add(identity)) {
                connection.rollback();
                return RecordResult.DUPLICATE;
            }
            applyTrade(stats, side, inventoryQuantity, quoteQuantity, commission,
                    commissionQuoteEquivalent, economicFeeQuote);
            upsert(stats);
            connection.commit();
            return RecordResult.APPLIED;
        } catch (Exception e) {
            rollbackQuietly();
            log.error("持久化每日交易统计失败: accountId={} alias={} symbol={} orderId={} tradeId={}",
                    normalizedAccountId, alias, normalizedSymbol, orderId, tradeId, e);
            return RecordResult.FAILED;
        } finally {
            setAutoCommitQuietly(true);
        }
    }

    private void applyTrade(MutableStats stats, String side, BigDecimal inventoryQuantity,
                            BigDecimal quoteQuantity, BigDecimal commission,
                            BigDecimal commissionQuoteEquivalent, BigDecimal economicFeeQuote) {
        boolean buy = "BUY".equalsIgnoreCase(side);
        stats.tradeCount++;
        stats.totalVolume = stats.totalVolume.add(quoteQuantity);
        if (buy) stats.buyVolume = stats.buyVolume.add(quoteQuantity);
        else stats.sellVolume = stats.sellVolume.add(quoteQuantity);

        if (commission != null && commission.signum() > 0) {
            if (commissionQuoteEquivalent == null) stats.commissionComplete = false;
            else stats.commissionQuote = stats.commissionQuote.add(commissionQuoteEquivalent.max(BigDecimal.ZERO));
        }
        if (economicFeeQuote != null) {
            stats.economicFeeQuote = stats.economicFeeQuote.add(economicFeeQuote.max(BigDecimal.ZERO));
        }

        if (buy) {
            stats.positionQty = stats.positionQty.add(inventoryQuantity);
            stats.positionCostQuote = stats.positionCostQuote.add(quoteQuantity);
            return;
        }
        if (stats.positionQty.signum() <= 0) {
            // The fee and volume remain authoritative, but an externally sourced sell has unknown cost basis.
            stats.commissionComplete = false;
            return;
        }
        BigDecimal closedQty = inventoryQuantity.min(stats.positionQty);
        BigDecimal averageCost = stats.positionCostQuote.divide(stats.positionQty, MC);
        BigDecimal allocatedCost = averageCost.multiply(closedQty);
        BigDecimal proceedsShare = inventoryQuantity.compareTo(closedQty) == 0 ? quoteQuantity
                : quoteQuantity.multiply(closedQty).divide(inventoryQuantity, MC);
        stats.realizedGrossPnl = stats.realizedGrossPnl.add(proceedsShare.subtract(allocatedCost));
        stats.positionQty = stats.positionQty.subtract(closedQty);
        stats.positionCostQuote = stats.positionCostQuote.subtract(allocatedCost);
        if (stats.positionQty.signum() == 0) {
            stats.positionQty = BigDecimal.ZERO;
            stats.positionCostQuote = BigDecimal.ZERO;
            stats.roundTrips++;
        }
    }

    public synchronized DailyStatsSnapshot today(String accountId, String accountAlias, String symbol) {
        return snapshot(LocalDate.now(ZoneOffset.UTC), accountId, accountAlias, symbol);
    }

    /** Legacy-compatible lookup where the old alias becomes both immutable id and display alias. */
    public synchronized DailyStatsSnapshot today(String accountId, String symbol) {
        return today(accountId, accountId, symbol);
    }

    /** Normalizes only an untradeable remainder after the exchange has proved the account flat. */
    public synchronized boolean reconcileFlatDust(String accountId, String symbol, BigDecimal stepSize) {
        LocalDate date = LocalDate.now(ZoneOffset.UTC);
        String normalizedAccountId = normalizeAccountId(accountId);
        String normalizedSymbol = symbol.toUpperCase();
        try {
            connection.setAutoCommit(false);
            MutableStats stats = load(date, normalizedAccountId, normalizedSymbol);
            if (stats == null || stats.positionQty.signum() == 0) {
                connection.rollback();
                return true;
            }
            if (stepSize == null || stepSize.signum() <= 0 || stats.positionQty.compareTo(stepSize) >= 0) {
                connection.rollback();
                return false;
            }
            stats.realizedGrossPnl = stats.realizedGrossPnl.subtract(stats.positionCostQuote);
            stats.positionQty = BigDecimal.ZERO;
            stats.positionCostQuote = BigDecimal.ZERO;
            stats.roundTrips++;
            upsert(stats);
            connection.commit();
            return true;
        } catch (Exception e) {
            rollbackQuietly();
            log.error("每日账本粉尘归零失败: accountId={} symbol={}", normalizedAccountId, normalizedSymbol, e);
            return false;
        } finally {
            setAutoCommitQuietly(true);
        }
    }

    public synchronized DailyStatsSnapshot snapshot(LocalDate date, String accountId, String accountAlias,
                                                    String symbol) {
        try {
            String normalizedAccountId = normalizeAccountId(accountId);
            MutableStats stats = load(date, normalizedAccountId, symbol.toUpperCase());
            return stats == null ? DailyStatsSnapshot.empty(date, normalizedAccountId,
                    normalizeAlias(accountAlias), symbol.toUpperCase())
                    : stats.snapshot();
        } catch (Exception e) {
            throw new IllegalStateException("读取每日交易统计失败", e);
        }
    }

    public synchronized DailyStatsSnapshot snapshot(LocalDate date, String accountId, String symbol) {
        return snapshot(date, accountId, accountId, symbol);
    }

    public synchronized List<DailyStatsSnapshot> recent(String accountId, String symbol, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 90));
        List<DailyStatsSnapshot> result = new ArrayList<>();
        String sql = "SELECT * FROM daily_trade_stats WHERE account_id=? AND symbol=? ORDER BY trade_date DESC LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, normalizeAccountId(accountId));
            statement.setString(2, symbol.toUpperCase());
            statement.setInt(3, safeLimit);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) result.add(fromRow(rows).snapshot());
            }
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("读取历史每日交易统计失败", e);
        }
    }

    public synchronized void saveActiveSymbol(String accountId, String symbol) {
        saveSetting("active_symbol:" + normalizeAccountId(accountId), symbol.toUpperCase(), "保存当前交易对失败");
    }

    public synchronized java.util.Optional<String> loadActiveSymbol(String accountId) {
        try {
            java.util.Optional<String> scoped = loadSetting("active_symbol:" + normalizeAccountId(accountId));
            return scoped.isPresent() ? scoped : loadSetting("active_symbol");
        } catch (SQLException e) {
            throw new IllegalStateException("读取当前交易对失败", e);
        }
    }

    private void saveSetting(String key, String value, String errorMessage) {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO runtime_setting(setting_key, setting_value, updated_at) VALUES(?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
                """)) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.setLong(3, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(errorMessage, e);
        }
    }

    private java.util.Optional<String> loadSetting(String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT setting_value FROM runtime_setting WHERE setting_key=?")) {
            statement.setString(1, key);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? java.util.Optional.of(row.getString(1)) : java.util.Optional.empty();
            }
        }
    }

    private MutableStats newStats(LocalDate date, String accountId, String accountAlias, String symbol) throws Exception {
        MutableStats stats = new MutableStats(date, accountId, accountAlias, symbol);
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT position_qty, position_cost_quote FROM daily_trade_stats
                WHERE account_id=? AND symbol=? AND trade_date<? ORDER BY trade_date DESC LIMIT 1
                """)) {
            statement.setString(1, accountId);
            statement.setString(2, symbol);
            statement.setString(3, date.toString());
            try (ResultSet row = statement.executeQuery()) {
                if (row.next()) {
                    stats.positionQty = decimal(row.getString("position_qty"));
                    stats.positionCostQuote = decimal(row.getString("position_cost_quote"));
                }
            }
        }
        return stats;
    }

    private MutableStats load(LocalDate date, String accountId, String symbol) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM daily_trade_stats WHERE account_id=? AND trade_date=? AND symbol=?")) {
            statement.setString(1, accountId);
            statement.setString(2, date.toString());
            statement.setString(3, symbol);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? fromRow(row) : null;
            }
        }
    }

    private MutableStats fromRow(ResultSet row) throws Exception {
        MutableStats stats = new MutableStats(LocalDate.parse(row.getString("trade_date")),
                row.getString("account_id"), row.getString("account_alias"), row.getString("symbol"));
        stats.buyVolume = decimal(row.getString("buy_volume"));
        stats.sellVolume = decimal(row.getString("sell_volume"));
        stats.totalVolume = decimal(row.getString("total_volume"));
        stats.commissionQuote = decimal(row.getString("commission_quote"));
        stats.economicFeeQuote = decimal(row.getString("economic_fee_quote"));
        stats.realizedGrossPnl = decimal(row.getString("realized_gross_pnl"));
        stats.positionQty = decimal(row.getString("position_qty"));
        stats.positionCostQuote = decimal(row.getString("position_cost_quote"));
        stats.tradeCount = row.getInt("trade_count");
        stats.roundTrips = row.getInt("round_trips");
        stats.commissionComplete = row.getInt("commission_complete") == 1;
        stats.processedTradeIds.addAll(objectMapper.readValue(row.getString("processed_trade_ids"), STRING_SET));
        return stats;
    }

    private void upsert(MutableStats stats) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO daily_trade_stats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(account_id, trade_date, symbol) DO UPDATE SET
                  account_alias=excluded.account_alias,
                  buy_volume=excluded.buy_volume, sell_volume=excluded.sell_volume,
                  total_volume=excluded.total_volume, commission_quote=excluded.commission_quote,
                  economic_fee_quote=excluded.economic_fee_quote, realized_gross_pnl=excluded.realized_gross_pnl,
                  position_qty=excluded.position_qty, position_cost_quote=excluded.position_cost_quote,
                  trade_count=excluded.trade_count, round_trips=excluded.round_trips,
                  commission_complete=excluded.commission_complete,
                  processed_trade_ids=excluded.processed_trade_ids, updated_at=excluded.updated_at
                """)) {
            int i = 1;
            statement.setString(i++, stats.accountId);
            statement.setString(i++, stats.accountAlias);
            statement.setString(i++, stats.date.toString());
            statement.setString(i++, stats.symbol);
            statement.setString(i++, stats.buyVolume.toPlainString());
            statement.setString(i++, stats.sellVolume.toPlainString());
            statement.setString(i++, stats.totalVolume.toPlainString());
            statement.setString(i++, stats.commissionQuote.toPlainString());
            statement.setString(i++, stats.economicFeeQuote.toPlainString());
            statement.setString(i++, stats.realizedGrossPnl.toPlainString());
            statement.setString(i++, stats.positionQty.toPlainString());
            statement.setString(i++, stats.positionCostQuote.toPlainString());
            statement.setInt(i++, stats.tradeCount);
            statement.setInt(i++, stats.roundTrips);
            statement.setInt(i++, stats.commissionComplete ? 1 : 0);
            statement.setString(i++, objectMapper.writeValueAsString(stats.processedTradeIds));
            statement.setLong(i, System.currentTimeMillis());
            statement.executeUpdate();
        }
    }

    private String normalizeAlias(String alias) {
        return alias == null || alias.isBlank() ? "unlabeled" : alias.trim();
    }

    private String normalizeAccountId(String accountId) {
        String normalized = accountId == null ? "" : accountId.trim();
        if (!normalized.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("无效 accountId");
        }
        return normalized;
    }

    private BigDecimal decimal(String value) { return new BigDecimal(value); }
    private void rollbackQuietly() { try { connection.rollback(); } catch (SQLException ignored) { } }
    private void setAutoCommitQuietly(boolean value) { try { connection.setAutoCommit(value); } catch (SQLException ignored) { } }

    @PreDestroy
    public synchronized void close() {
        try { connection.close(); } catch (SQLException e) { log.warn("关闭每日统计数据库失败", e); }
    }

    public enum RecordResult { APPLIED, DUPLICATE, IGNORED, FAILED }

    public record DailyStatsSnapshot(LocalDate date, String accountId, String accountAlias, String symbol,
                                     BigDecimal buyVolumeQuote, BigDecimal sellVolumeQuote,
                                     BigDecimal totalVolumeQuote, BigDecimal totalCommissionQuoteEquivalent,
                                     BigDecimal costPerMillionVolume, BigDecimal realizedGrossPnlQuote,
                                     BigDecimal netRealizedPnlQuote, BigDecimal positionQty,
                                     BigDecimal positionCostQuote, int tradeCount, int roundTrips,
                                     boolean commissionConversionComplete) {
        public String apiAlias() { return accountAlias; }

        static DailyStatsSnapshot empty(LocalDate date, String accountId, String accountAlias, String symbol) {
            return new DailyStatsSnapshot(date, accountId, accountAlias, symbol, BigDecimal.ZERO, BigDecimal.ZERO,
                    BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                    BigDecimal.ZERO, BigDecimal.ZERO, 0, 0, true);
        }
    }

    private final class MutableStats {
        private final LocalDate date;
        private final String accountId;
        private String accountAlias;
        private final String symbol;
        private BigDecimal buyVolume = BigDecimal.ZERO;
        private BigDecimal sellVolume = BigDecimal.ZERO;
        private BigDecimal totalVolume = BigDecimal.ZERO;
        private BigDecimal commissionQuote = BigDecimal.ZERO;
        private BigDecimal economicFeeQuote = BigDecimal.ZERO;
        private BigDecimal realizedGrossPnl = BigDecimal.ZERO;
        private BigDecimal positionQty = BigDecimal.ZERO;
        private BigDecimal positionCostQuote = BigDecimal.ZERO;
        private int tradeCount;
        private int roundTrips;
        private boolean commissionComplete = true;
        private final Set<String> processedTradeIds = new LinkedHashSet<>();

        private MutableStats(LocalDate date, String accountId, String accountAlias, String symbol) {
            this.date = date;
            this.accountId = accountId;
            this.accountAlias = accountAlias;
            this.symbol = symbol;
        }

        private DailyStatsSnapshot snapshot() {
            BigDecimal costPerMillion = commissionComplete && totalVolume.signum() > 0
                    ? commissionQuote.multiply(ONE_MILLION).divide(totalVolume, MC) : null;
            return new DailyStatsSnapshot(date, accountId, accountAlias, symbol, buyVolume, sellVolume, totalVolume,
                    commissionQuote, costPerMillion, realizedGrossPnl,
                    realizedGrossPnl.subtract(economicFeeQuote), positionQty, positionCostQuote,
                    tradeCount, roundTrips, commissionComplete);
        }
    }
}
