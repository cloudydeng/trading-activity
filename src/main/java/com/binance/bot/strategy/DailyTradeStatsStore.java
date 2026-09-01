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
import java.util.LinkedHashSet;
import java.util.List;
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

    private final BinanceProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Connection connection;

    public DailyTradeStatsStore(BinanceProperties properties) {
        this.properties = properties;
        try {
            Path dbPath = Path.of(properties.getStorage().getDailyStatsDb()).toAbsolutePath().normalize();
            Path parent = dbPath.getParent();
            if (parent != null) Files.createDirectories(parent);
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            initializeSchema();
            loadPersistedSymbol().ifPresent(symbol -> properties.getStrategy().setSymbol(symbol));
            log.info("每日交易统计已启用: {}", dbPath);
        } catch (Exception e) {
            throw new IllegalStateException("无法初始化每日交易统计数据库", e);
        }
    }

    private void initializeSchema() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=FULL");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS daily_trade_stats (
                      trade_date TEXT NOT NULL,
                      api_alias TEXT NOT NULL,
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
                      PRIMARY KEY (trade_date, api_alias, symbol)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS runtime_setting (
                      setting_key TEXT PRIMARY KEY,
                      setting_value TEXT NOT NULL,
                      updated_at INTEGER NOT NULL
                    )
                    """);
        }
    }

    public synchronized RecordResult recordTrade(String apiAlias, String symbol, long orderId, long tradeId,
                                                  String side, BigDecimal inventoryQuantity,
                                                  BigDecimal quoteQuantity, BigDecimal commission,
                                                  BigDecimal commissionQuoteEquivalent,
                                                  BigDecimal economicFeeQuote, long tradeTimeMs) {
        if (inventoryQuantity == null || inventoryQuantity.signum() <= 0
                || quoteQuantity == null || quoteQuantity.signum() <= 0) return RecordResult.IGNORED;
        LocalDate date = Instant.ofEpochMilli(tradeTimeMs > 0 ? tradeTimeMs : System.currentTimeMillis())
                .atZone(ZoneOffset.UTC).toLocalDate();
        String alias = normalizeAlias(apiAlias);
        String normalizedSymbol = symbol.toUpperCase();
        String identity = orderId + ":" + (tradeId >= 0 ? Long.toString(tradeId)
                : side + ":" + inventoryQuantity.toPlainString() + ":" + quoteQuantity.toPlainString()
                + ":" + (commission == null ? "0" : commission.toPlainString()));
        try {
            connection.setAutoCommit(false);
            MutableStats stats = load(date, alias, normalizedSymbol);
            if (stats == null) stats = newStats(date, alias, normalizedSymbol);
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
            log.error("持久化每日交易统计失败: alias={} symbol={} orderId={} tradeId={}",
                    alias, normalizedSymbol, orderId, tradeId, e);
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

    public synchronized DailyStatsSnapshot today(String apiAlias, String symbol) {
        return snapshot(LocalDate.now(ZoneOffset.UTC), apiAlias, symbol);
    }

    public synchronized DailyStatsSnapshot snapshot(LocalDate date, String apiAlias, String symbol) {
        try {
            MutableStats stats = load(date, normalizeAlias(apiAlias), symbol.toUpperCase());
            return stats == null ? DailyStatsSnapshot.empty(date, normalizeAlias(apiAlias), symbol.toUpperCase())
                    : stats.snapshot();
        } catch (Exception e) {
            throw new IllegalStateException("读取每日交易统计失败", e);
        }
    }

    public synchronized List<DailyStatsSnapshot> recent(String apiAlias, String symbol, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 90));
        List<DailyStatsSnapshot> result = new ArrayList<>();
        String sql = "SELECT * FROM daily_trade_stats WHERE api_alias=? AND symbol=? ORDER BY trade_date DESC LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, normalizeAlias(apiAlias));
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

    public synchronized void saveActiveSymbol(String symbol) {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO runtime_setting(setting_key, setting_value, updated_at) VALUES('active_symbol', ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
                """)) {
            statement.setString(1, symbol.toUpperCase());
            statement.setLong(2, System.currentTimeMillis());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("保存当前交易对失败", e);
        }
    }

    private java.util.Optional<String> loadPersistedSymbol() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT setting_value FROM runtime_setting WHERE setting_key='active_symbol'");
             ResultSet row = statement.executeQuery()) {
            return row.next() ? java.util.Optional.of(row.getString(1)) : java.util.Optional.empty();
        }
    }

    private MutableStats newStats(LocalDate date, String alias, String symbol) throws Exception {
        MutableStats stats = new MutableStats(date, alias, symbol);
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT position_qty, position_cost_quote FROM daily_trade_stats
                WHERE api_alias=? AND symbol=? AND trade_date<? ORDER BY trade_date DESC LIMIT 1
                """)) {
            statement.setString(1, alias);
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

    private MutableStats load(LocalDate date, String alias, String symbol) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM daily_trade_stats WHERE trade_date=? AND api_alias=? AND symbol=?")) {
            statement.setString(1, date.toString());
            statement.setString(2, alias);
            statement.setString(3, symbol);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? fromRow(row) : null;
            }
        }
    }

    private MutableStats fromRow(ResultSet row) throws Exception {
        MutableStats stats = new MutableStats(LocalDate.parse(row.getString("trade_date")),
                row.getString("api_alias"), row.getString("symbol"));
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
                INSERT INTO daily_trade_stats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(trade_date, api_alias, symbol) DO UPDATE SET
                  buy_volume=excluded.buy_volume, sell_volume=excluded.sell_volume,
                  total_volume=excluded.total_volume, commission_quote=excluded.commission_quote,
                  economic_fee_quote=excluded.economic_fee_quote, realized_gross_pnl=excluded.realized_gross_pnl,
                  position_qty=excluded.position_qty, position_cost_quote=excluded.position_cost_quote,
                  trade_count=excluded.trade_count, round_trips=excluded.round_trips,
                  commission_complete=excluded.commission_complete,
                  processed_trade_ids=excluded.processed_trade_ids, updated_at=excluded.updated_at
                """)) {
            int i = 1;
            statement.setString(i++, stats.date.toString());
            statement.setString(i++, stats.apiAlias);
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

    private BigDecimal decimal(String value) { return new BigDecimal(value); }
    private void rollbackQuietly() { try { connection.rollback(); } catch (SQLException ignored) { } }
    private void setAutoCommitQuietly(boolean value) { try { connection.setAutoCommit(value); } catch (SQLException ignored) { } }

    @PreDestroy
    public synchronized void close() {
        try { connection.close(); } catch (SQLException e) { log.warn("关闭每日统计数据库失败", e); }
    }

    public enum RecordResult { APPLIED, DUPLICATE, IGNORED, FAILED }

    public record DailyStatsSnapshot(LocalDate date, String apiAlias, String symbol,
                                     BigDecimal buyVolumeQuote, BigDecimal sellVolumeQuote,
                                     BigDecimal totalVolumeQuote, BigDecimal totalCommissionQuoteEquivalent,
                                     BigDecimal costPerMillionVolume, BigDecimal realizedGrossPnlQuote,
                                     BigDecimal netRealizedPnlQuote, BigDecimal positionQty,
                                     BigDecimal positionCostQuote, int tradeCount, int roundTrips,
                                     boolean commissionConversionComplete) {
        static DailyStatsSnapshot empty(LocalDate date, String apiAlias, String symbol) {
            return new DailyStatsSnapshot(date, apiAlias, symbol, BigDecimal.ZERO, BigDecimal.ZERO,
                    BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO,
                    BigDecimal.ZERO, BigDecimal.ZERO, 0, 0, true);
        }
    }

    private final class MutableStats {
        private final LocalDate date;
        private final String apiAlias;
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

        private MutableStats(LocalDate date, String apiAlias, String symbol) {
            this.date = date;
            this.apiAlias = apiAlias;
            this.symbol = symbol;
        }

        private DailyStatsSnapshot snapshot() {
            BigDecimal costPerMillion = commissionComplete && totalVolume.signum() > 0
                    ? commissionQuote.multiply(ONE_MILLION).divide(totalVolume, MC) : null;
            return new DailyStatsSnapshot(date, apiAlias, symbol, buyVolume, sellVolume, totalVolume,
                    commissionQuote, costPerMillion, realizedGrossPnl,
                    realizedGrossPnl.subtract(economicFeeQuote), positionQty, positionCostQuote,
                    tradeCount, roundTrips, commissionComplete);
        }
    }
}
