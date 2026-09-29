package com.binance.bot.service;

import com.binance.bot.config.BinanceProperties;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/** Stores only funding identity, asset, amount and time; never addresses or transaction payloads. */
@Component
public class AccountFundingHistoryStore {
    private final Connection connection;

    public AccountFundingHistoryStore(BinanceProperties properties) {
        try {
            Path path = Path.of(properties.getStorage().getDailyStatsDb()).toAbsolutePath().normalize();
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA synchronous=FULL");
                statement.execute("PRAGMA busy_timeout=5000");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS funding_event (
                          account_id TEXT NOT NULL,
                          kind TEXT NOT NULL,
                          event_id TEXT NOT NULL,
                          asset TEXT NOT NULL,
                          amount TEXT NOT NULL,
                          event_time INTEGER NOT NULL,
                          PRIMARY KEY (account_id, kind, event_id)
                        )
                        """);
                statement.execute("""
                        CREATE INDEX IF NOT EXISTS idx_funding_event_account_time
                        ON funding_event(account_id, event_time)
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS funding_sync (
                          account_id TEXT NOT NULL,
                          kind TEXT NOT NULL,
                          last_full_sync_at INTEGER NOT NULL,
                          PRIMARY KEY (account_id, kind)
                        )
                        """);
            }
        } catch (Exception exception) {
            throw new IllegalStateException("无法初始化充值记录数据库", exception);
        }
    }

    public synchronized void save(Event event) {
        if (event.accountId() == null || event.accountId().isBlank()
                || event.eventId() == null || event.eventId().isBlank()
                || event.asset() == null || !event.asset().matches("[A-Z0-9]{2,30}")
                || event.amount() == null || event.amount().signum() <= 0 || event.eventTimeMs() <= 0) {
            throw new IllegalArgumentException("无效的充值或划转记录");
        }
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO funding_event(account_id, kind, event_id, asset, amount, event_time)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, kind, event_id) DO UPDATE SET
                  asset=excluded.asset, amount=excluded.amount, event_time=excluded.event_time
                """)) {
            statement.setString(1, event.accountId());
            statement.setString(2, event.kind());
            statement.setString(3, event.eventId());
            statement.setString(4, event.asset());
            statement.setString(5, event.amount().toPlainString());
            statement.setLong(6, event.eventTimeMs());
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("保存充值或划转记录失败", exception);
        }
    }

    public synchronized long lastFullSyncAt(String accountId, String kind) {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT last_full_sync_at FROM funding_sync WHERE account_id=? AND kind=?
                """)) {
            statement.setString(1, accountId);
            statement.setString(2, kind);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getLong(1) : 0L;
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("读取充值同步状态失败", exception);
        }
    }

    public synchronized void markFullSync(String accountId, String kind, long atMs) {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO funding_sync(account_id, kind, last_full_sync_at) VALUES (?, ?, ?)
                ON CONFLICT(account_id, kind) DO UPDATE SET last_full_sync_at=excluded.last_full_sync_at
                """)) {
            statement.setString(1, accountId);
            statement.setString(2, kind);
            statement.setLong(3, atMs);
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("保存充值同步状态失败", exception);
        }
    }

    public synchronized Map<String, Totals> totals(String accountId, long sinceMs) {
        Map<String, Totals> result = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT kind, asset, amount FROM funding_event
                WHERE account_id=? AND event_time>=?
                """)) {
            statement.setString(1, accountId);
            statement.setLong(2, sinceMs);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String asset = rows.getString("asset");
                    BigDecimal amount = new BigDecimal(rows.getString("amount"));
                    Totals current = result.getOrDefault(asset, new Totals(BigDecimal.ZERO, BigDecimal.ZERO));
                    result.put(asset, "deposit".equals(rows.getString("kind"))
                            ? new Totals(current.deposits().add(amount), current.transfers())
                            : new Totals(current.deposits(), current.transfers().add(amount)));
                }
            }
            return result;
        } catch (SQLException exception) {
            throw new IllegalStateException("读取充值汇总失败", exception);
        }
    }

    @PreDestroy
    public synchronized void close() throws SQLException {
        connection.close();
    }

    public record Event(String accountId, String kind, String eventId, String asset,
                        BigDecimal amount, long eventTimeMs) { }
    public record Totals(BigDecimal deposits, BigDecimal transfers) { }
}
