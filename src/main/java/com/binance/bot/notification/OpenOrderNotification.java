package com.binance.bot.notification;

import java.math.BigDecimal;

/** Current exchange order state used by the dashboard's WebSocket-backed order list. */
public record OpenOrderNotification(
        String accountId,
        String accountAlias,
        String symbol,
        String side,
        String type,
        String status,
        String price,
        String entryPrice,
        String originalQty,
        String executedQty,
        long orderId,
        long timeMs
) {
    public OpenOrderNotification {
        accountId = safe(accountId);
        accountAlias = safe(accountAlias);
        if (accountAlias.isBlank()) accountAlias = accountId;
        symbol = safe(symbol);
        side = safe(side);
        type = safe(type);
        status = safe(status);
        price = safeNumber(price);
        entryPrice = safeNumber(entryPrice);
        originalQty = safeNumber(originalQty);
        executedQty = safeNumber(executedQty);
    }

    public OpenOrderNotification(
            String accountId, String accountAlias, String symbol, String side, String type, String status,
            String price, String originalQty, String executedQty, long orderId, long timeMs
    ) {
        this(accountId, accountAlias, symbol, side, type, status, price, "0",
                originalQty, executedQty, orderId, timeMs);
    }

    public OpenOrderNotification withEntryPrice(BigDecimal value) {
        String normalized = value == null || value.signum() <= 0 ? "0" : value.toPlainString();
        return new OpenOrderNotification(accountId, accountAlias, symbol, side, type, status,
                price, normalized, originalQty, executedQty, orderId, timeMs);
    }

    public boolean active() {
        return "NEW".equalsIgnoreCase(status) || "PARTIALLY_FILLED".equalsIgnoreCase(status);
    }

    public boolean valid() {
        return !accountId.isBlank() && orderId > 0;
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static String safeNumber(String value) {
        return value == null || value.isBlank() ? "0" : value;
    }
}
