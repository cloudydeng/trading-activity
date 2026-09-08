package com.binance.bot.notification;

/** Current exchange order state used by the dashboard's WebSocket-backed order list. */
public record OpenOrderNotification(
        String accountId,
        String accountAlias,
        String symbol,
        String side,
        String type,
        String status,
        String price,
        String originalQty,
        String executedQty,
        long orderId,
        long timeMs
) {
    public boolean active() {
        return "NEW".equalsIgnoreCase(status) || "PARTIALLY_FILLED".equalsIgnoreCase(status);
    }
}
