package com.binance.bot.notification;

import java.math.BigDecimal;

/** Public-safe fill payload. Credentials are deliberately not part of the type. */
public record FillNotification(
        String accountId,
        String accountAlias,
        String symbol,
        String side,
        long orderId,
        long tradeId,
        String clientOrderId,
        BigDecimal quantity,
        BigDecimal price,
        BigDecimal quoteAmount,
        BigDecimal commission,
        String commissionAsset,
        long eventTime
) { }
