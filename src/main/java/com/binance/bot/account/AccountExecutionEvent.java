package com.binance.bot.account;

import java.math.BigDecimal;

/** One private execution event, permanently tagged with its owning account. */
public record AccountExecutionEvent(
        String accountId,
        String symbol,
        long orderId,
        long tradeId,
        String clientOrderId,
        String side,
        String executionType,
        String orderStatus,
        BigDecimal lastExecutedQty,
        BigDecimal lastExecutedPrice,
        BigDecimal cumulativeExecutedQty,
        BigDecimal cumulativeQuoteQty,
        BigDecimal commission,
        String commissionAsset,
        boolean maker,
        long eventTime
) { }
