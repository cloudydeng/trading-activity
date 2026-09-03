package com.binance.bot.account;

/** Binance trade ids are unique only inside one account and symbol. */
public record AccountTradeKey(String accountId, String symbol, long tradeId) { }
