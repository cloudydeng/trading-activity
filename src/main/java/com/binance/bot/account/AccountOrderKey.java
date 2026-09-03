package com.binance.bot.account;

/** Binance order ids are unique only inside one account. */
public record AccountOrderKey(String accountId, long orderId) { }
