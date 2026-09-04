package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable account identity and signing material. Never serialize or log this record. */
public record AccountCredentials(String accountId, String alias, String apiKey, String secretKey,
                                 Map<String, BigDecimal> orderAmountsUsdt,
                                 Map<String, BinanceProperties.SymbolStrategyProfile> symbolStrategies) {
    public AccountCredentials(String accountId, String alias, String apiKey, String secretKey) {
        this(accountId, alias, apiKey, secretKey, Map.of(), Map.of());
    }

    public AccountCredentials(String accountId, String alias, String apiKey, String secretKey,
                               Map<String, BigDecimal> orderAmountsUsdt) {
        this(accountId, alias, apiKey, secretKey, orderAmountsUsdt, Map.of());
    }

    public AccountCredentials {
        accountId = normalize(accountId, "accountId");
        alias = alias == null || alias.isBlank() ? accountId : alias.trim();
        apiKey = apiKey == null ? "" : apiKey.trim();
        secretKey = secretKey == null ? "" : secretKey.trim();
        Map<String, BigDecimal> normalizedAmounts = new LinkedHashMap<>();
        if (orderAmountsUsdt != null) {
            orderAmountsUsdt.forEach((symbol, amount) -> {
                if (symbol == null || amount == null || amount.signum() <= 0) return;
                String normalizedSymbol = symbol.trim().toUpperCase();
                if (normalizedSymbol.matches("[A-Z0-9]{5,20}") && normalizedSymbol.endsWith("USDT")) {
                    normalizedAmounts.put(normalizedSymbol, amount);
                }
            });
        }
        orderAmountsUsdt = Map.copyOf(normalizedAmounts);
        Map<String, BinanceProperties.SymbolStrategyProfile> normalizedStrategies = new LinkedHashMap<>();
        if (symbolStrategies != null) {
            symbolStrategies.forEach((symbol, strategy) -> {
                if (symbol == null || strategy == null) return;
                String normalizedSymbol = symbol.trim().toUpperCase();
                if (normalizedSymbol.matches("[A-Z0-9]{5,20}") && normalizedSymbol.endsWith("USDT")) {
                    normalizedStrategies.put(normalizedSymbol, strategy);
                }
            });
        }
        symbolStrategies = Map.copyOf(normalizedStrategies);
    }

    public boolean complete() {
        return !apiKey.isBlank() && !secretKey.isBlank();
    }

    private static String normalize(String value, String field) {
        if (value == null || !value.trim().matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException(field + " must match [A-Za-z0-9._-]{1,64}");
        }
        return value.trim();
    }

    /** Record defaults would expose secrets; keep diagnostics safe by construction. */
    @Override public String toString() {
        return "AccountCredentials[accountId=" + accountId + ", alias=" + alias + ", redacted]";
    }
}
