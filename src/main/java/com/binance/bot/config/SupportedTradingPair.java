package com.binance.bot.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Quote currencies whose order amounts and account risk can be handled safely. */
public final class SupportedTradingPair {
    private SupportedTradingPair() { }

    public static boolean isSupported(String symbol) {
        if (symbol == null) return false;
        String normalized = symbol.trim().toUpperCase(Locale.ROOT);
        return normalized.matches("[A-Z0-9]{5,20}")
                && (normalized.endsWith("USDT") || normalized.endsWith("USDC"));
    }

    public static String quoteAsset(String symbol) {
        if (!isSupported(symbol)) throw new IllegalArgumentException("不支持的交易对: " + symbol);
        return symbol.trim().toUpperCase(Locale.ROOT).endsWith("USDC") ? "USDC" : "USDT";
    }

    /** A base asset cannot be managed independently in two quote markets on one account. */
    public static void requireDistinctBaseAssets(Collection<String> symbols) {
        if (symbols == null) return;
        Map<String, String> symbolByBase = new HashMap<>();
        for (String value : symbols) {
            if (!isSupported(value)) continue;
            String symbol = value.trim().toUpperCase(Locale.ROOT);
            String base = symbol.substring(0, symbol.length() - quoteAsset(symbol).length());
            String previous = symbolByBase.putIfAbsent(base, symbol);
            if (previous != null && !previous.equals(symbol)) {
                throw new IllegalArgumentException("同一账户不可同时配置 " + previous + " 和 " + symbol
                        + "：两者共用 " + base + " 持仓");
            }
        }
    }
}
