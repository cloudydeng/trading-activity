package com.binance.bot.account;

/** Immutable account identity and signing material. Never serialize or log this record. */
public record AccountCredentials(String accountId, String alias, String apiKey, String secretKey) {
    public AccountCredentials {
        accountId = normalize(accountId, "accountId");
        alias = alias == null || alias.isBlank() ? accountId : alias.trim();
        apiKey = apiKey == null ? "" : apiKey.trim();
        secretKey = secretKey == null ? "" : secretKey.trim();
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
