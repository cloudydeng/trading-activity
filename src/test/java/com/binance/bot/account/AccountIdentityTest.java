package com.binance.bot.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AccountIdentityTest {
    @Test
    void credentialsNeverRenderSecrets() throws Exception {
        AccountCredentials credentials = new AccountCredentials("account-a", "A", "api-key-value", "secret-value");
        String rendered = credentials.toString();
        assertFalse(rendered.contains("api-key-value"));
        assertFalse(rendered.contains("secret-value"));
        assertFalse(new ObjectMapper().writeValueAsString(new PublicAccount(credentials.accountId(), credentials.alias()))
                .contains("secret-value"));
    }

    @Test
    void sameExchangeIdsRemainDistinctAcrossAccounts() {
        assertNotEquals(new AccountOrderKey("account-a", 42), new AccountOrderKey("account-b", 42));
        assertNotEquals(new AccountTradeKey("account-a", "ENSOUSDT", 7),
                new AccountTradeKey("account-b", "ENSOUSDT", 7));
    }

    @Test
    void rejectsApiKeyMaterialAsOversizedAccountId() {
        assertThrows(IllegalArgumentException.class,
                () -> new AccountCredentials("x".repeat(65), "A", "key", "secret"));
    }

    private record PublicAccount(String accountId, String alias) { }
}
