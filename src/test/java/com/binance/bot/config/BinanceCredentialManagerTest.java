package com.binance.bot.config;

import com.binance.bot.strategy.DailyTradeStatsStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BinanceCredentialManagerTest {

    @Test
    void restoresPersistedAliasAndPublicViewsNeverContainSecrets() throws Exception {
        BinanceProperties properties = configuredProperties();
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        when(store.loadActiveApiAlias()).thenReturn(Optional.of("second-bot"));

        BinanceCredentialManager manager = new BinanceCredentialManager(properties, store);

        assertEquals("second-bot", manager.currentAlias());
        assertEquals("second-key", manager.current().apiKey());
        String publicJson = new ObjectMapper().writeValueAsString(manager.profileViews());
        assertFalse(publicJson.contains("primary-key"));
        assertFalse(publicJson.contains("primary-secret"));
        assertFalse(publicJson.contains("second-key"));
        assertFalse(publicJson.contains("second-secret"));
        assertFalse(manager.current().toString().contains("second-key"));
    }

    @Test
    void rejectsPartiallyConfiguredProfile() {
        BinanceProperties properties = configuredProperties();
        properties.getApi().getProfiles().get("secondary").setSecretKey("");
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        when(store.loadActiveApiAlias()).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, () -> new BinanceCredentialManager(properties, store));
    }

    private BinanceProperties configuredProperties() {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().setApiKeyAlias("primary-bot");
        properties.getApi().setApiKey("primary-key");
        properties.getApi().setSecretKey("primary-secret");
        BinanceProperties.CredentialProfile secondary = new BinanceProperties.CredentialProfile();
        secondary.setAlias("second-bot");
        secondary.setApiKey("second-key");
        secondary.setSecretKey("second-secret");
        properties.getApi().getProfiles().put("secondary", secondary);
        return properties;
    }
}
