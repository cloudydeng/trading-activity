package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.service.AccountUserDataStream;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.DailyTradeStatsStore;
import com.binance.bot.strategy.PostFillOutcomeTracker;
import com.binance.bot.strategy.TradingRiskGuard;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TradingAccountManagerTest {
    @Test
    void savesStoppedAccountSymbolsToSqliteAndHotAppliesRuntime() {
        BinanceProperties properties = propertiesWith("account-a", "A", "key-a", "secret-a",
                "account-b", "B", "key-b", "secret-b");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        AccountTradingRuntime accountA = mock(AccountTradingRuntime.class);
        AccountTradingRuntime accountB = mock(AccountTradingRuntime.class);
        com.binance.bot.strategy.HighFrequencyVolumeChurnEngine engineA =
                mock(com.binance.bot.strategy.HighFrequencyVolumeChurnEngine.class);
        com.binance.bot.strategy.HighFrequencyVolumeChurnEngine engineB =
                mock(com.binance.bot.strategy.HighFrequencyVolumeChurnEngine.class);
        com.binance.bot.strategy.HighFrequencyVolumeChurnEngine saharaEngine =
                mock(com.binance.bot.strategy.HighFrequencyVolumeChurnEngine.class);
        when(accountA.accountId()).thenReturn("account-a");
        when(accountA.alias()).thenReturn("A");
        when(accountA.credentials()).thenReturn(new AccountCredentials("account-a", "A", "key-a", "secret-a"));
        when(accountA.tradeClient()).thenReturn(mock(BinanceAccountTradeClient.class));
        when(accountA.userDataStream()).thenReturn(mock(AccountUserDataStream.class));
        when(accountA.accountRiskCoordinator()).thenReturn(new AccountRiskCoordinator());
        when(accountA.engines()).thenReturn(List.of(engineA, saharaEngine));
        when(accountA.engine("PROMUSDT")).thenReturn(Optional.of(engineA));
        when(accountA.engine("SAHARAUSDT")).thenReturn(Optional.empty());
        when(accountA.canChangeConfiguredSymbols()).thenReturn(true);
        when(accountA.applyConfiguredSymbols(eq(List.of("PROMUSDT", "SAHARAUSDT")), anyMap()))
                .thenReturn(new AccountTradingRuntime.ApplySymbolsResult(true, "交易对配置已热应用"));
        when(engineA.getSymbol()).thenReturn("PROMUSDT");
        when(saharaEngine.getSymbol()).thenReturn("SAHARAUSDT");
        when(accountB.accountId()).thenReturn("account-b");
        when(accountB.engines()).thenReturn(List.of(engineB));
        when(engineB.getSymbol()).thenReturn("BTCUSDT");
        when(factory.create(any())).thenAnswer(invocation ->
                "account-a".equals(((AccountCredentials) invocation.getArgument(0)).accountId()) ? accountA : accountB);
        when(factory.createSymbolRuntime(any(), any(), any(), any(), eq("SAHARAUSDT")))
                .thenReturn(new AccountTradingRuntimeFactory.AccountSymbolRuntime(
                        saharaEngine, mock(TradingRiskGuard.class), mock(PostFillOutcomeTracker.class)));
        when(store.loadAccountSymbols("account-a"))
                .thenReturn(Optional.of(List.of("PROMUSDT", "SAHARAUSDT")));
        TradingAccountManager manager = new TradingAccountManager(properties, factory, store);
        manager.initialize();

        TradingAccountManager.SymbolsUpdateResult result = manager.updateAccountSymbols(
                "account-a", List.of("PROMUSDT", "SAHARAUSDT"));

        assertTrue(result.accepted());
        assertFalse(result.configuration().restartRequired());
        assertTrue(result.message().contains("热应用"));
        assertEquals(List.of("PROMUSDT", "SAHARAUSDT"), result.configuration().configuredSymbols());
        assertEquals(List.of("PROMUSDT", "SAHARAUSDT"), result.configuration().activeSymbols());
        verify(store).saveAccountSymbols("account-a", List.of("PROMUSDT", "SAHARAUSDT"));
        verify(saharaEngine).initialize();
        verify(accountA).applyConfiguredSymbols(eq(List.of("PROMUSDT", "SAHARAUSDT")), anyMap());
    }

    @Test
    void rejectsAccountSymbolChangesWhileRuntimeIsUnsafe() {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().getProfiles().put("account-a", profile("A", "key-a", "secret-a"));
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        com.binance.bot.strategy.HighFrequencyVolumeChurnEngine engine =
                mock(com.binance.bot.strategy.HighFrequencyVolumeChurnEngine.class);
        when(runtime.accountId()).thenReturn("account-a");
        when(runtime.alias()).thenReturn("A");
        when(runtime.engines()).thenReturn(List.of(engine));
        when(runtime.canChangeConfiguredSymbols()).thenReturn(false);
        when(engine.getSymbol()).thenReturn("PROMUSDT");
        when(factory.create(any())).thenReturn(runtime);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, store);
        manager.initialize();

        TradingAccountManager.SymbolsUpdateResult result = manager.updateAccountSymbols(
                "account-a", List.of("PROMUSDT", "SAHARAUSDT"));

        assertFalse(result.accepted());
        verify(store, never()).saveAccountSymbols(anyString(), anyList());
    }

    @Test
    void createsEveryEnabledProfileAndNeverAutoStartsTrading() {
        BinanceProperties properties = propertiesWith("account-a", "A", "key-a", "secret-a",
                "account-b", "B", "key-b", "secret-b");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        Map<String, AccountTradingRuntime> runtimeById = new HashMap<>();
        when(factory.create(any())).thenAnswer(invocation -> {
            AccountCredentials credentials = invocation.getArgument(0);
            AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
            when(runtime.accountId()).thenReturn(credentials.accountId());
            when(runtime.alias()).thenReturn(credentials.alias());
            runtimeById.put(credentials.accountId(), runtime);
            return runtime;
        });
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        ArgumentCaptor<AccountCredentials> captor = ArgumentCaptor.forClass(AccountCredentials.class);
        verify(factory, times(2)).create(captor.capture());
        assertEquals(java.util.Set.of("account-a", "account-b"),
                captor.getAllValues().stream().map(AccountCredentials::accountId).collect(java.util.stream.Collectors.toSet()));
        runtimeById.values().forEach(runtime -> {
            verify(runtime).initialize();
            verify(runtime, never()).start();
        });
    }

    @Test
    void oneInitializationFailureDoesNotRemoveHealthyAccount() {
        BinanceProperties properties = propertiesWith("account-a", "A", "key-a", "secret-a",
                "account-b", "B", "key-b", "secret-b");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime broken = mock(AccountTradingRuntime.class);
        AccountTradingRuntime healthy = mock(AccountTradingRuntime.class);
        when(broken.accountId()).thenReturn("account-a");
        when(healthy.accountId()).thenReturn("account-b");
        doThrow(new IllegalStateException("A unavailable")).when(broken).initialize();
        when(factory.create(any())).thenAnswer(invocation ->
                "account-a".equals(((AccountCredentials) invocation.getArgument(0)).accountId()) ? broken : healthy);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        assertTrue(manager.find("account-a").isEmpty());
        assertSame(healthy, manager.find("account-b").orElseThrow());
        verify(healthy).initialize();
        verify(broken).shutdown();
    }

    @Test
    void malformedProfileIdDoesNotPreventHealthyAccountInitialization() {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().getProfiles().put("invalid account", profile("broken", "key-a", "secret-a"));
        properties.getApi().getProfiles().put("account-b", profile("B", "key-b", "secret-b"));
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime healthy = mock(AccountTradingRuntime.class);
        when(healthy.accountId()).thenReturn("account-b");
        when(healthy.alias()).thenReturn("B");
        when(factory.create(any())).thenReturn(healthy);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        assertDoesNotThrow(manager::initialize);

        assertSame(healthy, manager.find("account-b").orElseThrow());
        assertTrue(manager.find("invalid account").isEmpty());
        verify(factory, times(1)).create(any());
        verify(healthy).initialize();
    }

    @Test
    void startAllContinuesAfterOneAccountThrows() {
        BinanceProperties properties = propertiesWith("account-a", "A", "key-a", "secret-a",
                "account-b", "B", "key-b", "secret-b");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime accountA = mock(AccountTradingRuntime.class);
        AccountTradingRuntime accountB = mock(AccountTradingRuntime.class);
        com.binance.bot.strategy.HighFrequencyVolumeChurnEngine engineA =
                mock(com.binance.bot.strategy.HighFrequencyVolumeChurnEngine.class);
        com.binance.bot.strategy.HighFrequencyVolumeChurnEngine engineB =
                mock(com.binance.bot.strategy.HighFrequencyVolumeChurnEngine.class);
        when(accountA.accountId()).thenReturn("account-a");
        when(accountB.accountId()).thenReturn("account-b");
        when(accountA.engines()).thenReturn(List.of(engineA));
        when(accountB.engines()).thenReturn(List.of(engineB));
        when(accountA.symbolCount()).thenReturn(1);
        when(accountB.symbolCount()).thenReturn(1);
        when(engineA.getSymbol()).thenReturn("ENSOUSDT");
        when(engineB.getSymbol()).thenReturn("BTCUSDT");
        when(factory.create(any())).thenAnswer(invocation ->
                "account-a".equals(((AccountCredentials) invocation.getArgument(0)).accountId()) ? accountA : accountB);
        when(accountA.start("ENSOUSDT")).thenThrow(new IllegalStateException("A failed"));
        when(accountB.start("BTCUSDT")).thenReturn(true);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));
        manager.initialize();

        Map<String, TradingAccountManager.OperationResult> result = manager.startAll();

        assertFalse(result.get("account-a").success());
        assertTrue(result.get("account-b").success());
        verify(accountB).start("BTCUSDT");
    }

    @Test
    void disabledProfilesAreNotResurrectedThroughLegacyFallback() {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().setApiKey("legacy-key");
        properties.getApi().setSecretKey("legacy-secret");
        BinanceProperties.CredentialProfile disabled = profile("A", "key-a", "secret-a");
        disabled.setEnabled(false);
        properties.getApi().getProfiles().put("account-a", disabled);
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        assertTrue(manager.runtimes().isEmpty());
        verifyNoInteractions(factory);
    }

    @Test
    void legacyCredentialsBecomeDefaultOnlyWhenProfilesAreAbsent() {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().setApiKeyAlias("legacy-bot");
        properties.getApi().setApiKey("legacy-key");
        properties.getApi().setSecretKey("legacy-secret");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        when(runtime.accountId()).thenReturn("default");
        when(factory.create(any())).thenReturn(runtime);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        ArgumentCaptor<AccountCredentials> credentials = ArgumentCaptor.forClass(AccountCredentials.class);
        verify(factory).create(credentials.capture());
        assertEquals("default", credentials.getValue().accountId());
        assertEquals("legacy-bot", credentials.getValue().alias());
    }

    @Test
    void profilesJsonSupportsArbitraryAccountCountWithoutNamedSlots() {
        BinanceProperties properties = new BinanceProperties();
        StringBuilder json = new StringBuilder("{");
        for (int i = 1; i <= 10; i++) {
            if (i > 1) json.append(',');
            json.append("\"account-").append(i).append("\":{")
                    .append("\"alias\":\"bot-").append(i).append("\",")
                    .append("\"apiKey\":\"key-").append(i).append("\",")
                    .append("\"secretKey\":\"secret-").append(i).append("\"}");
        }
        properties.setAccountProfilesJson(json.append('}').toString());
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        when(factory.create(any())).thenAnswer(invocation -> {
            AccountCredentials credentials = invocation.getArgument(0);
            AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
            when(runtime.accountId()).thenReturn(credentials.accountId());
            when(runtime.alias()).thenReturn(credentials.alias());
            return runtime;
        });
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        assertEquals(10, manager.runtimes().size());
        assertTrue(manager.find("account-10").isPresent());
        verify(factory, times(10)).create(any());
    }

    @Test
    void reloadAddsOnlyNewProfilesAndLeavesExistingRuntimeUntouched() throws Exception {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().getProfiles().put("account-a", profile("A", "key-a", "secret-a"));
        Path envFile = Files.createTempFile("trading-activity", ".env");
        properties.setAccountProfilesEnvFile(envFile.toString());
        Files.writeString(envFile, "BOT_ACCOUNT_PROFILES_JSON='{" +
                "\"account-a\":{\"alias\":\"A\",\"apiKey\":\"key-a\",\"secretKey\":\"secret-a\"}," +
                "\"account-c\":{\"alias\":\"C\",\"apiKey\":\"key-c\",\"secretKey\":\"secret-c\"}}'\n");

        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime existing = mock(AccountTradingRuntime.class);
        AccountTradingRuntime added = mock(AccountTradingRuntime.class);
        when(existing.accountId()).thenReturn("account-a");
        when(existing.alias()).thenReturn("A");
        when(added.accountId()).thenReturn("account-c");
        when(added.alias()).thenReturn("C");
        when(factory.create(any())).thenAnswer(invocation ->
                "account-a".equals(((AccountCredentials) invocation.getArgument(0)).accountId())
                        ? existing : added);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();
        clearInvocations(existing);
        TradingAccountManager.ReloadResult result = manager.reloadProfiles();

        assertEquals(List.of("account-c"), result.addedAccounts());
        assertTrue(result.errors().isEmpty());
        assertSame(existing, manager.find("account-a").orElseThrow());
        assertSame(added, manager.find("account-c").orElseThrow());
        verifyNoInteractions(existing);
        verify(added).initialize();
        verify(factory, times(2)).create(any());
        Files.deleteIfExists(envFile);
    }

    @Test
    void invalidProfilesJsonNeverFallsBackToLegacyCredential() {
        BinanceProperties properties = new BinanceProperties();
        properties.setAccountProfilesJson("not-json");
        properties.getApi().setApiKey("legacy-key");
        properties.getApi().setSecretKey("legacy-secret");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        assertTrue(manager.runtimes().isEmpty());
        assertTrue(manager.summaries().stream().anyMatch(summary ->
                "profiles-json".equals(summary.accountId()) && !summary.initialized()));
        verifyNoInteractions(factory);
    }

    @Test
    void passesPerSymbolStrategyToTheAccountRuntime() {
        BinanceProperties properties = new BinanceProperties();
        BinanceProperties.CredentialProfile profile = profile("A", "key-a", "secret-a");
        BinanceProperties.SymbolStrategyProfile strategy = new BinanceProperties.SymbolStrategyProfile();
        strategy.setMode("BID_ASK_MAKER");
        strategy.setEntryTimeoutMs(180_000L);
        strategy.setExitTimeoutMs(180_000L);
        strategy.setOrderAmountUsdt(new java.math.BigDecimal("6"));
        profile.getSymbolStrategies().put("btcusdt", strategy);
        properties.getApi().getProfiles().put("account-a", profile);
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        when(factory.create(any())).thenReturn(runtime);
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        ArgumentCaptor<AccountCredentials> captor = ArgumentCaptor.forClass(AccountCredentials.class);
        verify(factory).create(captor.capture());
        assertEquals("BID_ASK_MAKER", captor.getValue().symbolStrategies().get("BTCUSDT").getMode());
        assertEquals(new java.math.BigDecimal("6"),
                captor.getValue().symbolStrategies().get("BTCUSDT").getOrderAmountUsdt());
    }

    @Test
    void passesNormalizedConcurrentSymbolsToTheAccountRuntime() {
        BinanceProperties properties = new BinanceProperties();
        BinanceProperties.CredentialProfile profile = profile("A", "key-a", "secret-a");
        profile.setSymbols(List.of(" ensousdt ", "BTCUSDT", "ensousdt"));
        properties.getApi().getProfiles().put("account-a", profile);
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        when(factory.create(any())).thenReturn(mock(AccountTradingRuntime.class));
        TradingAccountManager manager = new TradingAccountManager(properties, factory, mock(DailyTradeStatsStore.class));

        manager.initialize();

        ArgumentCaptor<AccountCredentials> captor = ArgumentCaptor.forClass(AccountCredentials.class);
        verify(factory).create(captor.capture());
        assertEquals(List.of("ENSOUSDT", "BTCUSDT"), captor.getValue().symbols());
    }

    private BinanceProperties propertiesWith(String idA, String aliasA, String keyA, String secretA,
                                               String idB, String aliasB, String keyB, String secretB) {
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().getProfiles().put(idA, profile(aliasA, keyA, secretA));
        properties.getApi().getProfiles().put(idB, profile(aliasB, keyB, secretB));
        return properties;
    }

    private BinanceProperties.CredentialProfile profile(String alias, String key, String secret) {
        BinanceProperties.CredentialProfile profile = new BinanceProperties.CredentialProfile();
        profile.setAlias(alias);
        profile.setApiKey(key);
        profile.setSecretKey(secret);
        return profile;
    }
}
