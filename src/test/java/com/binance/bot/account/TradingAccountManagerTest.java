package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TradingAccountManagerTest {
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
        TradingAccountManager manager = new TradingAccountManager(properties, factory);

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
        TradingAccountManager manager = new TradingAccountManager(properties, factory);

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
        TradingAccountManager manager = new TradingAccountManager(properties, factory);

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
        when(accountA.accountId()).thenReturn("account-a");
        when(accountB.accountId()).thenReturn("account-b");
        when(factory.create(any())).thenAnswer(invocation ->
                "account-a".equals(((AccountCredentials) invocation.getArgument(0)).accountId()) ? accountA : accountB);
        when(accountA.start()).thenThrow(new IllegalStateException("A failed"));
        when(accountB.start()).thenReturn(true);
        TradingAccountManager manager = new TradingAccountManager(properties, factory);
        manager.initialize();

        Map<String, TradingAccountManager.OperationResult> result = manager.startAll();

        assertFalse(result.get("account-a").success());
        assertTrue(result.get("account-b").success());
        verify(accountB).start();
    }

    @Test
    void armAllArmsEveryAccountAndContinuesAfterOneFailure() {
        BinanceProperties properties = propertiesWith("account-a", "A", "key-a", "secret-a",
                "account-b", "B", "key-b", "secret-b");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime accountA = mock(AccountTradingRuntime.class);
        AccountTradingRuntime accountB = mock(AccountTradingRuntime.class);
        when(accountA.accountId()).thenReturn("account-a");
        when(accountB.accountId()).thenReturn("account-b");
        when(factory.create(any())).thenAnswer(invocation ->
                "account-a".equals(((AccountCredentials) invocation.getArgument(0)).accountId()) ? accountA : accountB);
        when(accountA.arm()).thenThrow(new IllegalStateException("A unavailable"));
        when(accountB.arm()).thenReturn(true);
        TradingAccountManager manager = new TradingAccountManager(properties, factory);
        manager.initialize();

        Map<String, TradingAccountManager.OperationResult> result = manager.armAll();

        assertFalse(result.get("account-a").success());
        assertEquals("A unavailable", result.get("account-a").reason());
        assertTrue(result.get("account-b").success());
        verify(accountA).arm();
        verify(accountB).arm();
        verify(accountA, never()).start();
        verify(accountB, never()).start();
    }

    @Test
    void disabledProfilesAreNotResurrectedThroughLegacyFallback() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStrategy().setExecutionMode("LIVE");
        properties.getApi().setApiKey("legacy-key");
        properties.getApi().setSecretKey("legacy-secret");
        BinanceProperties.CredentialProfile disabled = profile("A", "key-a", "secret-a");
        disabled.setEnabled(false);
        properties.getApi().getProfiles().put("account-a", disabled);
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        TradingAccountManager manager = new TradingAccountManager(properties, factory);

        manager.initialize();

        assertTrue(manager.runtimes().isEmpty());
        verifyNoInteractions(factory);
    }

    @Test
    void legacyCredentialsBecomeDefaultOnlyWhenProfilesAreAbsent() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStrategy().setExecutionMode("LIVE");
        properties.getApi().setApiKeyAlias("legacy-bot");
        properties.getApi().setApiKey("legacy-key");
        properties.getApi().setSecretKey("legacy-secret");
        AccountTradingRuntimeFactory factory = mock(AccountTradingRuntimeFactory.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        when(runtime.accountId()).thenReturn("default");
        when(factory.create(any())).thenReturn(runtime);
        TradingAccountManager manager = new TradingAccountManager(properties, factory);

        manager.initialize();

        ArgumentCaptor<AccountCredentials> credentials = ArgumentCaptor.forClass(AccountCredentials.class);
        verify(factory).create(credentials.capture());
        assertEquals("default", credentials.getValue().accountId());
        assertEquals("legacy-bot", credentials.getValue().alias());
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
