package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.manager.SymbolRuleManager;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceIpRateLimitCoordinator;
import com.binance.bot.service.BinanceSigner;
import com.binance.bot.strategy.DailyTradeStatsStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AccountTradingRuntimeFactoryTest {
    @Test
    void sqliteSymbolOverrideTakesPriorityOverEnvironmentProfileSymbols() {
        BinanceProperties properties = new BinanceProperties();
        DailyTradeStatsStore store = mock(DailyTradeStatsStore.class);
        when(store.loadStrategyOverrides("account-a")).thenReturn(Map.of());
        when(store.loadAccountSymbols("account-a"))
                .thenReturn(Optional.of(List.of("PROMUSDT", "SAHARAUSDT")));
        AccountTradingRuntimeFactory factory = new AccountTradingRuntimeFactory(
                properties, mock(BinanceSigner.class), mock(SymbolRuleManager.class),
                mock(BinanceIpRateLimitCoordinator.class), store, mock(TradeNotificationService.class));
        AccountCredentials credentials = new AccountCredentials(
                "account-a", "A", "key", "secret", Map.of(), Map.of(), List.of("BTCUSDT"));

        AccountTradingRuntime runtime = factory.create(credentials);

        assertEquals(List.of("PROMUSDT", "SAHARAUSDT"),
                runtime.engines().stream().map(engine -> engine.getSymbol()).toList());
    }
}
