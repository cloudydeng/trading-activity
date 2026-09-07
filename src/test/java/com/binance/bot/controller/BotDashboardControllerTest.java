package com.binance.bot.controller;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.binance.bot.notification.FillNotification;
import com.binance.bot.notification.TradeNotificationService;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BotDashboardControllerTest {
    @Test
    void globalRecentFillsComeFromEveryExchangeAccountAndAreCached() throws Exception {
        TradingAccountManager accountManager = mock(TradingAccountManager.class);
        TradeNotificationService notificationService = mock(TradeNotificationService.class);
        AccountTradingRuntime accountA = runtime("account-a", "A", "ENSOUSDT");
        AccountTradingRuntime accountB = runtime("account-b", "B", "ZKCUSDT");
        BinanceAccountTradeClient clientA = accountA.tradeClient();
        BinanceAccountTradeClient clientB = accountB.tradeClient();
        ObjectMapper mapper = new ObjectMapper();

        when(accountManager.runtimes()).thenReturn(List.of(accountA, accountB));
        when(notificationService.recentFills(500)).thenReturn(List.of());
        when(clientA.getRecentMyTrades("ENSOUSDT", 20)).thenReturn(mapper.readTree("""
                [{"id":11,"orderId":101,"price":"0.6000","qty":"10","quoteQty":"6",
                  "commission":"0.006","commissionAsset":"USDT","time":100,"isBuyer":true}]
                """));
        when(clientB.getRecentMyTrades("ZKCUSDT", 20)).thenReturn(mapper.readTree("""
                [{"id":12,"orderId":102,"price":"0.0500","qty":"200","quoteQty":"10",
                  "commission":"0.01","commissionAsset":"USDT","time":200,"isBuyer":false}]
                """));
        BotDashboardController controller = new BotDashboardController(
                accountManager, new BinanceProperties(), notificationService);

        List<FillNotification> first = controller.allNotifications(10);
        List<FillNotification> cached = controller.allNotifications(10);

        assertEquals(2, first.size());
        assertEquals("account-b", first.get(0).accountId());
        assertEquals("SELL", first.get(0).side());
        assertEquals("account-a", first.get(1).accountId());
        assertEquals(first, cached);
        verify(clientA, times(1)).getRecentMyTrades("ENSOUSDT", 20);
        verify(clientB, times(1)).getRecentMyTrades("ZKCUSDT", 20);
    }

    private AccountTradingRuntime runtime(String accountId, String alias, String symbol) {
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        BinanceAccountTradeClient client = mock(BinanceAccountTradeClient.class);
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        when(runtime.accountId()).thenReturn(accountId);
        when(runtime.alias()).thenReturn(alias);
        when(runtime.tradeClient()).thenReturn(client);
        when(runtime.engine()).thenReturn(engine);
        when(engine.getSymbol()).thenReturn(symbol);
        return runtime;
    }
}
