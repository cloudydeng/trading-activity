package com.binance.bot.service;

import com.binance.bot.account.AccountTradingRuntime;
import com.binance.bot.account.TradingAccountManager;
import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

class AccountFundingHistoryServiceTest {
    @TempDir Path tempDir;
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void importsSuccessfulDepositsAndIncomingTransfersWithoutDoubleCountingOnRefreshOrRestart() throws Exception {
        TradingAccountManager manager = mock(TradingAccountManager.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        BinanceAccountTradeClient client = mock(BinanceAccountTradeClient.class);
        when(manager.runtimes()).thenReturn(List.of(runtime));
        when(runtime.accountId()).thenReturn("account-a");
        when(runtime.tradeClient()).thenReturn(client);
        long now = System.currentTimeMillis();
        when(client.getDepositHistory(anyLong(), anyLong(), anyInt(), eq(1000))).thenReturn(mapper.readTree("""
                [{"id":"deposit-1","coin":"BABY","amount":"1.25000000","status":1,"walletType":0,"insertTime":%d},
                 {"id":"pending","coin":"BABY","amount":"50","status":0,"walletType":0,"insertTime":%d},
                 {"id":"funding-wallet","coin":"BABY","amount":"40","status":1,"walletType":1,"insertTime":%d}]
                """.formatted(now - 1000, now - 1000, now - 1000)));
        when(client.getUniversalTransferHistory(anyString(), anyLong(), anyLong(), anyInt(), eq(100)))
                .thenReturn(mapper.readTree("{" + "\"total\":0,\"rows\":[]}"));
        when(client.getUniversalTransferHistory(eq("FUNDING_MAIN"), anyLong(), anyLong(), anyInt(), eq(100)))
                .thenReturn(mapper.readTree("""
                        {"total":2,"rows":[{"tranId":123,"asset":"BABY","amount":"2.5","status":"CONFIRMED","timestamp":%d},
                                           {"tranId":124,"asset":"BABY","amount":"9","status":"PENDING","timestamp":%d}]}
                        """.formatted(now - 1000, now - 1000)));
        BinanceProperties properties = properties();
        AccountFundingHistoryStore store = new AccountFundingHistoryStore(properties);
        AccountFundingHistoryService service = new AccountFundingHistoryService(manager, store);

        AccountFundingHistoryService.Snapshot first = service.snapshot();
        AccountFundingHistoryService.Snapshot second = service.snapshot();

        assertNull(first.accounts().get(0).error());
        assertEquals(true, first.accounts().get(0).depositComplete());
        assertEquals(true, first.accounts().get(0).transferComplete());
        assertEquals(1, first.accounts().get(0).assets().size());
        assertEquals("BABY", first.accounts().get(0).assets().get(0).asset());
        assertEquals("1.25000000", first.accounts().get(0).assets().get(0).deposits());
        assertEquals("2.5", first.accounts().get(0).assets().get(0).transfers());
        assertEquals(first.accounts().get(0).assets(), second.accounts().get(0).assets());
        verify(client, times(3)).getDepositHistory(anyLong(), anyLong(), eq(0), eq(1000));
        store.close();

        AccountFundingHistoryStore reopened = new AccountFundingHistoryStore(properties);
        assertEquals(new BigDecimal("1.25000000"), reopened.totals("account-a", first.sinceMs())
                .get("BABY").deposits());
        assertEquals(new BigDecimal("2.5"), reopened.totals("account-a", first.sinceMs())
                .get("BABY").transfers());
        reopened.close();
    }

    @Test
    void failedHistoryDoesNotTurnMissingDataIntoZero() throws Exception {
        TradingAccountManager manager = mock(TradingAccountManager.class);
        AccountTradingRuntime runtime = mock(AccountTradingRuntime.class);
        BinanceAccountTradeClient client = mock(BinanceAccountTradeClient.class);
        when(manager.runtimes()).thenReturn(List.of(runtime));
        when(runtime.accountId()).thenReturn("account-b");
        when(runtime.tradeClient()).thenReturn(client);
        when(client.getDepositHistory(anyLong(), anyLong(), anyInt(), eq(1000))).thenReturn(null);
        when(client.getUniversalTransferHistory(anyString(), anyLong(), anyLong(), anyInt(), eq(100)))
                .thenReturn(mapper.readTree("{\"total\":0,\"rows\":[]}"));
        AccountFundingHistoryStore store = new AccountFundingHistoryStore(properties());

        AccountFundingHistoryService.Snapshot snapshot = new AccountFundingHistoryService(manager, store).snapshot();

        assertNotNull(snapshot.accounts().get(0).error());
        assertEquals(false, snapshot.accounts().get(0).depositComplete());
        assertEquals(true, snapshot.accounts().get(0).transferComplete());
        assertEquals(List.of(), snapshot.accounts().get(0).assets());
        store.close();
    }

    private BinanceProperties properties() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStorage().setDailyStatsDb(tempDir.resolve("funding.db").toString());
        return properties;
    }
}
