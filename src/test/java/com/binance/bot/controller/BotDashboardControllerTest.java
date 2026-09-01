package com.binance.bot.controller;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.service.BinanceOptimizedTradeService;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BotDashboardControllerTest {

    @Test
    void authenticatedCallerCanArmLiveWithoutSecondaryPayload() {
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        BinanceOptimizedTradeService tradeService = mock(BinanceOptimizedTradeService.class);
        when(engine.armLiveTrading()).thenReturn(true);
        BotDashboardController controller = new BotDashboardController(engine, new BinanceProperties(), tradeService);

        var response = controller.armLive();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("LIVE 已临时解锁；服务重启或停止后自动解除", response.getBody());
        verify(engine).armLiveTrading();
    }

    @Test
    void accountSnapshotContainsOnlyExecutedOrdersAndNonZeroBalances() throws Exception {
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        BinanceOptimizedTradeService tradeService = mock(BinanceOptimizedTradeService.class);
        when(engine.getSymbol()).thenReturn("ENSOUSDT");
        when(engine.getApiKeyAlias()).thenReturn("lee-sub-account-bot");
        when(engine.getUsedApiWeight()).thenReturn(30);
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().setApiKeyAlias("lee-sub-account-bot");
        ObjectMapper mapper = new ObjectMapper();
        when(tradeService.getAccountInfo()).thenReturn(mapper.readTree("""
                {"accountType":"SPOT","canTrade":true,"updateTime":123,
                 "balances":[{"asset":"USDT","free":"10.00","locked":"0"},
                              {"asset":"ENSO","free":"0","locked":"0"}]}
                """));
        when(tradeService.getAllOrders("ENSOUSDT", 100)).thenReturn(mapper.readTree("""
                [{"orderId":1,"side":"BUY","type":"LIMIT_MAKER","status":"FILLED","executedQty":"7","price":"0.8","time":10},
                 {"orderId":2,"side":"BUY","type":"LIMIT_MAKER","status":"CANCELED","executedQty":"0","price":"0.8","time":20}]
                """));
        when(tradeService.getOpenOrders("ENSOUSDT")).thenReturn(mapper.readTree("[]"));
        BotDashboardController controller = new BotDashboardController(engine, properties, tradeService);

        var response = controller.getAccountSnapshot();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        BotDashboardController.AccountSnapshot body = (BotDashboardController.AccountSnapshot) response.getBody();
        assertEquals("lee-sub-account-bot", body.apiKeyAlias());
        assertEquals(1, body.balances().size());
        assertEquals(1, body.filledOrders().size());
        assertEquals(0, body.openOrders().size());
        assertEquals(1L, body.filledOrders().get(0).orderId());
    }
}
