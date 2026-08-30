package com.binance.bot.service;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.http.WebSocket;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class UserDataStreamServiceTest {

    @Test
    void forwardsClientOrderIdAndCommissionFromExecutionReport() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStrategy().setSymbol("ENSOUSDT");
        UserDataStreamService service = new UserDataStreamService(
                mock(BinanceOptimizedTradeService.class), properties, mock(BinanceSigner.class));
        AtomicReference<Update> update = new AtomicReference<>();
        service.setExecutionCallback((orderId, clientOrderId, side, executionType, orderStatus,
                                      qty, price, commission, commissionAsset) ->
                update.set(new Update(orderId, clientOrderId, qty, commission, commissionAsset)));

        String event = """
                {"event":{"e":"executionReport","s":"ENSOUSDT","S":"BUY","x":"TRADE","X":"PARTIALLY_FILLED",
                "i":42,"c":"churn-BUY-1","l":"10","L":"0.60","n":"0.01","N":"ENSO"}}
                """;
        service.onText(mock(WebSocket.class), event, true);

        assertEquals(42, update.get().orderId());
        assertEquals("churn-BUY-1", update.get().clientOrderId());
        assertEquals(new BigDecimal("10"), update.get().qty());
        assertEquals(new BigDecimal("0.01"), update.get().commission());
        assertEquals("ENSO", update.get().commissionAsset());
    }

    private record Update(long orderId, String clientOrderId, BigDecimal qty,
                          BigDecimal commission, String commissionAsset) { }
}
