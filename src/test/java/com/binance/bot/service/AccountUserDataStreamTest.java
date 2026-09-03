package com.binance.bot.service;

import com.binance.bot.account.AccountCredentials;
import com.binance.bot.account.AccountExecutionEvent;
import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.net.http.WebSocket;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class AccountUserDataStreamTest {

    @Test
    void forwardsAccountIdClientOrderIdAndCommission() {
        BinanceProperties properties = properties();
        AtomicReference<AccountExecutionEvent> update = new AtomicReference<>();
        AccountUserDataStream service = service("account-a", properties, update::set, reason -> { });
        WebSocket webSocket = mock(WebSocket.class);
        activeSocket(service).set(webSocket);

        service.onText(webSocket, """
                {"event":{"e":"executionReport","s":"ENSOUSDT","S":"BUY","x":"TRADE","X":"PARTIALLY_FILLED",
                "i":42,"t":7001,"c":"ta-a-B-1","l":"10","L":"0.60","z":"10","Z":"6.00",
                "n":"0.01","N":"ENSO","m":true}}
                """, true);

        assertEquals("account-a", update.get().accountId());
        assertEquals(42, update.get().orderId());
        assertEquals(7001, update.get().tradeId());
        assertEquals("ta-a-B-1", update.get().clientOrderId());
        assertEquals(new BigDecimal("10"), update.get().lastExecutedQty());
        assertEquals(new BigDecimal("0.01"), update.get().commission());
        assertEquals("ENSO", update.get().commissionAsset());
    }

    @Test
    void callbacksAreBoundToOneAccountAndNeverBroadcast() {
        BinanceProperties properties = properties();
        AtomicInteger accountACallbacks = new AtomicInteger();
        AtomicInteger accountBCallbacks = new AtomicInteger();
        AccountUserDataStream streamA = service("account-a", properties, event -> accountACallbacks.incrementAndGet(), r -> { });
        AccountUserDataStream streamB = service("account-b", properties, event -> accountBCallbacks.incrementAndGet(), r -> { });
        WebSocket socketA = mock(WebSocket.class);
        activeSocket(streamA).set(socketA);

        streamA.onText(socketA, executionReport(77, 9001), true);

        assertEquals(1, accountACallbacks.get());
        assertEquals(0, accountBCallbacks.get());
    }

    @Test
    void disconnectCallbackOnlyInvokesOwningRuntime() {
        BinanceProperties properties = properties();
        AtomicInteger stoppedA = new AtomicInteger();
        AtomicInteger stoppedB = new AtomicInteger();
        AccountUserDataStream streamA = service("account-a", properties, e -> { }, r -> stoppedA.incrementAndGet());
        service("account-b", properties, e -> { }, r -> stoppedB.incrementAndGet());
        WebSocket socketA = mock(WebSocket.class);
        activeSocket(streamA).set(socketA);
        ((java.util.concurrent.atomic.AtomicBoolean) Objects.requireNonNull(
                ReflectionTestUtils.getField(streamA, "ready"))).set(true);

        streamA.onClose(socketA, 1006, "lost");

        assertEquals(1, stoppedA.get());
        assertEquals(0, stoppedB.get());
        streamA.shutdown();
    }

    @Test
    void subscriptionAckRequestsNextMessageSoReportsContinue() {
        BinanceProperties properties = properties();
        AtomicReference<AccountExecutionEvent> update = new AtomicReference<>();
        AccountUserDataStream service = service("account-a", properties, update::set, reason -> { });
        WebSocket webSocket = mock(WebSocket.class);
        activeSocket(service).set(webSocket);

        service.onText(webSocket, "{\"id\":\"account-events\",\"status\":200,\"result\":{}}", true);
        service.onText(webSocket, executionReport(90, 9002), true);

        verify(webSocket, times(2)).request(1);
        assertEquals(90, update.get().orderId());
    }

    private BinanceProperties properties() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStrategy().setSymbol("ENSOUSDT");
        return properties;
    }

    private String executionReport(long orderId, long tradeId) {
        return "{\"event\":{\"e\":\"executionReport\",\"s\":\"ENSOUSDT\",\"S\":\"SELL\"," +
                "\"x\":\"TRADE\",\"X\":\"FILLED\",\"i\":" + orderId + ",\"t\":" + tradeId + "," +
                "\"c\":\"ta-a-S-1\",\"l\":\"7.08\",\"L\":\"0.85\",\"z\":\"7.08\"," +
                "\"Z\":\"6.018\",\"n\":\"0.006\",\"N\":\"USDT\"}}";
    }

    private AccountUserDataStream service(String accountId, BinanceProperties properties,
                                          AccountUserDataStream.ExecutionCallback callback,
                                          AccountUserDataStream.StreamLifecycleCallback lifecycle) {
        AccountCredentials credentials = new AccountCredentials(accountId, accountId, "key-" + accountId,
                "secret-" + accountId);
        return new AccountUserDataStream(properties, mock(BinanceSigner.class), credentials, callback, lifecycle);
    }

    @SuppressWarnings("unchecked")
    private AtomicReference<WebSocket> activeSocket(AccountUserDataStream service) {
        return (AtomicReference<WebSocket>) ReflectionTestUtils.getField(service, "activeWebSocket");
    }
}
