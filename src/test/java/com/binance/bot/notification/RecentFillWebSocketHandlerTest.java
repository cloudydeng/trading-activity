package com.binance.bot.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecentFillWebSocketHandlerTest {
    @Test
    void brokenOrderSnapshotDoesNotPreventFillStreamConnection() throws Exception {
        TradeNotificationService service = mock(TradeNotificationService.class);
        AutoCloseable fillRegistration = mock(AutoCloseable.class);
        AutoCloseable orderRegistration = mock(AutoCloseable.class);
        when(service.addFillListener(org.mockito.ArgumentMatchers.any())).thenReturn(fillRegistration);
        when(service.addOpenOrderListener(org.mockito.ArgumentMatchers.any())).thenReturn(orderRegistration);
        when(service.recentFills(10)).thenReturn(List.of());
        when(service.currentOpenOrders()).thenThrow(new IllegalStateException("one account is malformed"));
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("dashboard-1");
        when(session.isOpen()).thenReturn(true);
        RecentFillWebSocketHandler handler = new RecentFillWebSocketHandler(service, new ObjectMapper());

        handler.afterConnectionEstablished(session);

        ArgumentCaptor<TextMessage> messages = ArgumentCaptor.forClass(TextMessage.class);
        verify(session, times(2)).sendMessage(messages.capture());
        assertTrue(messages.getAllValues().get(0).getPayload().contains("\"type\":\"snapshot\""));
        assertTrue(messages.getAllValues().get(1).getPayload().contains("\"type\":\"openOrdersError\""));
        handler.close();
    }
}
