package com.binance.bot.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/** Pushes exchange WebSocket fill events to authenticated dashboard sessions without REST backfill. */
@Component
public class RecentFillWebSocketHandler extends TextWebSocketHandler {
    private static final int SNAPSHOT_LIMIT = 10;
    private final TradeNotificationService notificationService;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final ExecutorService broadcaster = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("dashboard-fill-push-", 0).factory());
    private final AutoCloseable listenerRegistration;

    public RecentFillWebSocketHandler(TradeNotificationService notificationService, ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
        this.listenerRegistration = notificationService.addFillListener(this::queueBroadcast);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        WebSocketSession safeSession = new ConcurrentWebSocketSessionDecorator(session, 5_000, 64 * 1024);
        sessions.put(session.getId(), safeSession);
        send(safeSession, Map.of("type", "snapshot",
                "fills", notificationService.recentFills(SNAPSHOT_LIMIT)));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        sessions.remove(session.getId());
        if (session.isOpen()) session.close(CloseStatus.SERVER_ERROR);
    }

    private void queueBroadcast(FillNotification fill) {
        try {
            broadcaster.execute(() -> broadcast(fill));
        } catch (RejectedExecutionException ignored) {
            // Application is shutting down.
        }
    }

    private void broadcast(FillNotification fill) {
        for (Map.Entry<String, WebSocketSession> entry : sessions.entrySet()) {
            WebSocketSession session = entry.getValue();
            if (!session.isOpen()) {
                sessions.remove(entry.getKey(), session);
                continue;
            }
            try {
                send(session, Map.of("type", "fill", "fill", fill));
            } catch (IOException e) {
                sessions.remove(entry.getKey(), session);
                try {
                    session.close(CloseStatus.SERVER_ERROR);
                } catch (IOException ignored) {
                    // Session is already unusable.
                }
            }
        }
    }

    private void send(WebSocketSession session, Object payload) throws IOException {
        session.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
    }

    @PreDestroy
    void close() throws Exception {
        listenerRegistration.close();
        broadcaster.shutdownNow();
        for (WebSocketSession session : sessions.values()) {
            try {
                session.close(CloseStatus.GOING_AWAY);
            } catch (IOException ignored) {
                // Best-effort shutdown.
            }
        }
        sessions.clear();
    }
}
