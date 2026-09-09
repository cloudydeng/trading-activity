package com.binance.bot.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
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

/** Pushes exchange WebSocket fill and current-order events to authenticated dashboard sessions. */
@Component
@Slf4j
public class RecentFillWebSocketHandler extends TextWebSocketHandler {
    private static final int SNAPSHOT_LIMIT = 10;
    private final TradeNotificationService notificationService;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final ExecutorService broadcaster = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("dashboard-fill-push-", 0).factory());
    private final AutoCloseable fillListenerRegistration;
    private final AutoCloseable openOrderListenerRegistration;

    public RecentFillWebSocketHandler(TradeNotificationService notificationService, ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
        this.fillListenerRegistration = notificationService.addFillListener(this::queueFillBroadcast);
        this.openOrderListenerRegistration = notificationService.addOpenOrderListener(this::queueOpenOrderBroadcast);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        WebSocketSession safeSession = new ConcurrentWebSocketSessionDecorator(session, 5_000, 64 * 1024);
        sessions.put(session.getId(), safeSession);
        // Fills and orders are independent. An invalid order snapshot must not prevent the
        // dashboard connection (and therefore recent fills) from coming online.
        send(safeSession, Map.of("type", "snapshot",
                "fills", notificationService.recentFills(SNAPSHOT_LIMIT)));
        try {
            send(safeSession, Map.of("type", "openOrders",
                    "orders", notificationService.currentOpenOrders()));
        } catch (Exception e) {
            log.warn("控制台活动订单初始快照发送失败，成交流保持连接: {}", e.getMessage());
            try {
                send(safeSession, Map.of("type", "openOrdersError",
                        "message", "部分订单数据异常，等待下一次账户更新"));
            } catch (Exception sendError) {
                sessions.remove(session.getId(), safeSession);
                try {
                    safeSession.close(CloseStatus.SERVER_ERROR);
                } catch (IOException ignored) {
                    // The transport is already unusable.
                }
            }
        }
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

    private void queueFillBroadcast(FillNotification fill) {
        try {
            broadcaster.execute(() -> broadcastFill(fill));
        } catch (RejectedExecutionException ignored) {
            // Application is shutting down.
        }
    }

    private void queueOpenOrderBroadcast(java.util.List<OpenOrderNotification> orders) {
        try {
            broadcaster.execute(() -> broadcastPayload(Map.of("type", "openOrders", "orders", orders)));
        } catch (RejectedExecutionException ignored) {
            // Application is shutting down.
        }
    }

    private void broadcastFill(FillNotification fill) {
        broadcastPayload(Map.of("type", "fill", "fill", fill));
    }

    private void broadcastPayload(Object payload) {
        for (Map.Entry<String, WebSocketSession> entry : sessions.entrySet()) {
            WebSocketSession session = entry.getValue();
            if (!session.isOpen()) {
                sessions.remove(entry.getKey(), session);
                continue;
            }
            try {
                send(session, payload);
            } catch (Exception e) {
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
        fillListenerRegistration.close();
        openOrderListenerRegistration.close();
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
