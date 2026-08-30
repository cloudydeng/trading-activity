package com.binance.bot.service;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class UserDataStreamService implements WebSocket.Listener {

    private final BinanceOptimizedTradeService tradeService;
    private final BinanceProperties properties;
    private final BinanceSigner signer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicReference<String> currentListenKey = new AtomicReference<>();
    private final AtomicBoolean connectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean acceptingConnections = new AtomicBoolean(true);
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final StringBuilder inboundMessage = new StringBuilder();
    private ExecutionCallback executionCallback;

    @FunctionalInterface
    public interface ExecutionCallback {
        void onOrderUpdate(long orderId, String side, String executionType, String orderStatus,
                           BigDecimal lastExecutedQty, BigDecimal lastExecutedPrice);
    }

    public UserDataStreamService(BinanceOptimizedTradeService tradeService, BinanceProperties properties, BinanceSigner signer) {
        this.tradeService = tradeService;
        this.properties = properties;
        this.signer = signer;
    }

    public void setExecutionCallback(ExecutionCallback executionCallback) {
        this.executionCallback = executionCallback;
    }

    @PostConstruct
    public void start() {
        if (properties.getStrategy().isObserveMode()) {
            log.info("OBSERVE 模式：不连接账户 User Data Stream");
            return;
        }
        connect();
    }

    public synchronized void connect() {
        if (!acceptingConnections.get() || !connectInProgress.compareAndSet(false, true)) return;
        HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(properties.getApi().getWsUserUrl()), this)
                .thenAccept(ws -> {
                    connectInProgress.set(false);
                    subscribe(ws);
                })
                .exceptionally(ex -> {
                    connectInProgress.set(false);
                    scheduleReconnect("连接异常: " + ex.getMessage());
                    return null;
                });
    }

    private void subscribe(WebSocket webSocket) {
        long timestamp = System.currentTimeMillis();
        String apiKey = properties.getApi().getApiKey();
        String payload = "apiKey=" + apiKey + "&recvWindow=5000&timestamp=" + timestamp;
        var root = objectMapper.createObjectNode();
        root.put("id", "account-events");
        root.put("method", "userDataStream.subscribe.signature");
        var params = root.putObject("params");
        params.put("apiKey", apiKey);
        params.put("recvWindow", 5000);
        params.put("timestamp", timestamp);
        params.put("signature", signer.sign(payload, properties.getApi().getSecretKey()));
        webSocket.sendText(root.toString(), true);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        try {
            String payload;
            synchronized (inboundMessage) {
                inboundMessage.append(data);
                if (!last) return CompletableFuture.completedFuture(null);
                payload = inboundMessage.toString();
                inboundMessage.setLength(0);
            }
            JsonNode root = objectMapper.readTree(payload);
            if (root.has("id") && "account-events".equals(root.get("id").asText())) {
                if (root.path("status").asInt() == 200) {
                    ready.set(true);
                    log.info("已成功订阅币安账户 User Data Stream");
                } else {
                    ready.set(false);
                    webSocket.abort();
                    scheduleReconnect("账户流签名订阅失败: " + root.path("error").path("msg").asText("unknown"));
                }
                return CompletableFuture.completedFuture(null);
            }
            JsonNode node = root.has("event") ? root.get("event") : root;
            String eventType = node.has("e") ? node.get("e").asText() : "";

            if ("executionReport".equals(eventType)) {
                String symbol = node.get("s").asText();
                String side = node.get("S").asText();
                String currentExecutionType = node.get("x").asText();
                String orderStatus = node.get("X").asText();
                long orderId = node.get("i").asLong();
                BigDecimal lastFilledQty = new BigDecimal(node.get("l").asText());
                BigDecimal lastFilledPrice = new BigDecimal(node.get("L").asText());

                if ("TRADE".equals(currentExecutionType) && lastFilledQty.compareTo(BigDecimal.ZERO) > 0) {
                    log.info("【订单成交】{} {} | 数量: {} @ 价格: {}", symbol, side, lastFilledQty, lastFilledPrice);
                }
                if (executionCallback != null && properties.getStrategy().getSymbol().equalsIgnoreCase(symbol)) {
                    executionCallback.onOrderUpdate(orderId, side, currentExecutionType, orderStatus, lastFilledQty, lastFilledPrice);
                }
            } else if ("eventStreamTerminated".equals(eventType)) {
                scheduleReconnect("账户事件流已终止");
            }
        } catch (Exception e) {
            log.error("解析 UserDataStream 消息异常", e);
        }
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        ready.set(false);
        scheduleReconnect("账户流关闭 " + statusCode + ": " + reason);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        ready.set(false);
        scheduleReconnect("账户流错误: " + error.getMessage());
    }

    private void scheduleReconnect(String reason) {
        if (!acceptingConnections.get()) return;
        log.warn("账户成交流不可用，准备重连: {}", reason);
        if (reconnectScheduled.compareAndSet(false, true)) {
            CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
                reconnectScheduled.set(false);
                connect();
            });
        }
    }

    public boolean isReady() { return ready.get(); }

    @PreDestroy
    public void shutdown() {
        acceptingConnections.set(false);
        ready.set(false);
    }
}
