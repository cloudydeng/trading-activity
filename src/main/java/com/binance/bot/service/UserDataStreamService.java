package com.binance.bot.service;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class UserDataStreamService implements WebSocket.Listener {

    private final BinanceOptimizedTradeService tradeService;
    private final BinanceProperties properties;
    private final BinanceSigner signer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newHttpClient();

    private final AtomicBoolean connectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean acceptingConnections = new AtomicBoolean(true);
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicReference<WebSocket> activeWebSocket = new AtomicReference<>();
    private final AtomicLong lastFrameTimestamp = new AtomicLong(0);
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "binance-user-stream-watchdog");
        thread.setDaemon(true);
        return thread;
    });
    private final StringBuilder inboundMessage = new StringBuilder();
    private volatile ExecutionCallback executionCallback;
    private volatile StreamLifecycleCallback streamLifecycleCallback;

    @FunctionalInterface
    public interface ExecutionCallback {
        void onOrderUpdate(ExecutionUpdate update);
    }

    public record ExecutionUpdate(long orderId, long tradeId, String clientOrderId, String side,
                                  String executionType, String orderStatus, BigDecimal lastExecutedQty,
                                  BigDecimal lastExecutedPrice, BigDecimal cumulativeExecutedQty,
                                  BigDecimal cumulativeQuoteQty, BigDecimal commission,
                                  String commissionAsset, long tradeTimeMs) { }

    @FunctionalInterface
    public interface StreamLifecycleCallback {
        void onUnavailable(String reason);
    }

    public UserDataStreamService(BinanceOptimizedTradeService tradeService, BinanceProperties properties, BinanceSigner signer) {
        this.tradeService = tradeService;
        this.properties = properties;
        this.signer = signer;
    }

    public void setExecutionCallback(ExecutionCallback executionCallback) {
        this.executionCallback = executionCallback;
    }

    public void setStreamLifecycleCallback(StreamLifecycleCallback streamLifecycleCallback) {
        this.streamLifecycleCallback = streamLifecycleCallback;
    }

    @PostConstruct
    public void start() {
        if (properties.getStrategy().isObserveMode()) {
            log.info("OBSERVE 模式：不连接账户 User Data Stream");
            return;
        }
        connect();
        watchdog.scheduleWithFixedDelay(this::checkStreamHealth, 10, 10, TimeUnit.SECONDS);
    }

    public synchronized void connect() {
        if (!acceptingConnections.get() || !connectInProgress.compareAndSet(false, true)) return;
        httpClient.newWebSocketBuilder()
                .buildAsync(URI.create(properties.getApi().getWsUserUrl()), this)
                .thenAccept(ws -> {
                    connectInProgress.set(false);
                    if (!acceptingConnections.get()) {
                        ws.abort();
                        return;
                    }
                    ready.set(false);
                    lastFrameTimestamp.set(System.currentTimeMillis());
                    WebSocket previous = activeWebSocket.getAndSet(ws);
                    if (previous != null && previous != ws) previous.abort();
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
        WebSocket active = activeWebSocket.get();
        if (active != null && webSocket != active) {
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }
        lastFrameTimestamp.set(System.currentTimeMillis());
        try {
            String payload;
            synchronized (inboundMessage) {
                inboundMessage.append(data);
                if (!last) return WebSocket.Listener.super.onText(webSocket, data, false);
                payload = inboundMessage.toString();
                inboundMessage.setLength(0);
            }
            JsonNode root = objectMapper.readTree(payload);
            if (root.has("id") && "account-events".equals(root.get("id").asText())) {
                if (root.path("status").asInt() == 200) {
                    ready.set(true);
                    log.info("已成功订阅币安账户 User Data Stream");
                } else {
                    markUnavailable(webSocket,
                            "账户流签名订阅失败: " + root.path("error").path("msg").asText("unknown"));
                }
                // The JDK WebSocket API is demand-driven. Request the next message after
                // the subscription ACK, otherwise no execution reports are ever delivered.
                return WebSocket.Listener.super.onText(webSocket, data, true);
            }
            JsonNode node = root.has("event") ? root.get("event") : root;
            String eventType = node.has("e") ? node.get("e").asText() : "";

            if ("executionReport".equals(eventType)) {
                String symbol = node.get("s").asText();
                String side = node.get("S").asText();
                String currentExecutionType = node.get("x").asText();
                String orderStatus = node.get("X").asText();
                long orderId = node.get("i").asLong();
                long tradeId = node.path("t").asLong(-1);
                String clientOrderId = node.path("c").asText("");
                BigDecimal lastFilledQty = new BigDecimal(node.get("l").asText());
                BigDecimal lastFilledPrice = new BigDecimal(node.get("L").asText());
                BigDecimal cumulativeFilledQty = new BigDecimal(node.path("z").asText(lastFilledQty.toPlainString()));
                BigDecimal cumulativeQuoteQty = new BigDecimal(node.path("Z").asText(
                        lastFilledQty.multiply(lastFilledPrice).toPlainString()));
                BigDecimal commission = new BigDecimal(node.path("n").asText("0"));
                String commissionAsset = node.path("N").asText("");
                long tradeTimeMs = node.path("T").asLong(node.path("E").asLong(System.currentTimeMillis()));

                if ("TRADE".equals(currentExecutionType) && lastFilledQty.compareTo(BigDecimal.ZERO) > 0) {
                    log.info("【订单成交】{} {} | 数量: {} @ 价格: {}", symbol, side, lastFilledQty, lastFilledPrice);
                }
                if (executionCallback != null && properties.getStrategy().getSymbol().equalsIgnoreCase(symbol)) {
                    executionCallback.onOrderUpdate(new ExecutionUpdate(orderId, tradeId, clientOrderId, side,
                            currentExecutionType, orderStatus, lastFilledQty, lastFilledPrice, cumulativeFilledQty,
                            cumulativeQuoteQty, commission, commissionAsset, tradeTimeMs));
                }
            } else if ("eventStreamTerminated".equals(eventType)) {
                markUnavailable(webSocket, "账户事件流已终止");
            }
        } catch (Exception e) {
            log.error("解析 UserDataStream 消息异常", e);
        }
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        markUnavailable(webSocket, "账户流关闭 " + statusCode + ": " + reason);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        markUnavailable(webSocket, "账户流错误: " + error.getMessage());
    }

    @Override
    public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
        lastFrameTimestamp.set(System.currentTimeMillis());
        return WebSocket.Listener.super.onPing(webSocket, message);
    }

    @Override
    public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
        lastFrameTimestamp.set(System.currentTimeMillis());
        return WebSocket.Listener.super.onPong(webSocket, message);
    }

    private void markUnavailable(WebSocket source, String reason) {
        if (source != null && !activeWebSocket.compareAndSet(source, null)) return;
        if (source != null) source.abort();
        boolean wasReady = ready.getAndSet(false);
        if (wasReady && streamLifecycleCallback != null) {
            try {
                streamLifecycleCallback.onUnavailable(reason);
            } catch (Exception e) {
                log.error("账户流断线保护回调异常", e);
            }
        }
        scheduleReconnect(reason);
    }

    private void checkStreamHealth() {
        if (!acceptingConnections.get()) return;
        WebSocket socket = activeWebSocket.get();
        long lastFrame = lastFrameTimestamp.get();
        if (socket != null && lastFrame > 0 && System.currentTimeMillis() - lastFrame > 45_000) {
            markUnavailable(socket, "账户流 45 秒未收到任何帧");
        } else if (socket == null && !connectInProgress.get()) {
            scheduleReconnect("账户流连接不存在");
        }
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
        WebSocket socket = activeWebSocket.getAndSet(null);
        if (socket != null) socket.abort();
        watchdog.shutdownNow();
    }
}
