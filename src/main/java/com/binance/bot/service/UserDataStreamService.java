package com.binance.bot.service;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class UserDataStreamService implements WebSocket.Listener {

    private final BinanceOptimizedTradeService tradeService;
    private final BinanceProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicReference<String> currentListenKey = new AtomicReference<>();
    private ExecutionCallback executionCallback;

    @FunctionalInterface
    public interface ExecutionCallback {
        void onOrderUpdate(long orderId, String side, String executionType, String orderStatus,
                           BigDecimal lastExecutedQty, BigDecimal lastExecutedPrice);
    }

    public UserDataStreamService(BinanceOptimizedTradeService tradeService, BinanceProperties properties) {
        this.tradeService = tradeService;
        this.properties = properties;
    }

    public void setExecutionCallback(ExecutionCallback executionCallback) {
        this.executionCallback = executionCallback;
    }

    @PostConstruct
    public void start() {
        connect();
    }

    public synchronized void connect() {
        String listenKey = tradeService.createListenKey();
        if (listenKey == null) {
            log.error("无法获取 ListenKey，UserDataStream 启动失败");
            return;
        }
        this.currentListenKey.set(listenKey);
        String wsUrl = properties.getApi().getWsUserUrl() + "/" + listenKey;

        HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl), this)
                .thenAccept(ws -> log.info("已成功连入币安账户 User Data Stream!"))
                .exceptionally(ex -> {
                    log.error("连接 User Data Stream 异常", ex);
                    return null;
                });
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        try {
            JsonNode node = objectMapper.readTree(data.toString());
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
            }
        } catch (Exception e) {
            log.error("解析 UserDataStream 消息异常", e);
        }
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Scheduled(fixedRate = 1800000)
    public void keepAlive() {
        String key = currentListenKey.get();
        if (key != null) {
            tradeService.keepAliveListenKey(key);
        }
    }
}
