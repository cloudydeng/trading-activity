package com.binance.bot.service;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BinanceOptimizedTradeService {

    private final RestClient restClient;
    private final BinanceProperties properties;
    private final BinanceSigner signer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Getter
    private final AtomicInteger usedWeight1m = new AtomicInteger(0);
    private final AtomicLong lastWeightUpdateMs = new AtomicLong(0);

    public BinanceOptimizedTradeService(BinanceProperties properties, BinanceSigner signer) {
        this.properties = properties;
        this.signer = signer;
        this.restClient = RestClient.builder()
                .baseUrl(properties.getApi().getBaseUrl())
                .defaultHeader("X-MBX-APIKEY", properties.getApi().getApiKey())
                .build();
    }

    /**
     * POST /api/v3/order/cancelReplace 原子级撤换单
     */
    public JsonNode cancelAndReplaceOrder(String symbol, String side, BigDecimal price, BigDecimal quantity, Long cancelOrderId) {
        if (usedWeight1m.get() > 1000 && System.currentTimeMillis() - lastWeightUpdateMs.get() < 60_000) {
            log.warn("⚠️ API 权重过高 (used: {})，节流保护", usedWeight1m.get());
            return null;
        }

        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", side.toUpperCase());
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTX"); // 100% Maker Post-Only 保证
        params.put("quantity", quantity.toPlainString());
        params.put("price", price.toPlainString());
        if (cancelOrderId != null && cancelOrderId > 0) {
            params.put("cancelOrderId", String.valueOf(cancelOrderId));
            // Never create a replacement if cancelling the tracked order failed.
            params.put("cancelReplaceMode", "STOP_ON_FAILURE");
        }
        params.put("selfTradePreventionMode", "EXPIRE_MAKER"); // 防自成交封号
        params.put("timestamp", String.valueOf(timestamp));

        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        String uri = (cancelOrderId != null && cancelOrderId > 0)
                ? "/api/v3/order/cancelReplace?" + queryString + "&signature=" + signature
                : "/api/v3/order?" + queryString + "&signature=" + signature;

        try {
            return restClient.post()
                    .uri(uri)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        HttpHeaders headers = response.getHeaders();
                        String weightStr = headers.getFirst("x-mbx-used-weight-1m");
                        if (weightStr != null) {
                            usedWeight1m.set(Integer.parseInt(weightStr));
                            lastWeightUpdateMs.set(System.currentTimeMillis());
                        }
                        if (response.getStatusCode().is2xxSuccessful()) {
                            return objectMapper.readTree(response.getBody());
                        }
                        return null;
                    });
        } catch (Exception e) {
            log.debug("做市报单被保护拦截或失效: {}", e.getMessage());
            return null;
        }
    }

    public boolean cancelOrder(String symbol, long orderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("orderId", String.valueOf(orderId));
        params.put("timestamp", String.valueOf(timestamp));

        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        String uri = "/api/v3/order?" + queryString + "&signature=" + signature;

        try {
            var response = restClient.delete()
                    .uri(uri)
                    .retrieve()
                    .toBodilessEntity();
            updateWeight(response.getHeaders());
            log.debug("已撤单: ID={}", orderId);
            return true;
        } catch (Exception e) {
            log.debug("撤单异常: {}", e.getMessage());
            return false;
        }
    }

    public BigDecimal getFreeAssetBalance(String asset) {
        long timestamp = System.currentTimeMillis();
        String queryString = "timestamp=" + timestamp;
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        String uri = "/api/v3/account?" + queryString + "&signature=" + signature;

        try {
            String res = restClient.get().uri(uri).retrieve().body(String.class);
            JsonNode root = objectMapper.readTree(res);
            if (root.has("balances")) {
                for (JsonNode b : root.get("balances")) {
                    if (asset.equalsIgnoreCase(b.get("asset").asText())) {
                        return new BigDecimal(b.get("free").asText());
                    }
                }
            }
        } catch (Exception e) {
            log.error("查询账户余额失败", e);
        }
        return BigDecimal.ZERO;
    }

    /** Returns null on an indeterminate API failure; callers must fail closed in that case. */
    public JsonNode getOpenOrders(String symbol) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("timestamp", String.valueOf(timestamp));
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        try {
            String response = restClient.get().uri("/api/v3/openOrders?" + queryString + "&signature=" + signature)
                    .retrieve().body(String.class);
            return objectMapper.readTree(response);
        } catch (Exception e) {
            log.error("查询活动订单失败；拒绝在未知订单状态下启动: {}", e.getMessage());
            return null;
        }
    }

    public String createListenKey() {
        try {
            String response = restClient.post()
                    .uri("/api/v3/userDataStream")
                    .retrieve()
                    .body(String.class);
            JsonNode jsonNode = objectMapper.readTree(response);
            return jsonNode.get("listenKey").asText();
        } catch (Exception e) {
            log.error("创建 ListenKey 失败", e);
            return null;
        }
    }

    public void keepAliveListenKey(String listenKey) {
        try {
            restClient.put()
                    .uri("/api/v3/userDataStream?listenKey=" + listenKey)
                    .retrieve()
                    .toBodilessEntity();
            log.debug("刷新 ListenKey 保活");
        } catch (Exception e) {
            log.error("ListenKey 保活失败: {}", e.getMessage());
        }
    }

    private String buildQueryString(Map<String, String> params) {
        return params.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));
    }

    private void updateWeight(HttpHeaders headers) {
        String weightStr = headers.getFirst("x-mbx-used-weight-1m");
        if (weightStr != null) {
            usedWeight1m.set(Integer.parseInt(weightStr));
            lastWeightUpdateMs.set(System.currentTimeMillis());
        }
    }
}
