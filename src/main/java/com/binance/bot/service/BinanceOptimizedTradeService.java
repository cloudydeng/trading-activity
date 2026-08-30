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
    public JsonNode cancelAndReplaceOrder(String symbol, String side, BigDecimal price, BigDecimal quantity,
                                          Long cancelOrderId, String clientOrderId) {
        if (usedWeight1m.get() > 1000 && System.currentTimeMillis() - lastWeightUpdateMs.get() < 60_000) {
            log.warn("⚠️ API 权重过高 (used: {})，节流保护", usedWeight1m.get());
            return null;
        }

        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("side", side.toUpperCase());
        // LIMIT_MAKER is rejected rather than crossing the book, avoiding taker fills.
        params.put("type", "LIMIT_MAKER");
        params.put("quantity", quantity.toPlainString());
        params.put("price", price.toPlainString());
        params.put("newClientOrderId", clientOrderId);
        if (cancelOrderId != null && cancelOrderId > 0) {
            params.put("cancelOrderId", String.valueOf(cancelOrderId));
            // Never create a replacement if cancelling the tracked order failed.
            params.put("cancelReplaceMode", "STOP_ON_FAILURE");
        }
        params.put("selfTradePreventionMode", "EXPIRE_BOTH"); // Any same-account match expires both sides.
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
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("报单请求失败: HTTP {}, code={}, msg={}", response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.warn("报单请求状态未知: {}", e.getMessage());
            return null;
        }
    }

    public JsonNode cancelOrder(String symbol, long orderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol);
        params.put("orderId", String.valueOf(orderId));
        params.put("timestamp", String.valueOf(timestamp));

        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        String uri = "/api/v3/order?" + queryString + "&signature=" + signature;

        try {
            return restClient.delete()
                    .uri(uri)
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("撤单失败: ID={}, HTTP {}, code={}, msg={}", orderId,
                                    response.getStatusCode().value(), body.path("code").asText("unknown"),
                                    body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.warn("撤单请求状态未知: ID={}, {}", orderId, e.getMessage());
            return null;
        }
    }

    /** Emergency reducing order. Callers must use it only for an existing position. */
    public JsonNode placeMarketSell(String symbol, BigDecimal quantity, String clientOrderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("side", "SELL");
        params.put("type", "MARKET");
        params.put("quantity", quantity.toPlainString());
        params.put("newClientOrderId", clientOrderId);
        params.put("newOrderRespType", "ACK");
        params.put("selfTradePreventionMode", "EXPIRE_BOTH");
        params.put("timestamp", String.valueOf(timestamp));
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        try {
            return restClient.post()
                    .uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.error("紧急市价卖单失败: HTTP {}, code={}, msg={}", response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.error("紧急市价卖单请求状态未知: {}", e.getMessage());
            return null;
        }
    }

    /** Aggressive but price-capped entry: fills immediately up to limit price, then expires. */
    public JsonNode placeLimitIocBuy(String symbol, BigDecimal quantity, BigDecimal limitPrice, String clientOrderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("side", "BUY");
        params.put("type", "LIMIT");
        params.put("timeInForce", "IOC");
        params.put("quantity", quantity.toPlainString());
        params.put("price", limitPrice.toPlainString());
        params.put("newClientOrderId", clientOrderId);
        params.put("newOrderRespType", "ACK");
        params.put("selfTradePreventionMode", "EXPIRE_BOTH");
        params.put("timestamp", String.valueOf(timestamp));
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        try {
            return restClient.post()
                    .uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.error("IOC 买单失败: HTTP {}, code={}, msg={}", response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.error("IOC 买单请求状态未知: {}", e.getMessage());
            return null;
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
        return null;
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

    /** Queries the authoritative final state of one order after a cancel acknowledgement. */
    public JsonNode getOrder(String symbol, long orderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("orderId", String.valueOf(orderId));
        params.put("timestamp", String.valueOf(timestamp));
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, properties.getApi().getSecretKey());
        try {
            String response = restClient.get().uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .retrieve().body(String.class);
            return objectMapper.readTree(response);
        } catch (Exception e) {
            log.error("查询订单最终状态失败: {}", e.getMessage());
            return null;
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
