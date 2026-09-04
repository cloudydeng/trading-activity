package com.binance.bot.service;

import com.binance.bot.account.AccountCredentials;
import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class BinanceAccountTradeClient {

    private final RestClient restClient;
    private final BinanceProperties properties;
    private final AccountCredentials credentials;
    private final BinanceSigner signer;
    private final BinanceIpRateLimitCoordinator rateLimitCoordinator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BinanceAccountTradeClient(BinanceProperties properties, BinanceSigner signer,
                                     AccountCredentials credentials,
                                     BinanceIpRateLimitCoordinator rateLimitCoordinator) {
        this.properties = properties;
        this.signer = signer;
        this.credentials = credentials;
        this.rateLimitCoordinator = rateLimitCoordinator;
        this.restClient = RestClient.builder()
                .baseUrl(properties.getApi().getBaseUrl())
                .build();
    }

    /**
     * POST /api/v3/order/cancelReplace 原子级撤换单
     */
    public JsonNode cancelAndReplaceOrder(String symbol, String side, BigDecimal price, BigDecimal quantity,
                                          Long cancelOrderId, String clientOrderId) {
        boolean entryRequest = "BUY".equalsIgnoreCase(side);
        if (entryRequest) {
            BinanceIpRateLimitCoordinator.Permit permit = rateLimitCoordinator.tryAcquireEntryRequest(1);
            if (!permit.allowed()) {
                log.warn("[accountId={} alias={}] 共享 IP API 权重达到入场安全线 ({}/{})，暂缓新报单",
                        credentials.accountId(), credentials.alias(), permit.usedWeight1m(), permit.safeLimit1m());
                return localRateLimitedResponse(permit);
            }
        } else {
            rateLimitCoordinator.reserveSafetyRequest(1);
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
        String signature = signer.sign(queryString, credentials.secretKey());
        String uri = (cancelOrderId != null && cancelOrderId > 0)
                ? "/api/v3/order/cancelReplace?" + queryString + "&signature=" + signature
                : "/api/v3/order?" + queryString + "&signature=" + signature;

        try {
            return restClient.post()
                    .uri(uri)
                    .header("X-MBX-APIKEY", credentials.apiKey())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        HttpHeaders headers = response.getHeaders();
                        rateLimitCoordinator.updateFromHeaders(headers);
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("[accountId={} alias={}] 报单请求失败: HTTP {}, code={}, msg={}",
                                    credentials.accountId(), credentials.alias(), response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.warn("[accountId={} alias={}] 报单请求状态未知: {}",
                    credentials.accountId(), credentials.alias(), e.getMessage());
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
        String signature = signer.sign(queryString, credentials.secretKey());
        String uri = "/api/v3/order?" + queryString + "&signature=" + signature;

        try {
            return restClient.delete()
                    .uri(uri)
                    .header("X-MBX-APIKEY", credentials.apiKey())
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("[accountId={} alias={}] 撤单失败: ID={}, HTTP {}, code={}, msg={}",
                                    credentials.accountId(), credentials.alias(), orderId,
                                    response.getStatusCode().value(), body.path("code").asText("unknown"),
                                    body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.warn("[accountId={} alias={}] 撤单请求状态未知: ID={}, {}",
                    credentials.accountId(), credentials.alias(), orderId, e.getMessage());
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
        String signature = signer.sign(queryString, credentials.secretKey());
        try {
            return restClient.post()
                    .uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .header("X-MBX-APIKEY", credentials.apiKey())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.error("[accountId={} alias={}] 紧急市价卖单失败: HTTP {}, code={}, msg={}",
                                    credentials.accountId(), credentials.alias(), response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.error("[accountId={} alias={}] 紧急市价卖单请求状态未知: {}",
                    credentials.accountId(), credentials.alias(), e.getMessage());
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
        String signature = signer.sign(queryString, credentials.secretKey());
        try {
            return restClient.post()
                    .uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .header("X-MBX-APIKEY", credentials.apiKey())
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

    /** Price-bounded reducing exit: crosses only down to the supplied sell limit, then expires. */
    public JsonNode placeLimitIocSell(String symbol, BigDecimal quantity, BigDecimal limitPrice, String clientOrderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "IOC");
        params.put("quantity", quantity.toPlainString());
        params.put("price", limitPrice.toPlainString());
        params.put("newClientOrderId", clientOrderId);
        params.put("newOrderRespType", "ACK");
        params.put("selfTradePreventionMode", "EXPIRE_BOTH");
        params.put("timestamp", String.valueOf(timestamp));
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, credentials.secretKey());
        try {
            return restClient.post()
                    .uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .header("X-MBX-APIKEY", credentials.apiKey())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.error("IOC 卖单失败: HTTP {}, code={}, msg={}", response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.error("IOC 卖单请求状态未知: {}", e.getMessage());
            return null;
        }
    }

    /** Price-protected reducing exit that rests until filled or explicitly canceled. */
    public JsonNode placeLimitGtcSell(String symbol, BigDecimal quantity, BigDecimal limitPrice, String clientOrderId) {
        long timestamp = System.currentTimeMillis();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("side", "SELL");
        params.put("type", "LIMIT");
        params.put("timeInForce", "GTC");
        params.put("quantity", quantity.toPlainString());
        params.put("price", limitPrice.toPlainString());
        params.put("newClientOrderId", clientOrderId);
        params.put("newOrderRespType", "ACK");
        params.put("selfTradePreventionMode", "EXPIRE_BOTH");
        params.put("timestamp", String.valueOf(timestamp));
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, credentials.secretKey());
        try {
            return restClient.post()
                    .uri("/api/v3/order?" + queryString + "&signature=" + signature)
                    .header("X-MBX-APIKEY", credentials.apiKey())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.error("[accountId={} alias={}] GTC 限价卖单失败: HTTP {}, code={}, msg={}",
                                    credentials.accountId(), credentials.alias(), response.getStatusCode().value(),
                                    body.path("code").asText("unknown"), body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.error("[accountId={} alias={}] GTC 限价卖单请求状态未知: {}",
                    credentials.accountId(), credentials.alias(), e.getMessage());
            return null;
        }
    }

    public BigDecimal getFreeAssetBalance(String asset) {
        AssetBalance balance = getAssetBalance(asset);
        return balance == null ? null : balance.free();
    }

    /** Free plus locked is the authoritative inventory while an exit order is resting. */
    public AssetBalance getAssetBalance(String asset) {
        JsonNode root = getAccountInfo();
        if (root == null || !root.path("balances").isArray()) return null;
        for (JsonNode balance : root.path("balances")) {
            if (asset.equalsIgnoreCase(balance.path("asset").asText())) {
                BigDecimal free = new BigDecimal(balance.path("free").asText("0"));
                BigDecimal locked = new BigDecimal(balance.path("locked").asText("0"));
                return new AssetBalance(asset.toUpperCase(), free, locked, free.add(locked));
            }
        }
        return new AssetBalance(asset.toUpperCase(), BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);
    }

    /** Read the signed account snapshot used by the authenticated monitoring page. */
    public JsonNode getAccountInfo() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/account", params, "查询账户信息失败");
    }

    /** Account- and symbol-specific commission components used to price a fee-protected exit. */
    public JsonNode getAccountCommissionRates(String symbol) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/account/commission", params, "查询账户交易手续费率失败");
    }

    /** Read recent orders for one symbol; callers filter for executed orders for display. */
    public JsonNode getAllOrders(String symbol, int limit) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("limit", String.valueOf(Math.max(1, Math.min(limit, 1000))));
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/allOrders", params, "查询历史订单失败");
    }

    /** Authoritative fills, including trade id and actual commission, for one order. */
    public JsonNode getMyTrades(String symbol, long orderId) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("orderId", String.valueOf(orderId));
        params.put("limit", "1000");
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/myTrades", params, "查询订单成交明细失败");
    }

    /** Public conversion price used only to value commissions charged in a third asset such as BNB. */
    public BigDecimal getTickerPrice(String symbol) {
        try {
            String response = restClient.get().uri("/api/v3/ticker/price?symbol=" + symbol.toUpperCase())
                    .retrieve().body(String.class);
            JsonNode root = objectMapper.readTree(response);
            BigDecimal price = new BigDecimal(root.path("price").asText("0"));
            return price.signum() > 0 ? price : null;
        } catch (Exception e) {
            log.warn("查询手续费资产换算价格失败: symbol={}, {}", symbol, e.getMessage());
            return null;
        }
    }

    /** Returns null on an indeterminate API failure; callers must fail closed in that case. */
    public JsonNode getOpenOrders(String symbol) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/openOrders", params, "查询活动订单失败");
    }

    /** Account-wide open-order check used before abandoning one credential profile. */
    public JsonNode getAllOpenOrders() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/openOrders", params, "查询账户全部活动订单失败");
    }

    /** Queries the authoritative final state of one order after a cancel acknowledgement. */
    public JsonNode getOrder(String symbol, long orderId) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", symbol.toUpperCase());
        params.put("orderId", String.valueOf(orderId));
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return getSignedJson("/api/v3/order", params, "查询订单最终状态失败");
    }

    private String buildQueryString(Map<String, String> params) {
        return params.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));
    }

    private JsonNode getSignedJson(String path, Map<String, String> params, String errorMessage) {
        String queryString = buildQueryString(params);
        String signature = signer.sign(queryString, credentials.secretKey());
        try {
            return restClient.get().uri(path + "?" + queryString + "&signature=" + signature)
                    .header("X-MBX-APIKEY", credentials.apiKey())
                    .exchange((request, response) -> {
                        updateWeight(response.getHeaders());
                        JsonNode body = objectMapper.readTree(response.getBody());
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("[accountId={} alias={}] {}: HTTP {}, code={}, msg={}",
                                    credentials.accountId(), credentials.alias(), errorMessage,
                                    response.getStatusCode().value(), body.path("code").asText("unknown"),
                                    body.path("msg").asText("unknown"));
                        }
                        return body;
                    });
        } catch (Exception e) {
            log.error("[accountId={} alias={}] {}: {}", credentials.accountId(), credentials.alias(),
                    errorMessage, e.getMessage());
            return null;
        }
    }

    private void updateWeight(HttpHeaders headers) {
        rateLimitCoordinator.updateFromHeaders(headers);
    }

    private JsonNode localRateLimitedResponse(BinanceIpRateLimitCoordinator.Permit permit) {
        var response = objectMapper.createObjectNode();
        response.put("localRateLimited", true);
        response.put("code", "LOCAL_IP_WEIGHT_LIMIT");
        response.put("msg", "shared IP request-weight entry reserve reached");
        response.put("usedWeight1m", permit.usedWeight1m());
        response.put("requestWeightLimit1m", permit.limit1m());
        response.put("safeRequestWeightLimit1m", permit.safeLimit1m());
        response.put("retryAfterMs", permit.retryAfterMs());
        return response;
    }

    public int getUsedWeight1m() { return rateLimitCoordinator.snapshot().usedWeight1m(); }
    public int getRequestWeightLimit1m() { return rateLimitCoordinator.snapshot().limit1m(); }
    public int getSafeRequestWeightLimit1m() { return rateLimitCoordinator.snapshot().safeLimit1m(); }

    public record AssetBalance(String asset, BigDecimal free, BigDecimal locked, BigDecimal total) { }
}
