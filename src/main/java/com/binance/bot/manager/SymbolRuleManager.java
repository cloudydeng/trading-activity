package com.binance.bot.manager;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class SymbolRuleManager {

    private final RestClient restClient;
    private final BinanceProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, SymbolRule> symbolRules = new ConcurrentHashMap<>();

    public record SymbolRule(
            String symbol,
            BigDecimal tickSize,
            BigDecimal stepSize,
            BigDecimal minNotional
    ) {}

    public SymbolRuleManager(BinanceProperties properties) {
        this.properties = properties;
        this.restClient = RestClient.builder()
                .baseUrl(properties.getApi().getBaseUrl())
                .build();
    }

    @PostConstruct
    public void init() {
        refreshRules();
    }

    public void refreshRules() {
        refreshRule(properties.getStrategy().getSymbol());
    }

    /** Loads and validates one exact spot symbol. Returns null on any exchange or validation failure. */
    public SymbolRule refreshRule(String requestedSymbol) {
        try {
            String requested = requestedSymbol.toUpperCase();
            log.info("从 Binance 获取交易对精度规则: {}", requested);
            String response = restClient.get()
                    .uri("/api/v3/exchangeInfo?symbol=" + requested)
                    .retrieve()
                    .body(String.class);

            JsonNode root = objectMapper.readTree(response);
            if (root != null && root.has("symbols")) {
                for (JsonNode s : root.get("symbols")) {
                    String symbol = s.get("symbol").asText();
                    if (!requested.equalsIgnoreCase(symbol)
                            || !"TRADING".equalsIgnoreCase(s.path("status").asText())
                            || (s.has("isSpotTradingAllowed") && !s.path("isSpotTradingAllowed").asBoolean())) {
                        continue;
                    }
                    BigDecimal tickSize = new BigDecimal("0.0001");
                    BigDecimal stepSize = new BigDecimal("0.1");
                    BigDecimal minNotional = new BigDecimal("5.0");

                    for (JsonNode f : s.get("filters")) {
                        String filterType = f.get("filterType").asText();
                        if ("PRICE_FILTER".equals(filterType)) {
                            tickSize = new BigDecimal(f.get("tickSize").asText());
                        } else if ("LOT_SIZE".equals(filterType)) {
                            stepSize = new BigDecimal(f.get("stepSize").asText());
                        } else if ("NOTIONAL".equals(filterType) || "MIN_NOTIONAL".equals(filterType)) {
                            if (f.has("minNotional")) {
                                minNotional = new BigDecimal(f.get("minNotional").asText());
                            }
                        }
                    }
                    SymbolRule rule = new SymbolRule(symbol, tickSize, stepSize, minNotional);
                    symbolRules.put(symbol, rule);
                    log.info("加载交易规则成功: {}", rule);
                    return rule;
                }
            }
        } catch (Exception e) {
            log.error("加载交易对规则失败: {}", requestedSymbol, e);
        }
        return null;
    }

    public SymbolRule getRule(String symbol) {
        return symbolRules.get(symbol.toUpperCase());
    }
}
