package com.binance.bot.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;

@Data
@Configuration
@ConfigurationProperties(prefix = "binance")
public class BinanceProperties {

    private Api api = new Api();
    private Strategy strategy = new Strategy();

    @Data
    public static class Api {
        private String baseUrl;
        private String wsMarketUrl;
        private String wsUserUrl;
        private String apiKey;
        private String secretKey;
    }

    @Data
    public static class Strategy {
        private String symbol;
        private BigDecimal orderAmountUsdt;
        private int bidDepthOffsetTicks;
        private int askDepthOffsetTicks;
        private long orderTtlMs;
        private int minSpreadTicks;
        private double randomSizeJitter;
        private long signalLookbackMs = 3000;
        private long marketDataStaleMs = 1000;
        private double minBookImbalance = 0.05;
        private double minDepthImbalance = -0.10;
        private double minTakerFlowImbalance = -0.20;
        private double maxDownwardMoveBps = 8;
        private double maxShortTermVolatilityBps = 20;
    }
}
