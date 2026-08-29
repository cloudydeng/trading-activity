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
        /** OBSERVE never sends authenticated requests or orders; LIVE is an explicit opt-in. */
        private String executionMode = "OBSERVE";
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
        /** After a detected sell-off, require a quiet period and a measured reclaim before bidding again. */
        private long postSelloffCooldownMs = 1_500;
        private double minPostSelloffReclaimBps = 3;
        /** Conservative production guards. Values are denominated in the quote asset (USDT). */
        private BigDecimal maxInventoryUsdt = new BigDecimal("40");
        private BigDecimal maxDailyRealizedLossUsdt = new BigDecimal("5");
        private BigDecimal maxDailyDrawdownUsdt = new BigDecimal("8");
        private long maxInventoryAgeMs = 60_000;
        /** A pessimistic fee estimate, until actual commission events are accounted for. */
        private BigDecimal assumedMakerFeeBps = new BigDecimal("10");
        private long paperEntryIntervalMs = 1_500;
        private int minPaperObservations = 500;

        public boolean isObserveMode() {
            return "OBSERVE".equalsIgnoreCase(executionMode);
        }
    }
}
