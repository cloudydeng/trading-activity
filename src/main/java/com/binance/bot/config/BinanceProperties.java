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
    private Security security = new Security();

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
        /** A second explicit switch is required even when executionMode is LIVE. */
        private boolean liveTradingEnabled = false;
        private String symbol;
        private BigDecimal orderAmountUsdt;
        private BigDecimal maxLiveOrderNotionalUsdt = new BigDecimal("11");
        private int bidDepthOffsetTicks;
        private int askDepthOffsetTicks;
        private long orderTtlMs;
        /** Soft signal noise cannot cancel a fresh entry before this resting time. */
        private long minEntryOrderRestMs = 800;
        /** After this maker-only window, a still-valid signal may use a capped IOC limit buy. */
        private long makerEntryFallbackMs = 2_000;
        /** Maximum IOC buy limit above the observed best ask. */
        private int entryIocMaxSlippageTicks = 1;
        private double randomSizeJitter;
        private long signalLookbackMs = 3000;
        private long marketDataStaleMs = 1000;
        /**
         * Connection-liveness timeout. Binance sends WebSocket ping frames about every 20 seconds,
         * so this must be substantially longer than the trading-data freshness threshold.
         */
        private long marketStreamWatchdogMs = 45_000;
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
        /** Signal-driven exit defaults: target must cover both estimated maker fees. */
        private double takeProfitBps = 5;
        private double stopLossBps = 80;
        /** Keep the original fee-aware maker target untouched for this recovery window. */
        private long exitRepriceAfterMs = 60_000;
        /** Once recovery time expires, only lower a passive sell at this cadence. */
        private long exitRepriceIntervalMs = 5_000;
        /** Final time cap before a price-bounded IOC reduction is attempted. */
        private long maxHoldingMs = 90_000;
        /** IOC sell may cross this many ticks below the observed best bid. */
        private int exitIocMaxSlippageTicks = 1;
        /** A pessimistic fee estimate, until actual commission events are accounted for. */
        private BigDecimal assumedMakerFeeBps = new BigDecimal("10");
        /** Unconditional OBSERVE-only market baseline cadence; kept separate from qualified signal samples. */
        private long benchmarkObservationIntervalMs = 2_000;
        private long paperEntryIntervalMs = 1_500;
        private int minPaperObservations = 500;
        private int minQualifiedObservationsForLive = 0;
        private int minBaselineObservationsForLive = 0;
        private boolean collectObservations = false;
        private String observationOutputFile = "data/paper-outcomes.jsonl";

        public boolean isObserveMode() {
            return "OBSERVE".equalsIgnoreCase(executionMode);
        }

        public boolean isLiveMode() { return "LIVE".equalsIgnoreCase(executionMode); }
    }

    @Data
    public static class Security {
        /** Required for every dashboard/control API request; injected only through BOT_ADMIN_TOKEN. */
        private String adminToken;
        /** Browser login secret; injected only through BOT_ADMIN_PASSWORD. */
        private String adminPassword;
    }
}
