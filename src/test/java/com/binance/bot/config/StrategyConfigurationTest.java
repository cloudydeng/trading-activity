package com.binance.bot.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.ClassPathResource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StrategyConfigurationTest {
    private static final String ENV_KEY = "BINANCE_STRATEGY_MAX_DAILY_DRAWDOWN_USDT";
    private static final String PROPERTY_KEY = "binance.strategy.max-daily-drawdown-usdt";
    private static final String SYMBOL_LIMIT_ENV_KEY = "BINANCE_STRATEGY_MAX_CONCURRENT_ENTRIES_PER_SYMBOL";
    private static final String SYMBOL_LIMIT_PROPERTY_KEY =
            "binance.strategy.max-concurrent-entries-per-symbol";
    private static final String BUY_PRICE_EXIT_TIMEOUT_PROPERTY_KEY =
            "binance.strategy.buy-price-maker-limit-sell-timeout-ms";

    @Test
    void protectedEnvironmentFileValueOverridesDailyDrawdownFallback() throws Exception {
        MutablePropertySources sources = applicationPropertySources();
        sources.addFirst(new MapPropertySource("protectedEnvironmentFile", Map.of(ENV_KEY, "30")));

        assertEquals("30", new PropertySourcesPropertyResolver(sources).getProperty(PROPERTY_KEY));
    }

    @Test
    void dailyDrawdownFallsBackToEightWithoutEnvironmentOverride() throws Exception {
        assertEquals("8", new PropertySourcesPropertyResolver(applicationPropertySources())
                .getProperty(PROPERTY_KEY));
    }

    @Test
    void sameSymbolConcurrencyFallsBackToOneAndCanBeOverridden() throws Exception {
        assertEquals("1", new PropertySourcesPropertyResolver(applicationPropertySources())
                .getProperty(SYMBOL_LIMIT_PROPERTY_KEY));

        MutablePropertySources sources = applicationPropertySources();
        sources.addFirst(new MapPropertySource("protectedEnvironmentFile", Map.of(SYMBOL_LIMIT_ENV_KEY, "2")));

        assertEquals("2", new PropertySourcesPropertyResolver(sources).getProperty(SYMBOL_LIMIT_PROPERTY_KEY));
    }

    @Test
    void buyPriceMakerSellTimeoutDefaultsToThirtyMinutes() throws Exception {
        assertEquals("1800000", new PropertySourcesPropertyResolver(applicationPropertySources())
                .getProperty(BUY_PRICE_EXIT_TIMEOUT_PROPERTY_KEY));
        assertEquals(1_800_000L,
                new BinanceProperties().getStrategy().getBuyPriceMakerLimitSellTimeoutMs());
    }

    private MutablePropertySources applicationPropertySources() throws Exception {
        PropertySource<?> applicationYaml = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"))
                .getFirst();
        MutablePropertySources sources = new MutablePropertySources();
        sources.addLast(applicationYaml);
        return sources;
    }
}
