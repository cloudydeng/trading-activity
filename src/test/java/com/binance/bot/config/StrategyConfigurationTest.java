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

    private MutablePropertySources applicationPropertySources() throws Exception {
        PropertySource<?> applicationYaml = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"))
                .getFirst();
        MutablePropertySources sources = new MutablePropertySources();
        sources.addLast(applicationYaml);
        return sources;
    }
}
