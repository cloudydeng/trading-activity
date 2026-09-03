package com.binance.bot.service;

import com.binance.bot.account.AccountCredentials;
import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinanceIpRateLimitCoordinatorTest {
    @Test
    void sharesOneEntryBudgetAndResetsAtTheNextMinute() {
        AtomicLong now = new AtomicLong(120_000);
        BinanceIpRateLimitCoordinator coordinator = new BinanceIpRateLimitCoordinator(now::get);
        coordinator.recordExchangeWeight(4_799);

        assertTrue(coordinator.tryAcquireEntryRequest(1).allowed());
        BinanceIpRateLimitCoordinator.Permit denied = coordinator.tryAcquireEntryRequest(1);
        assertFalse(denied.allowed());
        assertEquals(4_800, denied.usedWeight1m());
        assertEquals(6_000, denied.limit1m());
        assertEquals(4_800, denied.safeLimit1m());

        now.set(180_000);
        assertTrue(coordinator.tryAcquireEntryRequest(1).allowed());
        assertEquals(1, coordinator.snapshot().usedWeight1m());
    }

    @Test
    void discoversRequestWeightLimitFromExchangeInfo() throws Exception {
        BinanceIpRateLimitCoordinator coordinator = new BinanceIpRateLimitCoordinator();
        coordinator.updateLimitFromExchangeInfo(new ObjectMapper().readTree("""
                {"rateLimits":[
                  {"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":100},
                  {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":1200}
                ]}
                """));

        assertEquals(1_200, coordinator.snapshot().limit1m());
        assertEquals(960, coordinator.snapshot().safeLimit1m());
    }

    @Test
    void locallyThrottledEntryReturnsExplicitResponseWithoutNetworkRequest() {
        BinanceIpRateLimitCoordinator coordinator = new BinanceIpRateLimitCoordinator();
        coordinator.recordExchangeWeight(4_800);
        BinanceProperties properties = new BinanceProperties();
        properties.getApi().setBaseUrl("http://127.0.0.1:9");
        BinanceAccountTradeClient client = new BinanceAccountTradeClient(properties, new BinanceSigner(),
                new AccountCredentials("account-a", "A", "key", "secret"), coordinator);

        var response = client.cancelAndReplaceOrder("ENSOUSDT", "BUY", new BigDecimal("0.85"),
                new BigDecimal("10"), null, "client-order-id");

        assertTrue(response.path("localRateLimited").asBoolean());
        assertEquals("LOCAL_IP_WEIGHT_LIMIT", response.path("code").asText());
        assertTrue(response.path("retryAfterMs").asLong() > 0);
    }
}
