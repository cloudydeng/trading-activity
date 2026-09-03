package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class MultiAccountRiskIsolationTest {
    @Test
    void trippingAccountADoesNotBlockAccountB() {
        TradingRiskGuard accountA = new TradingRiskGuard();
        TradingRiskGuard accountB = new TradingRiskGuard();
        BinanceProperties.Strategy config = new BinanceProperties.Strategy();
        config.setMaxInventoryUsdt(new BigDecimal("100"));

        accountA.trip("ACCOUNT_A_ONLY");

        assertFalse(accountA.permitsNewEntry(BigDecimal.ONE, BigDecimal.ONE, 1, config));
        assertTrue(accountB.permitsNewEntry(BigDecimal.ONE, BigDecimal.ONE, 1, config));
        assertNull(accountB.getEntryBlockReason());
    }
}
