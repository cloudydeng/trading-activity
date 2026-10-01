package com.binance.bot.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupportedTradingPairTest {
    @Test
    void acceptsUsdtAndUsdcSymbols() {
        assertTrue(SupportedTradingPair.isSupported(" babyusdt "));
        assertTrue(SupportedTradingPair.isSupported("algousdc"));
        assertTrue(SupportedTradingPair.isSupported(" pumpusdc "));
        assertEquals("USDC", SupportedTradingPair.quoteAsset("ALGOUSDC"));
        assertEquals("USDC", SupportedTradingPair.quoteAsset("PUMPUSDC"));
        assertEquals("USDT", SupportedTradingPair.quoteAsset("BABYUSDT"));
        assertTrue(SupportedTradingPair.isSupported("BABYUSDC"));
        assertFalse(SupportedTradingPair.isSupported("ALGOUSD"));
    }

    @Test
    void rejectsTwoMarketsSharingBaseInventoryOnOneAccount() {
        assertThrows(IllegalArgumentException.class, () -> SupportedTradingPair.requireDistinctBaseAssets(
                List.of("ALGOUSDT", "ALGOUSDC")));
        assertThrows(IllegalArgumentException.class, () -> SupportedTradingPair.requireDistinctBaseAssets(
                List.of("pumpusdc", "PUMPUSDT")));
        SupportedTradingPair.requireDistinctBaseAssets(List.of("PUMPUSDC", "ALGOUSDT"));
    }
}
