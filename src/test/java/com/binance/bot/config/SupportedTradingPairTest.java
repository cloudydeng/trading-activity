package com.binance.bot.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupportedTradingPairTest {
    @Test
    void acceptsExistingUsdtSymbolsAndAlgoUsdcOnly() {
        assertTrue(SupportedTradingPair.isSupported(" babyusdt "));
        assertTrue(SupportedTradingPair.isSupported("algousdc"));
        assertEquals("USDC", SupportedTradingPair.quoteAsset("ALGOUSDC"));
        assertEquals("USDT", SupportedTradingPair.quoteAsset("BABYUSDT"));
        assertFalse(SupportedTradingPair.isSupported("BABYUSDC"));
        assertFalse(SupportedTradingPair.isSupported("ALGOUSD"));
    }

    @Test
    void rejectsTwoMarketsSharingAlgoInventoryOnOneAccount() {
        assertThrows(IllegalArgumentException.class, () -> SupportedTradingPair.requireDistinctBaseAssets(
                List.of("ALGOUSDT", "ALGOUSDC")));
    }
}
