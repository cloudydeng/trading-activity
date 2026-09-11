package com.binance.bot.account;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolTradeCoordinatorTest {
    @Test
    void limitsConcurrentEntriesPerSymbolAndReleasesByEngine() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);

        assertTrue(coordinator.acquire("bnbusdt", "account-a::BNBUSDT", "A").accepted());
        assertTrue(coordinator.acquire("BNBUSDT", "account-b::BNBUSDT", "B").accepted());
        SymbolTradeCoordinator.EntryPermit third =
                coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C");

        assertFalse(third.accepted());
        assertTrue(third.reason().contains("已有 2/2 个账户交易中"));
        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());

        coordinator.release("BNBUSDT", "account-a::BNBUSDT");

        assertTrue(coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C").accepted());
        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
    }

    @Test
    void claimExistingCycleRecordsRestoredHolderWithoutCheckingLimit() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        assertTrue(coordinator.acquire("BNBUSDT", "account-a::BNBUSDT", "A").accepted());

        coordinator.claimExisting("BNBUSDT", "account-b::BNBUSDT", "B");

        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
    }
}
