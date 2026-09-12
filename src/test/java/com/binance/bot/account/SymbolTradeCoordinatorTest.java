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
        assertTrue(third.reason().contains("FIFO 排队第 1 位"));
        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
        assertEquals(1, coordinator.snapshot("BNBUSDT").waiters().size());

        coordinator.release("BNBUSDT", "account-a::BNBUSDT");

        assertTrue(coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C").accepted());
        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
        assertTrue(coordinator.snapshot("BNBUSDT").waiters().isEmpty());
    }

    @Test
    void preservesStrictFifoOrderAcrossRepeatedRetries() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        assertTrue(coordinator.acquire("BNBUSDT", "account-a::BNBUSDT", "A").accepted());
        assertFalse(coordinator.acquire("BNBUSDT", "account-b::BNBUSDT", "B").accepted());
        assertFalse(coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C").accepted());

        // Retrying does not move B to the tail, and C cannot jump B after the slot opens.
        assertFalse(coordinator.acquire("BNBUSDT", "account-b::BNBUSDT", "B").accepted());
        coordinator.release("BNBUSDT", "account-a::BNBUSDT");
        SymbolTradeCoordinator.EntryPermit cBeforeB =
                coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C");

        assertFalse(cBeforeB.accepted());
        assertTrue(cBeforeB.reason().contains("FIFO 排队第 2 位"));
        assertTrue(coordinator.acquire("BNBUSDT", "account-b::BNBUSDT", "B").accepted());
        coordinator.release("BNBUSDT", "account-b::BNBUSDT");
        assertTrue(coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C").accepted());
    }

    @Test
    void removingStoppedWaiterLetsNextAccountAdvance() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        assertTrue(coordinator.acquire("BNBUSDT", "account-a::BNBUSDT", "A").accepted());
        assertFalse(coordinator.acquire("BNBUSDT", "account-b::BNBUSDT", "B").accepted());
        assertFalse(coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C").accepted());

        coordinator.release("BNBUSDT", "account-b::BNBUSDT");
        coordinator.release("BNBUSDT", "account-a::BNBUSDT");

        assertTrue(coordinator.acquire("BNBUSDT", "account-c::BNBUSDT", "C").accepted());
        assertTrue(coordinator.snapshot("BNBUSDT").waiters().isEmpty());
    }

    @Test
    void claimExistingCycleRecordsRestoredHolderWithoutCheckingLimit() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        assertTrue(coordinator.acquire("BNBUSDT", "account-a::BNBUSDT", "A").accepted());

        coordinator.claimExisting("BNBUSDT", "account-b::BNBUSDT", "B");

        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
    }
}
