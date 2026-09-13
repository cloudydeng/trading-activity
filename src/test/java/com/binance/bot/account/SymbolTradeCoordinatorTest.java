package com.binance.bot.account;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolTradeCoordinatorTest {
    @Test
    void concurrencyTwoAllowsTwoCompleteCyclesAndBlocksThirdUntilOneCloses() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);

        assertTrue(coordinator.acquire("bnbusdt", "a", "A").accepted());
        assertTrue(coordinator.acquire("BNBUSDT", "b", "B").accepted());
        SymbolTradeCoordinator.EntryPermit third = coordinator.acquire("BNBUSDT", "c", "C");

        assertFalse(third.accepted());
        assertTrue(third.reason().contains("已有 2/2 个账户交易中"));
        assertTrue(third.reason().contains("FIFO 排队第 1 位"));

        assertTrue(coordinator.transitionToSell("BNBUSDT", "a", "A").accepted());
        assertTrue(coordinator.transitionToSell("BNBUSDT", "b", "B").accepted());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());
        assertFalse(coordinator.acquire("BNBUSDT", "c", "C").accepted());

        coordinator.release("BNBUSDT", "a");
        assertTrue(coordinator.acquire("BNBUSDT", "c", "C").accepted());
        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
    }

    @Test
    void concurrencyFourCapsTenUsersAtFourCompleteCyclesWithoutBuySellLaneSplit() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(4);

        for (int index = 1; index <= 10; index++) {
            SymbolTradeCoordinator.EntryPermit permit = coordinator.acquire(
                    "THEUSDT", "engine-" + index, "Account " + index);
            assertEquals(index <= 4, permit.accepted());
        }
        assertEquals(4, coordinator.snapshot("THEUSDT").holders().size());
        assertEquals(6, coordinator.snapshot("THEUSDT").waiters().size());

        for (int index = 1; index <= 4; index++) {
            assertTrue(coordinator.transitionToSell(
                    "THEUSDT", "engine-" + index, "Account " + index).accepted());
        }
        assertEquals(4, coordinator.snapshot("THEUSDT").holders().size());
        assertFalse(coordinator.acquire("THEUSDT", "engine-5", "Account 5").accepted());

        coordinator.release("THEUSDT", "engine-2");
        assertFalse(coordinator.acquire("THEUSDT", "engine-6", "Account 6").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "engine-5", "Account 5").accepted());
        assertEquals(4, coordinator.snapshot("THEUSDT").holders().size());
    }

    @Test
    void buyToSellTransitionKeepsTheOriginalCompleteCycleSlot() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        long acquiredAt = holder(coordinator, "a").acquiredAtMs();

        assertTrue(coordinator.transitionToSell("THEUSDT", "a", "A").accepted());

        assertEquals(1, coordinator.snapshot("THEUSDT").holders().size());
        assertEquals(acquiredAt, holder(coordinator, "a").acquiredAtMs());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
    }

    @Test
    void concurrencyIsIsolatedPerSymbol() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);

        assertTrue(coordinator.acquire("THEUSDT", "the-a", "A").accepted());
        assertTrue(coordinator.acquire("BNBUSDT", "bnb-b", "B").accepted());
        assertEquals(1, coordinator.snapshot("THEUSDT").holders().size());
        assertEquals(1, coordinator.snapshot("BNBUSDT").holders().size());
    }

    @Test
    void preservesStrictFifoAcrossRepeatedRetries() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "b", "B").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "c", "C").accepted());

        assertFalse(coordinator.acquire("THEUSDT", "b", "B").accepted());
        coordinator.release("THEUSDT", "a");
        SymbolTradeCoordinator.EntryPermit cBeforeB = coordinator.acquire("THEUSDT", "c", "C");

        assertFalse(cBeforeB.accepted());
        assertTrue(cBeforeB.reason().contains("FIFO 排队第 2 位"));
        assertTrue(coordinator.acquire("THEUSDT", "b", "B").accepted());
        coordinator.release("THEUSDT", "b");
        assertTrue(coordinator.acquire("THEUSDT", "c", "C").accepted());
    }

    @Test
    void heldDustCanReenterWithoutTakingAnotherCompleteCycleSlot() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        coordinator.markHolding("THEUSDT", "a", "A");

        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertEquals(1, coordinator.snapshot("THEUSDT").holders().size());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, holder(coordinator, "a").phase());
    }

    @Test
    void loweringLimitPreservesExistingCyclesAndBlocksNewOnesUntilWithinLimit() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(3);
        coordinator.claimExistingSell("THEUSDT", "a", "A");
        coordinator.claimExistingSell("THEUSDT", "b", "B");
        coordinator.claimHolding("THEUSDT", "c", "C");
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);

        SymbolTradeCoordinator.EntryPermit blocked = coordinator.acquire("THEUSDT", "d", "D");
        assertFalse(blocked.accepted());
        assertTrue(blocked.reason().contains("已有 3/2 个账户交易中"));
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());

        coordinator.release("THEUSDT", "a");
        assertFalse(coordinator.acquire("THEUSDT", "d", "D").accepted());
        coordinator.release("THEUSDT", "b");
        assertTrue(coordinator.acquire("THEUSDT", "d", "D").accepted());
    }

    @Test
    void releaseRemovesWaitingEntry() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "b", "B").accepted());

        coordinator.release("THEUSDT", "b");

        assertTrue(coordinator.snapshot("THEUSDT").waiters().isEmpty());
    }

    @Test
    void restoredActiveCyclesAreGrandfatheredAboveCurrentLimit() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.claimExistingSell("BNBUSDT", "a", "A");
        coordinator.claimExistingSell("BNBUSDT", "b", "B");

        assertEquals(2, coordinator.snapshot("BNBUSDT").holders().size());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());
    }

    private SymbolTradeCoordinator.Holder holder(SymbolTradeCoordinator coordinator, String engineId) {
        return coordinator.snapshot("THEUSDT").holders().stream()
                .filter(holder -> holder.engineId().equals(engineId))
                .findFirst()
                .orElseGet(() -> coordinator.snapshot("BNBUSDT").holders().stream()
                        .filter(holder -> holder.engineId().equals(engineId))
                        .findFirst()
                        .orElseThrow());
    }
}
