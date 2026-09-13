package com.binance.bot.account;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolTradeCoordinatorTest {
    @Test
    void concurrencyTwoRunsOneBuyAndOneSellThenQueuesCompletedBuy() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);

        assertTrue(coordinator.acquire("bnbusdt", "a", "A").accepted());
        SymbolTradeCoordinator.EntryPermit waitingBuy = coordinator.acquire("BNBUSDT", "b", "B");
        assertFalse(waitingBuy.accepted());
        assertTrue(waitingBuy.reason().contains("买入通道已有 1/1"));
        assertTrue(waitingBuy.reason().contains("BUY FIFO 排队第 1 位"));

        assertTrue(coordinator.acquireSell("BNBUSDT", "a", "A").accepted());
        assertTrue(coordinator.acquire("BNBUSDT", "b", "B").accepted());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, holder(coordinator, "b").phase());

        SymbolTradeCoordinator.EntryPermit waitingSell = coordinator.acquireSell("BNBUSDT", "b", "B");
        assertFalse(waitingSell.accepted());
        assertTrue(waitingSell.reason().contains("卖出通道已有 1/1"));
        assertTrue(waitingSell.reason().contains("持仓待卖 FIFO 排队第 1 位"));
        assertEquals(SymbolTradeCoordinator.Phase.WAITING_TO_SELL, holder(coordinator, "b").phase());

        coordinator.release("BNBUSDT", "a");
        assertTrue(coordinator.acquireSell("BNBUSDT", "b", "B").accepted());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());
    }

    @Test
    void concurrencyThreeRunsOneBuyAndTwoSells() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(3);

        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "b", "B").accepted());
        assertTrue(coordinator.acquireSell("THEUSDT", "a", "A").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "b", "B").accepted());
        assertTrue(coordinator.acquireSell("THEUSDT", "b", "B").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "c", "C").accepted());

        SymbolTradeCoordinator.Snapshot snapshot = coordinator.snapshot("THEUSDT");
        assertEquals(3, snapshot.holders().size());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, holder(coordinator, "c").phase());

        SymbolTradeCoordinator.EntryPermit waitingSell = coordinator.acquireSell("THEUSDT", "c", "C");
        assertFalse(waitingSell.accepted());
        assertEquals(SymbolTradeCoordinator.Phase.WAITING_TO_SELL, holder(coordinator, "c").phase());
        assertEquals(1, coordinator.snapshot("THEUSDT").sellWaiters().size());
    }

    @Test
    void concurrencyFourRunsTwoBuysAndTwoSells() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(4);

        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "b", "B").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "c", "C").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "d", "D").accepted());

        assertTrue(coordinator.acquireSell("THEUSDT", "a", "A").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "c", "C").accepted());
        assertTrue(coordinator.acquireSell("THEUSDT", "b", "B").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "d", "D").accepted());

        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, holder(coordinator, "c").phase());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, holder(coordinator, "d").phase());
        assertTrue(coordinator.snapshot("THEUSDT").waiters().isEmpty());

        SymbolTradeCoordinator.EntryPermit waitingSell = coordinator.acquireSell("THEUSDT", "c", "C");
        assertFalse(waitingSell.accepted());
        assertTrue(waitingSell.reason().contains("卖出通道已有 2/2"));
    }

    @Test
    void preservesStrictBuyFifoAcrossRepeatedRetries() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "b", "B").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "c", "C").accepted());

        assertTrue(coordinator.acquireSell("THEUSDT", "a", "A").accepted());
        SymbolTradeCoordinator.EntryPermit cRetry = coordinator.acquire("THEUSDT", "c", "C");
        assertFalse(cRetry.accepted());
        assertTrue(cRetry.reason().contains("BUY FIFO 排队第 2 位"));
        assertTrue(coordinator.acquire("THEUSDT", "b", "B").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "c", "C").accepted());
    }

    @Test
    void preservesStrictSellFifoWhenRestoredSellsExceedConfiguredCapacity() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(3);
        coordinator.claimExistingSell("THEUSDT", "a", "A");
        coordinator.claimExistingSell("THEUSDT", "b", "B");
        coordinator.claimHolding("THEUSDT", "c", "C");
        coordinator.claimHolding("THEUSDT", "d", "D");

        assertFalse(coordinator.acquireSell("THEUSDT", "c", "C").accepted());
        SymbolTradeCoordinator.EntryPermit dWaiting = coordinator.acquireSell("THEUSDT", "d", "D");
        assertFalse(dWaiting.accepted());
        assertTrue(dWaiting.reason().contains("FIFO 排队第 2 位"));

        coordinator.release("THEUSDT", "a");
        assertFalse(coordinator.acquireSell("THEUSDT", "d", "D").accepted());
        assertTrue(coordinator.acquireSell("THEUSDT", "c", "C").accepted());
        coordinator.release("THEUSDT", "b");
        assertTrue(coordinator.acquireSell("THEUSDT", "d", "D").accepted());
    }

    @Test
    void heldDustCanReenterThroughBuyLaneWithoutTakingAnotherTotalSlot() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        coordinator.markHolding("THEUSDT", "a", "A");

        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertEquals(1, coordinator.snapshot("THEUSDT").holders().size());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, holder(coordinator, "a").phase());
    }

    @Test
    void loweringLimitDoesNotCancelExistingSellsOrAllowNewBuyUntilWithinLimit() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(3);
        coordinator.claimExistingSell("THEUSDT", "a", "A");
        coordinator.claimExistingSell("THEUSDT", "b", "B");
        coordinator.claimHolding("THEUSDT", "c", "C");
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);

        SymbolTradeCoordinator.EntryPermit blocked = coordinator.acquire("THEUSDT", "c", "C");
        assertFalse(blocked.accepted());
        assertTrue(blocked.reason().contains("总交易轮次 3/2"));
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "a").phase());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING, holder(coordinator, "b").phase());

        coordinator.release("THEUSDT", "a");
        assertTrue(coordinator.acquire("THEUSDT", "c", "C").accepted());
    }

    @Test
    void releaseRemovesBothWaitingQueues() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);
        assertTrue(coordinator.acquire("THEUSDT", "a", "A").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "b", "B").accepted());
        coordinator.release("THEUSDT", "b");
        assertTrue(coordinator.snapshot("THEUSDT").waiters().isEmpty());

        coordinator.claimExistingSell("THEUSDT", "c", "C");
        coordinator.claimHolding("THEUSDT", "d", "D");
        assertFalse(coordinator.acquireSell("THEUSDT", "d", "D").accepted());
        coordinator.release("THEUSDT", "d");
        assertTrue(coordinator.snapshot("THEUSDT").sellWaiters().isEmpty());
    }

    @Test
    void restoredActiveSellsAreGrandfatheredAboveCurrentLimit() {
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
