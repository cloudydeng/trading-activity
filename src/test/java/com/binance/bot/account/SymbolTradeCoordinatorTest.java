package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.strategy.DailyTradeStatsStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolTradeCoordinatorTest {
    @TempDir Path tempDir;

    @Test
    void priceGapWaitExpiresAtOneHourAndSurvivesRestartButResetsOnReentry() {
        BinanceProperties properties = new BinanceProperties();
        properties.getStorage().setDailyStatsDb(tempDir.resolve("daily.db").toString());
        DailyTradeStatsStore store = new DailyTradeStatsStore(properties);
        long now = System.currentTimeMillis();
        assertEquals(DailyTradeStatsStore.RecordResult.APPLIED, store.recordTrade(
                "account-a", "A", "BABYUSDT", 1, 11, "BUY", BigDecimal.ONE,
                BigDecimal.ONE, new BigDecimal("0.6000"), new BigDecimal("0.6000"),
                BigDecimal.ZERO, "USDT", BigDecimal.ZERO, BigDecimal.ZERO, now - 1_000L));
        SymbolTradeCoordinator first = new SymbolTradeCoordinator(store);
        var reference = store.latestBuyFill("BABYUSDT").orElseThrow();
        first.restoreBuyFill("BABYUSDT", reference.price(), reference.tradeTimeMs());

        assertFalse(first.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.6031"), now).allowed());
        assertEquals(now, store.loadBuyPriceGapWaitStartedAt("BABYUSDT").orElseThrow());
        assertFalse(first.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.5969"),
                now + 3_599_999L).allowed());

        SymbolTradeCoordinator restarted = new SymbolTradeCoordinator(store);
        restarted.restoreBuyFill("BABYUSDT", reference.price(), reference.tradeTimeMs());
        assertTrue(restarted.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.5969"),
                now + 3_600_000L).allowed());
        assertTrue(restarted.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.6030"),
                now + 3_600_001L).allowed());
        assertTrue(store.loadBuyPriceGapWaitStartedAt("BABYUSDT").isEmpty());
        assertFalse(restarted.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.6031"),
                now + 3_600_002L).allowed());
        assertEquals(now + 3_600_002L,
                store.loadBuyPriceGapWaitStartedAt("BABYUSDT").orElseThrow());
        store.close();
    }

    @Test
    void crossAccountBuyFillBlocksBothPriceDirectionsOnlyBeyondHalfPercent() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        assertTrue(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01330")).allowed());
        coordinator.recordBuyFill("babyusdt", new BigDecimal("0.01340"), 1_000L);

        assertTrue(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01333300")).allowed());
        assertTrue(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01346700")).allowed());
        assertFalse(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01333299")).allowed());
        assertFalse(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01346701")).allowed());
        assertTrue(coordinator.checkBuyPriceGap("VTHOUSDT", new BigDecimal("0.00100")).allowed());

        coordinator.recordBuyFill("BABYUSDT", new BigDecimal("0.01200"), 999L);
        assertFalse(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01333299")).allowed());
        coordinator.recordBuyFill("BABYUSDT", new BigDecimal("0.01333"), 1_001L);
        assertTrue(coordinator.checkBuyPriceGap("BABYUSDT", new BigDecimal("0.01333299")).allowed());
    }

    @Test
    void spacesDifferentAccountsBuySubmissionsPerSymbolWithoutDelayingSameAccount() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();

        assertTrue(coordinator.reserveBuySubmission("ensousdt", "account-a", 10_000L).allowed());
        assertTrue(coordinator.reserveBuySubmission("ENSOUSDT", "account-a", 10_500L).allowed());
        SymbolTradeCoordinator.BuyPacePermit early = coordinator.reserveBuySubmission(
                "ENSOUSDT", "account-b", 11_000L);
        assertFalse(early.allowed());
        assertEquals(1_500L, early.retryAfterMs());
        assertTrue(early.reason().contains("2 秒"));
        assertTrue(coordinator.reserveBuySubmission("BTCUSDT", "account-b", 11_000L).allowed());
        assertTrue(coordinator.reserveBuySubmission("ENSOUSDT", "account-b", 12_500L).allowed());
        assertFalse(coordinator.reserveBuySubmission("ENSOUSDT", "account-a", 13_000L).allowed());
        assertTrue(coordinator.reserveBuySubmission("ENSOUSDT", "account-a", 14_500L).allowed());
    }

    @Test
    void newBuyWaitsAfterAnotherAccountSellsOrFinishesItsCycle() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        assertTrue(coordinator.reserveBuySubmission("ENSOUSDT", "account-a", 10_000L).allowed());

        coordinator.noteAccountActivity("ENSOUSDT", "account-a", 30_000L); // SELL submission
        assertEquals(1_500L, coordinator.reserveBuySubmission(
                "ENSOUSDT", "account-b", 30_500L).retryAfterMs());

        coordinator.noteAccountActivity("ENSOUSDT", "account-a", 40_000L); // flat cycle completion
        assertEquals(1_000L, coordinator.reserveBuySubmission(
                "ENSOUSDT", "account-b", 41_000L).retryAfterMs());
        assertTrue(coordinator.reserveBuySubmission("ENSOUSDT", "account-b", 42_000L).allowed());
    }

    @Test
    void concurrencyTwoAllowsTwoCompleteCyclesAndBlocksThirdUntilOneCloses() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);

        assertTrue(coordinator.acquire("bnbusdt", "a", "A").accepted());
        SymbolTradeCoordinator.EntryPermit simultaneousBuy = coordinator.acquire("BNBUSDT", "b", "B");
        assertFalse(simultaneousBuy.accepted());
        assertTrue(simultaneousBuy.reason().contains("一次只允许一张买单"));
        assertTrue(coordinator.transitionToSell("BNBUSDT", "a", "A").accepted());
        assertTrue(coordinator.acquire("BNBUSDT", "b", "B").accepted());
        SymbolTradeCoordinator.EntryPermit third = coordinator.acquire("BNBUSDT", "c", "C");

        assertFalse(third.accepted());
        assertTrue(third.reason().contains("已有 2/2 个账户交易中"));
        assertTrue(third.reason().contains("FIFO 排队第 1 位"));

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

        for (int index = 1; index <= 4; index++) {
            SymbolTradeCoordinator.EntryPermit permit = coordinator.acquire(
                    "THEUSDT", "engine-" + index, "Account " + index);
            assertTrue(permit.accepted());
            assertTrue(coordinator.transitionToSell("THEUSDT", "engine-" + index,
                    "Account " + index).accepted());
        }
        for (int index = 5; index <= 10; index++) {
            assertFalse(coordinator.acquire("THEUSDT", "engine-" + index,
                    "Account " + index).accepted());
        }
        assertEquals(4, coordinator.snapshot("THEUSDT").holders().size());
        assertEquals(6, coordinator.snapshot("THEUSDT").waiters().size());

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
    void perSymbolLimitsOverrideTheGlobalDefaultAndZeroPausesOnlyThatSymbol() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        coordinator.configureMaxConcurrentEntriesBySymbol(Map.of("REZUSDT", 0, "THEUSDT", 2));

        SymbolTradeCoordinator.EntryPermit rez = coordinator.acquire("rezusdt", "rez-a", "REZ A");
        assertFalse(rez.accepted());
        assertTrue(rez.reason().contains("并发设置为 0，暂停新买入"));
        assertTrue(coordinator.acquire("THEUSDT", "the-a", "THE A").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "the-b", "THE B").accepted());
        assertTrue(coordinator.transitionToSell("THEUSDT", "the-a", "THE A").accepted());
        assertTrue(coordinator.acquire("THEUSDT", "the-b", "THE B").accepted());
        assertFalse(coordinator.acquire("THEUSDT", "the-c", "THE C").accepted());
        assertTrue(coordinator.acquire("HOLOUSDT", "holo-a", "HOLO A").accepted());
        assertFalse(coordinator.acquire("HOLOUSDT", "holo-b", "HOLO B").accepted());
        assertEquals(0, coordinator.maxConcurrentEntriesPerSymbol("REZUSDT"));
        assertEquals(2, coordinator.maxConcurrentEntriesPerSymbol("THEUSDT"));
        assertEquals(1, coordinator.maxConcurrentEntriesPerSymbol("HOLOUSDT"));
    }

    @Test
    void settingSymbolToZeroDoesNotPreventAnExistingCycleFromTransitioningToSell() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("REZUSDT", "a", "A").accepted());

        coordinator.configureMaxConcurrentEntriesBySymbol(Map.of("REZUSDT", 0));

        assertTrue(coordinator.transitionToSell("REZUSDT", "a", "A").accepted());
        assertEquals(SymbolTradeCoordinator.Phase.SELLING,
                coordinator.snapshot("REZUSDT").holders().getFirst().phase());
        assertFalse(coordinator.acquire("REZUSDT", "b", "B").accepted());
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
    void heldDustReentersAheadOfWaitersBecauseItAlreadyOwnsTheCycleSlot() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(1);
        assertTrue(coordinator.acquire("HOLOUSDT", "dust-holder", "Dust Holder").accepted());
        coordinator.markHolding("HOLOUSDT", "dust-holder", "Dust Holder");
        assertFalse(coordinator.acquire("HOLOUSDT", "waiting", "Waiting").accepted());

        SymbolTradeCoordinator.EntryPermit resumed = coordinator.acquire(
                "HOLOUSDT", "dust-holder", "Dust Holder");

        assertTrue(resumed.accepted());
        SymbolTradeCoordinator.Snapshot snapshot = coordinator.snapshot("HOLOUSDT");
        assertEquals(1, snapshot.holders().size());
        assertEquals(SymbolTradeCoordinator.Phase.BUYING, snapshot.holders().getFirst().phase());
        assertEquals(List.of("waiting"), snapshot.waiters().stream()
                .map(SymbolTradeCoordinator.Waiter::engineId)
                .toList());
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

    @Test
    void onlyConfirmedSellOrdersEnableFourthBidAndClearingOneRetainsOtherSells() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(3);
        coordinator.claimExistingSell("ENSOUSDT", "a", "A");
        assertFalse(coordinator.hasActiveSellOrder("ENSOUSDT"));

        coordinator.markActiveSellOrder("ENSOUSDT", "a", 101L);
        coordinator.markActiveSellOrder("ENSOUSDT", "b", 102L);
        assertTrue(coordinator.hasActiveSellOrder("ensousdt"));
        coordinator.clearActiveSellOrder("ENSOUSDT", "a", 100L);
        assertTrue(coordinator.hasActiveSellOrder("ENSOUSDT"));
        coordinator.clearActiveSellOrder("ENSOUSDT", "a", 101L);
        assertTrue(coordinator.hasActiveSellOrder("ENSOUSDT"));
        coordinator.release("ENSOUSDT", "b");
        assertFalse(coordinator.hasActiveSellOrder("ENSOUSDT"));
    }

    @Test
    void heldDustCannotStartSecondBuyWhileAnotherAccountIsBuying() {
        SymbolTradeCoordinator coordinator = new SymbolTradeCoordinator();
        coordinator.configureMaxConcurrentEntriesPerSymbol(2);
        coordinator.claimHolding("HOLOUSDT", "dust", "Dust");
        assertTrue(coordinator.acquire("HOLOUSDT", "other", "Other").accepted());
        assertFalse(coordinator.acquire("HOLOUSDT", "dust", "Dust").accepted());
        assertEquals(SymbolTradeCoordinator.Phase.HOLDING,
                coordinator.snapshot("HOLOUSDT").holders().getFirst().phase());
        assertTrue(coordinator.transitionToSell("HOLOUSDT", "other", "Other").accepted());
        assertTrue(coordinator.acquire("HOLOUSDT", "dust", "Dust").accepted());
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
