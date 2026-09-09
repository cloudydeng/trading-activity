package com.binance.bot.account;

import com.binance.bot.strategy.TradingRiskGuard;
import com.binance.bot.service.BinanceAccountTradeClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AccountRiskCoordinatorTest {
    @Test
    void enforcesOneExposureLimitAcrossPositionsAndPendingBuys() {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        coordinator.register("a::ENSO", () -> risk("10", "0"));
        coordinator.register("a::BTC", () -> risk("0", "0"));

        assertTrue(coordinator.reserveEntry("a::BTC", new BigDecimal("12"),
                new BigDecimal("30"), new BigDecimal("8")).accepted());
        AccountRiskCoordinator.EntryReservation blocked = coordinator.reserveEntry(
                "a::ENSO", new BigDecimal("12"), new BigDecimal("30"), new BigDecimal("8"));

        assertFalse(blocked.accepted());
    }

    @Test
    void deniesNewEntriesWhenCombinedSymbolDrawdownReachesAccountLimit() {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        coordinator.register("a::ENSO", () -> risk("0", "-3"));
        coordinator.register("a::BTC", () -> risk("0", "-5"));

        AccountRiskCoordinator.EntryReservation blocked = coordinator.reserveEntry(
                "a::ENSO", new BigDecimal("1"), new BigDecimal("40"), new BigDecimal("8"));

        assertFalse(blocked.accepted());
    }

    @Test
    void unreadableSymbolRiskFailsClosed() {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        coordinator.register("a::ENSO", () -> { throw new IllegalStateException("broken"); });

        assertFalse(coordinator.reserveEntry("a::BTC", BigDecimal.ONE,
                new BigDecimal("40"), new BigDecimal("8")).accepted());
    }

    @Test
    void sharesOneBnbReadAcrossConcurrentSymbolStarts() {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        BinanceAccountTradeClient tradeClient = mock(BinanceAccountTradeClient.class);
        when(tradeClient.getAssetBalance("BNB")).thenReturn(
                new BinanceAccountTradeClient.AssetBalance("BNB", BigDecimal.ONE,
                        BigDecimal.ZERO, BigDecimal.ONE));
        when(tradeClient.getTickerPrice("BNBUSDT")).thenReturn(new BigDecimal("600"));

        coordinator.refreshBnbBalance(true, tradeClient);
        coordinator.refreshBnbBalance(true, tradeClient);

        verify(tradeClient, times(1)).getAssetBalance("BNB");
        verify(tradeClient, times(1)).getTickerPrice("BNBUSDT");
    }

    @Test
    void failedBnbReadBacksOffInsteadOfRetryingEveryTick() {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        BinanceAccountTradeClient tradeClient = mock(BinanceAccountTradeClient.class);
        when(tradeClient.getAssetBalance("BNB")).thenReturn(null);

        coordinator.refreshBnbBalance(false, tradeClient);
        coordinator.refreshBnbBalance(false, tradeClient);

        verify(tradeClient, times(1)).getAssetBalance("BNB");
        verify(tradeClient, times(0)).getTickerPrice("BNBUSDT");
    }

    @Test
    void sharedQuoteSnapshotPreventsTwoSymbolsFromOverbookingUsdt() throws Exception {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        coordinator.register("a::ENSO", () -> risk("0", "0"));
        coordinator.register("a::BTC", () -> risk("0", "0"));
        coordinator.updateQuoteBalance(new ObjectMapper().readTree(
                "{\"balances\":[{\"asset\":\"USDT\",\"free\":\"20\",\"locked\":\"0\"}]}"), "USDT");

        assertTrue(coordinator.reserveEntry("a::ENSO", new BigDecimal("12"),
                new BigDecimal("40"), new BigDecimal("8")).accepted());
        assertFalse(coordinator.reserveEntry("a::BTC", new BigDecimal("12"),
                new BigDecimal("40"), new BigDecimal("8")).accepted());
    }

    @Test
    void unreconciledConfiguredSymbolFailsClosed() {
        AccountRiskCoordinator coordinator = new AccountRiskCoordinator();
        coordinator.register("a::ENSO", () -> risk("0", "0"), () -> false);

        assertFalse(coordinator.reserveEntry("a::ENSO", BigDecimal.ONE,
                new BigDecimal("40"), new BigDecimal("8")).accepted());
    }

    private TradingRiskGuard.RiskSnapshot risk(String positionCost, String netPnl) {
        return new TradingRiskGuard.RiskSnapshot(null, BigDecimal.ZERO, new BigDecimal(positionCost),
                BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ZERO, new BigDecimal(netPnl),
                BigDecimal.ZERO, -1, LocalDate.now(java.time.ZoneOffset.UTC));
    }
}
