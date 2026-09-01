package com.binance.bot.strategy;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradeAccountingLedgerTest {

    @Test
    void duplicateTradeIdIsIgnoredForInventoryFeeAndVolumeAccounting() {
        TradeAccountingLedger ledger = new TradeAccountingLedger();

        assertTrue(ledger.record(42, 7001, d("10"), d("0.60"), d("6.00"),
                d("0.006"), "USDT", d("0.006")).applied());
        assertFalse(ledger.record(42, 7001, d("10"), d("0.60"), d("6.00"),
                d("0.006"), "USDT", d("0.006")).applied());

        var snapshot = ledger.snapshot();
        assertEquals(0, d("6.00").compareTo(snapshot.totalVolumeQuote()));
        assertEquals(0, d("0.006").compareTo(snapshot.totalCommissionQuoteEquivalent()));
        assertEquals(0, d("1000").compareTo(snapshot.costPerMillionVolume()));
        assertEquals(1, snapshot.processedTradeCount());
    }

    @Test
    void reportsCostPerMillionAsIncompleteWhenThirdAssetCannotBeConverted() {
        TradeAccountingLedger ledger = new TradeAccountingLedger();

        ledger.record(42, 7001, d("10"), d("0.60"), d("6.00"),
                d("0.001"), "BNB", null);

        var snapshot = ledger.snapshot();
        assertFalse(snapshot.commissionConversionComplete());
        assertTrue(snapshot.unconvertedCommissionAssets().contains("BNB"));
        assertNull(snapshot.costPerMillionVolume());
        assertEquals(0, d("0.001").compareTo(snapshot.commissionByAsset().get("BNB")));
    }

    private static BigDecimal d(String value) { return new BigDecimal(value); }
}
