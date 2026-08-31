package com.binance.bot.util;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrecisionUtilTest {

    @Test
    void roundsToAnActualStepMultiple() {
        assertEquals(new BigDecimal("1.00"), PrecisionUtil.roundDownToStep(new BigDecimal("1.24"), new BigDecimal("0.25")));
        assertEquals(new BigDecimal("1.20"), PrecisionUtil.roundDownToStep(new BigDecimal("1.23"), new BigDecimal("0.05")));
    }

    @Test
    void roundsUpWithoutDroppingBelowTarget() {
        assertEquals(new BigDecimal("0.885"), PrecisionUtil.roundUpToStep(
                new BigDecimal("0.884206"), new BigDecimal("0.001")));
    }
}
