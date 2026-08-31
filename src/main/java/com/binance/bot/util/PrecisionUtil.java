package com.binance.bot.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class PrecisionUtil {

    private PrecisionUtil() {}

    public static BigDecimal roundDownToStep(BigDecimal value, BigDecimal stepSize) {
        if (value == null || stepSize == null || stepSize.compareTo(BigDecimal.ZERO) == 0) {
            return value;
        }
        return value.divide(stepSize, 0, RoundingMode.DOWN).multiply(stepSize);
    }

    public static BigDecimal roundUpToStep(BigDecimal value, BigDecimal stepSize) {
        if (value == null || stepSize == null || stepSize.compareTo(BigDecimal.ZERO) == 0) {
            return value;
        }
        return value.divide(stepSize, 0, RoundingMode.CEILING).multiply(stepSize);
    }
}
