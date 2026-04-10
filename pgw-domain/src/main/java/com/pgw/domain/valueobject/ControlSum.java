package com.pgw.domain.valueobject;

import com.pgw.domain.exception.InvalidControlSumException;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * ISO 20022 control sum — a non-negative decimal rounded to 2 decimal places.
 *
 * <p>Used to verify that the sum of all transaction amounts within a payment
 * information block (or across all blocks in a message) matches the declared
 * header value.</p>
 */
public record ControlSum(BigDecimal value) {

    private static final BigDecimal EPSILON = new BigDecimal("0.005");

    public ControlSum {
        if (value == null || value.compareTo(BigDecimal.ZERO) < 0) {
            throw new InvalidControlSumException(
                    "value must be >= 0, got: " + value);
        }
        value = value.setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * Returns {@code true} if {@code sum} matches this control sum within a
     * half-penny (0.005) epsilon, accommodating floating-point rounding in
     * upstream systems.
     *
     * @param sum the computed transaction total to compare
     * @return true if the difference is within epsilon
     */
    public boolean matches(BigDecimal sum) {
        if (sum == null) {
            return false;
        }
        return value.subtract(sum.setScale(2, RoundingMode.HALF_UP)).abs()
                .compareTo(EPSILON) < 0;
    }
}
