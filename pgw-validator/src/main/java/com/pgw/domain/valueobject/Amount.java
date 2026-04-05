package com.pgw.domain.valueobject;

import com.pgw.domain.exception.InvalidAmountException;

import java.math.BigDecimal;

/**
 * A positive monetary amount paired with its ISO 4217 currency.
 *
 * <p>Invariants enforced at construction time:
 * <ul>
 *   <li>{@code value} must be strictly greater than zero.</li>
 *   <li>{@code currency} must be a valid {@link Currency} (3 uppercase letters).</li>
 * </ul>
 * </p>
 */
public record Amount(BigDecimal value, Currency currency) {

    public Amount {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidAmountException(
                    "value must be > 0, got: " + value);
        }
        if (currency == null) {
            throw new InvalidAmountException("currency must not be null");
        }
    }

    /**
     * Adds another amount to this one.
     *
     * @param other the amount to add; must use the same currency
     * @return a new {@code Amount} with the sum
     * @throws IllegalArgumentException if currencies differ
     */
    public Amount add(Amount other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException(
                    "Cannot add amounts in different currencies: "
                            + this.currency.code() + " vs " + other.currency.code());
        }
        return new Amount(this.value.add(other.value), this.currency);
    }
}
