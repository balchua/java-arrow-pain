package com.iso20022.pain.domain.valueobject;

import com.iso20022.pain.domain.exception.InvalidCurrencyException;

import java.util.regex.Pattern;

/**
 * ISO 4217 currency code — exactly 3 uppercase ASCII letters.
 */
public record Currency(String code) {

    private static final Pattern ISO_4217 = Pattern.compile("[A-Z]{3}");

    public Currency {
        if (code == null || !ISO_4217.matcher(code).matches()) {
            throw new InvalidCurrencyException(code);
        }
    }
}
