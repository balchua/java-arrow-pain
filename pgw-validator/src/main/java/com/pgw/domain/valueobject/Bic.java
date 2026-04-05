package com.pgw.domain.valueobject;

import com.pgw.domain.exception.InvalidBicException;

import java.util.regex.Pattern;

/**
 * SWIFT Business Identifier Code (BIC / ISO 9362).
 *
 * <p>Format: {@code [A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?}
 * — 8 characters (primary office) or 11 characters (branch).</p>
 */
public record Bic(String value) {

    private static final Pattern BIC_PATTERN =
            Pattern.compile("^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$");

    public Bic {
        if (value == null || !BIC_PATTERN.matcher(value).matches()) {
            throw new InvalidBicException(value);
        }
    }
}
