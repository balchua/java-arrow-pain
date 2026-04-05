package com.iso20022.pain.domain.valueobject;

import com.iso20022.pain.domain.exception.InvalidIbanException;

import java.math.BigInteger;

/**
 * International Bank Account Number validated against the MOD-97 checksum algorithm.
 *
 * <p>Validation steps (per ISO 13616-1):
 * <ol>
 *   <li>Move the first 4 characters to the end.</li>
 *   <li>Replace each letter with its numeric equivalent (A=10 … Z=35).</li>
 *   <li>Compute {@code numericValue mod 97}; a valid IBAN yields 1.</li>
 * </ol>
 * </p>
 */
public record Iban(String value) {

    public Iban {
        if (!isValidMod97(value)) {
            throw new InvalidIbanException(value);
        }
    }

    /**
     * Returns {@code true} if the supplied string passes MOD-97 IBAN validation.
     *
     * @param iban the candidate string (may be null)
     * @return true if valid
     */
    public static boolean isValidMod97(String iban) {
        if (iban == null || iban.length() < 5 || iban.length() > 34) {
            return false;
        }
        String rearranged = iban.substring(4) + iban.substring(0, 4);
        StringBuilder digits = new StringBuilder();
        for (char c : rearranged.toCharArray()) {
            if (Character.isLetter(c)) {
                digits.append(Character.toUpperCase(c) - 'A' + 10);
            } else if (Character.isDigit(c)) {
                digits.append(c);
            } else {
                return false;
            }
        }
        return new BigInteger(digits.toString()).mod(BigInteger.valueOf(97)).intValue() == 1;
    }
}
