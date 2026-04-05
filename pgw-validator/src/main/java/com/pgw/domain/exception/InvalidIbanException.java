package com.pgw.domain.exception;

/**
 * Thrown when an IBAN string fails MOD-97 checksum validation.
 */
public class InvalidIbanException extends RuntimeException {

    public InvalidIbanException(String iban) {
        super("Invalid IBAN: " + iban);
    }
}
