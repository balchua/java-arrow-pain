package com.pgw.domain.exception;

/**
 * Thrown when a currency code does not conform to the ISO 4217 format (3 uppercase letters).
 */
public class InvalidCurrencyException extends RuntimeException {

    public InvalidCurrencyException(String code) {
        super("Invalid currency code: " + code);
    }
}
