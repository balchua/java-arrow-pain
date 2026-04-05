package com.pgw.domain.exception;

/**
 * Thrown when a monetary amount is zero or negative.
 */
public class InvalidAmountException extends RuntimeException {

    public InvalidAmountException(String detail) {
        super("Invalid amount: " + detail);
    }
}
