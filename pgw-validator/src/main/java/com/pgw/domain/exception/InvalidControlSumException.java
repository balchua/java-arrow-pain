package com.pgw.domain.exception;

/**
 * Thrown when a control sum value is negative.
 */
public class InvalidControlSumException extends RuntimeException {

    public InvalidControlSumException(String detail) {
        super("Invalid control sum: " + detail);
    }
}
