package com.iso20022.pain.domain.exception;

/**
 * Thrown when a BIC string does not match the SWIFT BIC format.
 */
public class InvalidBicException extends RuntimeException {

    public InvalidBicException(String bic) {
        super("Invalid BIC: " + bic);
    }
}
