package com.iso20022.pain.validation;

import com.iso20022.pain.arrow.ArrowBatchResult;

/**
 * Base interface for all validators in the chainable validation framework.
 * 
 * <p>Validators can be chained together using {@link #andThen(Validator)} to create
 * validation pipelines. Each validator operates on Arrow data without copying it to
 * intermediate POJOs, maintaining memory efficiency.</p>
 */
public interface Validator {

    /**
     * Validates the Arrow batch result and reports errors/warnings to the context.
     *
     * @param result the Arrow batch result containing message, remittance, and transaction data
     * @param context the validation context for collecting errors and warnings
     */
    void validate(ArrowBatchResult result, ValidationContext context);

    /**
     * Indicates whether this validator can be run in parallel with other parallelizable validators.
     * 
     * @return true if this validator is safe to run in parallel, false if it must run sequentially
     */
    default boolean isParallelizable() {
        return true;
    }

    /**
     * Returns the name of this validator for error reporting.
     *
     * @return a descriptive name for this validator
     */
    String getName();

    /**
     * Chains this validator with another validator to be executed after this one completes.
     *
     * @param next the validator to execute after this one
     * @return a new chained validator that executes both in sequence
     */
    default Validator andThen(Validator next) {
        return new ChainedValidator(this, next);
    }
}
