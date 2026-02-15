package com.iso20022.pain.validation;

import com.iso20022.pain.arrow.ArrowBatchResult;

/**
 * Internal implementation for chaining validators together.
 * 
 * <p>This validator executes two validators in sequence and combines their
 * parallelizability status (both must be parallelizable for the chain to be).</p>
 */
final class ChainedValidator implements Validator {

    private final Validator first;
    private final Validator second;

    ChainedValidator(Validator first, Validator second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void validate(ArrowBatchResult result, ValidationContext context) {
        first.validate(result, context);
        second.validate(result, context);
    }

    @Override
    public boolean isParallelizable() {
        return first.isParallelizable() && second.isParallelizable();
    }

    @Override
    public String getName() {
        return first.getName() + " → " + second.getName();
    }
}
