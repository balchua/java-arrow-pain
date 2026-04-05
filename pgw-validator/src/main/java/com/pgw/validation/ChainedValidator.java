package com.pgw.validation;

import com.pgw.dal.PaymentRepository;

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
    public void validate(PaymentRepository repository, ValidationContext context) {
        first.validate(repository, context);
        second.validate(repository, context);
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
