package com.pgw.validation;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe context for collecting validation errors and warnings.
 * 
 * <p>This class uses concurrent collections to support parallel validator execution.
 * Validators add errors and warnings which are aggregated across all threads.</p>
 */
public final class ValidationContext {

    private final ConcurrentLinkedQueue<ValidationError> errors;
    private final ConcurrentLinkedQueue<ValidationWarning> warnings;
    private final Instant startTime;

    /**
     * Represents a validation error.
     *
     * @param validator the name of the validator that detected the error
     * @param message the error message
     * @param details optional additional details about the error
     */
    public record ValidationError(String validator, String message, Object[] details) {
        public ValidationError {
            details = details == null ? new Object[0] : Arrays.copyOf(details, details.length);
        }
    }

    /**
     * Represents a validation warning.
     *
     * @param validator the name of the validator that issued the warning
     * @param message the warning message
     * @param details optional additional details about the warning
     */
    public record ValidationWarning(String validator, String message, Object[] details) {
        public ValidationWarning {
            details = details == null ? new Object[0] : Arrays.copyOf(details, details.length);
        }
    }

    /**
     * Creates a new validation context.
     */
    public ValidationContext() {
        this.errors = new ConcurrentLinkedQueue<>();
        this.warnings = new ConcurrentLinkedQueue<>();
        this.startTime = Instant.now();
    }

    /**
     * Adds an error to this context.
     *
     * @param validatorName the name of the validator reporting the error
     * @param message the error message
     * @param details optional additional details
     */
    public void addError(String validatorName, String message, Object... details) {
        errors.add(new ValidationError(validatorName, message, details));
    }

    /**
     * Adds a warning to this context.
     *
     * @param validatorName the name of the validator reporting the warning
     * @param message the warning message
     * @param details optional additional details
     */
    public void addWarning(String validatorName, String message, Object... details) {
        warnings.add(new ValidationWarning(validatorName, message, details));
    }

    /**
     * Returns all validation errors.
     *
     * @return an immutable list of all errors
     */
    public List<ValidationError> getErrors() {
        return List.copyOf(errors);
    }

    /**
     * Returns all validation warnings.
     *
     * @return an immutable list of all warnings
     */
    public List<ValidationWarning> getWarnings() {
        return List.copyOf(warnings);
    }

    /**
     * Checks if any validation errors have been recorded.
     *
     * @return true if there are errors, false otherwise
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /**
     * Returns the elapsed time in milliseconds since this context was created.
     *
     * @return elapsed milliseconds
     */
    public long getElapsedMillis() {
        return Instant.now().toEpochMilli() - startTime.toEpochMilli();
    }
}
