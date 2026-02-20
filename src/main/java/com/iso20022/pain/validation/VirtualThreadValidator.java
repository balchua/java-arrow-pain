package com.iso20022.pain.validation;

import com.iso20022.pain.dal.Pain001Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Abstract base class for validators that optionally use virtual threads.
 *
 * <p>Subclasses implement {@link #doValidate(Pain001Repository, ValidationContext)}
 * to perform the actual SQL-based validation. DuckDB's vectorised query engine
 * handles internal parallelism, so this class simply provides a hook for
 * running the validation on a virtual thread when desired.</p>
 */
public abstract class VirtualThreadValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualThreadValidator.class);

    /**
     * Performs the actual validation using the repository.
     *
     * @param repository the SQL-based DAL
     * @param context    the validation context
     */
    protected abstract void doValidate(Pain001Repository repository, ValidationContext context);

    @Override
    public void validate(Pain001Repository repository, ValidationContext context) {
        runWithVirtualThread(repository, context);
    }

    private void runWithVirtualThread(Pain001Repository repository, ValidationContext context) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> future = executor.submit(() -> doValidate(repository, context));
            try {
                future.get();
            } catch (Exception e) {
                LOG.error("Virtual-thread validation failed: {}", e.getMessage(), e);
                context.addError(getName(), "Validation failed with exception", e.getMessage());
            }
        }
    }
}
