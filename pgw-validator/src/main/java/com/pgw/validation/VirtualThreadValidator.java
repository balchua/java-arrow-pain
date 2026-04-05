package com.pgw.validation;

import com.pgw.dal.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Abstract base class for validators that optionally use virtual threads.
 *
 * <p>Subclasses implement {@link #doValidate(PaymentRepository, ValidationContext)}
 * to perform the actual SQL-based validation.</p>
 */
public abstract class VirtualThreadValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualThreadValidator.class);

    /**
     * Performs the actual validation using the repository.
     *
     * @param repository the SQL-based DAL
     * @param context    the validation context
     */
    protected abstract void doValidate(PaymentRepository repository, ValidationContext context);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        runWithVirtualThread(repository, context);
    }

    private void runWithVirtualThread(PaymentRepository repository, ValidationContext context) {
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
