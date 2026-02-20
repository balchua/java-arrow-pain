package com.iso20022.pain.validation;

import com.iso20022.pain.dal.Pain001Repository;
import com.iso20022.pain.validation.validators.ControlSumValidator;
import com.iso20022.pain.validation.validators.MessageValidator;
import com.iso20022.pain.validation.validators.RemittanceValidator;
import com.iso20022.pain.validation.validators.TransactionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Fluent builder API for composing and executing validation pipelines.
 *
 * <p>Supports parallel execution of independent validators using virtual threads
 * on Java 21+ with automatic fallback to platform threads on older versions.</p>
 */
public final class ValidationPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ValidationPipeline.class);

    private final List<Validator> validators;
    private ExecutionMode executionMode;
    private boolean useVirtualThreads;

    private ValidationPipeline() {
        this.validators = new ArrayList<>();
        this.executionMode = ExecutionMode.AUTO;
        this.useVirtualThreads = isVirtualThreadsSupported();
    }

    /**
     * Creates a new empty validation pipeline.
     *
     * @return a new pipeline
     */
    public static ValidationPipeline create() {
        return new ValidationPipeline();
    }

    /**
     * Creates a new validation pipeline starting with the given validator.
     *
     * @param validator the first validator
     * @return a new pipeline with the validator added
     */
    public static ValidationPipeline startWith(Validator validator) {
        return new ValidationPipeline().add(validator);
    }

    /**
     * Creates a standard validation pipeline with all default validators.
     *
     * @return a pipeline with MessageValidator, RemittanceValidator,
     *         TransactionValidator, and ControlSumValidator
     */
    public static ValidationPipeline standard() {
        return new ValidationPipeline()
                .add(new MessageValidator())
                .add(new RemittanceValidator())
                .add(new TransactionValidator())
                .add(new ControlSumValidator());
    }

    /**
     * Adds a validator to this pipeline.
     *
     * @param validator the validator to add
     * @return this pipeline for chaining
     */
    public ValidationPipeline add(Validator validator) {
        validators.add(validator);
        return this;
    }

    /**
     * Adds multiple validators to run in parallel.
     *
     * @param parallelValidators validators to add
     * @return this pipeline for chaining
     */
    public ValidationPipeline addParallel(Validator... parallelValidators) {
        for (Validator v : parallelValidators) {
            validators.add(v);
        }
        return this;
    }

    /**
     * Sets the execution mode.
     *
     * @param mode the execution mode
     * @return this pipeline for chaining
     */
    public ValidationPipeline withExecutionMode(ExecutionMode mode) {
        this.executionMode = mode;
        return this;
    }

    /**
     * Configures whether to use virtual threads (if available).
     *
     * @param enabled true to use virtual threads, false to use platform threads
     * @return this pipeline for chaining
     */
    public ValidationPipeline withVirtualThreads(boolean enabled) {
        this.useVirtualThreads = enabled && isVirtualThreadsSupported();
        return this;
    }

    /**
     * Executes the validation pipeline using the given repository.
     *
     * @param repository the SQL-based DAL providing access to the pain.001 tables
     * @return the validation context with all errors and warnings
     */
    public ValidationContext execute(Pain001Repository repository) {
        ValidationContext context = new ValidationContext();
        Instant start = Instant.now();

        // Determine actual execution mode
        ExecutionMode actualMode = determineExecutionMode();

        // Separate parallelizable and sequential validators
        List<Validator> parallelizable = new ArrayList<>();
        List<Validator> sequential = new ArrayList<>();

        for (Validator v : validators) {
            if (v.isParallelizable()) {
                parallelizable.add(v);
            } else {
                sequential.add(v);
            }
        }

        LOG.info("Executing {} validator(s) in {} mode{}",
                validators.size(),
                actualMode,
                useVirtualThreads ? " (virtual threads)" : " (platform threads)");

        try {
            if (actualMode == ExecutionMode.PARALLEL && parallelizable.size() > 1) {
                // Execute parallelizable validators concurrently
                if (useVirtualThreads) {
                    executeWithVirtualThreads(repository, context, parallelizable);
                } else {
                    executeWithPlatformThreads(repository, context, parallelizable);
                }
            } else {
                // Execute all parallelizable validators sequentially
                for (Validator v : parallelizable) {
                    executeValidator(v, repository, context);
                }
            }

            // Always execute sequential validators after parallel ones complete
            for (Validator v : sequential) {
                executeValidator(v, repository, context);
            }

        } catch (Exception e) {
            LOG.error("Validation pipeline failed: {}", e.getMessage(), e);
            context.addError("ValidationPipeline", "Pipeline execution failed", e.getMessage());
        }

        long elapsed = Instant.now().toEpochMilli() - start.toEpochMilli();
        LOG.info("Validation completed in {} ms", elapsed);

        return context;
    }

    private ExecutionMode determineExecutionMode() {
        if (executionMode == ExecutionMode.AUTO) {
            long parallelizableCount = validators.stream()
                    .filter(Validator::isParallelizable)
                    .count();
            return parallelizableCount > 1 ? ExecutionMode.PARALLEL : ExecutionMode.SEQUENTIAL;
        }
        return executionMode;
    }

    private void executeWithVirtualThreads(Pain001Repository repository, ValidationContext context,
            List<Validator> validators) {
        LOG.debug("Using virtual threads for {} parallel validators", validators.size());

        ExecutorService executor = createVirtualThreadExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>();

            for (Validator v : validators) {
                Future<?> future = executor.submit(() -> {
                    executeValidator(v, repository, context);
                });
                futures.add(future);
            }

            // Wait for all to complete
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.error("Validator execution failed: {}", e.getMessage(), e);
                    context.addError("ValidationPipeline", "Validator failed", e.getMessage());
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    private void executeWithPlatformThreads(Pain001Repository repository, ValidationContext context,
            List<Validator> validators) {
        LOG.debug("Using platform threads for {} parallel validators", validators.size());

        ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(validators.size(), Runtime.getRuntime().availableProcessors()));
        try {
            List<Future<?>> futures = new ArrayList<>();

            for (Validator v : validators) {
                Future<?> future = executor.submit(() -> {
                    executeValidator(v, repository, context);
                });
                futures.add(future);
            }

            // Wait for all to complete
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.error("Validator execution failed: {}", e.getMessage(), e);
                    context.addError("ValidationPipeline", "Validator failed", e.getMessage());
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    private void executeValidator(Validator validator, Pain001Repository repository,
            ValidationContext context) {
        Instant start = Instant.now();
        try {
            validator.validate(repository, context);
            long elapsed = Instant.now().toEpochMilli() - start.toEpochMilli();
            LOG.debug("  ✓ {} completed in {} ms", validator.getName(), elapsed);
        } catch (Exception e) {
            LOG.error("  ✗ {} failed: {}", validator.getName(), e.getMessage(), e);
            context.addError(validator.getName(), "Validation failed with exception", e.getMessage());
        }
    }

    private static boolean isVirtualThreadsSupported() {
        try {
            // Check if we're running on Java 21+
            Method ofVirtualMethod = Thread.class.getMethod("ofVirtual");
            LOG.debug("Virtual threads are supported (Java 21+)");
            return true;
        } catch (NoSuchMethodException e) {
            LOG.debug("Virtual threads not supported (Java < 21)");
            return false;
        }
    }

    private static ExecutorService createVirtualThreadExecutor() {
        try {
            // Use reflection to call Executors.newVirtualThreadPerTaskExecutor()
            Method method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) method.invoke(null);
        } catch (Exception e) {
            LOG.warn("Failed to create virtual thread executor, falling back to platform threads: {}",
                    e.getMessage());
            return Executors.newCachedThreadPool();
        }
    }
}
