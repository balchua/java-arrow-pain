package com.iso20022.pain.validation;

import com.iso20022.pain.arrow.ArrowBatchResult;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Abstract base class for validators that use virtual threads for batch-level parallelism.
 * 
 * <p>Subclasses define how to extract batches and validate each batch. This class
 * handles the parallel execution using virtual threads when available.</p>
 */
public abstract class VirtualThreadValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualThreadValidator.class);

    /**
     * Extracts batches from the Arrow result for parallel processing.
     *
     * @param result the Arrow batch result
     * @return list of batches to process in parallel
     */
    protected abstract List<VectorSchemaRoot> getBatches(ArrowBatchResult result);

    /**
     * Validates a single batch.
     *
     * @param batch the batch to validate
     * @param batchIndex the index of this batch
     * @param context the validation context
     */
    protected abstract void validateBatch(VectorSchemaRoot batch, int batchIndex, ValidationContext context);

    @Override
    public void validate(ArrowBatchResult result, ValidationContext context) {
        List<VectorSchemaRoot> batches = getBatches(result);
        
        if (batches.isEmpty()) {
            return;
        }

        if (batches.size() == 1) {
            // Single batch, no need for parallelism
            validateBatch(batches.get(0), 0, context);
            return;
        }

        // Use virtual threads if available
        if (isVirtualThreadsSupported()) {
            executeWithVirtualThreads(batches, context);
        } else {
            executeWithPlatformThreads(batches, context);
        }
    }

    private void executeWithVirtualThreads(List<VectorSchemaRoot> batches, ValidationContext context) {
        try (ExecutorService executor = createVirtualThreadExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            
            for (int i = 0; i < batches.size(); i++) {
                final int batchIndex = i;
                final VectorSchemaRoot batch = batches.get(i);
                Future<?> future = executor.submit(() -> {
                    validateBatch(batch, batchIndex, context);
                });
                futures.add(future);
            }

            // Wait for all to complete
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.error("Batch validation failed: {}", e.getMessage(), e);
                    context.addError(getName(), "Batch validation failed", e.getMessage());
                }
            }
        }
    }

    private void executeWithPlatformThreads(List<VectorSchemaRoot> batches, ValidationContext context) {
        try (ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(batches.size(), Runtime.getRuntime().availableProcessors()))) {
            
            List<Future<?>> futures = new ArrayList<>();
            
            for (int i = 0; i < batches.size(); i++) {
                final int batchIndex = i;
                final VectorSchemaRoot batch = batches.get(i);
                Future<?> future = executor.submit(() -> {
                    validateBatch(batch, batchIndex, context);
                });
                futures.add(future);
            }

            // Wait for all to complete
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.error("Batch validation failed: {}", e.getMessage(), e);
                    context.addError(getName(), "Batch validation failed", e.getMessage());
                }
            }
        }
    }

    private static boolean isVirtualThreadsSupported() {
        try {
            Thread.class.getMethod("ofVirtual");
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static ExecutorService createVirtualThreadExecutor() {
        try {
            Method method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) method.invoke(null);
        } catch (Exception e) {
            LOG.warn("Failed to create virtual thread executor, falling back to platform threads");
            return Executors.newCachedThreadPool();
        }
    }
}
