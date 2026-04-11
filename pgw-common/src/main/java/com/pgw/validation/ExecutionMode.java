package com.pgw.validation;

/**
 * Execution mode for validation pipelines.
 */
public enum ExecutionMode {
    /**
     * Run all validators sequentially in the order they were added.
     */
    SEQUENTIAL,

    /**
     * Run parallelizable validators concurrently, sequential validators in order.
     */
    PARALLEL,

    /**
     * Automatically choose PARALLEL if multiple parallelizable validators exist,
     * otherwise SEQUENTIAL.
     */
    AUTO
}
