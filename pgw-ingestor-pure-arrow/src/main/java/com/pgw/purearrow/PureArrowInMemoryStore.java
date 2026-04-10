package com.pgw.purearrow;

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds references to the Arrow record batches produced by a pure-Arrow ingest.
 * One list of {@link ArrowRecordBatch}es is kept per table type (message, remittance,
 * transaction). Close this store to release all Arrow off-heap memory.
 */
public final class PureArrowInMemoryStore implements AutoCloseable {

    private final List<ArrowRecordBatch> messageBatches = new ArrayList<>();
    private final List<ArrowRecordBatch> remittanceBatches = new ArrayList<>();
    private final List<ArrowRecordBatch> transactionBatches = new ArrayList<>();

    private long messageRowCount;
    private long remittanceRowCount;
    private long transactionRowCount;

    PureArrowInMemoryStore() {}

    void addMessageBatch(ArrowRecordBatch batch, int rowCount) {
        messageBatches.add(batch);
        messageRowCount += rowCount;
    }

    void addRemittanceBatch(ArrowRecordBatch batch, int rowCount) {
        remittanceBatches.add(batch);
        remittanceRowCount += rowCount;
    }

    void addTransactionBatch(ArrowRecordBatch batch, int rowCount) {
        transactionBatches.add(batch);
        transactionRowCount += rowCount;
    }

    /** Total number of message rows accumulated across all batches. */
    public long getMessageRowCount() {
        return messageRowCount;
    }

    /** Total number of remittance rows accumulated across all batches. */
    public long getRemittanceRowCount() {
        return remittanceRowCount;
    }

    /** Total number of transaction rows accumulated across all batches. */
    public long getTransactionRowCount() {
        return transactionRowCount;
    }

    /**
     * Releases all {@link ArrowRecordBatch}es and their off-heap Arrow memory.
     * After this call the store is empty and row counts remain accessible but the
     * underlying buffers are freed.
     */
    @Override
    public void close() {
        for (ArrowRecordBatch b : messageBatches) b.close();
        messageBatches.clear();
        for (ArrowRecordBatch b : remittanceBatches) b.close();
        remittanceBatches.clear();
        for (ArrowRecordBatch b : transactionBatches) b.close();
        transactionBatches.clear();
    }
}
