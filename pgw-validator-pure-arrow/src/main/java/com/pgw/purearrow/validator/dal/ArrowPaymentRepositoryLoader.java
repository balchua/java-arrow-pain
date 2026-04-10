package com.pgw.purearrow.validator.dal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Factory that loads three Arrow IPC stream files into an
 * {@link ArrowPaymentRepositoryImpl}.
 *
 * <p>This is the sole entry point for creating an Arrow-backed
 * {@link com.pgw.dal.PaymentRepository}. Internally it delegates to
 * {@link ArrowTableLoader} to materialise each table's record batches and then
 * wraps them in the appropriate typed table class.</p>
 *
 * <pre>
 *   .arrow files on disk
 *        ↓  [ArrowTableLoader — ArrowStreamReader per table]
 *   ArrowMessageTable + ArrowRemittanceTable + ArrowTransactionTable
 *        ↓
 *   ArrowPaymentRepositoryImpl (implements PaymentRepository)
 * </pre>
 *
 * <p>The caller owns the lifecycle of the returned repository and must call
 * {@link ArrowPaymentRepositoryImpl#close()} when done to release all Arrow
 * off-heap memory.</p>
 */
public final class ArrowPaymentRepositoryLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowPaymentRepositoryLoader.class);

    private ArrowPaymentRepositoryLoader() {}

    /**
     * Loads the three Arrow IPC stream files into an in-memory Arrow-backed
     * repository.
     *
     * @param messageFile     path to the message Arrow IPC stream file
     * @param remittanceFile  path to the remittance Arrow IPC stream file
     * @param transactionFile path to the transaction Arrow IPC stream file
     * @param allocator       Arrow buffer allocator; all off-heap memory for the
     *                        loaded batches is allocated from this allocator
     * @return a fully loaded, ready-to-query {@link ArrowPaymentRepositoryImpl}
     * @throws IOException if any file cannot be read or is malformed
     */
    public static ArrowPaymentRepositoryImpl load(
            Path messageFile,
            Path remittanceFile,
            Path transactionFile,
            BufferAllocator allocator) throws IOException {

        long start = System.currentTimeMillis();

        List<VectorSchemaRoot> msgBatches = ArrowTableLoader.loadBatches(messageFile,     allocator);
        List<VectorSchemaRoot> rmtBatches = ArrowTableLoader.loadBatches(remittanceFile,  allocator);
        List<VectorSchemaRoot> txBatches  = ArrowTableLoader.loadBatches(transactionFile, allocator);

        ArrowMessageTable     msgTable = new ArrowMessageTable(msgBatches);
        ArrowRemittanceTable  rmtTable = new ArrowRemittanceTable(rmtBatches);
        ArrowTransactionTable txTable  = new ArrowTransactionTable(txBatches);

        long elapsed = System.currentTimeMillis() - start;
        LOG.debug("ArrowPaymentRepositoryLoader loaded {} msg, {} rmt, {} tx rows in {} ms",
                msgTable.getRowCount(), rmtTable.getRowCount(), txTable.getRowCount(), elapsed);

        return new ArrowPaymentRepositoryImpl(msgTable, rmtTable, txTable);
    }
}
