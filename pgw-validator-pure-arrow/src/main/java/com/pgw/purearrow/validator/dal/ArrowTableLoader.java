package com.pgw.purearrow.validator.dal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads an Arrow IPC stream file from disk and materialises its record batches
 * as independent {@link VectorSchemaRoot} instances.
 *
 * <p>Each call to {@link #loadBatches} returns one {@code VectorSchemaRoot} per
 * batch found in the file. The roots are independent copies — the reader is
 * closed after loading so the caller owns all returned roots and is responsible
 * for closing them to release off-heap Arrow memory.</p>
 *
 * <p>This class is stateless; every method is static and thread-safe.</p>
 */
public final class ArrowTableLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowTableLoader.class);

    private ArrowTableLoader() {}

    /**
     * Loads all record batches from the Arrow IPC stream file at {@code path}.
     *
     * <p>Each returned {@link VectorSchemaRoot} owns its off-heap buffers via a zero-copy
     * {@link org.apache.arrow.vector.util.TransferPair#transfer()} from the reader's internal
     * root — no data bytes are copied.  The caller MUST close every root when done to avoid
     * off-heap memory leaks.</p>
     *
     * @param path      Arrow IPC stream file (written by {@code ArrowStreamWriter})
     * @param allocator Arrow buffer allocator — child allocators are NOT created;
     *                  all buffers are allocated directly from this allocator
     * @return list of materialised record batches (may be empty for an empty file)
     * @throws IOException if the file cannot be read or is malformed
     */
    public static List<VectorSchemaRoot> loadBatches(Path path, BufferAllocator allocator)
            throws IOException {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        try (ReadableByteChannel channel = Files.newByteChannel(path);
             ArrowStreamReader reader  = new ArrowStreamReader(channel, allocator)) {

            VectorSchemaRoot sourceRoot = reader.getVectorSchemaRoot();

            while (reader.loadNextBatch()) {
                if (sourceRoot.getRowCount() == 0) {
                    continue;
                }
                // Transfer the reader's buffers directly into a fresh VectorSchemaRoot.
                // TransferPair.transfer() moves ownership of each off-heap ArrowBuf from
                // the source vector to the destination — zero bytes are copied.  The reader
                // will allocate new buffers for the next batch on the next loadNextBatch()
                // call, so the transfer is safe here.
                VectorSchemaRoot copy =
                        VectorSchemaRoot.create(sourceRoot.getSchema(), allocator);
                for (FieldVector src : sourceRoot.getFieldVectors()) {
                    src.makeTransferPair(copy.getVector(src.getName())).transfer();
                }
                copy.setRowCount(sourceRoot.getRowCount());
                batches.add(copy);
            }
        }
        LOG.debug("Loaded {} batch(es) from {}", batches.size(), path.getFileName());
        return batches;
    }

    /**
     * Counts the total number of rows across all batches in {@code batches}.
     *
     * @param batches list of record batches
     * @return sum of {@link VectorSchemaRoot#getRowCount()} across all batches
     */
    public static long totalRows(List<VectorSchemaRoot> batches) {
        long count = 0;
        for (VectorSchemaRoot b : batches) {
            count += b.getRowCount();
        }
        return count;
    }

    /**
     * Closes and releases all roots in the list, then clears the list.
     *
     * @param batches mutable list of roots to close
     */
    public static void closeBatches(List<VectorSchemaRoot> batches) {
        for (VectorSchemaRoot root : batches) {
            root.close();
        }
        batches.clear();
    }
}
