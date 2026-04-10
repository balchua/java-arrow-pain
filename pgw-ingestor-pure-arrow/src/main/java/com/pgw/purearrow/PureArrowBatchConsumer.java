package com.pgw.purearrow;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.benchmark.LoadBenchmark;
import com.pgw.parser.BatchConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorUnloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

/**
 * Implements {@link BatchConsumer}. On each {@link #accept} call:
 * <ol>
 *   <li>Uses {@link VectorUnloader} to extract an {@link ArrowRecordBatch} from the root.</li>
 *   <li>Appends the batch to the appropriate list in {@link PureArrowInMemoryStore}.</li>
 *   <li>Writes the batch immediately to the appropriate {@link ArrowStreamWriter} (IPC persistence).</li>
 *   <li>Optionally samples off-heap memory via {@link LoadBenchmark#sampleOffHeap} if a benchmark
 *       is provided.</li>
 * </ol>
 *
 * <p>Does NOT hold any reference to the passed {@link VectorSchemaRoot} after returning.</p>
 *
 * <p>Writers are opened lazily on the first batch per table type. Call {@link #close()} to
 * finalise all writers and flush all IPC stream files.</p>
 */
public final class PureArrowBatchConsumer implements BatchConsumer, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PureArrowBatchConsumer.class);

    private final BufferAllocator allocator;
    private final Path outputDir;
    private final String baseName;
    private final LoadBenchmark benchmark;
    private final PureArrowInMemoryStore store;

    // Lazy-opened writers — null until first batch arrives for that table
    private ArrowStreamWriter messageWriter;
    private ArrowStreamWriter remittanceWriter;
    private ArrowStreamWriter transactionWriter;

    private OutputStream messageStream;
    private OutputStream remittanceStream;
    private OutputStream transactionStream;

    // Track output file paths so PureArrowIngestor can return them
    private Path messageFile;
    private Path remittanceFile;
    private Path transactionFile;

    /**
     * Creates a new consumer that writes Arrow IPC stream files to {@code outputDir}.
     *
     * @param allocator  Arrow buffer allocator (used for writer channel)
     * @param outputDir  directory in which to create the three {@code .arrow} IPC files
     * @param baseName   base name (without extension) used to derive file names
     * @param benchmark  optional benchmark for off-heap memory sampling; may be {@code null}
     */
    public PureArrowBatchConsumer(BufferAllocator allocator, Path outputDir,
                                  String baseName, LoadBenchmark benchmark) {
        this.allocator = allocator;
        this.outputDir = outputDir;
        this.baseName = baseName;
        this.benchmark = benchmark;
        this.store = new PureArrowInMemoryStore();
    }

    @Override
    public void accept(TableType tableType, VectorSchemaRoot root) throws IOException {
        int rowCount = root.getRowCount();
        if (rowCount == 0) return;

        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch batch = unloader.getRecordBatch();

        switch (tableType) {
            case MESSAGE -> {
                if (messageWriter == null) {
                    messageFile = outputDir.resolve(baseName + "_message.arrow");
                    messageStream = new BufferedOutputStream(new FileOutputStream(messageFile.toFile()));
                    WritableByteChannel channel = Channels.newChannel(messageStream);
                    messageWriter = new ArrowStreamWriter(root, null, channel);
                    messageWriter.start();
                }
                store.addMessageBatch(batch, rowCount);
                messageWriter.writeBatch();
            }
            case REMITTANCE -> {
                if (remittanceWriter == null) {
                    remittanceFile = outputDir.resolve(baseName + "_remittance.arrow");
                    remittanceStream = new BufferedOutputStream(new FileOutputStream(remittanceFile.toFile()));
                    WritableByteChannel channel = Channels.newChannel(remittanceStream);
                    remittanceWriter = new ArrowStreamWriter(root, null, channel);
                    remittanceWriter.start();
                }
                store.addRemittanceBatch(batch, rowCount);
                remittanceWriter.writeBatch();
            }
            case TRANSACTION -> {
                if (transactionWriter == null) {
                    transactionFile = outputDir.resolve(baseName + "_transaction.arrow");
                    transactionStream = new BufferedOutputStream(new FileOutputStream(transactionFile.toFile()));
                    WritableByteChannel channel = Channels.newChannel(transactionStream);
                    transactionWriter = new ArrowStreamWriter(root, null, channel);
                    transactionWriter.start();
                }
                store.addTransactionBatch(batch, rowCount);
                transactionWriter.writeBatch();
            }
        }

        if (benchmark != null) {
            benchmark.sampleOffHeap(allocator.getAllocatedMemory());
        }
    }

    /**
     * Finalises all open {@link ArrowStreamWriter}s (writes EOS marker), closes output streams,
     * and ensures no resources are leaked. The {@link PureArrowInMemoryStore} is NOT closed here —
     * the caller owns its lifecycle.
     */
    @Override
    public void close() throws Exception {
        Exception firstException = null;

        if (messageWriter != null) {
            try {
                messageWriter.end();
                messageWriter.close();
            } catch (Exception e) {
                firstException = e;
            }
        }
        if (messageStream != null) {
            try { messageStream.close(); } catch (Exception e) {
                if (firstException == null) firstException = e;
            }
        }
        if (remittanceWriter != null) {
            try {
                remittanceWriter.end();
                remittanceWriter.close();
            } catch (Exception e) {
                if (firstException == null) firstException = e;
            }
        }
        if (remittanceStream != null) {
            try { remittanceStream.close(); } catch (Exception e) {
                if (firstException == null) firstException = e;
            }
        }
        if (transactionWriter != null) {
            try {
                transactionWriter.end();
                transactionWriter.close();
            } catch (Exception e) {
                if (firstException == null) firstException = e;
            }
        }
        if (transactionStream != null) {
            try { transactionStream.close(); } catch (Exception e) {
                if (firstException == null) firstException = e;
            }
        }

        if (firstException != null) throw firstException;
    }

    /** Returns the in-memory store populated during parsing. The caller owns the lifecycle. */
    public PureArrowInMemoryStore getStore() {
        return store;
    }

    /** Path to the message IPC stream file, or {@code null} if no message batches were received. */
    public Path getMessageFile() { return messageFile; }

    /** Path to the remittance IPC stream file, or {@code null} if no remittance batches were received. */
    public Path getRemittanceFile() { return remittanceFile; }

    /** Path to the transaction IPC stream file, or {@code null} if no transaction batches were received. */
    public Path getTransactionFile() { return transactionFile; }
}
