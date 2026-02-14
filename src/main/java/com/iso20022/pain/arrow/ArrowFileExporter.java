package com.iso20022.pain.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes Arrow IPC files from an {@link ArrowBatchResult}.
 * <p>
 * Produces three files in the output directory, derived from the source XML
 * file name:
 * </p>
 * <ul>
 * <li>{@code <base>_message.arrow} — GroupHeader rows</li>
 * <li>{@code <base>_remittance.arrow} — PaymentInformation rows</li>
 * <li>{@code <base>_transaction.arrow} — CdtTrfTxInf rows</li>
 * </ul>
 *
 * <p>
 * Uses the {@code VectorUnloader → ArrowRecordBatch → VectorLoader} pattern
 * so that multiple independent {@link VectorSchemaRoot} batches can be written
 * as record batches inside a single IPC file.
 * </p>
 */
public final class ArrowFileExporter {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowFileExporter.class);

    private ArrowFileExporter() {
        // utility class
    }

    /**
     * Writes the three Arrow tables to IPC files under {@code outputDir}.
     *
     * @param result      the parsed Arrow batch result
     * @param allocator   parent allocator (a child allocator will be created for
     *                    writing)
     * @param outputDir   directory in which to write the .arrow files
     * @param xmlFileName the original XML file name (used to derive output names)
     * @return total bytes written across all three files
     * @throws IOException if any file cannot be written
     */
    public static long export(ArrowBatchResult result, BufferAllocator allocator,
            Path outputDir, String xmlFileName) throws IOException {

        Files.createDirectories(outputDir);
        String base = xmlFileName.replaceAll("\\.[xX][mM][lL]$", "");

        long totalBytes = 0;

        // ── Message table ───────────────────────────────────────────────────
        Path msgPath = outputDir.resolve(base + "_message.arrow");
        totalBytes += writeSingle(result.getMessageRoot(), msgPath);

        // ── Remittance table (multi-batch) ──────────────────────────────────
        Path rmtPath = outputDir.resolve(base + "_remittance.arrow");
        totalBytes += writeBatches(result.getRemittanceBatches(), allocator, rmtPath);

        // ── Transaction table (multi-batch) ─────────────────────────────────
        Path txPath = outputDir.resolve(base + "_transaction.arrow");
        totalBytes += writeBatches(result.getTransactionBatches(), allocator, txPath);

        LOG.info("Arrow IPC files written to {}  ({} bytes total)", outputDir,
                String.format("%,d", totalBytes));
        LOG.info("  ✓ {}  ({} bytes)", msgPath.getFileName(),
                String.format("%,d", Files.size(msgPath)));
        LOG.info("  ✓ {}  ({} bytes)", rmtPath.getFileName(),
                String.format("%,d", Files.size(rmtPath)));
        LOG.info("  ✓ {}  ({} bytes)", txPath.getFileName(),
                String.format("%,d", Files.size(txPath)));

        return totalBytes;
    }

    // ─── Internal: write a single-batch table (message root) ─────────────────

    private static long writeSingle(VectorSchemaRoot root, Path filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath.toFile());
                ArrowFileWriter writer = new ArrowFileWriter(
                        root, null, fos.getChannel())) {

            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return Files.size(filePath);
    }

    // ─── Internal: write multiple batches via VectorUnloader/VectorLoader ────

    /**
     * Writes a list of independent {@link VectorSchemaRoot} batches into a single
     * Arrow IPC file using the Unloader → RecordBatch → Loader pattern.
     * <p>
     * A temporary "writer root" is created with the shared schema. Each source
     * batch is unloaded into an {@link ArrowRecordBatch}, loaded into the writer
     * root, and then serialised via {@link ArrowFileWriter#writeBatch()}.
     * </p>
     */
    private static long writeBatches(List<VectorSchemaRoot> batches,
            BufferAllocator parentAllocator,
            Path filePath) throws IOException {
        if (batches.isEmpty()) {
            LOG.warn("No batches to write for {}", filePath.getFileName());
            return 0;
        }

        Schema schema = batches.get(0).getSchema();

        try (BufferAllocator childAlloc = parentAllocator.newChildAllocator(
                "arrow-writer", 0, parentAllocator.getLimit());
                VectorSchemaRoot writerRoot = VectorSchemaRoot.create(schema, childAlloc);
                FileOutputStream fos = new FileOutputStream(filePath.toFile());
                ArrowFileWriter writer = new ArrowFileWriter(
                        writerRoot, null, fos.getChannel())) {

            VectorLoader loader = new VectorLoader(writerRoot);
            writer.start();

            for (VectorSchemaRoot batch : batches) {
                VectorUnloader unloader = new VectorUnloader(batch);
                try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
                    loader.load(recordBatch);
                }
                writer.writeBatch();
            }

            writer.end();
        }

        return Files.size(filePath);
    }
}
