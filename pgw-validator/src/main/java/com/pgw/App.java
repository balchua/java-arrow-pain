package com.pgw;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.benchmark.LoadBenchmark;
import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.parser.BatchConsumer;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.persistence.PersistenceService;
import com.pgw.persistence.PersistenceServiceFactory;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;

/**
 * Main entry point for the ISO 20022 pain.001.001.09 → Apache Arrow / DuckDB loader.
 *
 * <p>Usage: {@code App <pain001.xml>}</p>
 *
 * <p>Pipeline: XML → Arrow (streaming parse) → DuckDB (live INSERT) →
 * Arrow IPC Stream files → SQL validation → report</p>
 */
public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    /** Off-heap limit: 2 GB. */
    private static final long ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024;

    public static void main(String[] args) {
        LOG.info("═══════════════════════════════════════════════════════════════");
        LOG.info("  ISO 20022 pain.001.001.09 → Apache Arrow / DuckDB Loader");
        LOG.info("  Parser : StAX (Streaming API for XML)");
        LOG.info("  Tables : Message │ Remittance │ Transaction");
        LOG.info("  Memory : off-heap limit {} MB", ALLOCATOR_LIMIT / (1024 * 1024));
        LOG.info("═══════════════════════════════════════════════════════════════");

        if (args.length == 0) {
            System.err.println("Usage: App <pain001.xml>");
            System.err.println("  <pain001.xml>  path to an ISO 20022 pain.001.001.09 XML file");
            System.exit(1);
        }

        Path xmlFile = Paths.get(args[0]);
        if (!Files.exists(xmlFile)) {
            System.err.println("ERROR: File not found: " + xmlFile.toAbsolutePath());
            System.exit(1);
        }

        try {
            LOG.info("");
            LOG.info("Mode: streaming → {}", xmlFile);
            processFileStreaming(xmlFile);
            LOG.info("");
            LOG.info("All processing complete.");
        } catch (Exception e) {
            LOG.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    // ── Streaming pipeline ────────────────────────────────────────────────────

    private static void processFileStreaming(Path xmlFile) throws Exception {
        String label = xmlFile.getFileName().toString();
        String baseName = label.replaceAll("\\.[xX][mM][lL]$", "");
        LoadBenchmark benchmark = new LoadBenchmark(label);
        benchmark.setDuckDbMemoryLimitBytes(LoadBenchmark.parseDuckDbMemoryLimit("1GB"));

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        try {
            long fileSizeBytes = Files.size(xmlFile);
            benchmark.setXmlFileSizeBytes(fileSizeBytes);

            // Resolve output directory
            String outputDirEnv = System.getenv("PAIN_LOCAL_OUTPUT_DIR");
            Path resolvedOutputDir = (outputDirEnv != null && !outputDirEnv.isBlank())
                    ? Paths.get(outputDirEnv)
                    : Paths.get("src", "main", "resources", "output");
            LOG.info("  Output dir : {}", resolvedOutputDir);

            LOG.info("");
            LOG.info("Processing (streaming): {} ({} bytes)", label, fileSizeBytes);

            System.gc();
            benchmark.setHeapUsedBeforeBytes(LoadBenchmark.captureHeapUsed());
            benchmark.setHeapMaxBytes(Runtime.getRuntime().maxMemory());

            try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
                benchmark.setOffHeapLimitBytes(allocator.getLimit());

                PainParser parser = new PainParserImpl();

                DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SET memory_limit='1GB'");
                }

                PersistenceService persistence = PersistenceServiceFactory.create(
                        baseName, msgSchema, rmtSchema, txSchema);

                StreamingBatchConsumer streamingConsumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);

                // Wrap consumer to sample off-heap memory at each batch flush
                BatchConsumer wrappedConsumer = (tableType, root) -> {
                    streamingConsumer.accept(tableType, root);
                    benchmark.sampleOffHeap(allocator.getAllocatedMemory());
                };

                Instant parseStart = Instant.now();
                var stats = parser.parseStreaming(xmlFile, allocator, wrappedConsumer);
                Duration parseDuration = Duration.between(parseStart, Instant.now());
                benchmark.recordPhase("XML→Arrow Parse", parseDuration);

                benchmark.setTotalRows(stats.transactionRows());
                benchmark.setMessageRows(stats.messageRows());
                benchmark.setRemittanceRows(stats.remittanceRows());

                benchmark.setOffHeapAllocatedBytes(allocator.getAllocatedMemory());
                benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());

                Instant writeStart = Instant.now();
                persistence.finish();
                Duration writeDuration = Duration.between(writeStart, Instant.now());
                benchmark.recordPhase("Arrow IPC Write", writeDuration);
                benchmark.setArrowFileSizeBytes(persistence.getBytesWritten());

                LOG.info("");
                LOG.info("Validating with SQL-based validators (DuckDB)...");
                Instant valStart = Instant.now();

                try (PaymentRepository repository = new PaymentRepositoryImpl(conn)) {
                    ValidationContext valContext = ValidationPipeline.standard().execute(repository);
                    Duration valDuration = Duration.between(valStart, Instant.now());
                    benchmark.recordPhase("SQL Validation", valDuration);

                    if (valContext.hasErrors()) {
                        LOG.warn("✗ Validation failed with {} error(s)",
                                valContext.getErrors().size());
                        valContext.getErrors().stream()
                                .limit(10)
                                .forEach(err -> LOG.warn("  • [{}] {}: {}",
                                        err.validator(), err.message(),
                                        err.details().length > 0 ? err.details()[0] : ""));
                        benchmark.setValidationResult(false, 0, 0,
                                valContext.getErrors().size());
                    } else {
                        LOG.info("✓ All validations passed (4 validators, {} ms)",
                                valContext.getElapsedMillis());
                        benchmark.setValidationResult(true,
                                stats.remittanceRows(),
                                stats.transactionRows(), 0);
                    }

                    if (!valContext.getWarnings().isEmpty()) {
                        LOG.info("⚠ {} warning(s)", valContext.getWarnings().size());
                    }
                }
            }

            benchmark.setHeapUsedAfterBytes(LoadBenchmark.captureHeapUsed());
            LOG.info(benchmark.toReport());

        } catch (Exception e) {
            LOG.error("Error processing {}: {}", label, e.getMessage(), e);
        }
    }
}


