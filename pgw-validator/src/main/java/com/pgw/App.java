package com.pgw;

import com.pgw.benchmark.LoadBenchmark;
import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.parser.BatchConsumer;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

/**
 * Main entry point for the ISO 20022 pain.001.001.09 → Apache Arrow / DuckDB loader.
 *
 * <p>Usage: {@code App <pain001.xml>}</p>
 *
 * <p>Pipeline: XML → Arrow (streaming parse) → DuckDB (live INSERT) →
 * DuckDB COPY TO (FORMAT arrow) → .arrow files → SQL validation → report</p>
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

        try {
            long fileSizeBytes = Files.size(xmlFile);
            benchmark.setXmlFileSizeBytes(fileSizeBytes);

            // Resolve output directory
            String outputDirEnv = System.getenv("PAIN_LOCAL_OUTPUT_DIR");
            Path resolvedOutputDir = (outputDirEnv != null && !outputDirEnv.isBlank())
                    ? Paths.get(outputDirEnv)
                    : Paths.get("src", "main", "resources", "output");
            Files.createDirectories(resolvedOutputDir);
            LOG.info("  Output dir : {}", resolvedOutputDir);

            // Resolve output Arrow file paths
            Path msgPath = resolvedOutputDir.resolve(baseName + "_message.arrow").toAbsolutePath();
            Path rmtPath = resolvedOutputDir.resolve(baseName + "_remittance.arrow").toAbsolutePath();
            Path txPath  = resolvedOutputDir.resolve(baseName + "_transaction.arrow").toAbsolutePath();

            LOG.info("");
            LOG.info("Processing (streaming): {} ({} bytes)", label, fileSizeBytes);

            System.gc();
            benchmark.setHeapUsedBeforeBytes(LoadBenchmark.captureHeapUsed());
            benchmark.setHeapMaxBytes(Runtime.getRuntime().maxMemory());

            try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
                benchmark.setOffHeapLimitBytes(allocator.getLimit());

                PainParser parser = new PainParserImpl();

                DuckDBConnection conn = DuckDbFactory.newConnection();
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SET memory_limit='1GB'");
                }

                StreamingBatchConsumer streamingConsumer =
                        new StreamingBatchConsumer(conn, allocator);

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
                try (var stmt = conn.createStatement()) {
                    stmt.execute("COPY message TO '" + msgPath + "' (FORMAT arrow)");
                    stmt.execute("COPY remittance TO '" + rmtPath + "' (FORMAT arrow)");
                    stmt.execute("COPY transactions TO '" + txPath + "' (FORMAT arrow)");
                }
                Duration writeDuration = Duration.between(writeStart, Instant.now());
                benchmark.recordPhase("Arrow File Export", writeDuration);

                long arrowBytes = Files.size(msgPath) + Files.size(rmtPath) + Files.size(txPath);
                benchmark.setArrowFileSizeBytes(arrowBytes);

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


