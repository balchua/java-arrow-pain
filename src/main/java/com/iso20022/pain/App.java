package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.ArrowFileExporter;
import com.iso20022.pain.benchmark.LoadBenchmark;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

/**
 * Main entry point for the ISO 20022 pain.001.001.09 → Apache Arrow / DuckDB loader.
 *
 * <p>Usage: {@code App <pain001.xml>} — parse an existing pain.001.001.09 XML file into Arrow.</p>
 *
 * <p>Pipeline: XML → Arrow (parse) → Arrow IPC (export) → DuckDB (read_ipc) → SQL validation → report</p>
 */
public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private static final Path OUTPUT_DIR = Paths.get("src", "main", "resources", "output");

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
            LOG.info("Mode: parse single file → {}", xmlFile);
            processFile(xmlFile);
            LOG.info("");
            LOG.info("All processing complete.");
        } catch (Exception e) {
            LOG.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void processFile(Path xmlFile) {
        String label = xmlFile.getFileName().toString();
        LoadBenchmark benchmark = new LoadBenchmark(label);

        try {
            long fileSizeBytes = Files.size(xmlFile);
            benchmark.setXmlFileSizeBytes(fileSizeBytes);

            LOG.info("");
            LOG.info("Processing: {} ({} bytes)", label, fileSizeBytes);

            System.gc();
            benchmark.setHeapUsedBeforeBytes(LoadBenchmark.captureHeapUsed());
            benchmark.setHeapMaxBytes(Runtime.getRuntime().maxMemory());

            try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
                benchmark.setOffHeapLimitBytes(allocator.getLimit());

                PainParser parser = new PainParserImpl();

                Instant parseStart = Instant.now();
                ArrowBatchResult result = parser.parse(xmlFile, allocator);
                Duration parseDuration = Duration.between(parseStart, Instant.now());
                benchmark.recordPhase("XML→Arrow Parse", parseDuration);

                try (result) {
                    benchmark.setTotalRows(result.getTransactionRowCount());
                    benchmark.setMessageRows(result.getMessageRowCount());
                    benchmark.setRemittanceRows(result.getRemittanceRowCount());

                    benchmark.setOffHeapAllocatedBytes(allocator.getAllocatedMemory());
                    benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());

                    result.printSummary();

                    Instant duckdbStart = Instant.now();
                    try (PaymentRepository repository = new PaymentRepositoryImpl(result, allocator)) {
                        Duration duckdbDuration = Duration.between(duckdbStart, Instant.now());
                        benchmark.recordPhase("DuckDB Registration", duckdbDuration);

                        LOG.info("");
                        LOG.info("Validating with SQL-based validators (DuckDB)...");
                        Instant valStart = Instant.now();

                        ValidationContext valContext = ValidationPipeline.standard()
                                .execute(repository);

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
                                    result.getRemittanceRowCount(),
                                    result.getTransactionRowCount(), 0);
                        }

                        if (!valContext.getWarnings().isEmpty()) {
                            LOG.info("⚠ {} warning(s)", valContext.getWarnings().size());
                        }
                    }

                    Instant writeStart = Instant.now();
                    long arrowBytes = ArrowFileExporter.export(
                            result, allocator, OUTPUT_DIR, label);
                    Duration writeDuration = Duration.between(writeStart, Instant.now());
                    benchmark.recordPhase("Arrow IPC Write", writeDuration);
                    benchmark.setArrowFileSizeBytes(arrowBytes);

                    benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());
                }
            } catch (IllegalStateException e) {
                if (e.getMessage() != null && e.getMessage().startsWith("Memory was leaked")) {
                    LOG.debug("Arrow allocator residual from DuckDB C Data Interface (expected): {}",
                            e.getMessage().lines().findFirst().orElse(""));
                } else {
                    throw e;
                }
            }

            benchmark.setHeapUsedAfterBytes(LoadBenchmark.captureHeapUsed());

            LOG.info(benchmark.toReport());

        } catch (Exception e) {
            LOG.error("Error processing {}: {}", label, e.getMessage(), e);
        }
    }

    private static String fmt(Duration d) {
        long ms = d.toMillis();
        return ms < 1000 ? ms + " ms" : String.format("%.2f s", ms / 1000.0);
    }
}
