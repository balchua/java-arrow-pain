package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.ArrowFileExporter;
import com.iso20022.pain.benchmark.LoadBenchmark;
import com.iso20022.pain.generator.Pain001XmlGenerator;
import com.iso20022.pain.generator.SampleFileSpec;
import com.iso20022.pain.parser.Pain001StaxParser;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Main entry point for the ISO 20022 pain.001.001.09 → Apache Arrow loader.
 *
 * <p>
 * Usage:
 * </p>
 * <ul>
 * <li>{@code App <file.xml>} — parse a single pain.001.001.09 XML file into
 * Arrow</li>
 * <li>{@code App} (no args) — generate all 3 sample files, then parse each into
 * Arrow</li>
 * <li>{@code App generate} — only generate the 3 sample files (no parsing)</li>
 * </ul>
 *
 * <p>
 * Arrow IPC output files are written to {@code src/main/resources/output/}
 * with the same base name as the input XML plus {@code _message.arrow},
 * {@code _remittance.arrow}, and {@code _transaction.arrow} suffixes.
 * </p>
 *
 * <p>
 * The Arrow off-heap allocator is capped at 2 GB to stay within a
 * reasonable memory envelope for large files.
 * </p>
 */
public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private static final Path SAMPLE_DATA_DIR = Paths.get("src", "main", "resources", "sample-data");
    private static final Path OUTPUT_DIR = Paths.get("src", "main", "resources", "output");

    /** Off-heap limit: 2 GB. */
    private static final long ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024;

    private static final List<SampleFileSpec> FILE_SPECS = List.of(
            SampleFileSpec.TYPE_A,
            SampleFileSpec.TYPE_B,
            SampleFileSpec.TYPE_C);

    public static void main(String[] args) {
        LOG.info("═══════════════════════════════════════════════════════════════");
        LOG.info("  ISO 20022 pain.001.001.09 → Apache Arrow Loader");
        LOG.info("  Parser : StAX (Streaming API for XML)");
        LOG.info("  Tables : Message │ Remittance │ Transaction");
        LOG.info("  Memory : off-heap limit {} MB", ALLOCATOR_LIMIT / (1024 * 1024));
        LOG.info("═══════════════════════════════════════════════════════════════");

        try {
            if (args.length > 0 && "generate-formatted".equalsIgnoreCase(args[0])) {
                // ── Generate formatted (pretty-printed) XML ─────────────────
                LOG.info("");
                LOG.info("Mode: generate pretty-printed sample files only");
                generateSampleFiles(true);
                LOG.info("Generation complete (formatted).");

            } else if (args.length > 0 && "generate".equalsIgnoreCase(args[0])) {
                // ── Generate-only mode ──────────────────────────────────────
                LOG.info("");
                LOG.info("Mode: generate sample files only");
                generateSampleFiles(false);
                LOG.info("Generation complete.");

            } else if (args.length > 0) {
                // ── Single-file mode: parse the given XML file ──────────────
                Path xmlFile = Paths.get(args[0]);
                if (!Files.isRegularFile(xmlFile)) {
                    LOG.error("File not found: {}", xmlFile);
                    System.exit(1);
                }
                LOG.info("");
                LOG.info("Mode: parse single file → {}", xmlFile);
                processFile(xmlFile);

            } else {
                // ── Full mode: generate all 3 + parse each ─────────────────
                LOG.info("");
                LOG.info("Phase 1: Generating pain.001.001.09 sample XML files");
                LOG.info("─────────────────────────────────────────────────────────");
                List<Path> generatedFiles = generateSampleFiles(false);

                LOG.info("");
                LOG.info("Phase 2: Parsing XML → Apache Arrow (with benchmarking)");
                LOG.info("─────────────────────────────────────────────────────────");
                for (Path file : generatedFiles) {
                    processFile(file);
                }
            }

            LOG.info("");
            LOG.info("All processing complete.");

        } catch (Exception e) {
            LOG.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    // ─── File generation ─────────────────────────────────────────────────────

    private static List<Path> generateSampleFiles(boolean prettyPrint) {
        return FILE_SPECS.stream()
                .map(spec -> {
                    try {
                        Instant start = Instant.now();
                        Path path = Pain001XmlGenerator.generate(spec, SAMPLE_DATA_DIR, prettyPrint);
                        Duration elapsed = Duration.between(start, Instant.now());
                        LOG.info("  ✓ {} ready in {}", spec.fileName(), fmt(elapsed));
                        return path;
                    } catch (IOException | XMLStreamException e) {
                        throw new RuntimeException("Failed to generate " + spec.fileName(), e);
                    }
                })
                .toList();
    }

    // ─── Parse + benchmark + write a single file ─────────────────────────────

    private static void processFile(Path xmlFile) {
        String label = xmlFile.getFileName().toString();
        LoadBenchmark benchmark = new LoadBenchmark(label);

        try {
            long fileSizeBytes = Files.size(xmlFile);
            benchmark.setXmlFileSizeBytes(fileSizeBytes);

            LOG.info("");
            LOG.info("Processing: {} ({} bytes)", label, fileSizeBytes);

            // ── Capture heap before parse ────────────────────────────────
            System.gc(); // best-effort to get a cleaner baseline
            benchmark.setHeapUsedBeforeBytes(LoadBenchmark.captureHeapUsed());
            benchmark.setHeapMaxBytes(Runtime.getRuntime().maxMemory());

            try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {

                benchmark.setOffHeapLimitBytes(allocator.getLimit());

                // ── Parse XML → Arrow ────────────────────────────────────
                Pain001StaxParser parser = new Pain001StaxParser(allocator);

                Instant parseStart = Instant.now();
                ArrowBatchResult result = parser.parse(xmlFile);
                Duration parseDuration = Duration.between(parseStart, Instant.now());
                benchmark.recordPhase("XML→Arrow Parse", parseDuration);

                try (result) {
                    benchmark.setTotalRows(result.getTransactionRowCount());
                    benchmark.setMessageRows(result.getMessageRowCount());
                    benchmark.setRemittanceRows(result.getRemittanceRowCount());

                    // Capture off-heap after parse (before write)
                    benchmark.setOffHeapAllocatedBytes(allocator.getAllocatedMemory());
                    benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());

                    result.printSummary();

                    // ── Validate using chainable validators ────────────────────
                    LOG.info("");
                    LOG.info("Validating with chained validators...");
                    Instant valStart = Instant.now();

                    ValidationContext valContext = ValidationPipeline.standard()
                        .execute(result);

                    Duration valDuration = Duration.between(valStart, Instant.now());
                    benchmark.recordPhase("Validation", valDuration);

                    if (valContext.hasErrors()) {
                        LOG.warn("✗ Validation failed with {} error(s)", valContext.getErrors().size());
                        valContext.getErrors().stream()
                            .limit(10)
                            .forEach(err -> LOG.warn("  • [{}] {}: {}", 
                                err.validator(), err.message(), 
                                err.details().length > 0 ? err.details()[0] : ""));
                        benchmark.setValidationResult(false, 0, 0, valContext.getErrors().size());
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

                    // ── Write Arrow IPC files ────────────────────────────
                    Instant writeStart = Instant.now();
                    long arrowBytes = ArrowFileExporter.export(
                            result, allocator, OUTPUT_DIR, label);
                    Duration writeDuration = Duration.between(writeStart, Instant.now());
                    benchmark.recordPhase("Arrow IPC Write", writeDuration);
                    benchmark.setArrowFileSizeBytes(arrowBytes);

                    // Update peak after write (may have grown during VectorLoader)
                    benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());
                }
            }

            // ── Capture heap after everything ────────────────────────────
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
