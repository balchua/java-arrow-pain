package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.benchmark.LoadBenchmark;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.PainFileSpec;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.BatchConsumer;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.parser.StreamingBatchConsumer;
import com.iso20022.pain.persistence.LocalFilePersistenceService;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full pipeline benchmark for all file types A–E.
 *
 * <p>Exercises the complete XML → Arrow (streaming parse) → DuckDB (live INSERT) →
 * Arrow IPC Stream Write → SQL Validation pipeline for every type,
 * capturing the same metrics that {@link LoadBenchmark} records
 * when {@code App} is run from the command line.</p>
 */
class FullPipelineBenchmarkTest {

    private static final long ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024; // 2 GB

    private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

    // Summarises one type for the consolidated table
    record TypeSummary(
            String typeLabel,
            long xmlSizeBytes,
            long arrowSizeBytes,
            long parseMs,
            long duckdbMs,
            long validateMs,
            long writeMs,
            long offHeapAllocatedBytes,
            long offHeapPeakBytes,
            long offHeapStreamingPeakBytes,
            long heapDeltaBytes,
            long txRows,
            boolean valid
    ) {}

    @Test
    @DisplayName("Full pipeline benchmark — all types A through E")
    void fullPipelineBenchmarkAllTypes() throws Exception {
        Files.createDirectories(OUTPUT_DIR);

        List<TypeSummary> summaries = new ArrayList<>();

        summaries.add(runPipeline(TestPainFileSpecs.TYPE_A));
        summaries.add(runPipeline(TestPainFileSpecs.TYPE_B));
        summaries.add(runPipeline(TestPainFileSpecs.TYPE_C));
        summaries.add(runPipeline(TestPainFileSpecs.TYPE_D));
        summaries.add(runPipeline(TestPainFileSpecs.TYPE_E));

        printSummaryTable(summaries);

        // Basic correctness assertions for each type
        assertEquals(1_000_000L, summaries.get(0).txRows(), "Type A: expected 1M tx rows");
        assertEquals(1_000_000L, summaries.get(1).txRows(), "Type B: expected 1M tx rows");
        assertEquals(1_000_000L, summaries.get(2).txRows(), "Type C: expected 1M tx rows");
        assertEquals(200L,       summaries.get(3).txRows(), "Type D: expected 200 tx rows");
        assertEquals(200L,       summaries.get(4).txRows(), "Type E: expected 200 tx rows");

        assertTrue(summaries.get(3).valid(), "Type D should pass validation");
        assertFalse(summaries.get(4).valid(), "Type E should fail validation (invalid CtrlSum)");
    }

    private TypeSummary runPipeline(PainFileSpec spec) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);

        String label = spec.name();
        String baseName = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        LoadBenchmark benchmark = new LoadBenchmark(label);
        benchmark.setDuckDbMemoryLimitBytes(LoadBenchmark.parseDuckDbMemoryLimit("1GB"));

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        long fileSizeBytes = Files.size(xmlFile);
        benchmark.setXmlFileSizeBytes(fileSizeBytes);

        System.gc();
        long heapBefore = LoadBenchmark.captureHeapUsed();
        benchmark.setHeapUsedBeforeBytes(heapBefore);
        benchmark.setHeapMaxBytes(Runtime.getRuntime().maxMemory());

        long parseMs;
        long duckdbMs = 0;
        long validateMs;
        long writeMs;
        long offHeapAllocated;
        long offHeapPeak;
        long offHeapStreamingPeak;
        long txRows;
        long arrowBytes = 0;
        boolean validationPassed;

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            benchmark.setOffHeapLimitBytes(allocator.getLimit());

            PainParser parser = new PainParserImpl();

            DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            try (var stmt = conn.createStatement()) {
                stmt.execute("SET memory_limit='1GB'");
            }

            Files.createDirectories(OUTPUT_DIR);
            LocalFilePersistenceService persistence = new LocalFilePersistenceService(
                    OUTPUT_DIR, baseName, msgSchema, rmtSchema, txSchema);

            StreamingBatchConsumer streamingConsumer =
                    new StreamingBatchConsumer(conn, persistence, allocator);

            BatchConsumer wrappedConsumer = (tableType, root) -> {
                streamingConsumer.accept(tableType, root);
                benchmark.sampleOffHeap(allocator.getAllocatedMemory());
            };

            Instant parseStart = Instant.now();
            var stats = parser.parseStreaming(xmlFile, allocator, wrappedConsumer);
            Duration parseDuration = Duration.between(parseStart, Instant.now());
            parseMs = parseDuration.toMillis();
            benchmark.recordPhase("XML\u2192Arrow Parse", parseDuration);

            benchmark.setTotalRows(stats.transactionRows());
            benchmark.setMessageRows(stats.messageRows());
            benchmark.setRemittanceRows(stats.remittanceRows());

            offHeapAllocated = allocator.getAllocatedMemory();
            benchmark.setOffHeapAllocatedBytes(offHeapAllocated);
            benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());

            Instant writeStart = Instant.now();
            persistence.finish();
            Duration writeDuration = Duration.between(writeStart, Instant.now());
            writeMs = writeDuration.toMillis();
            benchmark.recordPhase("Arrow IPC Write", writeDuration);
            arrowBytes = persistence.getBytesWritten();
            benchmark.setArrowFileSizeBytes(arrowBytes);

            Instant valStart = Instant.now();
            try (PaymentRepository repository = new PaymentRepositoryImpl(conn)) {
                ValidationContext valContext = ValidationPipeline.standard().execute(repository);
                Duration valDuration = Duration.between(valStart, Instant.now());
                validateMs = valDuration.toMillis();
                benchmark.recordPhase("SQL Validation", valDuration);

                validationPassed = !valContext.hasErrors();
                benchmark.setValidationResult(
                        validationPassed,
                        stats.remittanceRows(),
                        stats.transactionRows(),
                        valContext.hasErrors() ? valContext.getErrors().size() : 0);

                txRows = stats.transactionRows();
            }

            offHeapPeak = allocator.getPeakMemoryAllocation();
            benchmark.setOffHeapPeakBytes(offHeapPeak);
            offHeapStreamingPeak = benchmark.getOffHeapStreamingPeakBytes();
        }

        benchmark.setHeapUsedAfterBytes(LoadBenchmark.captureHeapUsed());

        // Print the full LoadBenchmark report for this type
        System.out.println(benchmark.toReport());

        long heapDelta = LoadBenchmark.captureHeapUsed() - heapBefore;

        return new TypeSummary(
                spec.name().replaceAll("\\s*\\(.*", "").trim(),
                fileSizeBytes,
                arrowBytes,
                parseMs, duckdbMs, validateMs, writeMs,
                offHeapAllocated, offHeapPeak, offHeapStreamingPeak,
                heapDelta, txRows, validationPassed
        );
    }

    private static void printSummaryTable(List<TypeSummary> rows) {
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Full Pipeline Benchmark Summary — XML→Arrow Streaming Parse → DuckDB Live INSERT → SQL Validate → Arrow IPC Write          ║");
        System.out.println("╠═════════╦══════════╦══════════╦══════════╦══════════╦══════════╦══════════╦═══════════╦═══════════╦══════════╦════════════╣");
        System.out.println("║  Type   ║ XML (MB) ║ Arr (MB) ║ Parse ms ║  Val ms  ║ Write ms ║ HWM (MB) ║ Stream MB ║ Savings % ║ Heap ΔMB ║  Tx Rows   ║");
        System.out.println("╠═════════╬══════════╬══════════╬══════════╬══════════╬══════════╬══════════╬═══════════╬═══════════╬══════════╬════════════╣");
        for (TypeSummary r : rows) {
            // Arrow file size is a proxy for what old batch-accumulation off-heap would have been
            double arrowMb = r.arrowSizeBytes() / (1024.0 * 1024.0);
            double hwmMb = r.offHeapPeakBytes() / (1024.0 * 1024.0);
            double streamMb = r.offHeapStreamingPeakBytes() / (1024.0 * 1024.0);
            // Savings only meaningful for files large enough to fill at least one batch
            String savingsStr = (arrowMb >= 1.0)
                    ? String.format("%7.1f%%", (1.0 - streamMb / arrowMb) * 100.0)
                    : "      N/A";
            System.out.printf("║  %-7s ║ %8.1f ║ %8.1f ║ %8s ║ %8s ║ %8s ║ %8.1f ║ %9.1f ║ %9s ║ %8.1f ║ %10s ║%n",
                    r.typeLabel(),
                    r.xmlSizeBytes() / (1024.0 * 1024.0),
                    arrowMb,
                    String.format("%,d", r.parseMs()),
                    String.format("%,d", r.validateMs()),
                    String.format("%,d", r.writeMs()),
                    hwmMb,
                    streamMb,
                    savingsStr,
                    r.heapDeltaBytes() / (1024.0 * 1024.0),
                    String.format("%,d", r.txRows()));
        }
        System.out.println("╚═════════╩══════════╩══════════╩══════════╩══════════╩══════════╩══════════╩═══════════╩═══════════╩══════════╩════════════╝");
        System.out.println();
        System.out.println("  HWM (MB)   = Arrow allocator high-water mark (allocator lifetime peak)");
        System.out.println("  Stream MB  = Peak Arrow off-heap sampled at each batch flush during parseStreaming()");
        System.out.println("  Savings %  = (1 - Stream MB / Arr MB) × 100  [vs batch-accumulation proxy: Arrow IPC file size]");
        System.out.println("  Write ms   = Time to flush/close IPC writers (actual write I/O is pipelined with parse)");
        System.out.println();
    }
}

