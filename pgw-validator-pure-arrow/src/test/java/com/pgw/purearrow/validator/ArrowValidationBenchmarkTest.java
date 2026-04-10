package com.pgw.purearrow.validator;

import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.purearrow.PureArrowIngestor;
import com.pgw.purearrow.PureArrowIngestResult;
import com.pgw.purearrow.PureArrowInMemoryStore;
import com.pgw.purearrow.validator.dal.ArrowPaymentRepositoryImpl;
import com.pgw.purearrow.validator.dal.ArrowPaymentRepositoryLoader;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import com.pgw.validation.validators.StreamingTransactionIteratorValidator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation-stage benchmark for the pure-Arrow pipeline — all file types A through J.
 *
 * <p>Equivalent of {@code ValidationBenchmarkTest} in {@code pgw-validator} but uses
 * {@link ArrowPaymentRepositoryImpl} (no DuckDB) instead of {@code PaymentRepositoryImpl}.</p>
 *
 * <p>Each run separates three distinct phases:</p>
 * <ol>
 *   <li><b>Arrow ingest ms</b> — time to parse XML and write Arrow IPC files (pure-Arrow path).</li>
 *   <li><b>Arrow load ms</b> — time to materialise Arrow IPC files into in-memory
 *       {@link ArrowPaymentRepositoryImpl} via {@link ArrowPaymentRepositoryLoader}.</li>
 *   <li><b>Validation ms</b> — time to run the full {@link ValidationPipeline} against the
 *       Arrow-backed repository (IBAN, CtrlSum, field checks … all in pure Java).</li>
 * </ol>
 *
 * <pre>
 *   pain.001 XML on disk
 *        ↓  [PureArrowIngestor — Arrow ingest ms]
 *   .arrow files on disk
 *        ↓  [ArrowPaymentRepositoryLoader — Arrow load ms]
 *   ArrowPaymentRepositoryImpl (in-memory Arrow vectors)
 *        ↓  [ValidationPipeline.standard() — Validation ms]
 *   ValidationContext (errors / warnings)
 * </pre>
 */
class ArrowValidationBenchmarkTest {

    private static final long SMALL_ALLOCATOR_LIMIT  = 512L  * 1024 * 1024;
    private static final long LARGE_ALLOCATOR_LIMIT  = 2L   * 1024 * 1024 * 1024;
    private static final long XLARGE_ALLOCATOR_LIMIT = 4L   * 1024 * 1024 * 1024;

    private static final Path OUTPUT_DIR =
            Paths.get("target", "pure-arrow-validator-output");

    record ValidationResult(
            String label,
            long totalArrowBytes,
            long remittanceRows,
            long transactionRows,
            long arrowIngestMs,
            long arrowLoadMs,
            long validationMs,
            long streamingIterationMs,
            long peakOffHeapBytes,
            boolean passed,
            int errorCount
    ) {}

    @Test
    @DisplayName("Pure-Arrow validation benchmark — all types A through J")
    void arrowValidationBenchmarkAllTypes() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
        List<ValidationResult> results = new ArrayList<>();

        results.add(runValidation(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_D, SMALL_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_E, SMALL_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_F, XLARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_G, XLARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_H, SMALL_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_I, SMALL_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_J, SMALL_ALLOCATOR_LIMIT));

        printReport(results);

        // ── Correctness assertions ────────────────────────────────────────────
        assertEquals(1_000_000L, results.get(0).transactionRows(), "Type A: 1M tx rows");
        assertEquals(1_000_000L, results.get(1).transactionRows(), "Type B: 1M tx rows");
        assertEquals(1_000_000L, results.get(2).transactionRows(), "Type C: 1M tx rows");
        assertEquals(200L,       results.get(3).transactionRows(), "Type D: 200 tx rows");
        assertEquals(200L,       results.get(4).transactionRows(), "Type E: 200 tx rows");
        assertEquals(2_000_000L, results.get(5).transactionRows(), "Type F: 2M tx rows");
        assertEquals(4_000_000L, results.get(6).transactionRows(), "Type G: 4M tx rows");
        assertEquals(2_000L,     results.get(7).transactionRows(), "Type H: 2000 tx rows");
        assertEquals(2_000L,     results.get(8).transactionRows(), "Type I: 2000 tx rows");
        assertEquals(1L,         results.get(9).transactionRows(), "Type J: 1 tx row");

        assertTrue(results.get(3).passed(),  "Type D should pass validation");
        assertFalse(results.get(4).passed(), "Type E should fail validation (invalid CtrlSum)");
    }

    // ── Core benchmark logic ──────────────────────────────────────────────────

    private ValidationResult runValidation(PainFileSpec spec, long allocatorLimit)
            throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        String base  = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        Path subDir  = OUTPUT_DIR.resolve(base);
        Files.createDirectories(subDir);

        long arrowIngestMs;
        long arrowLoadMs;
        long validationMs;
        long streamingIterationMs;
        long peakOffHeap;
        long totalArrowBytes;
        long remittanceRows;
        long transactionRows;
        boolean passed;
        int errorCount;

        try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {

            // Phase 1: XML → Arrow IPC files (PureArrowIngestor)
            long ingestStart = System.currentTimeMillis();
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult ingestResult =
                    ingestor.ingest(xmlFile, subDir, base, allocator, null);
            arrowIngestMs = System.currentTimeMillis() - ingestStart;

            try (PureArrowInMemoryStore ignored = ingestResult.store()) {
                totalArrowBytes = fileSizeOrZero(ingestResult.messageFile())
                        + fileSizeOrZero(ingestResult.remittanceFile())
                        + fileSizeOrZero(ingestResult.transactionFile());

                // Phase 2: Load Arrow IPC files into ArrowPaymentRepositoryImpl
                long loadStart = System.currentTimeMillis();
                try (ArrowPaymentRepositoryImpl repo = ArrowPaymentRepositoryLoader.load(
                        ingestResult.messageFile(),
                        ingestResult.remittanceFile(),
                        ingestResult.transactionFile(),
                        allocator)) {
                    arrowLoadMs = System.currentTimeMillis() - loadStart;

                    remittanceRows  = repo.getRemittanceCount();
                    transactionRows = repo.getTransactionCount();

                    // Phase 3: SQL-equivalent validation in pure Arrow / Java
                    long valStart = System.nanoTime();
                    ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                    validationMs = (System.nanoTime() - valStart) / 1_000_000L;

                    passed     = !ctx.hasErrors();
                    errorCount = ctx.hasErrors() ? ctx.getErrors().size() : 0;

                    // Phase 4: Streaming row-by-row iteration (simulates per-record external checks)
                    long streamStart = System.nanoTime();
                    ValidationContext streamCtx = new ValidationContext();
                    new StreamingTransactionIteratorValidator().validate(repo, streamCtx);
                    streamingIterationMs = (System.nanoTime() - streamStart) / 1_000_000L;

                    peakOffHeap = allocator.getPeakMemoryAllocation();
                }
            }
        }

        return new ValidationResult(
                spec.name().replaceAll("\\s*\\(.*", "").trim(),
                totalArrowBytes,
                remittanceRows, transactionRows,
                arrowIngestMs, arrowLoadMs, validationMs, streamingIterationMs,
                peakOffHeap, passed, errorCount);
    }

    private static long fileSizeOrZero(Path p) throws java.io.IOException {
        return p != null && Files.exists(p) ? Files.size(p) : 0L;
    }

    // ── Report ───────────────────────────────────────────────────────────────

    private static void printReport(List<ValidationResult> results) {
        final String HDR_SEP =
            "╠══════════╦════════════╦═══════════════╦═══════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣";

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║    Pure-Arrow Validation Benchmark — XML→Arrow + Arrow Load + SQL-equivalent Validation (no DuckDB)                                  ║");
        System.out.println(HDR_SEP);
        System.out.println("║  Type    ║ Arrow (KB) ║  Ingest ms    ║  Load ms  ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║");
        System.out.println(HDR_SEP.replace('╦', '╬'));

        long totalIngestMs    = 0;
        long totalLoadMs      = 0;
        long totalValidateMs  = 0;
        long totalTxRows      = 0;
        long totalArrowKb     = 0;
        long maxPeakBytes     = 0;

        for (ValidationResult r : results) {
            double rowsPerMs = r.validationMs() > 0
                    ? (double) r.transactionRows() / r.validationMs()
                    : (double) r.transactionRows();
            String resultStr = r.passed()
                    ? "✓ PASSED"
                    : String.format("✗ %d err", r.errorCount());
            System.out.printf("║  %-8s ║ %10s ║ %13s ║ %9s ║ %12s ║ %14s ║ %9s ║ %12s ║ %-9s ║%n",
                    r.label(),
                    String.format("%,d", r.totalArrowBytes() / 1024),
                    String.format("%,d", r.arrowIngestMs()),
                    String.format("%,d", r.arrowLoadMs()),
                    String.format("%,d", r.validationMs()),
                    String.format("%,d", r.peakOffHeapBytes()),
                    String.format("%,d", r.transactionRows()),
                    String.format("%,.0f", rowsPerMs),
                    resultStr);
            totalIngestMs   += r.arrowIngestMs();
            totalLoadMs     += r.arrowLoadMs();
            totalValidateMs += r.validationMs();
            totalTxRows     += r.transactionRows();
            totalArrowKb    += r.totalArrowBytes() / 1024;
            if (r.peakOffHeapBytes() > maxPeakBytes) maxPeakBytes = r.peakOffHeapBytes();
        }

        System.out.println("╠══════════╬════════════╬═══════════════╬═══════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣");
        double totalRowsPerMs = totalValidateMs > 0
                ? (double) totalTxRows / totalValidateMs : (double) totalTxRows;
        System.out.printf("║  %-8s ║ %10s ║ %13s ║ %9s ║ %12s ║ %14s ║ %9s ║ %12s ║ %-9s ║%n",
                "TOTAL",
                String.format("%,d", totalArrowKb),
                String.format("%,d", totalIngestMs),
                String.format("%,d", totalLoadMs),
                String.format("%,d", totalValidateMs),
                String.format("%,d", maxPeakBytes),
                String.format("%,d", totalTxRows),
                String.format("%,.0f", totalRowsPerMs),
                "—");
        System.out.println("╚══════════╩════════════╩═══════════════╩═══════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝");
        System.out.println();
        System.out.println("  Arrow (KB)     = combined size of the 3 .arrow IPC files on disk");
        System.out.println("  Ingest ms      = XML → StAX parse → PureArrowBatchConsumer → .arrow files (no DuckDB)");
        System.out.println("  Load ms        = time to materialise .arrow files into ArrowPaymentRepositoryImpl");
        System.out.println("  Validate ms    = time to run ValidationPipeline.standard() in pure Java/Arrow (no SQL)");
        System.out.println("  Peak Off-Heap  = max Arrow allocator off-heap bytes (includes ingested + loaded batches)");
        System.out.println("  rows/ms (val)  = transaction row scan throughput during validation");
        System.out.println();

        // ── Streaming Iteration Table ─────────────────────────────────────────
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║   Streaming Iteration — StreamingTransactionIteratorValidator         ║");
        System.out.println("╠══════════╦═══════════╦════════════════╦══════════════╣");
        System.out.println("║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║");
        System.out.println("╠══════════╬═══════════╬════════════════╬══════════════╣");
        long totalStreamMs = 0, totalStreamRows = 0;
        for (ValidationResult r : results) {
            double sRowsPerMs = r.streamingIterationMs() > 0
                    ? (double) r.transactionRows() / r.streamingIterationMs()
                    : (double) r.transactionRows();
            System.out.printf("║  %-8s ║ %9s ║ %14s ║ %12s ║%n",
                    r.label(),
                    String.format("%,d", r.transactionRows()),
                    String.format("%,d", r.streamingIterationMs()),
                    String.format("%,.0f", sRowsPerMs));
            totalStreamMs   += r.streamingIterationMs();
            totalStreamRows += r.transactionRows();
        }
        System.out.println("╠══════════╬═══════════╬════════════════╬══════════════╣");
        double totalSRowsPerMs = totalStreamMs > 0
                ? (double) totalStreamRows / totalStreamMs : (double) totalStreamRows;
        System.out.printf("║  %-8s ║ %9s ║ %14s ║ %12s ║%n",
                "TOTAL",
                String.format("%,d", totalStreamRows),
                String.format("%,d", totalStreamMs),
                String.format("%,.0f", totalSRowsPerMs));
        System.out.println("╚══════════╩═══════════╩════════════════╩══════════════╝");
        System.out.println();
        System.out.println("  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows");
        System.out.println("                   directly from Arrow vectors (no SQL, no JDBC cursor)");
        System.out.printf("%n  ► Grand Total Streaming Time (all types A–J): %,d ms for %,d transaction rows%n",
                totalStreamMs, totalStreamRows);
        System.out.println();
    }
}
