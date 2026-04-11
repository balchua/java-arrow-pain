package com.pgw;

import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.validators.StreamingTransactionIteratorValidator;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation-stage benchmark for all file types A through J.
 *
 * <p>Each run separates two distinct phases:
 * <ol>
 *   <li><b>DuckDB registration</b> — time to load pre-exported Arrow files into an
 *       in-process DuckDB database via {@link ArrowIpc#load} (C Data Interface, no extension)</li>
 *   <li><b>SQL validation</b> — time to run the full {@link ValidationPipeline} against
 *       the populated DuckDB tables (IBAN MOD-97, BIC, ControlSum, …)</li>
 * </ol>
 *
 * <p>Arrow files are generated on demand if absent (XML → DuckDB → {@link ArrowIpc#export}).
 * The DuckDB load metrics here are specifically in the context of validation, giving
 * you the full latency picture for the validator module:</p>
 *
 * <pre>
 *   .arrow files on disk
 *        ↓  [ArrowIpc.load time — C Data Interface]
 *   in-process DuckDB
 *        ↓  [SQL validation time]
 *   ValidationContext (errors / warnings)
 * </pre>
 */
class ValidationBenchmarkTest {

    private static final long SMALL_ALLOCATOR_LIMIT  = 512L  * 1024 * 1024;       //  512 MB
    private static final long LARGE_ALLOCATOR_LIMIT  = 2L   * 1024 * 1024 * 1024; //   2 GB
    private static final long XLARGE_ALLOCATOR_LIMIT = 4L   * 1024 * 1024 * 1024; //   4 GB

    private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

    record ValidationResult(
            String label,
            long totalArrowBytes,
            long remittanceRows,
            long transactionRows,
            long duckdbLoadMs,
            long sqlValidationMs,
            long streamingIterationMs,
            long peakOffHeapBytes,
            boolean passed,
            int errorCount
    ) {}

    @Test
    @DisplayName("Validation stage benchmark — Arrow→DuckDB load + SQL validation — all types A through J")
    void validationBenchmarkAllTypes() throws Exception {
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

        // Correctness assertions
        assertEquals(1_000_000L, results.get(0).transactionRows(), "Type A: expected 1M tx rows");
        assertEquals(1_000_000L, results.get(1).transactionRows(), "Type B: expected 1M tx rows");
        assertEquals(1_000_000L, results.get(2).transactionRows(), "Type C: expected 1M tx rows");
        assertEquals(200L,       results.get(3).transactionRows(), "Type D: expected 200 tx rows");
        assertEquals(200L,       results.get(4).transactionRows(), "Type E: expected 200 tx rows");
        assertEquals(2_000_000L, results.get(5).transactionRows(), "Type F: expected 2M tx rows");
        assertEquals(4_000_000L, results.get(6).transactionRows(), "Type G: expected 4M tx rows");
        assertEquals(2_000L,     results.get(7).transactionRows(), "Type H: expected 2000 tx rows");
        assertEquals(2_000L,     results.get(8).transactionRows(), "Type I: expected 2000 tx rows");
        assertEquals(1L,         results.get(9).transactionRows(), "Type J: expected 1 tx row");

        assertTrue(results.get(3).passed(),  "Type D should pass validation");
        assertFalse(results.get(4).passed(), "Type E should fail validation (invalid CtrlSum)");
    }

    // -------------------------------------------------------------------------
    // Core benchmark logic
    // -------------------------------------------------------------------------

    private ValidationResult runValidation(PainFileSpec spec, long allocatorLimit)
            throws Exception {
        // Step 1: Ensure Arrow files exist (generate XML + parse + ArrowIpc.export if absent)
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        Files.createDirectories(OUTPUT_DIR);

        String base = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        Path msgFile = OUTPUT_DIR.resolve(base + "_message.arrow");
        Path rmtFile = OUTPUT_DIR.resolve(base + "_remittance.arrow");
        Path txFile  = OUTPUT_DIR.resolve(base + "_transaction.arrow");

        String duckDbMemoryLimit = allocatorLimit >= XLARGE_ALLOCATOR_LIMIT ? "2GB" : "1GB";

        if (!Files.exists(msgFile) || !Files.exists(rmtFile) || !Files.exists(txFile)) {
            generateArrowFiles(xmlFile, base, allocatorLimit, duckDbMemoryLimit);
        }

        long totalArrowBytes = Files.size(msgFile) + Files.size(rmtFile) + Files.size(txFile);

        // Step 2: Load Arrow files into a fresh DuckDB via ArrowIpc.load (no extension)
        long remittanceRows;
        long transactionRows;
        long duckdbLoadMs;
        long sqlValidationMs;
        long streamingIterationMs;
        long peakOffHeap;
        boolean passed;
        int errorCount;

        long duckdbStart = System.currentTimeMillis();
        try (RootAllocator loadAllocator = new RootAllocator(allocatorLimit)) {
            DuckDBConnection loadConn = DuckDbFactory.newConnection();
            try (var stmt = loadConn.createStatement()) {
                stmt.execute("SET memory_limit='" + duckDbMemoryLimit + "'");
            }
            ArrowIpc.load(loadConn, "message",      msgFile, loadAllocator);
            ArrowIpc.load(loadConn, "remittance",   rmtFile, loadAllocator);
            ArrowIpc.load(loadConn, "transactions", txFile,  loadAllocator);

            // Step 3: Time DuckDB load and SQL validation
            try (PaymentRepository repository = new PaymentRepositoryImpl(loadConn)) {
                duckdbLoadMs = System.currentTimeMillis() - duckdbStart;
                peakOffHeap  = loadAllocator.getPeakMemoryAllocation();

                remittanceRows  = repository.getRemittanceCount();
                transactionRows = repository.getTransactionCount();

                // Step 4: Time SQL validation against the loaded DuckDB
                long valStart = System.nanoTime();
                ValidationContext ctx = ValidationPipeline.standard().execute(repository);
                sqlValidationMs = (System.nanoTime() - valStart) / 1_000_000L;

                passed     = !ctx.hasErrors();
                errorCount = ctx.hasErrors() ? ctx.getErrors().size() : 0;

                // Step 5: Time streaming row-by-row iteration (simulates per-record external checks)
                long streamingStart = System.nanoTime();
                ValidationContext streamingCtx = new ValidationContext();
                new StreamingTransactionIteratorValidator().validate(repository, streamingCtx);
                streamingIterationMs = (System.nanoTime() - streamingStart) / 1_000_000L;
            }
        }

        return new ValidationResult(
                spec.name().replaceAll("\\s*\\(.*", "").trim(),
                totalArrowBytes,
                remittanceRows, transactionRows,
                duckdbLoadMs, sqlValidationMs, streamingIterationMs, peakOffHeap,
                passed, errorCount);
    }

    // -------------------------------------------------------------------------
    // Arrow file generation (XML → DuckDB → ArrowIpc.export)
    // -------------------------------------------------------------------------

    private void generateArrowFiles(Path xmlFile, String base, long allocatorLimit,
            String duckDbMemoryLimit)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator(allocatorLimit)) {
            DuckDBConnection conn = DuckDbFactory.newConnection();
            try (var stmt = conn.createStatement()) {
                stmt.execute("SET memory_limit='" + duckDbMemoryLimit + "'");
            }
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
            PainParser parser = new PainParserImpl();
            parser.parseStreaming(xmlFile, allocator, consumer);
            ArrowIpc.export(conn, "message",      OUTPUT_DIR.resolve(base + "_message.arrow"),     allocator);
            ArrowIpc.export(conn, "remittance",   OUTPUT_DIR.resolve(base + "_remittance.arrow"),  allocator);
            ArrowIpc.export(conn, "transactions", OUTPUT_DIR.resolve(base + "_transaction.arrow"), allocator);
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------------

    private static void printReport(List<ValidationResult> results) {
        final String SEP =
            "╠══════════╦════════════╦══════════════╦══════════════╦════════════════╦═══════════╦══════════════╦═══════════╣";
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║         Validation Stage Benchmark — Arrow File → DuckDB Load + SQL Validation                                              ║");
        System.out.println(SEP.replace('╠', '╠').replace('╣', '╣'));
        System.out.println("║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║ Peak Off-Heap  ║  Tx Rows  ║ rows/ms (val)║  Result   ║");
        System.out.println(SEP.replace('╦', '╬'));
        long totalDuckdbMs   = 0;
        long totalValidateMs = 0;
        long totalTxRows     = 0;
        long totalArrowKb    = 0;
        long maxPeakBytes    = 0;
        for (ValidationResult r : results) {
            double rowsPerMs = r.sqlValidationMs() > 0
                    ? (double) r.transactionRows() / r.sqlValidationMs()
                    : (double) r.transactionRows();
            String resultStr = r.passed()
                    ? "✓ PASSED"
                    : String.format("✗ %d err", r.errorCount());
            System.out.printf("║  %-8s ║ %10s ║ %12s ║ %12s ║ %14s ║ %9s ║ %12s ║ %-9s ║%n",
                    r.label(),
                    String.format("%,d", r.totalArrowBytes() / 1024),
                    String.format("%,d", r.duckdbLoadMs()),
                    String.format("%,d", r.sqlValidationMs()),
                    String.format("%,d", r.peakOffHeapBytes()),
                    String.format("%,d", r.transactionRows()),
                    String.format("%,.0f", rowsPerMs),
                    resultStr);
            totalDuckdbMs   += r.duckdbLoadMs();
            totalValidateMs += r.sqlValidationMs();
            totalTxRows     += r.transactionRows();
            totalArrowKb    += r.totalArrowBytes() / 1024;
            if (r.peakOffHeapBytes() > maxPeakBytes) maxPeakBytes = r.peakOffHeapBytes();
        }
        System.out.println("╠══════════╬════════════╬══════════════╬══════════════╬════════════════╬═══════════╬══════════════╬═══════════╣");
        double totalRowsPerMs = totalValidateMs > 0
                ? (double) totalTxRows / totalValidateMs : (double) totalTxRows;
        System.out.printf("║  %-8s ║ %10s ║ %12s ║ %12s ║ %14s ║ %9s ║ %12s ║ %-9s ║%n",
                "TOTAL",
                String.format("%,d", totalArrowKb),
                String.format("%,d", totalDuckdbMs),
                String.format("%,d", totalValidateMs),
                String.format("%,d", maxPeakBytes),
                String.format("%,d", totalTxRows),
                String.format("%,.0f", totalRowsPerMs),
                "—");
        System.out.println("╚══════════╩════════════╩══════════════╩══════════════╩════════════════╩═══════════╩══════════════╩═══════════╝");
        System.out.println();
        System.out.println("  Arrow (KB)     = total size of the three .arrow files on disk (message + remittance + transaction)");
        System.out.println("  DuckDB ms      = time to load Arrow files into in-process DuckDB via ArrowIpc.load (C Data Interface)");
        System.out.println("  Validate ms    = time to run ValidationPipeline.standard() against the populated DuckDB tables");
        System.out.println("  Peak Off-Heap  = Arrow allocator off-heap bytes after loading all 3 tables into DuckDB");
        System.out.println("  rows/ms (val)  = transaction row scan throughput during validation");
        System.out.println();

        // ── Streaming Iteration Report ───────────────────────────────────────────
        final String STREAM_SEP =
            "╠══════════╦═══════════╦════════════════╦══════════════╣";
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║   Streaming Iteration Benchmark — StreamingTransactionIteratorValidator ║");
        System.out.println(STREAM_SEP);
        System.out.println("║  Type    ║  Tx Rows  ║ Streaming ms   ║ rows/ms (str)║");
        System.out.println(STREAM_SEP.replace('╦', '╬'));
        long totalStreamMs  = 0;
        long totalStreamRows = 0;
        for (ValidationResult r : results) {
            double streamRowsPerMs = r.streamingIterationMs() > 0
                    ? (double) r.transactionRows() / r.streamingIterationMs()
                    : (double) r.transactionRows();
            System.out.printf("║  %-8s ║ %9s ║ %14s ║ %12s ║%n",
                    r.label(),
                    String.format("%,d", r.transactionRows()),
                    String.format("%,d", r.streamingIterationMs()),
                    String.format("%,.0f", streamRowsPerMs));
            totalStreamMs   += r.streamingIterationMs();
            totalStreamRows += r.transactionRows();
        }
        System.out.println("╠══════════╬═══════════╬════════════════╬══════════════╣");
        double totalStreamRowsPerMs = totalStreamMs > 0
                ? (double) totalStreamRows / totalStreamMs : (double) totalStreamRows;
        System.out.printf("║  %-8s ║ %9s ║ %14s ║ %12s ║%n",
                "TOTAL",
                String.format("%,d", totalStreamRows),
                String.format("%,d", totalStreamMs),
                String.format("%,.0f", totalStreamRowsPerMs));
        System.out.println("╚══════════╩═══════════╩════════════════╩══════════════╝");
        System.out.println();
        System.out.println("  Streaming ms   = time for StreamingTransactionIteratorValidator to iterate all rows and");
        System.out.println("                   map each into a Transaction POJO, checking instructedAmount > 0");
        System.out.println("  rows/ms (str)  = transaction row streaming throughput (query + result fetch + object mapping + check)");
        System.out.printf("%n  ► Grand Total Streaming Time (all types A–J): %,d ms to iterate through %,d transaction rows%n",
                totalStreamMs, totalStreamRows);
        System.out.println();
    }
}
