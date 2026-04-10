package com.pgw.purearrow.validator;

import com.pgw.ArrowIpc;
import com.pgw.DuckDbFactory;
import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.purearrow.PureArrowIngestor;
import com.pgw.purearrow.PureArrowIngestResult;
import com.pgw.purearrow.PureArrowInMemoryStore;
import com.pgw.purearrow.validator.dal.ArrowPaymentRepositoryImpl;
import com.pgw.purearrow.validator.dal.ArrowPaymentRepositoryLoader;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Side-by-side performance comparison of the two validation pipelines:
 *
 * <h3>DuckDB validation pipeline</h3>
 * <pre>
 *   XML → StreamingBatchConsumer → DuckDB → ArrowIpc.export → .arrow files
 *       → ArrowIpc.load → DuckDB → PaymentRepositoryImpl (SQL queries)
 *       → ValidationPipeline
 * </pre>
 *
 * <h3>Pure-Arrow validation pipeline</h3>
 * <pre>
 *   XML → PureArrowBatchConsumer → .arrow files
 *       → ArrowPaymentRepositoryLoader → ArrowPaymentRepositoryImpl (Java scans)
 *       → ValidationPipeline
 * </pre>
 *
 * <p>Both pipelines run the exact same {@link ValidationPipeline#standard()} and must
 * agree on validation outcomes (pass/fail, error counts). Timing metrics are captured
 * separately for each phase:</p>
 * <ul>
 *   <li><b>Ingest ms</b>: XML parse + DuckDB INSERT + Arrow export <em>or</em> XML parse + Arrow write</li>
 *   <li><b>Load ms</b>: ArrowIpc.load into DuckDB <em>or</em> ArrowPaymentRepositoryLoader</li>
 *   <li><b>Validate ms</b>: ValidationPipeline.standard() via SQL <em>or</em> Java scans</li>
 *   <li><b>Peak off-heap</b>: captured from the Arrow allocator after load phase</li>
 * </ul>
 *
 * <p>Types A–J are benchmarked; large types (F, G) use a 4 GB allocator limit.</p>
 */
class ValidatorComparisonBenchmarkTest {

    private static final long SMALL_ALLOCATOR_LIMIT  = 512L  * 1024 * 1024;
    private static final long LARGE_ALLOCATOR_LIMIT  = 2L   * 1024 * 1024 * 1024;
    private static final long XLARGE_ALLOCATOR_LIMIT = 4L   * 1024 * 1024 * 1024;

    private static final Path DUCKDB_DIR = Paths.get("target", "validator-comparison", "duckdb");
    private static final Path ARROW_DIR  = Paths.get("target", "validator-comparison", "pure-arrow");

    private static final List<ComparisonResult> RESULTS = new ArrayList<>();

    record ComparisonResult(
            String  label,
            long    txRows,
            // DuckDB path
            long    duckdbIngestMs,
            long    duckdbLoadMs,
            long    duckdbValidateMs,
            long    duckdbPeakOffHeapBytes,
            boolean duckdbPassed,
            int     duckdbErrors,
            // Pure-Arrow path
            long    arrowIngestMs,
            long    arrowLoadMs,
            long    arrowValidateMs,
            long    arrowPeakOffHeapBytes,
            boolean arrowPassed,
            int     arrowErrors
    ) {
        long duckdbTotalMs()  { return duckdbIngestMs  + duckdbLoadMs  + duckdbValidateMs;  }
        long arrowTotalMs()   { return arrowIngestMs   + arrowLoadMs   + arrowValidateMs;   }

        /** Speedup of pure-Arrow end-to-end vs DuckDB end-to-end. */
        double speedup() {
            return duckdbTotalMs() > 0 ? (double) duckdbTotalMs() / arrowTotalMs() : 0.0;
        }
    }

    @BeforeAll
    static void createOutputDirs() throws Exception {
        Files.createDirectories(DUCKDB_DIR);
        Files.createDirectories(ARROW_DIR);
    }

    @Test
    @DisplayName("Validator comparison — DuckDB SQL vs Pure-Arrow Java — all types A through J")
    void validatorComparisonAllTypes() throws Exception {
        RESULTS.add(run(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_D, SMALL_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_E, SMALL_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_F, XLARGE_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_G, XLARGE_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_H, SMALL_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_I, SMALL_ALLOCATOR_LIMIT));
        RESULTS.add(run(TestPainFileSpecs.TYPE_J, SMALL_ALLOCATOR_LIMIT));

        // Correctness: both pipelines must agree on pass/fail
        for (ComparisonResult r : RESULTS) {
            assertEquals(r.duckdbPassed(), r.arrowPassed(),
                    "DuckDB and Arrow validators disagree on pass/fail for " + r.label());
        }

        // Type D (valid): both pass
        assertTrue(RESULTS.get(3).duckdbPassed(), "Type D (DuckDB): should pass");
        assertTrue(RESULTS.get(3).arrowPassed(),  "Type D (Arrow):  should pass");

        // Type E (invalid CtrlSum): both fail
        assertFalse(RESULTS.get(4).duckdbPassed(), "Type E (DuckDB): should fail");
        assertFalse(RESULTS.get(4).arrowPassed(),  "Type E (Arrow):  should fail");
    }

    @AfterAll
    static void printComparisonReport() {
        if (RESULTS.isEmpty()) return;
        printReport(RESULTS);
    }

    // ── Core logic ───────────────────────────────────────────────────────────

    private static ComparisonResult run(PainFileSpec spec, long allocatorLimit) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        String base  = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        String label = spec.name().replaceAll("\\s*\\(.*", "").trim();
        String duckDbMemLimit = allocatorLimit >= XLARGE_ALLOCATOR_LIMIT ? "2GB" : "1GB";

        // ── DuckDB path ───────────────────────────────────────────────────────
        Path duckDir = DUCKDB_DIR.resolve(base);
        Files.createDirectories(duckDir);
        Path msgFile = duckDir.resolve(base + "_message.arrow");
        Path rmtFile = duckDir.resolve(base + "_remittance.arrow");
        Path txFile  = duckDir.resolve(base + "_transaction.arrow");

        long duckdbIngestMs;
        long duckdbLoadMs;
        long duckdbValidateMs;
        long duckdbPeakOffHeapBytes;
        boolean duckdbPassed;
        int duckdbErrors;
        long txRows;

        try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
            // Ingest: XML → DuckDB → Arrow files
            if (!Files.exists(msgFile) || !Files.exists(rmtFile) || !Files.exists(txFile)) {
                long ingestStart = System.currentTimeMillis();
                DuckDBConnection ingestConn = DuckDbFactory.newConnection();
                try (var stmt = ingestConn.createStatement()) {
                    stmt.execute("SET memory_limit='" + duckDbMemLimit + "'");
                }
                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(ingestConn, allocator);
                new PainParserImpl().parseStreaming(xmlFile, allocator, consumer);
                ArrowIpc.export(ingestConn, "message",      msgFile, allocator);
                ArrowIpc.export(ingestConn, "remittance",   rmtFile, allocator);
                ArrowIpc.export(ingestConn, "transactions", txFile,  allocator);
                ingestConn.close();
                duckdbIngestMs = System.currentTimeMillis() - ingestStart;
            } else {
                duckdbIngestMs = 0; // already cached
            }

            // Load: Arrow files → DuckDB via ArrowIpc.load
            long loadStart = System.currentTimeMillis();
            DuckDBConnection loadConn = DuckDbFactory.newConnection();
            try (var stmt = loadConn.createStatement()) {
                stmt.execute("SET memory_limit='" + duckDbMemLimit + "'");
            }
            ArrowIpc.load(loadConn, "message",      msgFile, allocator);
            ArrowIpc.load(loadConn, "remittance",   rmtFile, allocator);
            ArrowIpc.load(loadConn, "transactions", txFile,  allocator);

            try (PaymentRepository repo = new PaymentRepositoryImpl(loadConn)) {
                duckdbLoadMs          = System.currentTimeMillis() - loadStart;
                duckdbPeakOffHeapBytes = allocator.getPeakMemoryAllocation();
                txRows                = repo.getTransactionCount();

                long valStart = System.nanoTime();
                ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                duckdbValidateMs = (System.nanoTime() - valStart) / 1_000_000L;
                duckdbPassed     = !ctx.hasErrors();
                duckdbErrors     = ctx.hasErrors() ? ctx.getErrors().size() : 0;
            }
        }

        // ── Pure-Arrow path ───────────────────────────────────────────────────
        Path arrowDir = ARROW_DIR.resolve(base);
        Files.createDirectories(arrowDir);

        long arrowIngestMs;
        long arrowLoadMs;
        long arrowValidateMs;
        long arrowPeakOffHeapBytes;
        boolean arrowPassed;
        int arrowErrors;

        try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
            // Ingest: XML → Arrow IPC files (no DuckDB)
            long ingestStart = System.currentTimeMillis();
            PureArrowIngestResult ingestResult =
                    new PureArrowIngestor().ingest(xmlFile, arrowDir, base, allocator, null);
            arrowIngestMs = System.currentTimeMillis() - ingestStart;

            try (PureArrowInMemoryStore ignored = ingestResult.store()) {
                // Load: Arrow IPC files → ArrowPaymentRepositoryImpl
                long loadStart = System.currentTimeMillis();
                try (ArrowPaymentRepositoryImpl repo = ArrowPaymentRepositoryLoader.load(
                        ingestResult.messageFile(),
                        ingestResult.remittanceFile(),
                        ingestResult.transactionFile(),
                        allocator)) {
                    arrowLoadMs = System.currentTimeMillis() - loadStart;

                    long valStart = System.nanoTime();
                    ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                    arrowValidateMs = (System.nanoTime() - valStart) / 1_000_000L;
                    arrowPassed     = !ctx.hasErrors();
                    arrowErrors     = ctx.hasErrors() ? ctx.getErrors().size() : 0;

                    arrowPeakOffHeapBytes = allocator.getPeakMemoryAllocation();
                }
            }
        }

        return new ComparisonResult(label, txRows,
                duckdbIngestMs, duckdbLoadMs, duckdbValidateMs,
                duckdbPeakOffHeapBytes, duckdbPassed, duckdbErrors,
                arrowIngestMs, arrowLoadMs, arrowValidateMs,
                arrowPeakOffHeapBytes, arrowPassed, arrowErrors);
    }

    // ── Report ────────────────────────────────────────────────────────────────

    private static void printReport(List<ComparisonResult> results) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║    Validator Pipeline Comparison — DuckDB SQL vs Pure-Arrow Java (same ValidationPipeline.standard())                                                    ║");
        System.out.println("╠══════════╦══════════════════════════════════════════════════════════════════╦══════════════════════════════════════════════════════════════════════╦════════╣");
        System.out.println("║          ║              DuckDB path (SQL)                                   ║            Pure-Arrow path (Java)                                     ║        ║");
        System.out.println("║  Type    ║ Ingest ms │ Load ms  │ Val ms   │ Peak Off-Heap  │  Result       ║ Ingest ms │ Load ms  │ Val ms   │ Peak Off-Heap  │  Result       ║Speedup ║");
        System.out.println("╠══════════╬══════════════════════════════════════════════════════════════════╬══════════════════════════════════════════════════════════════════════╬════════╣");

        for (ComparisonResult r : results) {
            String dRes = r.duckdbPassed() ? "✓ PASS" : String.format("✗ %d err", r.duckdbErrors());
            String aRes = r.arrowPassed()  ? "✓ PASS" : String.format("✗ %d err", r.arrowErrors());
            System.out.printf("║  %-8s ║ %9s │ %8s │ %8s │ %14s │ %-13s ║ %9s │ %8s │ %8s │ %14s │ %-13s ║ %6s ║%n",
                    r.label(),
                    String.format("%,d", r.duckdbIngestMs()),
                    String.format("%,d", r.duckdbLoadMs()),
                    String.format("%,d", r.duckdbValidateMs()),
                    String.format("%,d", r.duckdbPeakOffHeapBytes()),
                    dRes,
                    String.format("%,d", r.arrowIngestMs()),
                    String.format("%,d", r.arrowLoadMs()),
                    String.format("%,d", r.arrowValidateMs()),
                    String.format("%,d", r.arrowPeakOffHeapBytes()),
                    aRes,
                    String.format("%.2fx", r.speedup()));
        }

        System.out.println("╚══════════╩══════════════════════════════════════════════════════════════════╩══════════════════════════════════════════════════════════════════════╩════════╝");
        System.out.println();
        System.out.println("  Ingest ms      = (DuckDB) XML → StreamingBatchConsumer → DuckDB INSERT → ArrowIpc.export");
        System.out.println("                 = (Arrow)  XML → PureArrowBatchConsumer → ArrowStreamWriter → .arrow files");
        System.out.println("  Load ms        = (DuckDB) ArrowIpc.load (C Data Interface) + PaymentRepositoryImpl creation");
        System.out.println("                 = (Arrow)  ArrowPaymentRepositoryLoader.load (ArrowStreamReader + VectorLoader)");
        System.out.println("  Val ms         = ValidationPipeline.standard(): MessageValidator + RemittanceValidator");
        System.out.println("                   + TransactionValidator + ControlSumValidator");
        System.out.println("                   (DuckDB: SQL GROUP BY/HAVING/REGEXP | Arrow: pure Java BigDecimal/Pattern)");
        System.out.println("  Peak Off-Heap  = Arrow allocator peak off-heap bytes at end of load phase");
        System.out.println("  Speedup        = duckdb total ms / arrow total ms  (> 1.0x means Arrow is faster end-to-end)");
        System.out.println();

        // Summary totals
        long dTotalMs = results.stream().mapToLong(ComparisonResult::duckdbTotalMs).sum();
        long aTotalMs = results.stream().mapToLong(ComparisonResult::arrowTotalMs).sum();
        double overallSpeedup = dTotalMs > 0 ? (double) dTotalMs / aTotalMs : 0.0;
        long totalTxRows = results.stream().mapToLong(ComparisonResult::txRows).sum();
        System.out.printf("  ► Total DuckDB  path: %,d ms for %,d transaction rows%n", dTotalMs, totalTxRows);
        System.out.printf("  ► Total Arrow   path: %,d ms for %,d transaction rows%n", aTotalMs, totalTxRows);
        System.out.printf("  ► Overall speedup: %.2fx (Arrow vs DuckDB end-to-end)%n", overallSpeedup);
        System.out.println();
    }
}
