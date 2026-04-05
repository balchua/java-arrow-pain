package com.pgw;

import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
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
 * Arrow file to DuckDB load benchmark test - all file types A through E.
 *
 * <p>For each type, this test:</p>
 * <ol>
 *   <li>Generates the XML if absent</li>
 *   <li>Parses XML to DuckDB using streaming pipeline, then exports three Arrow files
 *       (message, remittance, transaction) via {@code COPY TO (FORMAT arrow)}</li>
 *   <li>Simulates a downstream consumer: loads the .arrow files back into a fresh
 *       in-process DuckDB using {@code read_arrow()}</li>
 *   <li>Records per-table file sizes and DuckDB load time</li>
 * </ol>
 */
class ArrowFileLoadBenchmarkTest {

    /** Allocator limit for small files (D, E). */
    private static final long SMALL_ALLOCATOR_LIMIT = 512L * 1024 * 1024;     // 512 MB
    /** Allocator limit for large files (A, B, C - up to ~500 MB Arrow off-heap). */
    private static final long LARGE_ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024; // 2 GB

    private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

    record BenchmarkResult(
            String label,
            long messageFileBytes,
            long remittanceFileBytes,
            long transactionFileBytes,
            long loadTimeMs,
            long remittanceRows,
            long transactionRows
    ) {
        long totalArrowBytes() {
            return messageFileBytes + remittanceFileBytes + transactionFileBytes;
        }
    }

    @Test
    @DisplayName("Arrow file to DuckDB load benchmark - all types A through E")
    void arrowIpcLoadBenchmarkAllTypes() throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();

        results.add(runBenchmark(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        results.add(runBenchmark(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        results.add(runBenchmark(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        results.add(runBenchmark(TestPainFileSpecs.TYPE_D, SMALL_ALLOCATOR_LIMIT));
        results.add(runBenchmark(TestPainFileSpecs.TYPE_E, SMALL_ALLOCATOR_LIMIT));

        printBenchmarkReport(results);

        assertRowCounts(results.get(0), 1L,         1_000_000L); // Type A
        assertRowCounts(results.get(1), 2L,         1_000_000L); // Type B
        assertRowCounts(results.get(2), 1_000_000L, 1_000_000L); // Type C
        assertRowCounts(results.get(3), 2L,         200L);        // Type D
        assertRowCounts(results.get(4), 2L,         200L);        // Type E
    }

    private static void assertRowCounts(BenchmarkResult r,
            long expectedRemittance, long expectedTx) {
        assertEquals(expectedRemittance, r.remittanceRows(),
                "Wrong remittance count for " + r.label());
        assertEquals(expectedTx, r.transactionRows(),
                "Wrong transaction count for " + r.label());
        assertTrue(r.loadTimeMs() >= 0,
                "Load time should be non-negative for " + r.label());
    }

    private BenchmarkResult runBenchmark(PainFileSpec spec, long allocatorLimit)
            throws Exception {
        // Step 1: Generate XML if absent
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);

        // Step 2: Parse XML using streaming pipeline and export three Arrow files via DuckDB COPY TO
        Files.createDirectories(OUTPUT_DIR);
        String base = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");

        Path msgFile = OUTPUT_DIR.resolve(base + "_message.arrow");
        Path rmtFile = OUTPUT_DIR.resolve(base + "_remittance.arrow");
        Path txFile  = OUTPUT_DIR.resolve(base + "_transaction.arrow");

        if (!Files.exists(msgFile) || !Files.exists(rmtFile) || !Files.exists(txFile)) {
            try (BufferAllocator allocator = new RootAllocator(allocatorLimit)) {
                DuckDBConnection conn = DuckDbFactory.newConnection();
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SET memory_limit='1GB'");
                }
                StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
                PainParser parser = new PainParserImpl();
                parser.parseStreaming(xmlFile, allocator, consumer);
                try (var stmt = conn.createStatement()) {
                    stmt.execute("COPY message TO '" + msgFile.toAbsolutePath() + "' (FORMAT arrow)");
                    stmt.execute("COPY remittance TO '" + rmtFile.toAbsolutePath() + "' (FORMAT arrow)");
                    stmt.execute("COPY transactions TO '" + txFile.toAbsolutePath() + "' (FORMAT arrow)");
                }
                conn.close();
            }
        }

        long msgBytes = Files.size(msgFile);
        long rmtBytes = Files.size(rmtFile);
        long txBytes  = Files.size(txFile);

        // Step 3: Simulate downstream consumer — load .arrow files into a fresh DuckDB via read_arrow()
        long remittanceRows;
        long transactionRows;
        long loadTimeMs;

        long start = System.currentTimeMillis();
        DuckDBConnection loadConn = DuckDbFactory.newConnection();
        try (var stmt = loadConn.createStatement()) {
            stmt.execute("SET memory_limit='1GB'");
            stmt.execute("CREATE TABLE message AS SELECT * FROM read_arrow('"
                    + msgFile.toAbsolutePath() + "')");
            stmt.execute("CREATE TABLE remittance AS SELECT * FROM read_arrow('"
                    + rmtFile.toAbsolutePath() + "')");
            stmt.execute("CREATE TABLE transactions AS SELECT * FROM read_arrow('"
                    + txFile.toAbsolutePath() + "')");
        }
        try (PaymentRepository repo = new PaymentRepositoryImpl(loadConn)) {
            loadTimeMs      = System.currentTimeMillis() - start;
            remittanceRows  = repo.getRemittanceCount();
            transactionRows = repo.getTransactionCount();
        }

        return new BenchmarkResult(spec.name(), msgBytes, rmtBytes, txBytes,
                loadTimeMs, remittanceRows, transactionRows);
    }

    // -------------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------------

    private static void printBenchmarkReport(List<BenchmarkResult> results) {
        final String LINE =
            "╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦══════════════╦═════════════╣";
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║           Arrow File -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)               ║");
        System.out.println(LINE.replace('╠', '╠').replace('╣', '╣'));
        System.out.println("║  Type    ║  Msg KB   ║  Rmt KB   ║  Tx KB     ║ Total KB  ║ Load (ms)  ║  Rows/sec    ║  Tx Rows    ║");
        System.out.println(LINE.replace('╦', '╬'));
        for (BenchmarkResult r : results) {
            long totalRows  = r.remittanceRows() + r.transactionRows();
            long rowsPerSec = r.loadTimeMs() > 0
                    ? (totalRows * 1000 / r.loadTimeMs()) : totalRows * 1000;
            System.out.printf("║  %-8s ║ %9s ║ %9s ║ %10s ║ %9s ║ %10s ║ %12s ║ %11s ║%n",
                    typeLabel(r.label()),
                    kb(r.messageFileBytes()),
                    kb(r.remittanceFileBytes()),
                    kb(r.transactionFileBytes()),
                    kb(r.totalArrowBytes()),
                    String.format("%,d", r.loadTimeMs()),
                    String.format("%,d", rowsPerSec),
                    String.format("%,d", r.transactionRows()));
        }
        System.out.println("╚══════════╩═══════════╩═══════════╩════════════╩═══════════╩════════════╩══════════════╩═════════════╝");
        System.out.println();
    }

    private static String typeLabel(String name) {
        return name.replaceAll("\\s*\\(.*", "").trim(); // "Type A (...)" -> "Type A"
    }

    private static String kb(long bytes) {
        return String.format("%,d", bytes / 1024);
    }
}

