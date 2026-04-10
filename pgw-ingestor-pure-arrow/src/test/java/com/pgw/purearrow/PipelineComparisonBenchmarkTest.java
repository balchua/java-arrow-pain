package com.pgw.purearrow;

import com.pgw.ArrowIpc;
import com.pgw.DuckDbFactory;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.BatchConsumer;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.ParseStats;
import com.pgw.parser.StreamingBatchConsumer;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Side-by-side performance comparison: DuckDB pipeline vs pure-Arrow pipeline.
 *
 * <h3>DuckDB pipeline</h3>
 * <pre>
 *   XML → StAX parse → StreamingBatchConsumer (Arrow C Data Interface INSERT per batch)
 *       → DuckDB → ArrowIpc.export() → .arrow files
 * </pre>
 *
 * <h3>Pure-Arrow pipeline</h3>
 * <pre>
 *   XML → StAX parse → PureArrowBatchConsumer (VectorUnloader per batch)
 *       → PureArrowInMemoryStore → ArrowStreamWriter → .arrow files
 * </pre>
 *
 * <p>Both pipelines produce the same Arrow IPC stream files on disk with identical
 * row counts, but take different paths through memory and different intermediate
 * representations. This test runs each type through both pipelines and prints
 * a comparison table showing:</p>
 * <ul>
 *   <li>Parse + write time (ms) for each pipeline</li>
 *   <li>Speedup ratio (pure-Arrow vs DuckDB)</li>
 *   <li>Peak off-heap bytes for each pipeline</li>
 *   <li>Transaction row count (must match)</li>
 * </ul>
 *
 * <p>Types A–G are benchmarked; large types (F, G) use a 4 GB allocator limit.</p>
 */
class PipelineComparisonBenchmarkTest {

    private static final long LARGE_ALLOCATOR_LIMIT  = 2L * 1024 * 1024 * 1024L; //  2 GB
    private static final long XLARGE_ALLOCATOR_LIMIT = 4L * 1024 * 1024 * 1024L; //  4 GB

    private static final Path DUCKDB_OUT  = Paths.get("target", "comparison-test-output", "duckdb");
    private static final Path ARROW_OUT   = Paths.get("target", "comparison-test-output", "pure-arrow");

    private static final List<ComparisonResult> RESULTS = new ArrayList<>();

    record ComparisonResult(
            String  label,
            long    xmlBytes,
            long    txRows,
            long    duckdbParseInsertMs,
            long    duckdbExportMs,
            long    duckdbPeakOffHeapBytes,
            long    pureArrowParseMs,
            long    pureArrowPeakOffHeapBytes
    ) {
        long duckdbTotalMs() { return duckdbParseInsertMs + duckdbExportMs; }

        /** Speedup: how many times faster is pure-Arrow end-to-end vs DuckDB (parse+export). */
        double speedup() {
            return duckdbTotalMs() > 0 ? (double) duckdbTotalMs() / pureArrowParseMs : 0.0;
        }
    }

    @BeforeAll
    static void createOutputDirs() throws Exception {
        Files.createDirectories(DUCKDB_OUT);
        Files.createDirectories(ARROW_OUT);
    }

    @Test
    @DisplayName("Pipeline comparison — DuckDB vs Pure Arrow — all types A through G")
    void pipelineComparisonAllTypes() throws Exception {
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_D, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_E, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_F, XLARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runComparison(TestPainFileSpecs.TYPE_G, XLARGE_ALLOCATOR_LIMIT));

        // Both pipelines must produce identical row counts
        for (ComparisonResult r : RESULTS) {
            assertTrue(r.txRows() >= 0,              "Row count must be non-negative: " + r.label());
            assertTrue(r.duckdbTotalMs() >= 0,       "DuckDB total time must be non-negative: " + r.label());
            assertTrue(r.pureArrowParseMs() >= 0,    "Pure-Arrow time must be non-negative: " + r.label());
        }

        assertEquals(1_000_000L, RESULTS.get(0).txRows(), "Type A: expected 1M tx rows");
        assertEquals(1_000_000L, RESULTS.get(1).txRows(), "Type B: expected 1M tx rows");
        assertEquals(1_000_000L, RESULTS.get(2).txRows(), "Type C: expected 1M tx rows");
        assertEquals(200L,       RESULTS.get(3).txRows(), "Type D: expected 200 tx rows");
        assertEquals(200L,       RESULTS.get(4).txRows(), "Type E: expected 200 tx rows");
        assertEquals(2_000_000L, RESULTS.get(5).txRows(), "Type F: expected 2M tx rows");
        assertEquals(4_000_000L, RESULTS.get(6).txRows(), "Type G: expected 4M tx rows");
    }

    @AfterAll
    static void printComparisonTable() {
        if (RESULTS.isEmpty()) return;

        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          PIPELINE COMPARISON — DuckDB  vs  Pure Arrow  (XML → .arrow files)                                                   ║");
        System.out.println("╠══════════╦══════════╦══════════════╦═══════════╦═══════════════╦══════════════╦═══════════════╦═══════════╦══════════════════╣");
        System.out.println("║  Type    ║ XML (MB) ║ DuckDB Prs+I ║ DuckDB Ex ║ DuckDB Peak   ║ PureArrow ms ║ PureArrow Pk  ║  Tx Rows  ║   Speedup (×)    ║");
        System.out.println("╠══════════╬══════════╬══════════════╬═══════════╬═══════════════╬══════════════╬═══════════════╬═══════════╬══════════════════╣");

        long totalXml      = 0;
        long totalDuckdbMs = 0;
        long totalArrowMs  = 0;
        long totalTxRows   = 0;

        for (ComparisonResult r : RESULTS) {
            double xmlMb       = r.xmlBytes() / (1024.0 * 1024.0);
            double duckdbPeakMb = r.duckdbPeakOffHeapBytes() / (1024.0 * 1024.0);
            double arrowPeakMb  = r.pureArrowPeakOffHeapBytes() / (1024.0 * 1024.0);
            System.out.printf(
                    "║  %-7s ║ %,7.1f  ║    %,9d ║  %,7d  ║  %,9.1f MB ║   %,9d  ║  %,9.1f MB ║ %,9d ║     %,6.2f×     ║%n",
                    r.label(),
                    xmlMb,
                    r.duckdbParseInsertMs(),
                    r.duckdbExportMs(),
                    duckdbPeakMb,
                    r.pureArrowParseMs(),
                    arrowPeakMb,
                    r.txRows(),
                    r.speedup()
            );
            totalXml      += r.xmlBytes();
            totalDuckdbMs += r.duckdbTotalMs();
            totalArrowMs  += r.pureArrowParseMs();
            totalTxRows   += r.txRows();
        }

        double overallSpeedup = totalDuckdbMs > 0 ? (double) totalDuckdbMs / totalArrowMs : 0.0;
        System.out.println("╠══════════╬══════════╬══════════════╬═══════════╬═══════════════╬══════════════╬═══════════════╬═══════════╬══════════════════╣");
        System.out.printf(
                "║  TOTAL   ║ %,7.1f  ║                           ║               ║   %,9d  ║               ║ %,9d ║     %,6.2f×     ║%n",
                totalXml / (1024.0 * 1024.0),
                totalArrowMs,
                totalTxRows,
                overallSpeedup
        );
        System.out.println("╚══════════╩══════════╩══════════════╩═══════════╩═══════════════╩══════════════╩═══════════════╩═══════════╩══════════════════╝");
        System.out.println();
        System.out.println("  DuckDB Prs+I   = StAX streaming parse + StreamingBatchConsumer INSERT into DuckDB (ms)");
        System.out.println("  DuckDB Ex      = ArrowIpc.export() for all 3 tables via C Data Interface (ms)");
        System.out.println("  DuckDB Peak    = peak Arrow allocator off-heap bytes during parse+insert phase");
        System.out.println("  PureArrow ms   = StAX streaming parse + PureArrowBatchConsumer → ArrowStreamWriter (ms)");
        System.out.println("  PureArrow Pk   = peak Arrow allocator off-heap bytes during pure-Arrow parse phase");
        System.out.println("  Speedup (×)    = DuckDB total (parse+insert+export) / PureArrow total (parse+write)");
        System.out.println("                   Values > 1.0× mean pure-Arrow is faster end-to-end");
        System.out.println();

        // ─── Analysis summary ────────────────────────────────────────────────
        System.out.println("  ── Analysis ─────────────────────────────────────────────────────────────────────────────────────");
        for (ComparisonResult r : RESULTS) {
            String verdict = r.speedup() >= 1.5 ? "⚡ Pure Arrow significantly faster"
                           : r.speedup() >= 1.0 ? "✓ Pure Arrow faster"
                           : "⚠ DuckDB pipeline faster";
            System.out.printf("  %-8s  DuckDB total %,d ms  vs  Pure Arrow %,d ms  →  %.2f×  %s%n",
                    r.label(),
                    r.duckdbTotalMs(),
                    r.pureArrowParseMs(),
                    r.speedup(),
                    verdict);
        }
        System.out.println();
    }

    // ── Core benchmark logic ─────────────────────────────────────────────────

    private static ComparisonResult runComparison(PainFileSpec spec, long allocatorLimit) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        long xmlBytes = Files.size(xmlFile);
        String base  = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        String label = spec.name().replaceAll("\\s*\\(.*", "").trim();

        // ── Phase 1: DuckDB pipeline ─────────────────────────────────────────
        long duckdbParseInsertMs;
        long duckdbExportMs;
        long duckdbPeakOffHeap;
        long duckdbTxRows;

        {
            Path msgFile = DUCKDB_OUT.resolve(base + "_message.arrow");
            Path rmtFile = DUCKDB_OUT.resolve(base + "_remittance.arrow");
            Path txFile  = DUCKDB_OUT.resolve(base + "_transaction.arrow");

            String duckDbMemoryLimit = allocatorLimit >= XLARGE_ALLOCATOR_LIMIT ? "2GB" : "1GB";

            try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
                DuckDBConnection conn = DuckDbFactory.newConnection();
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SET memory_limit='" + duckDbMemoryLimit + "'");
                }

                StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
                final long[] peakHolder = {0};
                BatchConsumer trackingConsumer = (tableType, root) -> {
                    consumer.accept(tableType, root);
                    long cur = allocator.getAllocatedMemory();
                    if (cur > peakHolder[0]) peakHolder[0] = cur;
                };

                long parseStart = System.currentTimeMillis();
                PainParser parser = new PainParserImpl();
                ParseStats stats  = parser.parseStreaming(xmlFile, allocator, trackingConsumer);
                duckdbParseInsertMs = System.currentTimeMillis() - parseStart;
                duckdbPeakOffHeap   = peakHolder[0];
                duckdbTxRows        = stats.transactionRows();

                long exportStart = System.currentTimeMillis();
                ArrowIpc.export(conn, "message",      msgFile, allocator);
                ArrowIpc.export(conn, "remittance",   rmtFile, allocator);
                ArrowIpc.export(conn, "transactions", txFile,  allocator);
                duckdbExportMs = System.currentTimeMillis() - exportStart;

                conn.close();
            }
        }

        // ── Phase 2: Pure-Arrow pipeline ─────────────────────────────────────
        long pureArrowParseMs;
        long pureArrowPeakOffHeap;
        long pureArrowTxRows;

        {
            try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
                long parseStart  = System.currentTimeMillis();
                PureArrowIngestor ingestor = new PureArrowIngestor();
                PureArrowIngestResult result =
                        ingestor.ingest(xmlFile, ARROW_OUT, base, allocator, null);
                pureArrowParseMs = System.currentTimeMillis() - parseStart;
                pureArrowPeakOffHeap = allocator.getPeakMemoryAllocation();

                try (PureArrowInMemoryStore store = result.store()) {
                    pureArrowTxRows = store.getTransactionRowCount();
                }
            }
        }

        // Both pipelines must agree on row count
        assertEquals(duckdbTxRows, pureArrowTxRows,
                "Row count mismatch for " + label + ": DuckDB=" + duckdbTxRows
                + " PureArrow=" + pureArrowTxRows);

        return new ComparisonResult(
                label,
                xmlBytes,
                duckdbTxRows,
                duckdbParseInsertMs,
                duckdbExportMs,
                duckdbPeakOffHeap,
                pureArrowParseMs,
                pureArrowPeakOffHeap
        );
    }
}
