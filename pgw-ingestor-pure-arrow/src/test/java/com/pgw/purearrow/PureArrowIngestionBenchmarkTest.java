package com.pgw.purearrow;

import com.pgw.benchmark.LoadBenchmark;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import org.apache.arrow.memory.RootAllocator;
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
 * Ingestion benchmark for the pure-Arrow pipeline: XML → StAX parse → {@link PureArrowBatchConsumer}
 * → Arrow IPC stream files. No DuckDB is involved.
 *
 * <p>For each type (A–G) this test measures:
 * <ol>
 *   <li><b>XML→Arrow Parse ms</b> — wall-clock time for StAX streaming parse + direct Arrow write</li>
 *   <li><b>Peak off-heap bytes</b> — maximum Arrow allocator off-heap bytes observed per batch</li>
 *   <li><b>Combined .arrow file size on disk</b></li>
 *   <li><b>Row counts</b> (message, remittance, transaction)</li>
 * </ol>
 */
class PureArrowIngestionBenchmarkTest {

    private static final long LARGE_ALLOCATOR_LIMIT  = 2L * 1024 * 1024 * 1024L; //  2 GB
    private static final long XLARGE_ALLOCATOR_LIMIT = 4L * 1024 * 1024 * 1024L; //  4 GB

    private static final Path OUTPUT_DIR = Paths.get("target", "pure-arrow-test-output", "benchmark");

    private static final List<BenchmarkResult> RESULTS = new ArrayList<>();

    record BenchmarkResult(
            String label,
            long xmlBytes,
            long parseMs,
            long peakOffHeapBytes,
            long messageArrowBytes,
            long remittanceArrowBytes,
            long transactionArrowBytes,
            long messageRows,
            long remittanceRows,
            long transactionRows
    ) {
        long totalArrowBytes() {
            return messageArrowBytes + remittanceArrowBytes + transactionArrowBytes;
        }
    }

    @BeforeAll
    static void createOutputDir() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
    }

    @Test
    @DisplayName("Pure Arrow benchmark — XML → Arrow IPC — all types A through J")
    void ingestionBenchmarkAllTypes() throws Exception {
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_D, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_E, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_F, XLARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_G, XLARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_H, LARGE_ALLOCATOR_LIMIT));
        RESULTS.add(runBenchmark(TestPainFileSpecs.TYPE_J, LARGE_ALLOCATOR_LIMIT));

        // Correctness assertions
        assertEquals(1_000_000L, RESULTS.get(0).transactionRows(), "Type A: expected 1M tx rows");
        assertEquals(1_000_000L, RESULTS.get(1).transactionRows(), "Type B: expected 1M tx rows");
        assertEquals(1_000_000L, RESULTS.get(2).transactionRows(), "Type C: expected 1M tx rows");
        assertEquals(200L,       RESULTS.get(3).transactionRows(), "Type D: expected 200 tx rows");
        assertEquals(200L,       RESULTS.get(4).transactionRows(), "Type E: expected 200 tx rows");
        assertEquals(2_000_000L, RESULTS.get(5).transactionRows(), "Type F: expected 2M tx rows");
        assertEquals(4_000_000L, RESULTS.get(6).transactionRows(), "Type G: expected 4M tx rows");
        assertEquals(2_000L,     RESULTS.get(7).transactionRows(), "Type H: expected 2000 tx rows");
        assertEquals(2_000L,     RESULTS.get(8).transactionRows(), "Type J: expected 2000 tx rows");

        assertEquals(1L,         RESULTS.get(0).messageRows(),     "Type A: expected 1 message row");
        assertEquals(1L,         RESULTS.get(1).messageRows(),     "Type B: expected 1 message row");
        assertEquals(1L,         RESULTS.get(2).messageRows(),     "Type C: expected 1 message row");
        assertEquals(1L,         RESULTS.get(3).messageRows(),     "Type D: expected 1 message row");
        assertEquals(1L,         RESULTS.get(4).messageRows(),     "Type E: expected 1 message row");
        assertEquals(1L,         RESULTS.get(5).messageRows(),     "Type F: expected 1 message row");
        assertEquals(1L,         RESULTS.get(6).messageRows(),     "Type G: expected 1 message row");
        assertEquals(1L,         RESULTS.get(7).messageRows(),     "Type H: expected 1 message row");
        assertEquals(1L,         RESULTS.get(8).messageRows(),     "Type J: expected 1 message row");

        assertEquals(1L,         RESULTS.get(0).remittanceRows(),  "Type A: expected 1 remittance row");
        assertEquals(2L,         RESULTS.get(1).remittanceRows(),  "Type B: expected 2 remittance rows");
        assertEquals(1_000_000L, RESULTS.get(2).remittanceRows(),  "Type C: expected 1M remittance rows");
        assertEquals(2L,         RESULTS.get(3).remittanceRows(),  "Type D: expected 2 remittance rows");
        assertEquals(2L,         RESULTS.get(4).remittanceRows(),  "Type E: expected 2 remittance rows");
        assertEquals(1L,         RESULTS.get(5).remittanceRows(),  "Type F: expected 1 remittance row");
        assertEquals(1L,         RESULTS.get(6).remittanceRows(),  "Type G: expected 1 remittance row");
        assertEquals(10L,        RESULTS.get(7).remittanceRows(),  "Type H: expected 10 remittance rows");
        assertEquals(5L,         RESULTS.get(8).remittanceRows(),  "Type J: expected 5 remittance rows");

        for (BenchmarkResult r : RESULTS) {
            assertTrue(r.parseMs() >= 0,          "Parse time must be non-negative: "  + r.label());
            assertTrue(r.totalArrowBytes() > 0,   "Arrow files must be non-empty: "    + r.label());
        }
    }

    @AfterAll
    static void printSummaryTable() {
        if (RESULTS.isEmpty()) return;

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║   PURE ARROW INGESTION BENCHMARK SUMMARY (no DuckDB)                                   ║");
        System.out.println("╠══════════╦═══════════╦══════════════╦════════════════╦═══════════╦═════════╦══════════╣");
        System.out.println("║  Type    ║  XML (MB) ║  Parse (ms)  ║ Peak Off-Heap  ║  Tx Rows  ║ MB/sec  ║Arrow(MB) ║");
        System.out.println("╠══════════╬═══════════╬══════════════╬════════════════╬═══════════╬═════════╬══════════╣");

        for (BenchmarkResult r : RESULTS) {
            double xmlMb = r.xmlBytes() / (1024.0 * 1024.0);
            double peakMb = r.peakOffHeapBytes() / (1024.0 * 1024.0);
            double arrowMb = r.totalArrowBytes() / (1024.0 * 1024.0);
            double mbPerSec = r.parseMs() > 0
                    ? xmlMb / (r.parseMs() / 1000.0)
                    : 0.0;
            System.out.printf("║  %-7s ║  %,7.1f  ║  %,10d  ║  %,9.1f MB ║ %,9d ║ %,7.1f ║ %,7.1f ║%n",
                    r.label(),
                    xmlMb,
                    r.parseMs(),
                    peakMb,
                    r.transactionRows(),
                    mbPerSec,
                    arrowMb);
        }

        System.out.println("╚══════════╩═══════════╩══════════════╩════════════════╩═══════════╩═════════╩══════════╝");
        System.out.println();
        System.out.println("  XML (MB)        = source XML file size on disk");
        System.out.println("  Parse (ms)      = StAX streaming parse + direct ArrowStreamWriter write");
        System.out.println("  Peak Off-Heap   = max Arrow allocator bytes while all ingested batches are in PureArrowInMemoryStore");
        System.out.println("                    NOTE: larger than DuckDB path because ALL batches are kept in memory until store.close()");
        System.out.println("  Tx Rows         = transaction rows ingested");
        System.out.println("  MB/sec          = parse throughput (XML MB / parse seconds)");
        System.out.println("  Arrow (MB)      = combined size of the 3 exported .arrow files on disk");
        System.out.println();
    }

    // ── Core benchmark logic ──────────────────────────────────────────────────

    private static BenchmarkResult runBenchmark(PainFileSpec spec, long allocatorLimit) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        long xmlBytes = Files.size(xmlFile);

        String base = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        String label = spec.name().replaceAll("\\s*\\(.*", "").trim();

        LoadBenchmark benchmark = new LoadBenchmark(label);
        benchmark.setXmlFileSizeBytes(xmlBytes);
        benchmark.setHeapUsedBeforeBytes(LoadBenchmark.captureHeapUsed());
        benchmark.setHeapMaxBytes(Runtime.getRuntime().maxMemory());
        benchmark.setOffHeapLimitBytes(allocatorLimit);

        long parseMs;
        long remittanceRows;
        long transactionRows;
        long messageRows;

        try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
            long parseStart = System.currentTimeMillis();
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, base, allocator, benchmark);
            parseMs = System.currentTimeMillis() - parseStart;

            benchmark.recordPhase("XML→Arrow Parse", Duration.ofMillis(parseMs));
            benchmark.setHeapUsedAfterBytes(LoadBenchmark.captureHeapUsed());
            benchmark.setOffHeapAllocatedBytes(allocator.getAllocatedMemory());
            benchmark.setOffHeapPeakBytes(allocator.getPeakMemoryAllocation());

            try (PureArrowInMemoryStore store = result.store()) {
                messageRows    = store.getMessageRowCount();
                remittanceRows = store.getRemittanceRowCount();
                transactionRows = store.getTransactionRowCount();

                benchmark.setMessageRows(messageRows);
                benchmark.setRemittanceRows(remittanceRows);
                benchmark.setTotalRows(transactionRows);

                long totalArrow = 0;
                if (result.messageFile()     != null) totalArrow += Files.size(result.messageFile());
                if (result.remittanceFile()  != null) totalArrow += Files.size(result.remittanceFile());
                if (result.transactionFile() != null) totalArrow += Files.size(result.transactionFile());
                benchmark.setArrowFileSizeBytes(totalArrow);

                System.out.println(benchmark.toReport());

                return new BenchmarkResult(
                        label,
                        xmlBytes,
                        parseMs,
                        benchmark.getOffHeapStreamingPeakBytes(),
                        result.messageFile()     != null ? Files.size(result.messageFile())     : 0,
                        result.remittanceFile()  != null ? Files.size(result.remittanceFile())  : 0,
                        result.transactionFile() != null ? Files.size(result.transactionFile()) : 0,
                        messageRows,
                        remittanceRows,
                        transactionRows
                );
            }
        }
    }
}
