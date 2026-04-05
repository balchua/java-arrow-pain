package com.pgw;

import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.BatchConsumer;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.ParseStats;
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
 * Ingestion benchmark: XML → StAX parse → {@link StreamingBatchConsumer} → DuckDB →
 * {@link ArrowIpc#export(DuckDBConnection, String, Path, BufferAllocator)}.
 *
 * <p>For each type (A–E) this test measures:
 * <ol>
 *   <li><b>Parse + INSERT ms</b> — wall-clock time for StAX streaming parse and
 *       all {@code registerArrowStream} + {@code INSERT INTO … SELECT} calls via
 *       {@link StreamingBatchConsumer}</li>
 *   <li><b>Export ms</b> — time to call {@link ArrowIpc#export} for all three tables
 *       (message, remittance, transactions) using the C Data Interface
 *       ({@code DuckDBResultSet.arrowExportStream} + {@code ArrowStreamWriter})</li>
 *   <li><b>Peak off-heap (bytes)</b> — maximum Arrow allocator off-heap bytes observed
 *       during the parse + INSERT phase (sampled after every batch)</li>
 *   <li><b>Arrow file size (MB)</b> — combined on-disk size of the three exported files</li>
 * </ol>
 *
 * <p>Arrow files are reused across test runs; delete the output directory to force regeneration.
 */
class IngestionBenchmarkTest {

    private static final long SMALL_ALLOCATOR_LIMIT  = 512L  * 1024 * 1024;       //  512 MB
    private static final long LARGE_ALLOCATOR_LIMIT  = 2L   * 1024 * 1024 * 1024L; //   2 GB
    private static final long XLARGE_ALLOCATOR_LIMIT = 4L   * 1024 * 1024 * 1024L; //   4 GB

    private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

    record IngestionResult(
            String label,
            long xmlBytes,
            long parseAndInsertMs,
            long arrowExportMs,
            long peakOffHeapBytes,
            long messageArrowBytes,
            long remittanceArrowBytes,
            long transactionArrowBytes,
            long remittanceRows,
            long transactionRows
    ) {
        long totalArrowBytes() {
            return messageArrowBytes + remittanceArrowBytes + transactionArrowBytes;
        }
    }

    @Test
    @DisplayName("Ingestion benchmark — XML → DuckDB INSERT → ArrowIpc.export() — all types A through G")
    void ingestionBenchmarkAllTypes() throws Exception {
        List<IngestionResult> results = new ArrayList<>();

        results.add(runIngestion(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        results.add(runIngestion(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        results.add(runIngestion(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        results.add(runIngestion(TestPainFileSpecs.TYPE_D, SMALL_ALLOCATOR_LIMIT));
        results.add(runIngestion(TestPainFileSpecs.TYPE_E, SMALL_ALLOCATOR_LIMIT));
        results.add(runIngestion(TestPainFileSpecs.TYPE_F, XLARGE_ALLOCATOR_LIMIT));
        results.add(runIngestion(TestPainFileSpecs.TYPE_G, XLARGE_ALLOCATOR_LIMIT));

        printReport(results);

        // Correctness assertions
        assertEquals(1_000_000L, results.get(0).transactionRows(), "Type A: expected 1M tx rows");
        assertEquals(1_000_000L, results.get(1).transactionRows(), "Type B: expected 1M tx rows");
        assertEquals(1_000_000L, results.get(2).transactionRows(), "Type C: expected 1M tx rows");
        assertEquals(200L,       results.get(3).transactionRows(), "Type D: expected 200 tx rows");
        assertEquals(200L,       results.get(4).transactionRows(), "Type E: expected 200 tx rows");
        assertEquals(2_000_000L, results.get(5).transactionRows(), "Type F: expected 2M tx rows");
        assertEquals(4_000_000L, results.get(6).transactionRows(), "Type G: expected 4M tx rows");

        for (IngestionResult r : results) {
            assertTrue(r.parseAndInsertMs() >= 0, "Parse+insert time must be non-negative: " + r.label());
            assertTrue(r.arrowExportMs()    >= 0, "Export time must be non-negative: "        + r.label());
            assertTrue(r.totalArrowBytes()  > 0,  "Arrow files must be non-empty: "           + r.label());
        }
    }

    // -------------------------------------------------------------------------
    // Core benchmark logic
    // -------------------------------------------------------------------------

    private IngestionResult runIngestion(PainFileSpec spec, long allocatorLimit) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        Files.createDirectories(OUTPUT_DIR);

        String base = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        Path msgFile = OUTPUT_DIR.resolve(base + "_message.arrow");
        Path rmtFile = OUTPUT_DIR.resolve(base + "_remittance.arrow");
        Path txFile  = OUTPUT_DIR.resolve(base + "_transaction.arrow");

        long xmlBytes = Files.size(xmlFile);
        long peakOffHeap;
        long remittanceRows;
        long transactionRows;
        long parseAndInsertMs;
        long arrowExportMs;

        String duckDbMemoryLimit = allocatorLimit >= XLARGE_ALLOCATOR_LIMIT ? "2GB" : "1GB";

        try (RootAllocator allocator = new RootAllocator(allocatorLimit)) {
            DuckDBConnection conn = DuckDbFactory.newConnection();
            try (var stmt = conn.createStatement()) {
                stmt.execute("SET memory_limit='" + duckDbMemoryLimit + "'");
            }

            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);

            // Wrap consumer to track peak off-heap Arrow allocator usage per batch
            final long[] peakHolder = {0};
            BatchConsumer trackingConsumer = (tableType, root) -> {
                consumer.accept(tableType, root);
                long current = allocator.getAllocatedMemory();
                if (current > peakHolder[0]) peakHolder[0] = current;
            };

            // Phase 1: XML → StAX streaming parse → DuckDB INSERT (one INSERT per 65k-row batch)
            long parseStart = System.currentTimeMillis();
            PainParser parser = new PainParserImpl();
            ParseStats stats = parser.parseStreaming(xmlFile, allocator, trackingConsumer);
            parseAndInsertMs = System.currentTimeMillis() - parseStart;
            peakOffHeap = peakHolder[0];

            remittanceRows  = stats.remittanceRows();
            transactionRows = stats.transactionRows();

            // Phase 2: DuckDB → Arrow IPC files (C Data Interface — no extension)
            long exportStart = System.currentTimeMillis();
            ArrowIpc.export(conn, "message",      msgFile, allocator);
            ArrowIpc.export(conn, "remittance",   rmtFile, allocator);
            ArrowIpc.export(conn, "transactions", txFile,  allocator);
            arrowExportMs = System.currentTimeMillis() - exportStart;

            conn.close();
        }

        return new IngestionResult(
                spec.name().replaceAll("\\s*\\(.*", "").trim(),
                xmlBytes,
                parseAndInsertMs,
                arrowExportMs,
                peakOffHeap,
                Files.size(msgFile),
                Files.size(rmtFile),
                Files.size(txFile),
                remittanceRows,
                transactionRows
        );
    }

    // -------------------------------------------------------------------------
    // Console report
    // -------------------------------------------------------------------------

    private static final String HDR_SEP =
            "╠══════════╬══════════╬══════════════╬════════════╬════════════════╬════════════╬═══════════╣";

    private static void printReport(List<IngestionResult> results) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║       Ingestion Benchmark — XML → StAX Parse → DuckDB INSERT → ArrowIpc.export()                       ║");
        System.out.println(HDR_SEP);
        System.out.println("║  Type    ║ XML (MB) ║ Parse+Ins ms ║ Export ms  ║ Peak Off-Heap  ║ Arrow (MB) ║  Tx Rows  ║");
        System.out.println(HDR_SEP.replace('╠', '╠').replace('╣', '╣'));

        long totalXmlBytes   = 0;
        long totalArrowBytes = 0;
        long totalTxRows     = 0;
        long totalParseMs    = 0;
        long totalExportMs   = 0;
        long maxPeakBytes    = 0;

        for (IngestionResult r : results) {
            long txPerMs = r.parseAndInsertMs() > 0 ? r.transactionRows() / r.parseAndInsertMs() : 0;
            System.out.printf(
                    "║  %-7s ║ %,7.1f  ║   %,9d  ║  %,8d  ║  %,11d   ║ %,9.2f ║ %,9d ║%n",
                    r.label(),
                    r.xmlBytes()       / (1024.0 * 1024.0),
                    r.parseAndInsertMs(),
                    r.arrowExportMs(),
                    r.peakOffHeapBytes(),
                    r.totalArrowBytes() / (1024.0 * 1024.0),
                    r.transactionRows()
            );
            totalXmlBytes   += r.xmlBytes();
            totalArrowBytes += r.totalArrowBytes();
            totalTxRows     += r.transactionRows();
            totalParseMs    += r.parseAndInsertMs();
            totalExportMs   += r.arrowExportMs();
            if (r.peakOffHeapBytes() > maxPeakBytes) maxPeakBytes = r.peakOffHeapBytes();
        }

        System.out.println(HDR_SEP);
        long totalTxPerMs = totalParseMs > 0 ? totalTxRows / totalParseMs : 0;
        System.out.printf(
                "║  TOTAL   ║ %,7.1f  ║   %,9d  ║  %,8d  ║  %,11d   ║ %,9.2f ║ %,9d ║%n",
                totalXmlBytes   / (1024.0 * 1024.0),
                totalParseMs,
                totalExportMs,
                maxPeakBytes,
                totalArrowBytes / (1024.0 * 1024.0),
                totalTxRows
        );
        System.out.println("╚══════════╩══════════╩══════════════╩════════════╩════════════════╩════════════╩═══════════╝");
        System.out.println();
        System.out.println("  XML (MB)       = source XML file size on disk");
        System.out.println("  Parse+Ins ms   = StAX streaming parse + StreamingBatchConsumer INSERT into DuckDB");
        System.out.println("  Export ms      = ArrowIpc.export() for all 3 tables (C Data Interface, no extension)");
        System.out.println("  Peak Off-Heap  = peak Arrow allocator off-heap bytes during parse+insert phase");
        System.out.println("  Arrow (MB)     = combined size of the 3 exported .arrow files on disk");
        System.out.println("  Tx Rows        = transaction rows ingested");
        System.out.println();
    }
}
