package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.ArrowFileExporter;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.PainFileSpec;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Arrow IPC to DuckDB load benchmark test - all file types A through E.
 *
 * <p>For each type, this test:</p>
 * <ol>
 *   <li>Generates the XML if absent</li>
 *   <li>Parses XML to Arrow and exports the three Arrow IPC files
 *       (message, remittance, transaction)</li>
 *   <li>Simulates a downstream consumer: reads the .arrow files back from disk
 *       and loads them directly into a fresh DuckDB in-process database</li>
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
    @DisplayName("Arrow IPC to DuckDB load benchmark - all types A through E")
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

        // Step 2: Parse XML to Arrow and export three Arrow IPC files
        Files.createDirectories(OUTPUT_DIR);
        try (BufferAllocator allocator = new RootAllocator(allocatorLimit)) {
            PainParser parser = new PainParserImpl();
            try (ArrowBatchResult result = parser.parse(xmlFile, allocator)) {
                ArrowFileExporter.export(result, allocator, OUTPUT_DIR,
                        xmlFile.getFileName().toString());
            }
        }

        // Step 3: Resolve the three exported Arrow IPC files
        String base   = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        Path msgFile  = OUTPUT_DIR.resolve(base + "_message.arrow");
        Path rmtFile  = OUTPUT_DIR.resolve(base + "_remittance.arrow");
        Path txFile   = OUTPUT_DIR.resolve(base + "_transaction.arrow");

        long msgBytes = Files.size(msgFile);
        long rmtBytes = Files.size(rmtFile);
        long txBytes  = Files.size(txFile);

        // Step 4: Simulate downstream consumer - read Arrow files then load into DuckDB
        long remittanceRows;
        long transactionRows;
        long loadTimeMs;

        try (BufferAllocator allocator = new RootAllocator(allocatorLimit)) {
            VectorSchemaRoot           msgRoot    = readArrowFile(msgFile, allocator);
            List<VectorSchemaRoot> rmtBatches = readArrowFileBatches(rmtFile, allocator);
            List<VectorSchemaRoot> txBatches  = readArrowFileBatches(txFile, allocator);

            ArrowBatchResult reconstructed =
                    new ArrowBatchResult(msgRoot, rmtBatches, txBatches);

            long start = System.currentTimeMillis();
            try (PaymentRepository repo = new PaymentRepositoryImpl(reconstructed, allocator)) {
                loadTimeMs      = System.currentTimeMillis() - start;
                remittanceRows  = repo.getRemittanceCount();
                transactionRows = repo.getTransactionCount();
            }
            reconstructed.close();
        }

        return new BenchmarkResult(spec.name(), msgBytes, rmtBytes, txBytes,
                loadTimeMs, remittanceRows, transactionRows);
    }

    // -------------------------------------------------------------------------
    // Arrow file readers
    // -------------------------------------------------------------------------

    private static VectorSchemaRoot readArrowFile(Path arrowFile, BufferAllocator allocator)
            throws Exception {
        try (FileInputStream fis = new FileInputStream(arrowFile.toFile());
             FileChannel channel = fis.getChannel();
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {

            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
            copy.allocateNew();
            for (int i = 0; i < root.getFieldVectors().size(); i++) {
                root.getFieldVectors().get(i)
                    .makeTransferPair(copy.getFieldVectors().get(i)).transfer();
            }
            copy.setRowCount(root.getRowCount());
            return copy;
        }
    }

    private static List<VectorSchemaRoot> readArrowFileBatches(Path arrowFile,
            BufferAllocator allocator) throws Exception {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(arrowFile.toFile());
             FileChannel channel = fis.getChannel();
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {

            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
                copy.allocateNew();
                for (int i = 0; i < root.getFieldVectors().size(); i++) {
                    root.getFieldVectors().get(i)
                        .makeTransferPair(copy.getFieldVectors().get(i)).transfer();
                }
                copy.setRowCount(root.getRowCount());
                batches.add(copy);
            }
        }
        return batches;
    }

    // -------------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------------

    private static void printBenchmarkReport(List<BenchmarkResult> results) {
        final String LINE =
            "╠══════════╦═══════════╦═══════════╦════════════╦═══════════╦════════════╦══════════════╦═════════════╣";
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║           Arrow IPC -> DuckDB Load Benchmark - All Types (Downstream Consumer Simulation)               ║");
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
