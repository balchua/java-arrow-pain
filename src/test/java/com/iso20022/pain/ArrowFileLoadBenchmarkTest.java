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
 * Arrow IPC → DuckDB load benchmark test.
 *
 * <p>Simulates a downstream consumer application receiving Arrow IPC files
 * and loading them directly into DuckDB — without any XML parsing.</p>
 */
class ArrowFileLoadBenchmarkTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB
    private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

    record BenchmarkResult(
            String label,
            long arrowFileSizeBytes,
            long loadTimeMs,
            long remittanceRows,
            long transactionRows,
            String notes
    ) {}

    @Test
    @DisplayName("Arrow IPC → DuckDB load benchmark (Type D and Type E)")
    void arrowIpcLoadBenchmark() throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();

        results.add(runBenchmark(TestPainFileSpecs.TYPE_D, "Valid (2×100)"));
        results.add(runBenchmark(TestPainFileSpecs.TYPE_E, "Invalid CtrlSum (2×100)"));

        printBenchmarkReport(results);

        // Assert correctness
        for (BenchmarkResult result : results) {
            assertEquals(2L, result.remittanceRows(),
                    "Expected 2 remittance rows for " + result.label());
            assertEquals(200L, result.transactionRows(),
                    "Expected 200 transaction rows for " + result.label());
            assertTrue(result.loadTimeMs() >= 0,
                    "Load time should be non-negative for " + result.label());
        }
    }

    private BenchmarkResult runBenchmark(PainFileSpec spec, String notes) throws Exception {
        // Step 1: Generate XML if absent
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);

        // Step 2: Parse XML → Arrow → export Arrow IPC files
        Files.createDirectories(OUTPUT_DIR);
        String base = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            try (ArrowBatchResult result = parser.parse(xmlFile, allocator)) {
                ArrowFileExporter.export(result, allocator, OUTPUT_DIR,
                        xmlFile.getFileName().toString());
            }
        }

        // Step 3: Simulate downstream consumer — load Arrow IPC files into DuckDB
        Path msgFile = OUTPUT_DIR.resolve(base + "_message.arrow");
        Path rmtFile = OUTPUT_DIR.resolve(base + "_remittance.arrow");
        Path txFile  = OUTPUT_DIR.resolve(base + "_transaction.arrow");

        long arrowSize = Files.size(msgFile) + Files.size(rmtFile) + Files.size(txFile);

        long remittanceRows;
        long transactionRows;
        long loadTimeMs;

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            // Read Arrow IPC files back into memory
            VectorSchemaRoot msgRoot = readArrowFile(msgFile, allocator);
            List<VectorSchemaRoot> rmtBatches = readArrowFileBatches(rmtFile, allocator);
            List<VectorSchemaRoot> txBatches  = readArrowFileBatches(txFile, allocator);

            ArrowBatchResult reconstructed = new ArrowBatchResult(msgRoot, rmtBatches, txBatches);

            long start = System.currentTimeMillis();
            try (PaymentRepository repository = new PaymentRepositoryImpl(reconstructed, allocator)) {
                loadTimeMs = System.currentTimeMillis() - start;
                remittanceRows = repository.getRemittanceCount();
                transactionRows = repository.getTransactionCount();
            }

            reconstructed.close();
        }

        return new BenchmarkResult(spec.name(), arrowSize, loadTimeMs,
                remittanceRows, transactionRows, notes);
    }

    private static VectorSchemaRoot readArrowFile(Path arrowFile, BufferAllocator allocator)
            throws Exception {
        try (FileInputStream fis = new FileInputStream(arrowFile.toFile());
             FileChannel channel = fis.getChannel();
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {

            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // Transfer to a new root so we can close the reader
            VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
            copy.allocateNew();
            for (int i = 0; i < root.getFieldVectors().size(); i++) {
                root.getFieldVectors().get(i).makeTransferPair(copy.getFieldVectors().get(i)).transfer();
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
                    root.getFieldVectors().get(i).makeTransferPair(copy.getFieldVectors().get(i)).transfer();
                }
                copy.setRowCount(root.getRowCount());
                batches.add(copy);
            }
        }
        return batches;
    }

    private static void printBenchmarkReport(List<BenchmarkResult> results) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║        Arrow IPC → DuckDB Load Benchmark (Downstream Consumer Simulation)   ║");
        System.out.println("╠══════════════╦══════════╦═══════════╦══════════════╦═════════════════════════╣");
        System.out.println("║  File        ║ Arrow KB ║ Load (ms) ║ Rows/sec     ║ Notes                   ║");
        System.out.println("╠══════════════╬══════════╬═══════════╬══════════════╬═════════════════════════╣");
        for (BenchmarkResult r : results) {
            long totalRows = r.remittanceRows() + r.transactionRows();
            long rowsPerSec = r.loadTimeMs() > 0 ? (totalRows * 1000 / r.loadTimeMs()) : totalRows * 1000;
            String arrowKb = String.format("%,d", r.arrowFileSizeBytes() / 1024);
            String loadMs  = String.format("%,d", r.loadTimeMs());
            String rps     = String.format("%,d", rowsPerSec);
            String label   = truncate(r.label().replaceAll(".*\\(", "").replaceAll("\\).*", ""), 12);
            String notes   = truncate(r.notes(), 23);
            System.out.printf("║  %-12s ║ %8s ║ %9s ║ %12s ║ %-23s ║%n",
                    label, arrowKb, loadMs, rps, notes);
        }
        System.out.println("╚══════════════╩══════════╩═══════════╩══════════════╩═════════════════════════╝");
        System.out.println();
    }

    private static String truncate(String s, int maxLen) {
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 1) + "…";
    }
}
