package com.pgw;

import com.pgw.arrow.ArrowBatchResult;
import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.persistence.LocalFilePersistenceService;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation-stage benchmark for all file types A through E.
 *
 * <p>Each run separates two distinct phases:
 * <ol>
 *   <li><b>DuckDB registration</b> — time to register pre-exported Arrow IPC Stream
 *       files into an in-process DuckDB database (zero-copy via Arrow C Data Interface)</li>
 *   <li><b>SQL validation</b> — time to run the full {@link ValidationPipeline} against
 *       the populated DuckDB tables (IBAN MOD-97, BIC, ControlSum, …)</li>
 * </ol>
 *
 * <p>Arrow IPC Stream files are generated on demand if absent (same output directory
 * used by {@code ArrowFileLoadBenchmarkTest} and {@code FullPipelineBenchmarkTest}).
 * The DuckDB load metrics here are specifically in the context of validation, giving
 * you the full latency picture for the validator module:</p>
 *
 * <pre>
 *   .arrows files on disk
 *        ↓  [DuckDB registration time]
 *   in-process DuckDB
 *        ↓  [SQL validation time]
 *   ValidationContext (errors / warnings)
 * </pre>
 */
class ValidationBenchmarkTest {

    private static final long SMALL_ALLOCATOR_LIMIT = 512L * 1024 * 1024;     // 512 MB
    private static final long LARGE_ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024; // 2 GB

    private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

    record ValidationResult(
            String label,
            long totalArrowBytes,
            long remittanceRows,
            long transactionRows,
            long duckdbLoadMs,
            long sqlValidationMs,
            boolean passed,
            int errorCount
    ) {}

    @Test
    @DisplayName("Validation stage benchmark — Arrow→DuckDB load + SQL validation — all types A through E")
    void validationBenchmarkAllTypes() throws Exception {
        List<ValidationResult> results = new ArrayList<>();

        results.add(runValidation(TestPainFileSpecs.TYPE_A, LARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_B, LARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_C, LARGE_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_D, SMALL_ALLOCATOR_LIMIT));
        results.add(runValidation(TestPainFileSpecs.TYPE_E, SMALL_ALLOCATOR_LIMIT));

        printReport(results);

        // Correctness assertions
        assertEquals(1_000_000L, results.get(0).transactionRows(), "Type A: expected 1M tx rows");
        assertEquals(1_000_000L, results.get(1).transactionRows(), "Type B: expected 1M tx rows");
        assertEquals(1_000_000L, results.get(2).transactionRows(), "Type C: expected 1M tx rows");
        assertEquals(200L,       results.get(3).transactionRows(), "Type D: expected 200 tx rows");
        assertEquals(200L,       results.get(4).transactionRows(), "Type E: expected 200 tx rows");

        assertTrue(results.get(3).passed(),  "Type D should pass validation");
        assertFalse(results.get(4).passed(), "Type E should fail validation (invalid CtrlSum)");
    }

    // -------------------------------------------------------------------------
    // Core benchmark logic
    // -------------------------------------------------------------------------

    private ValidationResult runValidation(PainFileSpec spec, long allocatorLimit)
            throws Exception {
        // Step 1: Ensure Arrow IPC Stream files exist (generate XML + parse if absent)
        Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
        Files.createDirectories(OUTPUT_DIR);

        String base = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
        Path msgFile = OUTPUT_DIR.resolve(base + "_message.arrows");
        Path rmtFile = OUTPUT_DIR.resolve(base + "_remittance.arrows");
        Path txFile  = OUTPUT_DIR.resolve(base + "_transaction.arrows");

        if (!Files.exists(msgFile) || !Files.exists(rmtFile) || !Files.exists(txFile)) {
            generateArrowFiles(spec, xmlFile, base, allocatorLimit);
        }

        long totalArrowBytes = Files.size(msgFile) + Files.size(rmtFile) + Files.size(txFile);

        // Step 2: Read Arrow IPC Stream files from disk
        try (BufferAllocator allocator = new RootAllocator(allocatorLimit)) {

            VectorSchemaRoot           msgRoot    = readArrowFile(msgFile, allocator);
            List<VectorSchemaRoot> rmtBatches = readArrowFileBatches(rmtFile, allocator);
            List<VectorSchemaRoot> txBatches  = readArrowFileBatches(txFile, allocator);
            ArrowBatchResult arrowData = new ArrowBatchResult(msgRoot, rmtBatches, txBatches);

            long remittanceRows;
            long transactionRows;
            long duckdbLoadMs;
            long sqlValidationMs;
            boolean passed;
            int errorCount;

            // Step 3: Time DuckDB registration from Arrow IPC Stream files
            long duckdbStart = System.currentTimeMillis();
            try (PaymentRepository repository = new PaymentRepositoryImpl(arrowData, allocator)) {
                duckdbLoadMs = System.currentTimeMillis() - duckdbStart;

                remittanceRows  = repository.getRemittanceCount();
                transactionRows = repository.getTransactionCount();

                // Step 4: Time SQL validation against the loaded DuckDB
                long valStart = System.nanoTime();
                ValidationContext ctx = ValidationPipeline.standard().execute(repository);
                sqlValidationMs = (System.nanoTime() - valStart) / 1_000_000L;

                passed     = !ctx.hasErrors();
                errorCount = ctx.hasErrors() ? ctx.getErrors().size() : 0;
            }

            arrowData.close();
            return new ValidationResult(
                    spec.name().replaceAll("\\s*\\(.*", "").trim(),
                    totalArrowBytes,
                    remittanceRows, transactionRows,
                    duckdbLoadMs, sqlValidationMs,
                    passed, errorCount);
        }
    }

    // -------------------------------------------------------------------------
    // Arrow IPC Stream file generation (if not already present)
    // -------------------------------------------------------------------------

    private void generateArrowFiles(PainFileSpec spec, Path xmlFile,
            String base, long allocatorLimit) throws Exception {
        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        try (BufferAllocator allocator = new RootAllocator(allocatorLimit)) {
            DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            try (var stmt = conn.createStatement()) {
                stmt.execute("SET memory_limit='1GB'");
            }
            LocalFilePersistenceService persistence = new LocalFilePersistenceService(
                    OUTPUT_DIR, base, msgSchema, rmtSchema, txSchema);
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, persistence, allocator);
            PainParser parser = new PainParserImpl();
            parser.parseStreaming(xmlFile, allocator, consumer);
            persistence.finish();
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Arrow IPC Stream readers
    // -------------------------------------------------------------------------

    private static VectorSchemaRoot readArrowFile(Path file, BufferAllocator allocator)
            throws Exception {
        try (FileInputStream fis = new FileInputStream(file.toFile());
             ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
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

    private static List<VectorSchemaRoot> readArrowFileBatches(Path file,
            BufferAllocator allocator) throws Exception {
        List<VectorSchemaRoot> batches = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file.toFile());
             ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                if (root.getRowCount() == 0) continue;
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

    private static void printReport(List<ValidationResult> results) {
        final String SEP =
            "╠══════════╦════════════╦══════════════╦══════════════╦═══════════╦══════════════╦═══════════╣";
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║         Validation Stage Benchmark — Arrow IPC → DuckDB Registration + SQL Validation               ║");
        System.out.println(SEP.replace('╠', '╠').replace('╣', '╣'));
        System.out.println("║  Type    ║ Arrow (KB) ║ DuckDB ms    ║ Validate ms  ║  Tx Rows  ║ rows/ms (val)║  Result   ║");
        System.out.println(SEP.replace('╦', '╬'));
        long totalDuckdbMs   = 0;
        long totalValidateMs = 0;
        long totalTxRows     = 0;
        long totalArrowKb    = 0;
        for (ValidationResult r : results) {
            double rowsPerMs = r.sqlValidationMs() > 0
                    ? (double) r.transactionRows() / r.sqlValidationMs()
                    : (double) r.transactionRows();
            String resultStr = r.passed()
                    ? "✓ PASSED"
                    : String.format("✗ %d err", r.errorCount());
            System.out.printf("║  %-8s ║ %10s ║ %12s ║ %12s ║ %9s ║ %12s ║ %-9s ║%n",
                    r.label(),
                    String.format("%,d", r.totalArrowBytes() / 1024),
                    String.format("%,d", r.duckdbLoadMs()),
                    String.format("%,d", r.sqlValidationMs()),
                    String.format("%,d", r.transactionRows()),
                    String.format("%,.0f", rowsPerMs),
                    resultStr);
            totalDuckdbMs   += r.duckdbLoadMs();
            totalValidateMs += r.sqlValidationMs();
            totalTxRows     += r.transactionRows();
            totalArrowKb    += r.totalArrowBytes() / 1024;
        }
        System.out.println("╠══════════╬════════════╬══════════════╬══════════════╬═══════════╬══════════════╬═══════════╣");
        double totalRowsPerMs = totalValidateMs > 0
                ? (double) totalTxRows / totalValidateMs : (double) totalTxRows;
        System.out.printf("║  %-8s ║ %10s ║ %12s ║ %12s ║ %9s ║ %12s ║ %-9s ║%n",
                "TOTAL",
                String.format("%,d", totalArrowKb),
                String.format("%,d", totalDuckdbMs),
                String.format("%,d", totalValidateMs),
                String.format("%,d", totalTxRows),
                String.format("%,.0f", totalRowsPerMs),
                "—");
        System.out.println("╚══════════╩════════════╩══════════════╩══════════════╩═══════════╩══════════════╩═══════════╝");
        System.out.println();
        System.out.println("  Arrow (KB)     = total size of the three .arrows files on disk (message + remittance + transaction)");
        System.out.println("  DuckDB ms      = time to register Arrow IPC Stream files into in-process DuckDB (zero-copy ABI)");
        System.out.println("  Validate ms    = time to run ValidationPipeline.standard() against the populated DuckDB tables");
        System.out.println("  rows/ms (val)  = transaction row scan throughput during validation");
        System.out.println();
    }
}
