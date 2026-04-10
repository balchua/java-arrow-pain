package com.pgw.purearrow;

import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.ParseStats;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional correctness tests for the pure-Arrow parse pipeline.
 *
 * <p>Uses Type D (2 PmtInf × 100 TxInf — valid) and Type E (invalid CtrlSum)
 * to verify row counts, IPC file creation, and round-trip readability.</p>
 */
class PureArrowParsePipelineTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB
    private static final Path OUTPUT_DIR = Paths.get("target", "pure-arrow-test-output");

    @BeforeAll
    static void createOutputDir() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
    }

    // ── Type D tests ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("Type D: row counts correct (1 msg, 2 rmt, 200 tx)")
    void parseTypeD_rowCountsCorrect() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_d_pipeline", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                assertEquals(1L,   store.getMessageRowCount(),     "Type D: expected 1 message row");
                assertEquals(2L,   store.getRemittanceRowCount(),  "Type D: expected 2 remittance rows");
                assertEquals(200L, store.getTransactionRowCount(), "Type D: expected 200 transaction rows");
                assertEquals(1L,   result.parseStats().messageRows(),     "ParseStats: expected 1 message row");
                assertEquals(2L,   result.parseStats().remittanceRows(),  "ParseStats: expected 2 remittance rows");
                assertEquals(200L, result.parseStats().transactionRows(), "ParseStats: expected 200 transaction rows");
            }
        }
    }

    @Test
    @DisplayName("Type D: all three .arrow IPC files created and non-empty")
    void parseTypeD_arrowFilesCreated() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_d_files", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                assertNotNull(result.messageFile(),    "message file path must not be null");
                assertNotNull(result.remittanceFile(), "remittance file path must not be null");
                assertNotNull(result.transactionFile(),"transaction file path must not be null");
                assertTrue(Files.exists(result.messageFile()),     "message .arrow file must exist");
                assertTrue(Files.exists(result.remittanceFile()),  "remittance .arrow file must exist");
                assertTrue(Files.exists(result.transactionFile()), "transaction .arrow file must exist");
                assertTrue(Files.size(result.messageFile())     > 0, "message .arrow file must not be empty");
                assertTrue(Files.size(result.remittanceFile())  > 0, "remittance .arrow file must not be empty");
                assertTrue(Files.size(result.transactionFile()) > 0, "transaction .arrow file must not be empty");
            }
        }
    }

    @Test
    @DisplayName("Type D: .arrow files readable via ArrowStreamReader with correct row counts")
    void parseTypeD_arrowFilesReadable() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_d_roundtrip", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                assertEquals(1L,   countRowsInArrowFile(result.messageFile(),     allocator), "Round-trip: message rows");
                assertEquals(2L,   countRowsInArrowFile(result.remittanceFile(),  allocator), "Round-trip: remittance rows");
                assertEquals(200L, countRowsInArrowFile(result.transactionFile(), allocator), "Round-trip: transaction rows");
            }
        }
    }

    // ── Type E tests ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("Type E: row counts correct (invalid CtrlSum doesn't change row counts)")
    void parseTypeE_rowCountsCorrect() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_e_pipeline", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                assertEquals(1L,   store.getMessageRowCount(),     "Type E: expected 1 message row");
                assertEquals(2L,   store.getRemittanceRowCount(),  "Type E: expected 2 remittance rows");
                assertEquals(200L, store.getTransactionRowCount(), "Type E: expected 200 transaction rows");
            }
        }
    }

    @Test
    @DisplayName("Type E: .arrow IPC files created for invalid CtrlSum file too")
    void parseTypeE_arrowFilesCreated() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_e_files", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                assertTrue(Files.exists(result.messageFile()),     "Type E: message .arrow file must exist");
                assertTrue(Files.exists(result.remittanceFile()),  "Type E: remittance .arrow file must exist");
                assertTrue(Files.exists(result.transactionFile()), "Type E: transaction .arrow file must exist");
                assertTrue(Files.size(result.messageFile())     > 0, "Type E: message .arrow file must not be empty");
                assertTrue(Files.size(result.remittanceFile())  > 0, "Type E: remittance .arrow file must not be empty");
                assertTrue(Files.size(result.transactionFile()) > 0, "Type E: transaction .arrow file must not be empty");
            }
        }
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static long countRowsInArrowFile(Path file, BufferAllocator allocator) throws Exception {
        long totalRows = 0;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file.toFile()));
             ArrowStreamReader reader = new ArrowStreamReader(bis, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                totalRows += root.getRowCount();
            }
        }
        return totalRows;
    }
}
