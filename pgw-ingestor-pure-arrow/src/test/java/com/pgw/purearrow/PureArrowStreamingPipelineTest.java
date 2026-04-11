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
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the pure-Arrow streaming pipeline: memory behaviour, file existence,
 * IPC round-trip readability, and output path configuration.
 */
class PureArrowStreamingPipelineTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB
    private static final Path OUTPUT_DIR = Paths.get("..", "test-data", "output", "ingestor-pure-arrow");

    @BeforeAll
    static void createOutputDir() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
    }

    @Test
    @DisplayName("Memory is released after store.close() — allocator reaches 0 bytes")
    void memoryIsReleasedAfterIngest() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_d_memory", allocator, null);

            // Before closing the store, some memory may be allocated
            PureArrowInMemoryStore store = result.store();
            store.close();

            long leaked = allocator.getAllocatedMemory();
            assertEquals(0L, leaked,
                    String.format("Arrow allocator should be at 0 bytes after store.close(); got %,d bytes", leaked));
        }
    }

    @Test
    @DisplayName("Row counts from store match ParseStats returned by parser")
    void rowCountsMatchParseStats() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, OUTPUT_DIR, "type_d_counts", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                ParseStats stats = result.parseStats();
                assertEquals(stats.transactionRows(), store.getTransactionRowCount(),
                        "Transaction row count must match ParseStats");
                assertEquals(stats.remittanceRows(), store.getRemittanceRowCount(),
                        "Remittance row count must match ParseStats");
                assertEquals(stats.messageRows(), store.getMessageRowCount(),
                        "Message row count must match ParseStats");
            }
        }
    }

    @Test
    @DisplayName("Two sequential ingests sharing one RootAllocator — no memory leak")
    void multipleIngestsShareAllocator() throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();

            for (int i = 0; i < 2; i++) {
                PureArrowIngestResult result =
                        ingestor.ingest(xmlFile, OUTPUT_DIR, "type_d_multi_" + i, allocator, null);
                result.store().close();

                long leaked = allocator.getAllocatedMemory();
                assertEquals(0L, leaked,
                        String.format("Arrow allocator should be at 0 bytes after ingest %d; got %,d bytes",
                                i + 1, leaked));
            }
        }
    }

    @Test
    @DisplayName("Arrow IPC files are written to the specified output directory")
    void arrowFilesWrittenToOutputDir(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, tempDir, "type_d_outdir", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                assertTrue(result.messageFile().startsWith(tempDir),    "message file must be in tempDir");
                assertTrue(result.remittanceFile().startsWith(tempDir), "remittance file must be in tempDir");
                assertTrue(result.transactionFile().startsWith(tempDir),"transaction file must be in tempDir");
                assertTrue(Files.exists(result.messageFile()),     "message .arrow file must exist");
                assertTrue(Files.exists(result.remittanceFile()),  "remittance .arrow file must exist");
                assertTrue(Files.exists(result.transactionFile()), "transaction .arrow file must exist");
            }
        }
    }

    @Test
    @DisplayName("Arrow IPC files are readable via ArrowStreamReader and row counts match")
    void arrowFilesRoundTrip(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();
            PureArrowIngestResult result =
                    ingestor.ingest(xmlFile, tempDir, "type_d_roundtrip", allocator, null);
            try (PureArrowInMemoryStore store = result.store()) {
                long rmtRows = countRowsInArrowFile(result.remittanceFile(),  allocator);
                long txRows  = countRowsInArrowFile(result.transactionFile(), allocator);
                assertEquals(2L,   rmtRows, "Round-trip: expected 2 remittance rows");
                assertEquals(200L, txRows,  "Round-trip: expected 200 transaction rows");
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
