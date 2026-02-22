package com.iso20022.pain;

import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.BatchConsumer;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.parser.ParseStats;
import com.iso20022.pain.parser.StreamingBatchConsumer;
import com.iso20022.pain.persistence.LocalFilePersistenceService;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the streaming pipeline: memory footprint, DuckDB row count correctness,
 * .arrows file readability, and output directory configuration.
 */
class StreamingPipelineTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Streaming pipeline: memory footprint stays flat (≤ 2× batch budget)")
    void testMemoryFootprintIsFlat(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        long peakOffHeap = 0;

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection)
                    DriverManager.getConnection("jdbc:duckdb:");

            try (LocalFilePersistenceService persistence =
                    new LocalFilePersistenceService(tempDir, "test", msgSchema, rmtSchema, txSchema)) {

                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);

                final long[] peak = {0};
                BatchConsumer wrapping = (tableType, root) -> {
                    consumer.accept(tableType, root);
                    long current = allocator.getAllocatedMemory();
                    if (current > peak[0]) peak[0] = current;
                };

                PainParser parser = new PainParserImpl();
                parser.parseStreaming(xmlFile, allocator, wrapping);
                persistence.finish();
                peakOffHeap = peak[0];
            }
            conn.close();
        }

        // For Type D (2 PmtInf × 100 TxInf — 200 transactions), the streaming
        // pipeline should use far less than 2 × 1 MB (generous upper bound per table).
        long upperBoundBytes = 2L * 1024 * 1024 * 3; // 2 MB × 3 tables
        assertTrue(peakOffHeap <= upperBoundBytes,
                String.format("Peak off-heap %,d bytes exceeds expected upper bound %,d bytes",
                        peakOffHeap, upperBoundBytes));
    }

    @Test
    @DisplayName("Streaming pipeline: DuckDB row counts match parsed stats")
    void testDuckDbRowCountsMatchParsed(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        ParseStats stats;
        DuckDBConnection conn = (DuckDBConnection)
                DriverManager.getConnection("jdbc:duckdb:");

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            try (LocalFilePersistenceService persistence =
                    new LocalFilePersistenceService(tempDir, "test", msgSchema, rmtSchema, txSchema)) {

                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);
                PainParser parser = new PainParserImpl();
                stats = parser.parseStreaming(xmlFile, allocator, consumer);
                persistence.finish();
            }
        }

        try (PaymentRepository repo = new PaymentRepositoryImpl(conn)) {
            assertEquals(stats.remittanceRows(), repo.getRemittanceCount(),
                    "Remittance row count mismatch");
            assertEquals(stats.transactionRows(), repo.getTransactionCount(),
                    "Transaction row count mismatch");
            // Type D: 2 remittances, 200 transactions
            assertEquals(2L,   stats.remittanceRows(),  "Type D: expected 2 remittances");
            assertEquals(200L, stats.transactionRows(), "Type D: expected 200 transactions");
        }
    }

    @Test
    @DisplayName("Streaming pipeline: .arrows files are readable by ArrowStreamReader")
    void testArrowStreamFilesReadable(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection)
                    DriverManager.getConnection("jdbc:duckdb:");

            try (LocalFilePersistenceService persistence =
                    new LocalFilePersistenceService(tempDir, "test", msgSchema, rmtSchema, txSchema)) {

                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);
                PainParser parser = new PainParserImpl();
                parser.parseStreaming(xmlFile, allocator, consumer);
                persistence.finish();
            }
            conn.close();
        }

        // Verify the three .arrows files exist and are readable
        Path msgFile = tempDir.resolve("test_message.arrows");
        Path rmtFile = tempDir.resolve("test_remittance.arrows");
        Path txFile  = tempDir.resolve("test_transaction.arrows");

        assertTrue(Files.exists(msgFile), "message .arrows file must exist");
        assertTrue(Files.exists(rmtFile), "remittance .arrows file must exist");
        assertTrue(Files.exists(txFile),  "transaction .arrows file must exist");

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            // Read message file
            long msgRows = 0;
            try (FileInputStream fis = new FileInputStream(msgFile.toFile());
                 ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
                while (reader.loadNextBatch()) {
                    msgRows += reader.getVectorSchemaRoot().getRowCount();
                }
            }
            assertTrue(msgRows > 0, "message file must have at least one row");

            // Read remittance file
            long rmtRows = 0;
            try (FileInputStream fis = new FileInputStream(rmtFile.toFile());
                 ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
                while (reader.loadNextBatch()) {
                    rmtRows += reader.getVectorSchemaRoot().getRowCount();
                }
            }
            assertEquals(2L, rmtRows, "Type D: expected 2 remittance rows in .arrows file");

            // Read transaction file
            long txRows = 0;
            try (FileInputStream fis = new FileInputStream(txFile.toFile());
                 ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {
                while (reader.loadNextBatch()) {
                    txRows += reader.getVectorSchemaRoot().getRowCount();
                }
            }
            assertEquals(200L, txRows, "Type D: expected 200 transaction rows in .arrows file");
        }
    }

    @Test
    @DisplayName("LocalFilePersistenceService: respects output directory parameter")
    void testOutputDirParameter(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        Path customDir = tempDir.resolve("custom-output");

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection)
                    DriverManager.getConnection("jdbc:duckdb:");

            try (LocalFilePersistenceService persistence =
                    new LocalFilePersistenceService(customDir, "test", msgSchema, rmtSchema, txSchema)) {

                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);
                PainParser parser = new PainParserImpl();
                parser.parseStreaming(xmlFile, allocator, consumer);
                persistence.finish();
            }
            conn.close();
        }

        // Verify files landed in the custom directory
        assertTrue(Files.exists(customDir.resolve("test_message.arrows")),
                "message .arrows file must exist in custom directory");
        assertTrue(Files.exists(customDir.resolve("test_remittance.arrows")),
                "remittance .arrows file must exist in custom directory");
        assertTrue(Files.exists(customDir.resolve("test_transaction.arrows")),
                "transaction .arrows file must exist in custom directory");
    }
}
