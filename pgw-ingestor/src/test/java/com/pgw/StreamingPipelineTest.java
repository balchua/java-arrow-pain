package com.pgw;

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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the streaming pipeline: memory footprint, DuckDB row count correctness,
 * Arrow file export via DuckDB COPY TO, and output directory configuration.
 */
class StreamingPipelineTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Streaming pipeline: memory footprint stays flat (≤ 2× batch budget)")
    void testMemoryFootprintIsFlat(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        long peakOffHeap = 0;

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = DuckDbFactory.newConnection();

            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);

            final long[] peak = {0};
            BatchConsumer wrapping = (tableType, root) -> {
                consumer.accept(tableType, root);
                long current = allocator.getAllocatedMemory();
                if (current > peak[0]) peak[0] = current;
            };

            PainParser parser = new PainParserImpl();
            parser.parseStreaming(xmlFile, allocator, wrapping);
            peakOffHeap = peak[0];
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

        ParseStats stats;
        DuckDBConnection conn = DuckDbFactory.newConnection();

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
            PainParser parser = new PainParserImpl();
            stats = parser.parseStreaming(xmlFile, allocator, consumer);
        }

        long rmtCount, txCount;
        try (var stmt = conn.createStatement()) {
            try (var rs = stmt.executeQuery("SELECT COUNT(*) FROM remittance")) {
                rmtCount = rs.next() ? rs.getLong(1) : 0L;
            }
            try (var rs2 = stmt.executeQuery("SELECT COUNT(*) FROM transactions")) {
                txCount = rs2.next() ? rs2.getLong(1) : 0L;
            }
        }
        conn.close();

        assertEquals(stats.remittanceRows(), rmtCount, "Remittance row count mismatch");
        assertEquals(stats.transactionRows(), txCount, "Transaction row count mismatch");
        // Type D: 2 remittances, 200 transactions
        assertEquals(2L,   stats.remittanceRows(),  "Type D: expected 2 remittances");
        assertEquals(200L, stats.transactionRows(), "Type D: expected 200 transactions");
    }

    @Test
    @DisplayName("Streaming pipeline: DuckDB COPY TO exports readable .arrow files")
    void testArrowFilesExported(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        // Parse and populate DuckDB
        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = DuckDbFactory.newConnection();
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
            PainParser parser = new PainParserImpl();
            parser.parseStreaming(xmlFile, allocator, consumer);

            // Export Arrow files via DuckDB COPY TO
            Path msgFile = tempDir.resolve("test_message.arrow");
            Path rmtFile = tempDir.resolve("test_remittance.arrow");
            Path txFile  = tempDir.resolve("test_transaction.arrow");

            try (var stmt = conn.createStatement()) {
                stmt.execute("COPY message TO '" + msgFile.toAbsolutePath() + "' (FORMAT arrow)");
                stmt.execute("COPY remittance TO '" + rmtFile.toAbsolutePath() + "' (FORMAT arrow)");
                stmt.execute("COPY transactions TO '" + txFile.toAbsolutePath() + "' (FORMAT arrow)");
            }
            conn.close();

            // Verify files exist and are non-empty
            assertTrue(Files.exists(msgFile), "message .arrow file must exist");
            assertTrue(Files.exists(rmtFile), "remittance .arrow file must exist");
            assertTrue(Files.exists(txFile),  "transaction .arrow file must exist");
            assertTrue(Files.size(msgFile) > 0, "message .arrow file must not be empty");
            assertTrue(Files.size(rmtFile) > 0, "remittance .arrow file must not be empty");
            assertTrue(Files.size(txFile)  > 0, "transaction .arrow file must not be empty");

            // Verify files are loadable with DuckDB read_arrow() and row counts match
            DuckDBConnection loadConn = DuckDbFactory.newConnection();
            try (var stmt = loadConn.createStatement()) {
                stmt.execute("CREATE TABLE msg AS SELECT * FROM read_arrow('"
                        + msgFile.toAbsolutePath() + "')");
                stmt.execute("CREATE TABLE rmt AS SELECT * FROM read_arrow('"
                        + rmtFile.toAbsolutePath() + "')");
                stmt.execute("CREATE TABLE txs AS SELECT * FROM read_arrow('"
                        + txFile.toAbsolutePath() + "')");
            }
            try (var stmt = loadConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM msg")) {
                assertTrue(rs.next() && rs.getLong(1) > 0, "message file must have at least one row");
            }
            try (var stmt = loadConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM rmt")) {
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong(1), "Type D: expected 2 remittance rows");
            }
            try (var stmt = loadConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM txs")) {
                assertTrue(rs.next());
                assertEquals(200L, rs.getLong(1), "Type D: expected 200 transaction rows");
            }
            loadConn.close();
        }
    }

    @Test
    @DisplayName("Streaming pipeline: DuckDB COPY TO respects the given output path")
    void testOutputPathParameter(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        Path customDir = tempDir.resolve("custom-output");
        Files.createDirectories(customDir);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = DuckDbFactory.newConnection();
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
            PainParser parser = new PainParserImpl();
            parser.parseStreaming(xmlFile, allocator, consumer);

            try (var stmt = conn.createStatement()) {
                stmt.execute("COPY message TO '"
                        + customDir.resolve("test_message.arrow").toAbsolutePath() + "' (FORMAT arrow)");
                stmt.execute("COPY remittance TO '"
                        + customDir.resolve("test_remittance.arrow").toAbsolutePath() + "' (FORMAT arrow)");
                stmt.execute("COPY transactions TO '"
                        + customDir.resolve("test_transaction.arrow").toAbsolutePath() + "' (FORMAT arrow)");
            }
            conn.close();
        }

        assertTrue(Files.exists(customDir.resolve("test_message.arrow")),
                "message .arrow file must exist in custom directory");
        assertTrue(Files.exists(customDir.resolve("test_remittance.arrow")),
                "remittance .arrow file must exist in custom directory");
        assertTrue(Files.exists(customDir.resolve("test_transaction.arrow")),
                "transaction .arrow file must exist in custom directory");
    }
}
