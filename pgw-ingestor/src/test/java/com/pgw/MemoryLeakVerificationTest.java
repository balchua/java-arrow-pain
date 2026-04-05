package com.pgw;

import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.util.LongSummaryStatistics;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that the streaming parse → DuckDB load pipeline does not leak Arrow
 * allocator memory across repeated iterations — simulating a long-running process.
 *
 * <p>A single {@link RootAllocator} is shared across all iterations.
 * After each iteration {@link BufferAllocator#getAllocatedMemory()} must be
 * exactly {@code 0} — any non-zero value indicates an Arrow allocator leak.</p>
 */
class MemoryLeakVerificationTest {

    private static final int ITERATIONS = 50;
    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Type D: no Arrow allocator leak over " + ITERATIONS + " iterations (valid file)")
    void noLeakTypeDValidFile() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);
        runStreamingLeakCheck(file, ITERATIONS, "Type D (valid, 2×100)");
    }

    @Test
    @DisplayName("Type E: no Arrow allocator leak over " + ITERATIONS + " iterations (invalid CtrlSum)")
    void noLeakTypeEInvalidCtrlSum() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);
        runStreamingLeakCheck(file, ITERATIONS, "Type E (invalid CtrlSum, 2×100)");
    }

    @Test
    @DisplayName("Type D: no Arrow allocator leak over 3 streaming iterations")
    void noLeakStreamingTypeDValidFile() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);
        runStreamingLeakCheck(file, 3, "Type D streaming (valid, 2×100)");
    }

    /**
     * Runs the full streaming parse pipeline {@code iterations} times with a shared allocator,
     * asserting zero bytes remain allocated after each iteration.
     */
    private static void runStreamingLeakCheck(Path xmlFile, int iterations, String label)
            throws Exception {

        long[] parseTimes = new long[iterations];

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();

            for (int i = 0; i < iterations; i++) {
                long t0 = System.currentTimeMillis();

                DuckDBConnection conn = (DuckDBConnection)
                        DriverManager.getConnection("jdbc:duckdb:");
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SET memory_limit='512MB'");
                }

                StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
                parser.parseStreaming(xmlFile, allocator, consumer);
                conn.close();

                parseTimes[i] = System.currentTimeMillis() - t0;

                long leaked = allocator.getAllocatedMemory();
                assertEquals(0L, leaked,
                        String.format("[%s] Arrow allocator leak detected after streaming iteration %d/%d:"
                                + " %,d bytes still allocated", label, i + 1, iterations, leaked));
            }
        }

        LongSummaryStatistics parseStats = LongStream.of(parseTimes).summaryStatistics();
        System.out.printf("%n[%s] Streaming leak check: %d iterations, 0 bytes leaked.%n"
                + "  Parse time — min: %,d ms, max: %,d ms, avg: %,.1f ms%n",
                label, iterations, parseStats.getMin(), parseStats.getMax(), parseStats.getAverage());
    }
}
