package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LongSummaryStatistics;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that the full parse → load → validate pipeline does not leak Arrow
 * allocator memory across repeated iterations — simulating a long-running process.
 *
 * <p>DuckDB &ge;1.4.4.0 fixes the native connection-level leak (issue #9712).
 * {@link PaymentRepositoryImpl} also closes Arrow C Data Interface resources
 * <em>after</em> {@code conn.close()}, ensuring DuckDB has already invoked all
 * C Data Interface release callbacks before the Arrow allocator is checked.</p>
 *
 * <p>A single {@link RootAllocator} is shared across all iterations.
 * After each iteration {@link BufferAllocator#getAllocatedMemory()} must be
 * exactly {@code 0} — any non-zero value indicates an Arrow allocator leak.
 * Per-iteration timings are collected and a summary table is printed to stdout.</p>
 */
class MemoryLeakVerificationTest {

    private static final int ITERATIONS = 50;
    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Type D: no Arrow allocator leak over " + ITERATIONS + " iterations (valid file)")
    void noLeakTypeDValidFile() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);
        runLeakCheck(file, ITERATIONS, "Type D (valid, 2×100)");
    }

    @Test
    @DisplayName("Type E: no Arrow allocator leak over " + ITERATIONS + " iterations (invalid CtrlSum)")
    void noLeakTypeEInvalidCtrlSum() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);
        runLeakCheck(file, ITERATIONS, "Type E (invalid CtrlSum, 2×100)");
    }

    /**
     * Runs the full pipeline {@code iterations} times with a shared allocator,
     * asserting zero bytes remain allocated after each iteration.
     * Prints a performance summary table to stdout.
     */
    private static void runLeakCheck(Path xmlFile, int iterations, String label)
            throws Exception {

        long[] parseTimes    = new long[iterations];
        long[] loadTimes     = new long[iterations];
        long[] validateTimes = new long[iterations];

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();

            for (int i = 0; i < iterations; i++) {
                long t0 = System.currentTimeMillis();
                try (ArrowBatchResult result = parser.parse(xmlFile, allocator)) {
                    long t1 = System.currentTimeMillis();
                    parseTimes[i] = t1 - t0;

                    try (PaymentRepository repo = new PaymentRepositoryImpl(result, allocator)) {
                        long t2 = System.currentTimeMillis();
                        loadTimes[i] = t2 - t1;

                        ValidationPipeline.standard().execute(repo);
                        validateTimes[i] = System.currentTimeMillis() - t2;
                    }
                }

                long leaked = allocator.getAllocatedMemory();
                assertEquals(0L, leaked,
                        String.format("[%s] Arrow allocator leak detected after iteration %d/%d:"
                                + " %,d bytes still allocated", label, i + 1, iterations, leaked));
            }
        }

        printSummary(label, iterations, parseTimes, loadTimes, validateTimes);
    }

    private static void printSummary(String label, int iterations,
            long[] parseTimes, long[] loadTimes, long[] validateTimes) {

        LongSummaryStatistics parseStats    = LongStream.of(parseTimes).summaryStatistics();
        LongSummaryStatistics loadStats     = LongStream.of(loadTimes).summaryStatistics();
        LongSummaryStatistics validateStats = LongStream.of(validateTimes).summaryStatistics();
        long totalMs = LongStream.of(parseTimes).sum()
                     + LongStream.of(loadTimes).sum()
                     + LongStream.of(validateTimes).sum();

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.printf( "║  Memory Leak Verification — %-47s ║%n", label);
        System.out.println("╠══════════════════╦══════════════╦══════════════╦══════════════╦═════════════╣");
        System.out.println("║  Phase           ║  Min (ms)    ║  Max (ms)    ║  Avg (ms)    ║  Total (ms) ║");
        System.out.println("╠══════════════════╬══════════════╬══════════════╬══════════════╬═════════════╣");
        printRow("XML→Arrow Parse",  parseStats);
        printRow("Arrow→DuckDB Load", loadStats);
        printRow("SQL Validation",   validateStats);
        System.out.println("╠══════════════════╩══════════════╩══════════════╩══════════════╬═════════════╣");
        System.out.printf( "║  %d iterations, 0 bytes leaked after every iteration          ║ %11s ║%n",
                iterations, String.format("%,d", totalMs));
        System.out.println("╚══════════════════════════════════════════════════════════════╩═════════════╝");
        System.out.println();
    }

    private static void printRow(String phase, LongSummaryStatistics stats) {
        System.out.printf("║  %-16s ║ %12s ║ %12s ║ %12s ║             ║%n",
                phase,
                String.format("%,d", stats.getMin()),
                String.format("%,d", stats.getMax()),
                String.format("%,.1f", stats.getAverage()));
    }
}
