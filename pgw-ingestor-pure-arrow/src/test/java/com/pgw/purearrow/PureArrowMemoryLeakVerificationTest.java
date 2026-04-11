package com.pgw.purearrow;

import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LongSummaryStatistics;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that the pure-Arrow parse pipeline does not leak Arrow allocator memory
 * across repeated iterations — simulating a long-running process.
 *
 * <p>A single {@link RootAllocator} is shared across all iterations.
 * After each iteration, {@link BufferAllocator#getAllocatedMemory()} must be
 * exactly {@code 0} — any non-zero value indicates an Arrow allocator leak.</p>
 */
class PureArrowMemoryLeakVerificationTest {

    private static final int ITERATIONS = 50;
    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB
    private static final Path OUTPUT_DIR = Paths.get("..", "test-data", "output", "ingestor-pure-arrow");

    @BeforeAll
    static void createOutputDir() throws Exception {
        Files.createDirectories(OUTPUT_DIR);
    }

    @Test
    @DisplayName("Type D: no Arrow allocator leak over " + ITERATIONS + " iterations (valid file)")
    void noMemoryLeakAfter50Iterations_typeD() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);
        runLeakCheck(file, ITERATIONS, "Type D (valid, 2×100)");
    }

    @Test
    @DisplayName("Type E: no Arrow allocator leak over " + ITERATIONS + " iterations (invalid CtrlSum)")
    void noMemoryLeakAfter50Iterations_typeE() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);
        runLeakCheck(file, ITERATIONS, "Type E (invalid CtrlSum, 2×100)");
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static void runLeakCheck(Path xmlFile, int iterations, String label) throws Exception {
        long[] parseTimes = new long[iterations];

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestor ingestor = new PureArrowIngestor();

            for (int i = 0; i < iterations; i++) {
                long t0 = System.currentTimeMillis();

                PureArrowIngestResult result =
                        ingestor.ingest(xmlFile, OUTPUT_DIR, "leak_" + label.replace(" ", "_") + "_" + i,
                                allocator, null);
                result.store().close();

                parseTimes[i] = System.currentTimeMillis() - t0;

                long leaked = allocator.getAllocatedMemory();
                assertEquals(0L, leaked,
                        String.format("[%s] Arrow allocator leak after iteration %d/%d: %,d bytes still allocated",
                                label, i + 1, iterations, leaked));
            }
        }

        LongSummaryStatistics stats = LongStream.of(parseTimes).summaryStatistics();
        System.out.printf("%n[%s] Leak check: %d iterations, 0 bytes leaked.%n"
                + "  Parse time — min: %,d ms, max: %,d ms, avg: %,.1f ms%n",
                label, iterations, stats.getMin(), stats.getMax(), stats.getAverage());
    }
}
