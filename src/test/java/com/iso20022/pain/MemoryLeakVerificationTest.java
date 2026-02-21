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
 * exactly {@code 0} — any non-zero value indicates an Arrow allocator leak.</p>
 */
class MemoryLeakVerificationTest {

    private static final int ITERATIONS = 50;
    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Type D: no Arrow allocator leak over " + ITERATIONS + " iterations (valid file)")
    void noLeakTypeDValidFile() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);
        runLeakCheck(file, ITERATIONS, "Type D");
    }

    @Test
    @DisplayName("Type E: no Arrow allocator leak over " + ITERATIONS + " iterations (invalid CtrlSum)")
    void noLeakTypeEInvalidCtrlSum() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);
        runLeakCheck(file, ITERATIONS, "Type E");
    }

    /**
     * Runs the full pipeline {@code iterations} times with a shared allocator,
     * asserting zero bytes remain allocated after each iteration.
     */
    private static void runLeakCheck(Path xmlFile, int iterations, String label)
            throws Exception {

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();

            for (int i = 1; i <= iterations; i++) {
                try (ArrowBatchResult result = parser.parse(xmlFile, allocator)) {
                    try (PaymentRepository repo = new PaymentRepositoryImpl(result, allocator)) {
                        ValidationPipeline.standard().execute(repo);
                    }
                }

                long leaked = allocator.getAllocatedMemory();
                assertEquals(0L, leaked,
                        String.format("[%s] Arrow allocator leak detected after iteration %d/%d:"
                                + " %,d bytes still allocated", label, i, iterations, leaked));
            }
        }
    }
}
