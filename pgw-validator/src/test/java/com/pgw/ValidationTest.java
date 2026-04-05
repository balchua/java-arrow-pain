package com.pgw;

import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation-focused tests for pain.001 parsing and domain rule checking.
 *
 * <p>Type D: valid file — validation must pass with 0 errors.</p>
 * <p>Type E: invalid CtrlSum file — validation must report CtrlSum errors.</p>
 */
class ValidationTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Type D: validation passes with 0 errors")
    void typeDValidationPasses(@TempDir Path tempDir) throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
            new PainParserImpl().parseStreaming(file, allocator, consumer);
            try (PaymentRepository repository = new PaymentRepositoryImpl(conn)) {
                ValidationContext context = ValidationPipeline.standard().execute(repository);
                assertFalse(context.hasErrors(),
                        "Type D (valid file) should pass validation with 0 errors, but got: "
                                + context.getErrors());
            }
        }
    }

    @Test
    @DisplayName("Type E: validation reports at least 1 CtrlSum error")
    void typeEValidationReportsCtrlSumError(@TempDir Path tempDir) throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            StreamingBatchConsumer consumer = new StreamingBatchConsumer(conn, allocator);
            new PainParserImpl().parseStreaming(file, allocator, consumer);
            try (PaymentRepository repository = new PaymentRepositoryImpl(conn)) {
                ValidationContext context = ValidationPipeline.standard().execute(repository);
                assertTrue(context.hasErrors(),
                        "Type E (invalid CtrlSum) should fail validation");
                long ctrlSumErrors = context.getErrors().stream()
                        .filter(e -> e.validator().equals("ControlSumValidator"))
                        .count();
                assertTrue(ctrlSumErrors >= 1,
                        "Type E should have at least 1 CtrlSum error, but got: "
                                + context.getErrors());
            }
        }
    }
}
