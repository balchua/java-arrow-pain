package com.iso20022.pain;

import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.parser.ParseStats;
import com.iso20022.pain.parser.StreamingBatchConsumer;
import com.iso20022.pain.persistence.LocalFilePersistenceService;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for sample XML generation, parsing, and validation.
 *
 * <p>Type D: valid file (2 PmtInf × 100 TxInf)</p>
 * <p>Type E: invalid CtrlSum file (2 PmtInf × 100 TxInf)</p>
 */
class SampleGenerationTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Type D: file exists after generation")
    void typeDFileExists() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);
        assertTrue(Files.exists(file), "Type D file should exist after generation");
        assertTrue(Files.size(file) > 0, "Type D file should not be empty");
    }

    @Test
    @DisplayName("Type D: parse returns correct row counts (2 remittances, 200 transactions)")
    void typeDParseRowCounts(@TempDir Path tempDir) throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            ParseStats stats = parser.parseStreaming(file, allocator, (t, r) -> {});
            assertEquals(2L, stats.remittanceRows(),
                    "Type D should have 2 remittance rows");
            assertEquals(200L, stats.transactionRows(),
                    "Type D should have 200 transaction rows");
        }
    }

    @Test
    @DisplayName("Type D: validation passes with 0 errors")
    void typeDValidationPasses(@TempDir Path tempDir) throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            try (LocalFilePersistenceService persistence =
                    new LocalFilePersistenceService(tempDir, "typed", msgSchema, rmtSchema, txSchema)) {
                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);
                new PainParserImpl().parseStreaming(file, allocator, consumer);
                persistence.finish();
            }
            try (PaymentRepository repository = new PaymentRepositoryImpl(conn)) {
                ValidationContext context = ValidationPipeline.standard().execute(repository);
                assertFalse(context.hasErrors(),
                        "Type D (valid file) should pass validation with 0 errors, but got: "
                                + context.getErrors());
            }
        }
    }

    @Test
    @DisplayName("Type E: file exists after generation")
    void typeEFileExists() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);
        assertTrue(Files.exists(file), "Type E file should exist after generation");
        assertTrue(Files.size(file) > 0, "Type E file should not be empty");
    }

    @Test
    @DisplayName("Type E: parse returns correct row counts (2 remittances, 200 transactions)")
    void typeEParseRowCounts(@TempDir Path tempDir) throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            ParseStats stats = parser.parseStreaming(file, allocator, (t, r) -> {});
            assertEquals(2L, stats.remittanceRows(),
                    "Type E should have 2 remittance rows");
            assertEquals(200L, stats.transactionRows(),
                    "Type E should have 200 transaction rows");
        }
    }

    @Test
    @DisplayName("Type E: validation reports at least 1 CtrlSum error")
    void typeEValidationReportsCtrlSumError(@TempDir Path tempDir) throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
        Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            try (LocalFilePersistenceService persistence =
                    new LocalFilePersistenceService(tempDir, "typee", msgSchema, rmtSchema, txSchema)) {
                StreamingBatchConsumer consumer =
                        new StreamingBatchConsumer(conn, persistence, allocator);
                new PainParserImpl().parseStreaming(file, allocator, consumer);
                persistence.finish();
            }
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

