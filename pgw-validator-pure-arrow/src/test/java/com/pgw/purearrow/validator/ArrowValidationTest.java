package com.pgw.purearrow.validator;

import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.purearrow.PureArrowIngestor;
import com.pgw.purearrow.PureArrowIngestResult;
import com.pgw.purearrow.PureArrowInMemoryStore;
import com.pgw.purearrow.validator.dal.ArrowPaymentRepositoryImpl;
import com.pgw.purearrow.validator.dal.ArrowPaymentRepositoryLoader;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Correctness tests for the pure-Arrow validation pipeline.
 *
 * <p>Equivalent of {@code ValidationTest} in {@code pgw-validator}, but uses
 * {@link ArrowPaymentRepositoryImpl} backed by Apache Arrow vectors (no DuckDB)
 * instead of {@code PaymentRepositoryImpl} backed by DuckDB SQL.</p>
 *
 * <p><b>Type D</b>: 2 PmtInf × 100 TxInf — valid file; validation must pass with 0 errors.</p>
 * <p><b>Type E</b>: 2 PmtInf × 100 TxInf — invalid CtrlSum; validation must report at least
 * 1 {@code ControlSumValidator} error.</p>
 */
class ArrowValidationTest {

    private static final long ALLOCATOR_LIMIT = 512L * 1024 * 1024; // 512 MB

    @Test
    @DisplayName("Type D: pure-Arrow validation passes with 0 errors")
    void typeDValidationPasses(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestResult result =
                    new PureArrowIngestor().ingest(xmlFile, tempDir, "type_d", allocator, null);
            try (PureArrowInMemoryStore ignored = result.store();
                 ArrowPaymentRepositoryImpl repo = ArrowPaymentRepositoryLoader.load(
                         result.messageFile(), result.remittanceFile(),
                         result.transactionFile(), allocator)) {

                ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                assertFalse(ctx.hasErrors(),
                        "Type D (valid file) should pass validation with 0 errors, but got: "
                                + ctx.getErrors());
            }
        }
    }

    @Test
    @DisplayName("Type E: pure-Arrow validation reports at least 1 CtrlSum error")
    void typeEValidationReportsCtrlSumError(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestResult result =
                    new PureArrowIngestor().ingest(xmlFile, tempDir, "type_e", allocator, null);
            try (PureArrowInMemoryStore ignored = result.store();
                 ArrowPaymentRepositoryImpl repo = ArrowPaymentRepositoryLoader.load(
                         result.messageFile(), result.remittanceFile(),
                         result.transactionFile(), allocator)) {

                ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                assertTrue(ctx.hasErrors(),
                        "Type E (invalid CtrlSum) should fail validation");
                long ctrlSumErrors = ctx.getErrors().stream()
                        .filter(e -> e.validator().equals("ControlSumValidator"))
                        .count();
                assertTrue(ctrlSumErrors >= 1,
                        "Type E should have at least 1 CtrlSum error, but got: "
                                + ctx.getErrors());
            }
        }
    }

    @Test
    @DisplayName("Type H: pure-Arrow validation passes — 10 PmtInf × 200 TxInf")
    void typeHValidationPasses(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_H);

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestResult result =
                    new PureArrowIngestor().ingest(xmlFile, tempDir, "type_h", allocator, null);
            try (PureArrowInMemoryStore ignored = result.store();
                 ArrowPaymentRepositoryImpl repo = ArrowPaymentRepositoryLoader.load(
                         result.messageFile(), result.remittanceFile(),
                         result.transactionFile(), allocator)) {

                assertEquals(10L,    repo.getRemittanceCount(),  "Type H: 10 remittances");
                assertEquals(2_000L, repo.getTransactionCount(), "Type H: 2000 transactions");
                ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                assertFalse(ctx.hasErrors(),
                        "Type H should pass validation, but got: " + ctx.getErrors());
            }
        }
    }

    @Test
    @DisplayName("Type J: pure-Arrow validation passes — 1 PmtInf × 1 TxInf (unitary)")
    void typeJValidationPasses(@TempDir Path tempDir) throws Exception {
        Path xmlFile = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_J);

        try (RootAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PureArrowIngestResult result =
                    new PureArrowIngestor().ingest(xmlFile, tempDir, "type_j", allocator, null);
            try (PureArrowInMemoryStore ignored = result.store();
                 ArrowPaymentRepositoryImpl repo = ArrowPaymentRepositoryLoader.load(
                         result.messageFile(), result.remittanceFile(),
                         result.transactionFile(), allocator)) {

                assertEquals(1L, repo.getRemittanceCount(),  "Type J: 1 remittance");
                assertEquals(1L, repo.getTransactionCount(), "Type J: 1 transaction");
                ValidationContext ctx = ValidationPipeline.standard().execute(repo);
                assertFalse(ctx.hasErrors(),
                        "Type J should pass validation, but got: " + ctx.getErrors());
            }
        }
    }
}
