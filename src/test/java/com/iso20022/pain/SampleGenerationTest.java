package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

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
    void typeDParseRowCounts() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            try (ArrowBatchResult result = parser.parse(file, allocator)) {
                assertEquals(2L, result.getRemittanceRowCount(),
                        "Type D should have 2 remittance rows");
                assertEquals(200L, result.getTransactionRowCount(),
                        "Type D should have 200 transaction rows");
            }
        }
    }

    @Test
    @DisplayName("Type D: validation passes with 0 errors")
    void typeDValidationPasses() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_D);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            try (ArrowBatchResult result = parser.parse(file, allocator)) {
                try (PaymentRepository repository = new PaymentRepositoryImpl(result, allocator)) {
                    ValidationContext context = ValidationPipeline.standard().execute(repository);
                    assertFalse(context.hasErrors(),
                            "Type D (valid file) should pass validation with 0 errors, but got: "
                                    + context.getErrors());
                }
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
    void typeEParseRowCounts() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            try (ArrowBatchResult result = parser.parse(file, allocator)) {
                assertEquals(2L, result.getRemittanceRowCount(),
                        "Type E should have 2 remittance rows");
                assertEquals(200L, result.getTransactionRowCount(),
                        "Type E should have 200 transaction rows");
            }
        }
    }

    @Test
    @DisplayName("Type E: validation reports at least 1 CtrlSum error")
    void typeEValidationReportsCtrlSumError() throws Exception {
        Path file = TestFileGenerator.generateIfAbsent(TestPainFileSpecs.TYPE_E);

        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            PainParser parser = new PainParserImpl();
            try (ArrowBatchResult result = parser.parse(file, allocator)) {
                try (PaymentRepository repository = new PaymentRepositoryImpl(result, allocator)) {
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
}
