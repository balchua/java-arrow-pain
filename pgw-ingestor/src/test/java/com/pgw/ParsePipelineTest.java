package com.pgw;

import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.ParseStats;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for pain.001 XML generation and parse pipeline row counts.
 *
 * <p>Type D: valid file (2 PmtInf × 100 TxInf)</p>
 * <p>Type E: invalid CtrlSum file (2 PmtInf × 100 TxInf)</p>
 */
class ParsePipelineTest {

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
}
