package com.pgw;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.dal.PaymentRepository;
import com.pgw.dal.PaymentRepositoryImpl;
import com.pgw.generator.PainFileSpec;
import com.pgw.generator.TestFileGenerator;
import com.pgw.generator.TestPainFileSpecs;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.ParseStats;
import com.pgw.parser.StreamingBatchConsumer;
import com.pgw.persistence.LocalFilePersistenceService;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone runner for generating, parsing, and validating pain.001 sample files.
 *
 * <p>Usage: {@code SampleGeneratorRunner [type-a] [type-b] [type-c] [type-d] [type-e]}</p>
 * <p>If no args given, generates all five types.</p>
 */
public final class SampleGeneratorRunner {

    private static final long ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024; // 2 GB

    public static void main(String[] args) throws Exception {
        List<PainFileSpec> specs = resolveSpecs(args);

        System.out.println("SampleGeneratorRunner — generating " + specs.size() + " file(s)");
        System.out.println("─────────────────────────────────────────────────────────");

        for (PainFileSpec spec : specs) {
            System.out.println("Processing: " + spec.name());
            long start = System.currentTimeMillis();

            Path xmlFile = TestFileGenerator.generateIfAbsent(spec);
            long genMs = System.currentTimeMillis() - start;
            System.out.printf("  ✓ Generated in %d ms → %s%n", genMs, xmlFile);

            String baseName = spec.fileName().replaceAll("\\.[xX][mM][lL]$", "");
            Path outputDir = Paths.get("src", "test", "resources", "output");
            Files.createDirectories(outputDir);

            Schema msgSchema = Pain001ArrowSchema.createMessageSchema();
            Schema rmtSchema = Pain001ArrowSchema.createRemittanceSchema();
            Schema txSchema  = Pain001ArrowSchema.createTransactionSchema();

            try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
                PainParser parser = new PainParserImpl();
                DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SET memory_limit='1GB'");
                }

                long parseStart = System.currentTimeMillis();
                ParseStats stats;
                try (LocalFilePersistenceService persistence =
                        new LocalFilePersistenceService(outputDir, baseName,
                                msgSchema, rmtSchema, txSchema)) {
                    StreamingBatchConsumer consumer =
                            new StreamingBatchConsumer(conn, persistence, allocator);
                    stats = parser.parseStreaming(xmlFile, allocator, consumer);
                    persistence.finish();
                }
                long parseMs = System.currentTimeMillis() - parseStart;
                System.out.printf("  ✓ Parsed in %d ms (%d remittances, %d transactions)%n",
                        parseMs, stats.remittanceRows(), stats.transactionRows());

                try (PaymentRepository repository = new PaymentRepositoryImpl(conn)) {
                    long valStart = System.currentTimeMillis();
                    ValidationContext context = ValidationPipeline.standard().execute(repository);
                    long valMs = System.currentTimeMillis() - valStart;

                    if (context.hasErrors()) {
                        System.out.printf("  ✗ Validation: %d error(s) in %d ms%n",
                                context.getErrors().size(), valMs);
                        context.getErrors().stream().limit(5).forEach(e ->
                                System.out.printf("      [%s] %s: %s%n",
                                        e.validator(), e.message(),
                                        e.details().length > 0 ? e.details()[0] : ""));
                    } else {
                        System.out.printf("  ✓ Validation passed in %d ms%n", valMs);
                    }
                }
            }
            System.out.println();
        }

        System.out.println("Done.");
    }

    private static List<PainFileSpec> resolveSpecs(String[] args) {
        if (args.length == 0) {
            return List.of(
                    TestPainFileSpecs.TYPE_A,
                    TestPainFileSpecs.TYPE_B,
                    TestPainFileSpecs.TYPE_C,
                    TestPainFileSpecs.TYPE_D,
                    TestPainFileSpecs.TYPE_E);
        }

        List<PainFileSpec> specs = new ArrayList<>();
        for (String arg : args) {
            switch (arg.toLowerCase().trim()) {
                case "type-a", "a" -> specs.add(TestPainFileSpecs.TYPE_A);
                case "type-b", "b" -> specs.add(TestPainFileSpecs.TYPE_B);
                case "type-c", "c" -> specs.add(TestPainFileSpecs.TYPE_C);
                case "type-d", "d" -> specs.add(TestPainFileSpecs.TYPE_D);
                case "type-e", "e" -> specs.add(TestPainFileSpecs.TYPE_E);
                default -> System.err.println("Unknown spec: " + arg + " (use type-a through type-e)");
            }
        }
        return specs;
    }
}

