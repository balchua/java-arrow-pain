package com.iso20022.pain;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.dal.PaymentRepositoryImpl;
import com.iso20022.pain.generator.PainFileSpec;
import com.iso20022.pain.generator.TestFileGenerator;
import com.iso20022.pain.generator.TestPainFileSpecs;
import com.iso20022.pain.parser.PainParser;
import com.iso20022.pain.parser.PainParserImpl;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.ValidationPipeline;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.file.Path;
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

            try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
                PainParser parser = new PainParserImpl();
                long parseStart = System.currentTimeMillis();
                try (ArrowBatchResult result = parser.parse(xmlFile, allocator)) {
                    long parseMs = System.currentTimeMillis() - parseStart;
                    System.out.printf("  ✓ Parsed in %d ms (%d remittances, %d transactions)%n",
                            parseMs, result.getRemittanceRowCount(), result.getTransactionRowCount());

                    try (PaymentRepository repository = new PaymentRepositoryImpl(result, allocator)) {
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
            } catch (IllegalStateException e) {
                if (e.getMessage() != null && e.getMessage().startsWith("Memory was leaked")) {
                    // expected DuckDB C Data Interface residual
                } else {
                    throw e;
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
