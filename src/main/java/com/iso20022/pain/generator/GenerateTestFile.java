package com.iso20022.pain.generator;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility to generate a small test XML file for quick validation.
 * Usage: {@code GenerateTestFile [outputDir]}
 */
public final class GenerateTestFile {

    private GenerateTestFile() {
    }

    public static void main(String[] args) throws IOException, XMLStreamException {
        Path outputDir = args.length > 0
                ? Paths.get(args[0])
                : Paths.get("src", "main", "resources", "sample-data");

        SampleFileSpec small = new SampleFileSpec(
                "Test (1 PmtInf × 10 TxInf)",
                "pain001_test_10.xml",
                1,
                10);

        Path generated = Pain001XmlGenerator.generate(small, outputDir);
        System.out.println("Generated: " + generated.toAbsolutePath());
    }
}
