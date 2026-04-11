package com.pgw.generator;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Test utility for generating pain.001 sample files.
 * Skips generation if the file already exists and is complete (ends with {@code </Document>}).
 * This is the ONLY place where file-existence checks are performed.
 */
public final class TestFileGenerator {

    public static final Path SAMPLE_DATA_DIR =
            Paths.get("..", "test-data", "sample-data");

    private TestFileGenerator() {}

    /**
     * Generates the given spec if the output file does not already exist and is complete.
     *
     * @param spec spec to generate
     * @return path of the (possibly pre-existing) generated file
     */
    public static Path generateIfAbsent(PainFileSpec spec)
            throws IOException, XMLStreamException {
        Files.createDirectories(SAMPLE_DATA_DIR);
        Path outputFile = SAMPLE_DATA_DIR.resolve(spec.fileName());

        if (Files.exists(outputFile) && isFileComplete(outputFile)) {
            return outputFile;
        }

        PainXmlGenerator generator = new PainXmlGeneratorImpl();
        return generator.generate(spec, SAMPLE_DATA_DIR);
    }

    /**
     * Fast tail-check: file is complete if it ends with {@code </Document>}.
     */
    static boolean isFileComplete(Path xmlFile) {
        try {
            long size = Files.size(xmlFile);
            if (size < 100)
                return false;

            try (RandomAccessFile raf = new RandomAccessFile(xmlFile.toFile(), "r")) {
                long seekPos = Math.max(0, size - 64);
                raf.seek(seekPos);
                byte[] tail = new byte[(int) (size - seekPos)];
                raf.readFully(tail);
                String tailStr = new String(tail, java.nio.charset.StandardCharsets.UTF_8);
                return tailStr.contains("</Document>");
            }
        } catch (IOException e) {
            return false;
        }
    }
}
