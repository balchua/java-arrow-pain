package com.pgw.purearrow;

import com.pgw.benchmark.LoadBenchmark;
import com.pgw.parser.PainParser;
import com.pgw.parser.PainParserImpl;
import com.pgw.parser.ParseStats;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Orchestrates the pure-Arrow ingest pipeline:
 * {@code pain.001 XML → PainParserImpl → PureArrowBatchConsumer → Arrow IPC stream files + in-memory store}.
 *
 * <p>No DuckDB is used anywhere in this pipeline.</p>
 */
public final class PureArrowIngestor {

    private static final Logger LOG = LoggerFactory.getLogger(PureArrowIngestor.class);

    /**
     * Parses the XML file, writes three Arrow IPC stream files to {@code outputDir},
     * and returns a result containing parse statistics, the in-memory store, and file paths.
     *
     * <p>The caller is responsible for closing the {@link PureArrowIngestResult#store()} when
     * done to release all Arrow off-heap memory.</p>
     *
     * @param xmlFile   path to the pain.001 XML file to parse
     * @param outputDir directory to write Arrow IPC stream files into
     * @param baseName  base name (without extension) used to derive output file names
     * @param allocator Arrow buffer allocator
     * @param benchmark optional benchmark for memory/timing instrumentation; may be {@code null}
     * @return ingest result containing parse stats, in-memory store, and file paths
     * @throws Exception on I/O or XML parse failure
     */
    public PureArrowIngestResult ingest(
            Path xmlFile,
            Path outputDir,
            String baseName,
            BufferAllocator allocator,
            LoadBenchmark benchmark) throws Exception {

        Files.createDirectories(outputDir);

        PureArrowBatchConsumer consumer =
                new PureArrowBatchConsumer(allocator, outputDir, baseName, benchmark);
        try {
            PainParser parser = new PainParserImpl();
            ParseStats parseStats = parser.parseStreaming(xmlFile, allocator, consumer);
            consumer.close();

            PureArrowInMemoryStore store = consumer.getStore();
            return new PureArrowIngestResult(
                    parseStats,
                    store,
                    consumer.getMessageFile(),
                    consumer.getRemittanceFile(),
                    consumer.getTransactionFile()
            );
        } catch (Exception e) {
            try { consumer.close(); } catch (Exception ce) { e.addSuppressed(ce); }
            consumer.getStore().close();
            throw e;
        }
    }
}
