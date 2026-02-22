package com.iso20022.pain.parser;

import com.iso20022.pain.arrow.ArrowBatchResult;
import org.apache.arrow.memory.BufferAllocator;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Contract for parsing ISO 20022 pain.001.001.09 XML files into Apache Arrow tables.
 */
public interface PainParser {
    /**
     * Parses the given XML file into three Arrow tables (message, remittance, transaction).
     *
     * @param xmlFile   path to an existing pain.001.001.09 XML file
     * @param allocator Arrow buffer allocator for off-heap memory
     * @return parsed Arrow batch result — caller must close() when done
     * @throws IOException        on I/O failure
     * @throws XMLStreamException on XML parse failure
     */
    ArrowBatchResult parse(Path xmlFile, BufferAllocator allocator)
            throws IOException, XMLStreamException;

    /**
     * Streaming parse — invokes {@code consumer} once per finalized batch.
     * Arrow RAM is cleared after each consumer callback; memory stays flat.
     *
     * @param xmlFile   path to an existing pain.001.001.09 XML file
     * @param allocator Arrow buffer allocator for off-heap memory
     * @param consumer  callback invoked once per finalized batch
     * @return lightweight parse statistics
     * @throws IOException        on I/O failure
     * @throws XMLStreamException on XML parse failure
     */
    ParseStats parseStreaming(Path xmlFile, BufferAllocator allocator, BatchConsumer consumer)
            throws IOException, XMLStreamException;
}
