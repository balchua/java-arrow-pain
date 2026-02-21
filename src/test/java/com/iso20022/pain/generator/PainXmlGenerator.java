package com.iso20022.pain.generator;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Contract for generating ISO 20022 pain.001.001.09 XML files.
 */
public interface PainXmlGenerator {
    /**
     * Generates a pain.001.001.09 XML file according to the given spec.
     *
     * @param spec      generation parameters (remittance count, tx count, control sum correctness)
     * @param outputDir directory in which to write the file
     * @return path of the generated file
     * @throws IOException        on I/O failure
     * @throws XMLStreamException on XML write failure
     */
    Path generate(PainFileSpec spec, Path outputDir) throws IOException, XMLStreamException;
}
