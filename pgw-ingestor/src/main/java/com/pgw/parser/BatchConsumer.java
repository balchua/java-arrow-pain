package com.pgw.parser;

import org.apache.arrow.vector.VectorSchemaRoot;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;

/**
 * Functional interface invoked once per finalized Arrow batch (65,536 rows).
 * Implementations MUST NOT hold a reference to {@code root} after the call
 * returns — the parser clears and reuses the buffers immediately.
 *
 * @param <T> table type tag (MESSAGE, REMITTANCE, TRANSACTION)
 */
@FunctionalInterface
public interface BatchConsumer {
    void accept(TableType tableType, VectorSchemaRoot root) throws IOException, XMLStreamException;

    enum TableType { MESSAGE, REMITTANCE, TRANSACTION }
}
