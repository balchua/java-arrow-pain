package com.pgw.persistence;

import org.apache.arrow.vector.VectorSchemaRoot;
import com.pgw.parser.BatchConsumer.TableType;
import java.io.IOException;

/**
 * Sink that receives one Arrow batch at a time and persists it.
 * Callers must invoke {@link #finish()} after the last batch.
 */
public interface PersistenceService extends AutoCloseable {
    void writeBatch(TableType tableType, VectorSchemaRoot root) throws IOException;
    void finish() throws IOException;
    /** Total bytes written across all tables. */
    long getBytesWritten();
}
