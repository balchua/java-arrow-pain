package com.pgw.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extension-free Arrow IPC export and import for DuckDB.
 *
 * <p>Both operations use DuckDB's built-in Arrow C Data Interface — no community
 * extension is required.</p>
 *
 * <ul>
 *   <li>{@link #export} uses {@code DuckDBResultSet.arrowExportStream()} to read
 *       columnar batches out of DuckDB and writes them with Apache Arrow's
 *       {@code ArrowStreamWriter}.</li>
 *   <li>{@link #load} reads an Arrow IPC stream file with Apache Arrow's
 *       {@code ArrowStreamReader} and registers it in DuckDB via
 *       {@code DuckDBConnection.registerArrowStream()}, then materialises it
 *       as a permanent table with {@code CREATE TABLE AS SELECT}.</li>
 * </ul>
 */
public final class DuckDbArrowIpc {

    /** Rows per Arrow record batch when exporting. */
    public static final long DEFAULT_BATCH_SIZE = 65_536L;

    private DuckDbArrowIpc() {}

    /**
     * Exports a DuckDB table to an Arrow IPC stream file.
     *
     * <p>Uses {@code DuckDBResultSet.arrowExportStream()} — DuckDB's built-in Arrow
     * C Data Interface. No community extension required.</p>
     *
     * @param conn      live DuckDB connection whose {@code tableName} table is populated
     * @param tableName name of the DuckDB table to export
     * @param output    destination file path (will be created or overwritten)
     * @param allocator Arrow off-heap memory allocator
     * @throws Exception on SQL or I/O error
     */
    public static void export(DuckDBConnection conn,
                              String tableName,
                              Path output,
                              BufferAllocator allocator) throws Exception {
        try (Statement stmt = conn.createStatement();
             DuckDBResultSet rs = (DuckDBResultSet)
                     stmt.executeQuery("SELECT * FROM " + tableName)) {

            ArrowReader reader =
                    (ArrowReader) rs.arrowExportStream(allocator, DEFAULT_BATCH_SIZE);

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Files.createDirectories(output.getParent());
            try (OutputStream fos = Files.newOutputStream(output);
                 ArrowStreamWriter writer =
                         new ArrowStreamWriter(root, null, Channels.newChannel(fos))) {

                writer.start();
                while (reader.loadNextBatch()) {
                    writer.writeBatch();
                }
                writer.end();
            }
        }
    }

    /**
     * Loads an Arrow IPC stream file into a new DuckDB table.
     *
     * <p>Uses {@code ArrowStreamReader} + {@code DuckDBConnection.registerArrowStream()}
     * — DuckDB's built-in Arrow C Data Interface. No community extension required.</p>
     *
     * @param conn      DuckDB connection to create the table in
     * @param arrowFile Arrow IPC stream file to load
     * @param tableName name of the DuckDB table to create
     * @param allocator Arrow off-heap memory allocator
     * @throws SQLException on SQL error
     * @throws IOException  on file read error
     */
    public static void load(DuckDBConnection conn,
                            Path arrowFile,
                            String tableName,
                            BufferAllocator allocator) throws SQLException, IOException {
        // Use a unique stream reference to avoid clashing with the target table name.
        String streamRef = "_arrow_stream_" + tableName;
        try (FileInputStream fis = new FileInputStream(arrowFile.toFile());
             ArrowStreamReader reader = new ArrowStreamReader(fis, allocator)) {

            conn.registerArrowStream(streamRef, reader);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + tableName
                        + " AS SELECT * FROM " + streamRef);
            }
        }
        // The stream is automatically deregistered when the reader is closed.
    }
}
