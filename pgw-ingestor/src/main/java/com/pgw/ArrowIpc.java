package com.pgw;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Extension-less Arrow IPC export and import for DuckDB.
 *
 * <p>Uses the Arrow C Data Interface — the same zero-copy mechanism as
 * {@link DuckDBConnection#registerArrowStream} — to exchange data between
 * DuckDB and Arrow IPC stream files on disk.  No DuckDB arrow community
 * extension ({@code LOAD arrow}) is required.</p>
 *
 * <ul>
 *   <li>{@link #export} — DuckDB table → Arrow IPC stream file</li>
 *   <li>{@link #load}   — Arrow IPC stream file → DuckDB table</li>
 * </ul>
 *
 * <p>Memory overhead is bounded to one batch at a time; no full-table
 * accumulation occurs in either direction.</p>
 *
 * <h3>Export path</h3>
 * <pre>
 *  DuckDB table
 *      ↓  DuckDBResultSet.arrowExportStream(allocator, batchSize)
 *         — DuckDB-native: wraps native stream → ArrowArrayStream → ArrowReader
 *  ArrowReader (one batch at a time)
 *      ↓  ArrowStreamWriter
 *  .arrow file (Arrow IPC stream format)
 * </pre>
 *
 * <h3>Load path</h3>
 * <pre>
 *  .arrow file → ArrowStreamReader
 *      ↓  Data.exportArrayStream → ArrowArrayStream
 *  DuckDBConnection.registerArrowStream
 *      ↓  CREATE TABLE … AS SELECT * FROM &lt;stream&gt;
 *  DuckDB table
 * </pre>
 */
public final class ArrowIpc {

    /** Number of rows per Arrow IPC batch produced during export. */
    private static final int DEFAULT_BATCH_SIZE = 65_536;

    private static final AtomicLong TMP_SEQ = new AtomicLong(0);

    private ArrowIpc() {}

    private static final java.util.regex.Pattern SAFE_IDENTIFIER =
            java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Validates that {@code name} is a safe SQL identifier (letters, digits, underscores). */
    private static String requireSafeIdentifier(String name) {
        if (name == null || !SAFE_IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Unsafe SQL identifier: '" + name + "'. Only [A-Za-z_][A-Za-z0-9_]* allowed.");
        }
        return name;
    }

    // ── Export ────────────────────────────────────────────────────────────────

    /**
     * Exports a DuckDB table to an Arrow IPC stream file on disk.
     *
     * <p>Calls {@code DuckDBResultSet.arrowExportStream(allocator, batchSize)} which
     * uses DuckDB's native C Data Interface to produce an {@link ArrowReader}; Java
     * then writes each batch to {@code outputFile} using {@link ArrowStreamWriter}.
     * Only one batch ({@value DEFAULT_BATCH_SIZE} rows) is held in off-heap memory
     * at a time — no extension required.</p>
     *
     * @param conn       open DuckDB connection that contains the table
     * @param tableName  name of the DuckDB table to export
     * @param outputFile destination file (created or overwritten)
     * @param allocator  Arrow buffer allocator
     * @throws Exception if the export fails for any reason
     */
    public static void export(DuckDBConnection conn,
                              String tableName,
                              Path outputFile,
                              BufferAllocator allocator) throws Exception {
        try (var stmt = conn.createStatement();
             var rs   = stmt.executeQuery("SELECT * FROM " + requireSafeIdentifier(tableName))) {

            DuckDBResultSet drs = (DuckDBResultSet) rs;

            // DuckDB-native: allocator → native stream → ArrowArrayStream → ArrowReader
            // No arrow extension required — uses built-in C Data Interface support.
            try (ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, DEFAULT_BATCH_SIZE);
                 var fos            = new FileOutputStream(outputFile.toFile());
                 var channel        = fos.getChannel()) {

                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                try (var writer = new ArrowStreamWriter(root, null, channel)) {
                    writer.start();
                    while (reader.loadNextBatch()) {
                        writer.writeBatch();
                    }
                    writer.end();
                }
            }
        }
    }

    // ── Load ──────────────────────────────────────────────────────────────────

    /**
     * Loads an Arrow IPC stream file from disk into a new DuckDB table.
     *
     * <p>Reads the file using {@link ArrowStreamReader}, feeds each batch to DuckDB
     * via the Arrow C Data Interface (same path as
     * {@link DuckDBConnection#registerArrowStream}), then materialises the stream
     * as a new DuckDB table in a single scan pass.</p>
     *
     * @param conn       open DuckDB connection to receive the table
     * @param tableName  name of the DuckDB table to create
     * @param arrowFile  Arrow IPC stream file to read
     * @param allocator  Arrow buffer allocator
     * @throws Exception if loading fails for any reason
     */
    public static void load(DuckDBConnection conn,
                            String tableName,
                            Path arrowFile,
                            BufferAllocator allocator) throws Exception {
        String tmpName = "__arrow_" + requireSafeIdentifier(tableName) + "_" + TMP_SEQ.getAndIncrement();

        try (var fis    = new FileInputStream(arrowFile.toFile());
             var reader = new ArrowStreamReader(fis, allocator);
             var stream = ArrowArrayStream.allocateNew(allocator)) {

            // Export lazily into the C Data Interface stream (no reading yet)
            Data.exportArrayStream(allocator, reader, stream);

            // Register the stream so DuckDB can read it as a virtual table
            conn.registerArrowStream(tmpName, stream);

            // Materialise in one scan — DuckDB calls get_next() batch by batch
            try (var stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + requireSafeIdentifier(tableName)
                        + " AS SELECT * FROM " + tmpName);
            }
        }
    }
}
