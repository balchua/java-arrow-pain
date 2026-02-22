package com.iso20022.pain.parser;

import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.persistence.PersistenceService;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.duckdb.DuckDBConnection;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements {@link BatchConsumer}. On each {@link #accept} call:
 * <ol>
 * <li>Appends the batch to the live DuckDB table using Arrow C Data Interface.</li>
 * <li>Calls {@link PersistenceService#writeBatch(BatchConsumer.TableType, VectorSchemaRoot)}.</li>
 * </ol>
 *
 * <p>Does NOT hold any reference to {@code root} after returning.</p>
 *
 * <p>The constructor creates the three DuckDB tables (empty, with correct schema) using
 * {@code CREATE TABLE IF NOT EXISTS}.</p>
 */
public final class StreamingBatchConsumer implements BatchConsumer {

    private static final AtomicLong TMP_SEQ = new AtomicLong(0);

    private final DuckDBConnection conn;
    private final PersistenceService persistenceService;
    private final BufferAllocator allocator;

    /**
     * Creates a StreamingBatchConsumer that writes to DuckDB and the given persistence service.
     *
     * @param conn               a live DuckDB connection (will be populated with three tables)
     * @param persistenceService the sink for Arrow IPC stream output
     * @param allocator          Arrow buffer allocator (for C Data Interface operations)
     * @throws SQLException if the DuckDB tables cannot be created
     */
    public StreamingBatchConsumer(DuckDBConnection conn, PersistenceService persistenceService,
            BufferAllocator allocator) throws SQLException {
        this.conn = conn;
        this.persistenceService = persistenceService;
        this.allocator = allocator;
        createTables();
    }

    private void createTables() throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS message ("
                    + "msg_id VARCHAR, "
                    + "msg_cre_dt_tm VARCHAR, "
                    + "msg_nb_of_txs VARCHAR, "
                    + "msg_ctrl_sum DECIMAL(18,2), "
                    + "msg_initg_pty_nm VARCHAR"
                    + ")");

            stmt.execute("CREATE TABLE IF NOT EXISTS remittance ("
                    + "msg_id VARCHAR, "
                    + "pmt_inf_id VARCHAR, "
                    + "pmt_mtd VARCHAR, "
                    + "nb_of_txs VARCHAR, "
                    + "ctrl_sum DECIMAL(18,2), "
                    + "svc_lvl_cd VARCHAR, "
                    + "reqd_exctn_dt DATE, "
                    + "dbtr_nm VARCHAR, "
                    + "dbtr_acct_iban VARCHAR, "
                    + "dbtr_agt_bicfi VARCHAR"
                    + ")");

            stmt.execute("CREATE TABLE IF NOT EXISTS transactions ("
                    + "pmt_inf_id VARCHAR, "
                    + "instr_id VARCHAR, "
                    + "end_to_end_id VARCHAR, "
                    + "instd_amt DECIMAL(18,5), "
                    + "ccy VARCHAR, "
                    + "cdtr_agt_bicfi VARCHAR, "
                    + "cdtr_nm VARCHAR, "
                    + "cdtr_acct_iban VARCHAR, "
                    + "rmt_inf_ustrd VARCHAR, "
                    + "rglty_rptg_cd VARCHAR"
                    + ")");
        }
    }

    @Override
    public void accept(TableType tableType, VectorSchemaRoot root)
            throws IOException, XMLStreamException {
        // 1. Insert batch into DuckDB via Arrow C Data Interface
        try {
            insertIntoDuckDb(tableType, root);
        } catch (Exception e) {
            throw new IOException("Failed to insert batch into DuckDB: " + e.getMessage(), e);
        }

        // 2. Write batch to persistence service
        persistenceService.writeBatch(tableType, root);
    }

    private void insertIntoDuckDb(TableType tableType, VectorSchemaRoot root) throws Exception {
        String tableName = switch (tableType) {
            case MESSAGE -> "message";
            case REMITTANCE -> "remittance";
            case TRANSACTION -> "transactions";
        };

        // Use a unique temporary view name to avoid collisions
        String tmpName = "_tmp_" + tableName + "_" + TMP_SEQ.getAndIncrement();

        // Create a single-batch reader that serves the current root
        SingleBatchArrowReader reader = new SingleBatchArrowReader(root, allocator);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        try {
            Data.exportArrayStream(allocator, reader, stream);
            conn.registerArrowStream(tmpName, stream);
            try (var stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + tableName + " SELECT * FROM " + tmpName);
            }
        } finally {
            closeSilently(stream);
            closeSilently(reader);
        }
    }

    private static void closeSilently(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ignored) {
        }
    }

    // ── Internal: single-batch ArrowReader ────────────────────────────────────

    private static final class SingleBatchArrowReader extends ArrowReader {

        private final VectorSchemaRoot source;
        private final VectorSchemaRoot sharedRoot;
        private boolean batched = false;
        private boolean closed = false;

        SingleBatchArrowReader(VectorSchemaRoot source, BufferAllocator allocator) {
            super(allocator);
            this.source = source;
            this.sharedRoot = VectorSchemaRoot.create(source.getSchema(), allocator);
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot() {
            return sharedRoot;
        }

        @Override
        protected Schema readSchema() {
            return source.getSchema();
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            if (batched) return false;
            batched = true;
            try (ArrowRecordBatch rb = new VectorUnloader(source).getRecordBatch()) {
                new VectorLoader(sharedRoot).load(rb);
                sharedRoot.setRowCount(source.getRowCount());
            } catch (Exception e) {
                throw new IOException("Failed to load batch", e);
            }
            return true;
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        public synchronized void close() throws IOException {
            if (!closed) {
                closed = true;
                sharedRoot.close();
            }
        }

        @Override
        protected void closeReadSource() throws IOException {
            // no-op
        }
    }
}
