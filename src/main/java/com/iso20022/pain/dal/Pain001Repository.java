package com.iso20022.pain.dal;

import com.iso20022.pain.arrow.ArrowBatchResult;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Data access layer (DAL) that wraps an in-process DuckDB connection and
 * provides SQL-based access to the three pain.001 tables
 * (message, remittance, transactions).
 *
 * <p>
 * Arrow data is loaded using DuckDB's zero-copy
 * {@link DuckDBConnection#registerArrowStream} API backed by a custom
 * {@link BatchArrowReader} that serves existing {@link VectorSchemaRoot}
 * batches directly via the Arrow C Data Interface — no intermediate IPC
 * serialisation or re-allocation.  Each batch is loaded into a shared root
 * via {@link VectorLoader} (buffer-reference transfer only) and exported
 * as an {@link ArrowArrayStream}; DuckDB then materialises the stream into a
 * persistent in-memory table in a single vectorised bulk operation.
 * </p>
 *
 * <p><b>Memory model:</b> because DuckDB always copies data on
 * {@code CREATE TABLE AS}, peak memory during registration is exactly
 * 2× the Arrow data size (original Arrow off-heap + DuckDB buffer pool).
 * The previous IPC-based approach produced 3–4× because it also held an
 * in-memory IPC byte array on the Java heap and a second set of Arrow
 * off-heap buffers allocated by {@code ArrowStreamReader}.
 * </p>
 *
 * <p>
 * All public query methods are {@code synchronized} because the underlying
 * DuckDB JDBC connection is not thread-safe.
 * </p>
 *
 * <p>
 * Implements {@link AutoCloseable} — close the repository when done to release
 * the DuckDB connection and its buffer pool.
 * </p>
 */
public final class Pain001Repository implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Pain001Repository.class);

    /** DuckDB memory budget — keeps the in-process engine from using unbounded RAM. */
    private static final String DUCKDB_MEMORY_LIMIT = "1GB";

    /** Simple record for a validation issue returned by the repository. */
    public record Issue(String id, String message) {}

    private final DuckDBConnection conn;
    private final BufferAllocator allocator;

    /**
     * Opens an in-process DuckDB database and loads the Arrow batch result into
     * three tables: {@code message}, {@code remittance}, and {@code transactions}.
     *
     * <p>Arrow data is transferred via the C Data Interface zero-copy path:
     * each table's batches are serialised to an in-memory Arrow IPC stream,
     * wrapped in an {@link ArrowArrayStream}, then registered with DuckDB's
     * {@code registerArrowStream} and materialised with {@code CREATE TABLE AS}.
     * </p>
     *
     * @param result    the parsed Arrow batch result
     * @param allocator the Arrow buffer allocator (same root as the batch allocators)
     * @throws Exception if DuckDB cannot be opened or data cannot be loaded
     */
    public Pain001Repository(ArrowBatchResult result, BufferAllocator allocator) throws Exception {
        this.allocator = allocator;
        this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        conn.createStatement().execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");

        loadViaStream("message",      List.of(result.getMessageRoot()));
        loadViaStream("remittance",   result.getRemittanceBatches());
        loadViaStream("transactions", result.getTransactionBatches());

        LOG.debug("Pain001Repository loaded: {} message, {} remittance, {} transaction rows",
                result.getMessageRowCount(), result.getRemittanceRowCount(),
                result.getTransactionRowCount());
    }

    // ─── Zero-copy data loading via Arrow C Data Interface ────────────────────

    /**
     * Loads a list of Arrow batches into a DuckDB in-memory table using the
     * zero-copy {@link DuckDBConnection#registerArrowStream} API.
     *
     * <p>A {@link BatchArrowReader} wraps the existing batches and serves them
     * directly via the C Data Interface — no IPC serialisation, no heap copy,
     * no second set of Arrow off-heap buffers.  Peak additional memory during
     * ingestion is exactly 1× the Arrow data size (DuckDB's own buffer pool),
     * for a combined peak of 2× while both Arrow and DuckDB tables are live.</p>
     */
    private void loadViaStream(String tableName, List<VectorSchemaRoot> batches)
            throws Exception {

        try (BatchArrowReader reader = new BatchArrowReader(batches, allocator);
             ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {

            Data.exportArrayStream(allocator, reader, stream);

            String tmpName = "_tmp_" + tableName;
            conn.registerArrowStream(tmpName, stream);
            conn.createStatement().execute(
                    "CREATE TABLE " + tableName + " AS SELECT * FROM " + tmpName);
        }

        LOG.debug("Loaded '{}': {} batch(es) via direct-batch Arrow stream", tableName, batches.size());
    }

    /**
     * Minimal {@link ArrowReader} that serves existing in-memory
     * {@link VectorSchemaRoot} batches directly via the Arrow C Data Interface,
     * without any intermediate IPC serialisation.
     *
     * <p>Each call to {@link #loadNextBatch()} uses {@link VectorUnloader} to
     * obtain buffer references from the source batch and {@link VectorLoader} to
     * load them into a shared root — a reference-count increment only, not a data
     * copy.  The shared root is then exported by {@link Data#exportArrayStream}
     * directly to the C Data Interface pointer that DuckDB reads from.</p>
     */
    private static final class BatchArrowReader extends ArrowReader {

        private final List<VectorSchemaRoot> batches;
        private final VectorSchemaRoot sharedRoot;
        private int nextIndex = 0;
        private long totalBytesRead = 0L;

        BatchArrowReader(List<VectorSchemaRoot> batches, BufferAllocator allocator) {
            super(allocator);
            if (batches.isEmpty()) {
                throw new IllegalArgumentException("batches list must not be empty");
            }
            this.batches = batches;
            // Initialise eagerly so getVectorSchemaRoot() always returns a valid root
            // before the first loadNextBatch() call (needed for the schema callback).
            this.sharedRoot = VectorSchemaRoot.create(batches.get(0).getSchema(), allocator);
        }

        /** Returns the shared root that is populated on each {@link #loadNextBatch()}. */
        @Override
        public VectorSchemaRoot getVectorSchemaRoot() {
            return sharedRoot;
        }

        @Override
        protected Schema readSchema() {
            return batches.get(0).getSchema();
        }

        /**
         * Advances to the next source batch, loading its buffers into the shared
         * root via {@link VectorLoader} (buffer-reference transfer, no data copy).
         * Accumulates buffer capacities in {@link #bytesRead()} for observability.
         */
        @Override
        public boolean loadNextBatch() throws IOException {
            if (nextIndex >= batches.size()) {
                return false;
            }
            VectorSchemaRoot src = batches.get(nextIndex++);
            try (ArrowRecordBatch rb = new VectorUnloader(src).getRecordBatch()) {
                rb.getBuffers().forEach(buf -> totalBytesRead += buf.capacity());
                new VectorLoader(sharedRoot).load(rb);
                sharedRoot.setRowCount(src.getRowCount());
            } catch (Exception e) {
                throw new IOException("Failed to load batch " + (nextIndex - 1), e);
            }
            return true;
        }

        /** Returns cumulative capacity of all Arrow buffers served so far. */
        @Override
        public long bytesRead() {
            return totalBytesRead;
        }

        /** Closes the shared root; the parent's null root field is a safe no-op. */
        @Override
        public void close() throws IOException {
            sharedRoot.close();
        }

        /** No external read source to close — batches are in-memory. */
        @Override
        protected void closeReadSource() throws IOException {
            // no-op: source batches are owned by the caller (ArrowBatchResult)
        }
    }

    // ─── Validation queries ───────────────────────────────────────────────────

    /**
     * Validates message-level fields:
     * <ul>
     *   <li>MsgId length &le; 35</li>
     *   <li>InitgPty name present</li>
     *   <li>CreDtTm present</li>
     * </ul>
     *
     * @return list of issues found (empty means all OK)
     * @throws SQLException on query failure
     */
    public synchronized List<Issue> validateMessageFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message WHERE LENGTH(msg_id) > 35")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "MsgId exceeds maximum length of 35 characters"));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message"
                        + " WHERE msg_initg_pty_nm IS NULL OR msg_initg_pty_nm = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "WARN:Initiating party (InitgPty) is missing"));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message"
                        + " WHERE msg_cre_dt_tm IS NULL OR msg_cre_dt_tm = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "Creation date/time (CreDtTm) is required but missing"));
                }
            }
        }

        return issues;
    }

    /**
     * Validates remittance-level fields:
     * <ul>
     *   <li>Debtor IBAN matches {@code ^[A-Z]{2}[0-9]{2}[A-Z0-9]+$}</li>
     *   <li>Payment method present</li>
     * </ul>
     *
     * @return list of issues found (empty means all OK)
     * @throws SQLException on query failure
     */
    public synchronized List<Issue> validateRemittanceFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id, dbtr_acct_iban FROM remittance"
                        + " WHERE NOT regexp_matches(dbtr_acct_iban,"
                        + " '^[A-Z]{2}[0-9]{2}[A-Z0-9]+$')")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "Invalid IBAN format: " + rs.getString(2)));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id FROM remittance"
                        + " WHERE pmt_mtd IS NULL OR pmt_mtd = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "Payment method is required"));
                }
            }
        }

        return issues;
    }

    /**
     * Validates transaction-level fields:
     * <ul>
     *   <li>Instructed amount &gt; 0</li>
     *   <li>Creditor name present</li>
     * </ul>
     *
     * @return list of issues found (empty means all OK)
     * @throws SQLException on query failure
     */
    public synchronized List<Issue> validateTransactionFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id, end_to_end_id, instd_amt FROM transactions"
                        + " WHERE instd_amt <= 0")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(2),
                            "Amount must be positive: PmtInfId=" + rs.getString(1)
                                    + ", Amount=" + rs.getBigDecimal(3)));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id, end_to_end_id FROM transactions"
                        + " WHERE cdtr_nm IS NULL OR cdtr_nm = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(2),
                            "Creditor name is required: PmtInfId=" + rs.getString(1)));
                }
            }
        }

        return issues;
    }

    /**
     * Validates control sums at both remittance and message level.
     *
     * @return list of issues found (empty means all control sums match)
     * @throws SQLException on query failure
     */
    public synchronized List<Issue> validateControlSums() throws SQLException {
        List<Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT r.pmt_inf_id,"
                        + " CAST(r.ctrl_sum AS DOUBLE) AS declared,"
                        + " CAST(SUM(t.instd_amt) AS DOUBLE) AS actual"
                        + " FROM remittance r"
                        + " JOIN transactions t ON r.pmt_inf_id = t.pmt_inf_id"
                        + " WHERE r.ctrl_sum IS NOT NULL"
                        + " GROUP BY r.pmt_inf_id, r.ctrl_sum"
                        + " HAVING ABS(CAST(r.ctrl_sum AS DOUBLE)"
                        + "        - CAST(SUM(t.instd_amt) AS DOUBLE)) > 0.001")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "Remittance CtrlSum mismatch: declared=" + rs.getDouble(2)
                                    + ", actual=" + rs.getDouble(3)));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT m.msg_id,"
                        + " CAST(m.msg_ctrl_sum AS DOUBLE) AS declared,"
                        + " CAST(SUM(t.instd_amt) AS DOUBLE) AS actual"
                        + " FROM message m"
                        + " JOIN remittance r ON m.msg_id = r.msg_id"
                        + " JOIN transactions t ON r.pmt_inf_id = t.pmt_inf_id"
                        + " GROUP BY m.msg_id, m.msg_ctrl_sum"
                        + " HAVING ABS(CAST(m.msg_ctrl_sum AS DOUBLE)"
                        + "        - CAST(SUM(t.instd_amt) AS DOUBLE)) > 0.001")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "Message CtrlSum mismatch: declared=" + rs.getDouble(2)
                                    + ", actual=" + rs.getDouble(3)));
                }
            }
        }

        return issues;
    }

    // ─── Analytics queries ────────────────────────────────────────────────────

    /**
     * Returns IBANs that do not match the standard IBAN regex.
     *
     * @return list of invalid debtor IBANs
     * @throws SQLException on query failure
     */
    public synchronized List<String> findInvalidIbans() throws SQLException {
        List<String> ibans = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT dbtr_acct_iban FROM remittance"
                        + " WHERE NOT regexp_matches(dbtr_acct_iban,"
                        + " '^[A-Z]{2}[0-9]{2}[A-Z0-9]+$')")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ibans.add(rs.getString(1));
                }
            }
        }
        return ibans;
    }

    /**
     * Returns the sum of instructed amounts for a given payment information block.
     *
     * @param pmtInfId the payment information ID to filter on
     * @return sum of {@code instd_amt}, or {@code BigDecimal.ZERO} if not found
     * @throws SQLException on query failure
     */
    public synchronized BigDecimal sumTransactionsByRemittance(String pmtInfId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SUM(instd_amt) FROM transactions WHERE pmt_inf_id = ?")) {
            ps.setString(1, pmtInfId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    BigDecimal sum = rs.getBigDecimal(1);
                    return sum != null ? sum : BigDecimal.ZERO;
                }
            }
        }
        return BigDecimal.ZERO;
    }

    /**
     * Returns message-level summary fields.
     *
     * @return single-row result as a formatted string, or empty string if no message
     * @throws SQLException on query failure
     */
    public synchronized String getMessageSummary() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id, msg_cre_dt_tm, msg_nb_of_txs,"
                        + " msg_ctrl_sum, msg_initg_pty_nm FROM message LIMIT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return "msg_id=" + rs.getString(1)
                            + ", cre_dt_tm=" + rs.getString(2)
                            + ", nb_of_txs=" + rs.getString(3)
                            + ", ctrl_sum=" + rs.getBigDecimal(4)
                            + ", initg_pty_nm=" + rs.getString(5);
                }
            }
        }
        return "";
    }

    /**
     * Returns the total number of remittance rows.
     *
     * @return remittance row count
     * @throws SQLException on query failure
     */
    public synchronized long getRemittanceCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM remittance")) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    /**
     * Returns the total number of transaction rows.
     *
     * @return transaction row count
     * @throws SQLException on query failure
     */
    public synchronized long getTransactionCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM transactions")) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    /**
     * Returns the sum of all instructed amounts across all transactions.
     *
     * @return grand total of {@code instd_amt}, or {@code BigDecimal.ZERO}
     * @throws SQLException on query failure
     */
    public synchronized BigDecimal getTotalTransactionAmount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SUM(instd_amt) FROM transactions")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    BigDecimal total = rs.getBigDecimal(1);
                    return total != null ? total : BigDecimal.ZERO;
                }
            }
        }
        return BigDecimal.ZERO;
    }

    // ─── AutoCloseable ────────────────────────────────────────────────────────

    /**
     * Closes the underlying DuckDB connection and releases its buffer pool.
     *
     * @throws SQLException if the connection cannot be closed
     */
    @Override
    public void close() throws SQLException {
        conn.close();
        LOG.debug("Pain001Repository closed");
    }
}
