package com.pgw.dal;

import com.pgw.arrow.ArrowBatchResult;
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
 * DuckDB-backed implementation of {@link PaymentRepository}.
 *
 * <p>
 * Arrow data is loaded using DuckDB's zero-copy
 * {@link DuckDBConnection#registerArrowStream} API backed by a custom
 * {@link BatchArrowReader} that serves existing {@link VectorSchemaRoot}
 * batches directly via the Arrow C Data Interface.
 * </p>
 */
public final class PaymentRepositoryImpl implements PaymentRepository {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentRepositoryImpl.class);

    /** DuckDB memory budget. */
    private static final String DUCKDB_MEMORY_LIMIT = "1GB";

    private final DuckDBConnection conn;
    private final BufferAllocator allocator;
    /** Arrow C Data Interface resources held open until after conn.close(). */
    private final List<AutoCloseable> deferredCloseables = new ArrayList<>();

    /**
     * Opens an in-process DuckDB database and loads the Arrow batch result into
     * three tables: {@code message}, {@code remittance}, and {@code transactions}.
     *
     * @param result    the parsed Arrow batch result
     * @param allocator the Arrow buffer allocator (same root as the batch allocators)
     * @throws Exception if DuckDB cannot be opened or data cannot be loaded
     */
    public PaymentRepositoryImpl(ArrowBatchResult result, BufferAllocator allocator) throws Exception {
        this.allocator = allocator;
        this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (var stmt = conn.createStatement()) {
            stmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
        }

        loadViaStream("message",      List.of(result.getMessageRoot()));
        loadViaStream("remittance",   result.getRemittanceBatches());
        loadViaStream("transactions", result.getTransactionBatches());

        LOG.debug("PaymentRepositoryImpl loaded: {} message, {} remittance, {} transaction rows",
                result.getMessageRowCount(), result.getRemittanceRowCount(),
                result.getTransactionRowCount());
    }

    /**
     * Connects to a pre-populated DuckDB instance. No data loading occurs.
     * Used by the streaming pipeline where tables are populated live during parsing.
     *
     * @param conn a DuckDB connection whose tables are already populated
     */
    public PaymentRepositoryImpl(DuckDBConnection conn) {
        this.conn = conn;
        this.allocator = null;
    }

    private void loadViaStream(String tableName, List<VectorSchemaRoot> batches)
            throws Exception {

        // Reader and stream are intentionally NOT closed here.
        // DuckDB's registerArrowStream transfers C Data Interface ownership of the
        // stream struct to DuckDB. The release callback (which frees Arrow allocator
        // memory) is only invoked when DuckDB drops the scan — i.e. during conn.close().
        // Closing them before conn.close() would free memory DuckDB still needs.
        // They are stored in deferredCloseables and closed in close() after conn.close().
        BatchArrowReader reader = new BatchArrowReader(batches, allocator);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        try {
            Data.exportArrayStream(allocator, reader, stream);

            String tmpName = "_tmp_" + tableName;
            conn.registerArrowStream(tmpName, stream);
            try (var stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + tableName + " AS SELECT * FROM " + tmpName);
            }
            deferredCloseables.add(stream);
            deferredCloseables.add(reader);
        } catch (Exception e) {
            // Export or registration failed — DuckDB never took ownership, close now.
            closeSilently(stream);
            closeSilently(reader);
            throw e;
        }

        LOG.debug("Loaded '{}': {} batch(es) via direct-batch Arrow stream", tableName, batches.size());
    }

    private static void closeSilently(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            LOG.warn("Error closing Arrow resource: {}", e.getMessage());
        }
    }

    private static final class BatchArrowReader extends ArrowReader {

        private final List<VectorSchemaRoot> batches;
        private final VectorSchemaRoot sharedRoot;
        private int nextIndex = 0;
        private long totalBytesRead = 0L;
        private boolean closed = false;

        BatchArrowReader(List<VectorSchemaRoot> batches, BufferAllocator allocator) {
            super(allocator);
            if (batches.isEmpty()) {
                throw new IllegalArgumentException("batches list must not be empty");
            }
            this.batches = batches;
            this.sharedRoot = VectorSchemaRoot.create(batches.get(0).getSchema(), allocator);
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot() {
            return sharedRoot;
        }

        @Override
        protected Schema readSchema() {
            return batches.get(0).getSchema();
        }

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

        @Override
        public long bytesRead() {
            return totalBytesRead;
        }

        /**
         * Idempotent close — DuckDB's C Data Interface release callback may call this,
         * and our deferred close in {@link PaymentRepositoryImpl#close()} calls it too.
         */
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

    @Override
    public synchronized List<PaymentRepository.Issue> validateMessageFields() throws SQLException {
        List<PaymentRepository.Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message WHERE LENGTH(msg_id) > 35")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
                            "MsgId exceeds maximum length of 35 characters"));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message"
                        + " WHERE msg_initg_pty_nm IS NULL OR msg_initg_pty_nm = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
                            "WARN:Initiating party (InitgPty) is missing"));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message"
                        + " WHERE msg_cre_dt_tm IS NULL OR msg_cre_dt_tm = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
                            "Creation date/time (CreDtTm) is required but missing"));
                }
            }
        }

        return issues;
    }

    @Override
    public synchronized List<PaymentRepository.Issue> validateRemittanceFields() throws SQLException {
        List<PaymentRepository.Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id, dbtr_acct_iban FROM remittance"
                        + " WHERE NOT regexp_matches(dbtr_acct_iban,"
                        + " '^[A-Z]{2}[0-9]{2}[A-Z0-9]+$')")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
                            "Invalid IBAN format: " + rs.getString(2)));
                }
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id FROM remittance"
                        + " WHERE pmt_mtd IS NULL OR pmt_mtd = ''")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
                            "Payment method is required"));
                }
            }
        }

        return issues;
    }

    @Override
    public synchronized List<PaymentRepository.Issue> validateTransactionFields() throws SQLException {
        List<PaymentRepository.Issue> issues = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id, end_to_end_id, instd_amt FROM transactions"
                        + " WHERE instd_amt <= 0")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new PaymentRepository.Issue(rs.getString(2),
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
                    issues.add(new PaymentRepository.Issue(rs.getString(2),
                            "Creditor name is required: PmtInfId=" + rs.getString(1)));
                }
            }
        }

        return issues;
    }

    @Override
    public synchronized List<PaymentRepository.Issue> validateControlSums() throws SQLException {
        List<PaymentRepository.Issue> issues = new ArrayList<>();

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
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
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
                    issues.add(new PaymentRepository.Issue(rs.getString(1),
                            "Message CtrlSum mismatch: declared=" + rs.getDouble(2)
                                    + ", actual=" + rs.getDouble(3)));
                }
            }
        }

        return issues;
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    public synchronized long getRemittanceCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM remittance")) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    @Override
    public synchronized long getTransactionCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM transactions")) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    @Override
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

    // ── Streaming domain-object access ───────────────────────────────────────

    @Override
    public synchronized void streamMessages(java.util.function.Consumer<com.pgw.domain.model.Message> handler)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id, msg_cre_dt_tm, msg_nb_of_txs, msg_ctrl_sum, msg_initg_pty_nm"
                        + " FROM message")) {
            ps.setFetchSize(100);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    handler.accept(new com.pgw.domain.model.Message(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getBigDecimal(4),
                            rs.getString(5)));
                }
            }
        }
    }

    @Override
    public synchronized void streamRemittances(String messageId,
            java.util.function.Consumer<com.pgw.domain.model.Remittance> handler)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id, pmt_inf_id, pmt_mtd, nb_of_txs, ctrl_sum, svc_lvl_cd,"
                        + " reqd_exctn_dt, dbtr_nm, dbtr_acct_iban, dbtr_agt_bicfi"
                        + " FROM remittance WHERE msg_id = ?")) {
            ps.setFetchSize(100);
            ps.setString(1, messageId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.sql.Date sqlDate = rs.getDate(7);
                    java.time.LocalDate execDate = sqlDate != null ? sqlDate.toLocalDate() : null;
                    handler.accept(new com.pgw.domain.model.Remittance(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getBigDecimal(5),
                            rs.getString(6),
                            execDate,
                            rs.getString(8),
                            rs.getString(9),
                            rs.getString(10)));
                }
            }
        }
    }

    @Override
    public synchronized void streamTransactions(String remittanceId,
            java.util.function.Consumer<com.pgw.domain.model.Transaction> handler)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT pmt_inf_id, instr_id, end_to_end_id, instd_amt, ccy,"
                        + " cdtr_agt_bicfi, cdtr_nm, cdtr_acct_iban, rmt_inf_ustrd, rglty_rptg_cd"
                        + " FROM transactions WHERE pmt_inf_id = ?")) {
            ps.setFetchSize(1000);
            ps.setString(1, remittanceId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    handler.accept(new com.pgw.domain.model.Transaction(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getBigDecimal(4),
                            rs.getString(5),
                            rs.getString(6),
                            rs.getString(7),
                            rs.getString(8),
                            rs.getString(9),
                            rs.getString(10)));
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        // Close DuckDB connection FIRST — this invokes all C Data Interface release
        // callbacks synchronously, freeing DuckDB's references to the Arrow stream structs.
        conn.close();
        // THEN close the Arrow stream objects. The release callbacks have already fired,
        // so the Arrow allocator can reclaim the memory without conflict.
        if (allocator != null) {
            for (AutoCloseable c : deferredCloseables) {
                closeSilently(c);
            }
        }
        LOG.debug("PaymentRepositoryImpl closed");
    }
}
