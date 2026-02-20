package com.iso20022.pain.dal;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Data access layer (DAL) that wraps an in-process DuckDB connection and
 * provides SQL-based access to the three pain.001 tables
 * (message, remittance, transactions).
 *
 * <p>
 * The constructor loads the Arrow data from the {@link ArrowBatchResult} into
 * DuckDB in-memory tables via the DuckDB JDBC Appender, then all public
 * methods execute SQL queries against those tables.
 * Callers (validators) never see Arrow types.
 * </p>
 *
 * <p>
 * Implements {@link AutoCloseable} — close the repository when done to release
 * the DuckDB connection and its buffer pool.
 * </p>
 */
public final class Pain001Repository implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Pain001Repository.class);

    /** Simple record for a validation issue returned by the repository. */
    public record Issue(String id, String message) {}

    private final DuckDBConnection conn;

    /**
     * Opens an in-process DuckDB database and loads the Arrow batch result into
     * three tables: {@code message}, {@code remittance}, and {@code transactions}.
     *
     * @param result the parsed Arrow batch result
     * @throws SQLException if DuckDB cannot be opened or data cannot be loaded
     */
    public Pain001Repository(ArrowBatchResult result) throws SQLException {
        this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        createTables();
        loadMessage(result.getMessageRoot());
        loadRemittance(result.getRemittanceBatches());
        loadTransactions(result.getTransactionBatches());
        LOG.debug("Pain001Repository loaded: {} message, {} remittance, {} transaction rows",
                result.getMessageRowCount(), result.getRemittanceRowCount(),
                result.getTransactionRowCount());
    }

    // ─── Schema creation ──────────────────────────────────────────────────────

    private void createTables() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE message ("
                            + "msg_id VARCHAR,"
                            + "msg_cre_dt_tm VARCHAR,"
                            + "msg_nb_of_txs VARCHAR,"
                            + "msg_ctrl_sum DECIMAL(18,2),"
                            + "msg_initg_pty_nm VARCHAR)");
            st.execute(
                    "CREATE TABLE remittance ("
                            + "msg_id VARCHAR,"
                            + "pmt_inf_id VARCHAR,"
                            + "pmt_mtd VARCHAR,"
                            + "nb_of_txs VARCHAR,"
                            + "ctrl_sum DECIMAL(18,2),"
                            + "svc_lvl_cd VARCHAR,"
                            + "reqd_exctn_dt VARCHAR,"
                            + "dbtr_nm VARCHAR,"
                            + "dbtr_acct_iban VARCHAR,"
                            + "dbtr_agt_bicfi VARCHAR)");
            st.execute(
                    "CREATE TABLE transactions ("
                            + "pmt_inf_id VARCHAR,"
                            + "instr_id VARCHAR,"
                            + "end_to_end_id VARCHAR,"
                            + "instd_amt DECIMAL(18,5),"
                            + "ccy VARCHAR,"
                            + "cdtr_agt_bicfi VARCHAR,"
                            + "cdtr_nm VARCHAR,"
                            + "cdtr_acct_iban VARCHAR,"
                            + "rmt_inf_ustrd VARCHAR)");
        }
    }

    // ─── Data loading ─────────────────────────────────────────────────────────

    private void loadMessage(VectorSchemaRoot root) throws SQLException {
        try (DuckDBAppender appender = conn.createAppender(
                DuckDBConnection.DEFAULT_SCHEMA, "message")) {
            int rows = root.getRowCount();
            VarCharVector msgId   = (VarCharVector)  root.getVector(Pain001ArrowSchema.MSG_ID);
            VarCharVector creDtTm = (VarCharVector)  root.getVector(Pain001ArrowSchema.MSG_CRE_DT_TM);
            VarCharVector nbOfTxs = (VarCharVector)  root.getVector(Pain001ArrowSchema.MSG_NB_OF_TXS);
            DecimalVector ctrlSum = (DecimalVector)  root.getVector(Pain001ArrowSchema.MSG_CTRL_SUM);
            VarCharVector initgPty = (VarCharVector) root.getVector(Pain001ArrowSchema.MSG_INITG_PTY_NM);

            for (int i = 0; i < rows; i++) {
                appender.beginRow();
                appendVarChar(appender, msgId, i);
                appendVarChar(appender, creDtTm, i);
                appendVarChar(appender, nbOfTxs, i);
                appendDecimal(appender, ctrlSum, i);
                appendVarChar(appender, initgPty, i);
                appender.endRow();
            }
            appender.flush();
        }
    }

    private void loadRemittance(List<VectorSchemaRoot> batches) throws SQLException {
        try (DuckDBAppender appender = conn.createAppender(
                DuckDBConnection.DEFAULT_SCHEMA, "remittance")) {
            for (VectorSchemaRoot batch : batches) {
                int rows = batch.getRowCount();
                VarCharVector msgId      = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_MSG_ID);
                VarCharVector pmtInfId   = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_PMT_INF_ID);
                VarCharVector pmtMtd     = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_PMT_MTD);
                VarCharVector nbOfTxs    = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_NB_OF_TXS);
                DecimalVector ctrlSum    = (DecimalVector)  batch.getVector(Pain001ArrowSchema.RMT_CTRL_SUM);
                VarCharVector svcLvlCd   = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_SVC_LVL_CD);
                DateDayVector reqdDt     = (DateDayVector)  batch.getVector(Pain001ArrowSchema.RMT_REQD_EXCTN_DT);
                VarCharVector dbtrNm     = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_DBTR_NM);
                VarCharVector dbtrIban   = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN);
                VarCharVector dbtrBicfi  = (VarCharVector)  batch.getVector(Pain001ArrowSchema.RMT_DBTR_AGT_BICFI);

                for (int i = 0; i < rows; i++) {
                    appender.beginRow();
                    appendVarChar(appender, msgId, i);
                    appendVarChar(appender, pmtInfId, i);
                    appendVarChar(appender, pmtMtd, i);
                    appendVarChar(appender, nbOfTxs, i);
                    appendDecimal(appender, ctrlSum, i);
                    appendVarChar(appender, svcLvlCd, i);
                    appendDate(appender, reqdDt, i);
                    appendVarChar(appender, dbtrNm, i);
                    appendVarChar(appender, dbtrIban, i);
                    appendVarChar(appender, dbtrBicfi, i);
                    appender.endRow();
                }
            }
            appender.flush();
        }
    }

    private void loadTransactions(List<VectorSchemaRoot> batches) throws SQLException {
        try (DuckDBAppender appender = conn.createAppender(
                DuckDBConnection.DEFAULT_SCHEMA, "transactions")) {
            for (VectorSchemaRoot batch : batches) {
                int rows = batch.getRowCount();
                VarCharVector pmtInfId   = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_PMT_INF_ID);
                VarCharVector instrId    = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_INSTR_ID);
                VarCharVector e2eId      = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_END_TO_END_ID);
                DecimalVector instdAmt   = (DecimalVector)  batch.getVector(Pain001ArrowSchema.TX_INSTD_AMT);
                VarCharVector ccy        = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_CCY);
                VarCharVector cdtrBicfi  = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_CDTR_AGT_BICFI);
                VarCharVector cdtrNm     = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_CDTR_NM);
                VarCharVector cdtrIban   = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_CDTR_ACCT_IBAN);
                VarCharVector rmtInf     = (VarCharVector)  batch.getVector(Pain001ArrowSchema.TX_RMT_INF_USTRD);

                for (int i = 0; i < rows; i++) {
                    appender.beginRow();
                    appendVarChar(appender, pmtInfId, i);
                    appendVarChar(appender, instrId, i);
                    appendVarChar(appender, e2eId, i);
                    appendDecimal(appender, instdAmt, i);
                    appendVarChar(appender, ccy, i);
                    appendVarChar(appender, cdtrBicfi, i);
                    appendVarChar(appender, cdtrNm, i);
                    appendVarChar(appender, cdtrIban, i);
                    appendVarChar(appender, rmtInf, i);
                    appender.endRow();
                }
            }
            appender.flush();
        }
    }

    // ─── Appender helpers ────────────────────────────────────────────────────

    private static void appendVarChar(DuckDBAppender appender, VarCharVector vec, int i)
            throws SQLException {
        if (vec.isNull(i)) {
            appender.append((String) null);
        } else {
            appender.append(new String(vec.get(i), StandardCharsets.UTF_8));
        }
    }

    private static void appendDecimal(DuckDBAppender appender, DecimalVector vec, int i)
            throws SQLException {
        if (vec.isNull(i)) {
            appender.appendBigDecimal(null);
        } else {
            appender.appendBigDecimal(vec.getObject(i));
        }
    }

    private static void appendDate(DuckDBAppender appender, DateDayVector vec, int i)
            throws SQLException {
        if (vec.isNull(i)) {
            appender.append((String) null);
        } else {
            // Arrow DateDay stores epoch days; convert to ISO date string
            appender.append(LocalDate.ofEpochDay(vec.get(i)).toString());
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

        // MsgId too long
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT msg_id FROM message WHERE LENGTH(msg_id) > 35")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    issues.add(new Issue(rs.getString(1),
                            "MsgId exceeds maximum length of 35 characters"));
                }
            }
        }

        // Missing InitgPty name (warn)
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

        // Missing CreDtTm
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

        // Invalid IBAN
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

        // Missing payment method
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

        // Non-positive amount
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

        // Missing creditor name
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
     * <p>Remittance-level: each {@code remittance.ctrl_sum} is compared to the
     * sum of its child transaction amounts.</p>
     * <p>Message-level: {@code message.msg_ctrl_sum} is compared to the grand
     * total of all transaction amounts.</p>
     *
     * @return list of issues found (empty means all control sums match)
     * @throws SQLException on query failure
     */
    public synchronized List<Issue> validateControlSums() throws SQLException {
        List<Issue> issues = new ArrayList<>();

        // Remittance-level control sum mismatch
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

        // Message-level control sum mismatch
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

