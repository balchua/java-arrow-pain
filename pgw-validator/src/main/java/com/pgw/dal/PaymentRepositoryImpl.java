package com.pgw.dal;

import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DuckDB-backed implementation of {@link PaymentRepository}.
 *
 * <p>Wraps a pre-populated {@link DuckDBConnection} whose {@code message},
 * {@code remittance}, and {@code transactions} tables have already been loaded
 * by the streaming ingestor pipeline.</p>
 */
public final class PaymentRepositoryImpl implements PaymentRepository {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentRepositoryImpl.class);

    private final DuckDBConnection conn;

    /**
     * Wraps a pre-populated DuckDB connection. No data loading occurs.
     * Used by the streaming pipeline (tables populated live during parsing)
     * and by the benchmark tests (tables loaded via {@link com.pgw.ArrowIpc#load}).
     *
     * @param conn a DuckDB connection whose tables are already populated
     */
    public PaymentRepositoryImpl(DuckDBConnection conn) {
        this.conn = conn;
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
                        + " reqd_exctn_dt, dbtr_nm, dbtr_acct_iban, dbtr_agt_bicfi,"
                        + " btch_bookg, instr_prty, lcl_instrm_cd, ctgy_purp_cd, chrg_br, ultmt_dbtr_nm"
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
                            rs.getString(10),
                            rs.getString(11),
                            rs.getString(12),
                            rs.getString(13),
                            rs.getString(14),
                            rs.getString(15),
                            rs.getString(16)));
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
                        + " cdtr_agt_bicfi, cdtr_nm, cdtr_acct_iban, rmt_inf_ustrd, rglty_rptg_cd,"
                        + " rmt_inf_strd_ref, rmt_inf_strd_ref_tp, purp_cd, ultmt_cdtr_nm, cdtr_ctry"
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
                            rs.getString(10),
                            rs.getString(11),
                            rs.getString(12),
                            rs.getString(13),
                            rs.getString(14),
                            rs.getString(15)));
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        conn.close();
        LOG.debug("PaymentRepositoryImpl closed");
    }
}
