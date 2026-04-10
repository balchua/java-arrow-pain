package com.pgw.purearrow.validator.dal;

import com.pgw.dal.PaymentRepository;
import com.pgw.domain.model.Message;
import com.pgw.domain.model.Remittance;
import com.pgw.domain.model.Transaction;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Pure-Arrow implementation of {@link PaymentRepository}.
 *
 * <p>Reads ISO 20022 pain.001 data from three in-memory Arrow table objects
 * ({@link ArrowMessageTable}, {@link ArrowRemittanceTable},
 * {@link ArrowTransactionTable}) loaded from Arrow IPC stream files on disk.
 * No DuckDB, no JDBC, no SQL is involved at any point.</p>
 *
 * <p>Validation logic mirrors the SQL predicates in {@code PaymentRepositoryImpl}
 * exactly, translated to pure Java:
 * <ul>
 *   <li>String length checks replace {@code LENGTH()} SQL functions.</li>
 *   <li>Java {@link Pattern} replaces DuckDB {@code regexp_matches()}.</li>
 *   <li>BigDecimal arithmetic replaces SQL {@code SUM} and {@code HAVING ABS(...) > 0.001}.</li>
 * </ul>
 * </p>
 *
 * <p>Instances must be obtained via {@link ArrowPaymentRepositoryLoader}. Call
 * {@link #close()} to release all Arrow off-heap memory when done.</p>
 */
public final class ArrowPaymentRepositoryImpl implements PaymentRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowPaymentRepositoryImpl.class);

    /** Mirrors DuckDB: {@code regexp_matches(iban, '^[A-Z]{2}[0-9]{2}[A-Z0-9]+$')} */
    private static final Pattern IBAN_PATTERN =
            Pattern.compile("^[A-Z]{2}[0-9]{2}[A-Z0-9]+$");

    /** Threshold for control-sum mismatch; mirrors DuckDB: {@code ABS(...) > 0.001} */
    private static final BigDecimal CTRL_SUM_TOLERANCE = new BigDecimal("0.001");

    private final ArrowMessageTable     messageTable;
    private final ArrowRemittanceTable  remittanceTable;
    private final ArrowTransactionTable transactionTable;

    ArrowPaymentRepositoryImpl(
            ArrowMessageTable     messageTable,
            ArrowRemittanceTable  remittanceTable,
            ArrowTransactionTable transactionTable) {
        this.messageTable     = messageTable;
        this.remittanceTable  = remittanceTable;
        this.transactionTable = transactionTable;
    }

    // ── SQL-level bulk validators ────────────────────────────────────────────

    @Override
    public List<Issue> validateMessageFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        messageTable.forEach(msg -> {
            String id = msg.messageId();
            if (id != null && id.length() > 35) {
                issues.add(new Issue(id, "MsgId exceeds maximum length of 35 characters"));
            }
            String initgPty = msg.initiatingParty();
            if (initgPty == null || initgPty.isEmpty()) {
                issues.add(new Issue(id, "WARN:Initiating party (InitgPty) is missing"));
            }
            String creDtTm = msg.creationDateTime();
            if (creDtTm == null || creDtTm.isEmpty()) {
                issues.add(new Issue(id, "Creation date/time (CreDtTm) is required but missing"));
            }
        });
        LOG.debug("validateMessageFields: {} issue(s)", issues.size());
        return issues;
    }

    @Override
    public List<Issue> validateRemittanceFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        remittanceTable.forEach(rmt -> {
            String id    = rmt.remittanceId();
            String iban  = rmt.debtorAccountIban();
            if (iban != null && !IBAN_PATTERN.matcher(iban).matches()) {
                issues.add(new Issue(id, "Invalid IBAN format: " + iban));
            }
            String pmtMtd = rmt.paymentMethod();
            if (pmtMtd == null || pmtMtd.isEmpty()) {
                issues.add(new Issue(id, "Payment method is required"));
            }
        });
        LOG.debug("validateRemittanceFields: {} issue(s)", issues.size());
        return issues;
    }

    @Override
    public List<Issue> validateTransactionFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        transactionTable.forEach(tx -> {
            String e2eId = tx.endToEndId();
            BigDecimal amt = tx.instructedAmount();
            if (amt == null || amt.compareTo(BigDecimal.ZERO) <= 0) {
                issues.add(new Issue(e2eId,
                        "Amount must be positive: PmtInfId=" + tx.remittanceId()
                                + ", Amount=" + amt));
            }
            String cdtrNm = tx.creditor();
            if (cdtrNm == null || cdtrNm.isEmpty()) {
                issues.add(new Issue(e2eId,
                        "Creditor name is required: PmtInfId=" + tx.remittanceId()));
            }
        });
        LOG.debug("validateTransactionFields: {} issue(s)", issues.size());
        return issues;
    }

    /**
     * Validates control sums at both the remittance level and the message level.
     *
     * <p>Algorithm (mirrors the DuckDB SQL GROUP BY / HAVING logic):
     * <ol>
     *   <li>Build {@code Map<pmtInfId, BigDecimal>} for declared remittance ctrl sums.</li>
     *   <li>Scan all transactions and accumulate actual sums per {@code pmtInfId}.</li>
     *   <li>Compare declared vs actual; flag mismatches where abs diff &gt; 0.001.</li>
     *   <li>Build {@code Map<msgId, BigDecimal>} for declared message ctrl sums.</li>
     *   <li>Roll up actual sums from step 2 to message level via pmtInfId → msgId join.</li>
     *   <li>Compare declared vs actual; flag mismatches where abs diff &gt; 0.001.</li>
     * </ol>
     * </p>
     */
    @Override
    public List<Issue> validateControlSums() throws SQLException {
        List<Issue> issues = new ArrayList<>();

        // ── Remittance-level ──────────────────────────────────────────────────
        // Step 1: declared ctrl sums per pmtInfId from remittance table
        Map<String, BigDecimal> declaredRmt = new HashMap<>();
        // Step 5a: pmtInfId → msgId reverse lookup for message-level check
        Map<String, String>     pmtInfToMsg = new HashMap<>();

        remittanceTable.forEach(rmt -> {
            if (rmt.controlSum() != null) {
                declaredRmt.put(rmt.remittanceId(), rmt.controlSum());
            }
            pmtInfToMsg.put(rmt.remittanceId(), rmt.messageId());
        });

        // Step 2: actual sums per pmtInfId from transactions
        Map<String, BigDecimal> actualRmt = new HashMap<>();
        transactionTable.forEach(tx -> {
            String pmtInfId = tx.remittanceId();
            BigDecimal amt  = tx.instructedAmount();
            if (pmtInfId != null && amt != null) {
                actualRmt.merge(pmtInfId, amt, BigDecimal::add);
            }
        });

        // Step 3: compare
        for (Map.Entry<String, BigDecimal> e : declaredRmt.entrySet()) {
            String     pmtInfId = e.getKey();
            BigDecimal declared = e.getValue();
            BigDecimal actual   = actualRmt.getOrDefault(pmtInfId, BigDecimal.ZERO);
            BigDecimal diff     = declared.subtract(actual).abs();
            if (diff.compareTo(CTRL_SUM_TOLERANCE) > 0) {
                issues.add(new Issue(pmtInfId,
                        "Remittance CtrlSum mismatch: declared=" + declared.doubleValue()
                                + ", actual=" + actual.doubleValue()));
            }
        }

        // ── Message-level ─────────────────────────────────────────────────────
        // Step 4: declared ctrl sums per msgId from message table
        Map<String, BigDecimal> declaredMsg = new HashMap<>();
        messageTable.forEach(msg -> {
            if (msg.controlSum() != null) {
                declaredMsg.put(msg.messageId(), msg.controlSum());
            }
        });

        // Step 5b: roll up actual transaction sums to message level
        Map<String, BigDecimal> actualMsg = new HashMap<>();
        for (Map.Entry<String, BigDecimal> e : actualRmt.entrySet()) {
            String msgId = pmtInfToMsg.get(e.getKey());
            if (msgId != null) {
                actualMsg.merge(msgId, e.getValue(), BigDecimal::add);
            }
        }

        // Step 6: compare
        for (Map.Entry<String, BigDecimal> e : declaredMsg.entrySet()) {
            String     msgId    = e.getKey();
            BigDecimal declared = e.getValue();
            BigDecimal actual   = actualMsg.getOrDefault(msgId, BigDecimal.ZERO);
            BigDecimal diff     = declared.subtract(actual).abs();
            if (diff.compareTo(CTRL_SUM_TOLERANCE) > 0) {
                issues.add(new Issue(msgId,
                        "Message CtrlSum mismatch: declared=" + declared.doubleValue()
                                + ", actual=" + actual.doubleValue()));
            }
        }

        LOG.debug("validateControlSums: {} issue(s)", issues.size());
        return issues;
    }

    @Override
    public List<String> findInvalidIbans() throws SQLException {
        List<String> ibans = new ArrayList<>();
        remittanceTable.forEach(rmt -> {
            String iban = rmt.debtorAccountIban();
            if (iban != null && !IBAN_PATTERN.matcher(iban).matches()) {
                ibans.add(iban);
            }
        });
        return ibans;
    }

    @Override
    public BigDecimal sumTransactionsByRemittance(String pmtInfId) throws SQLException {
        BigDecimal[] sum = {BigDecimal.ZERO};
        transactionTable.forEach(tx -> {
            if (pmtInfId.equals(tx.remittanceId()) && tx.instructedAmount() != null) {
                sum[0] = sum[0].add(tx.instructedAmount());
            }
        });
        return sum[0];
    }

    @Override
    public String getMessageSummary() throws SQLException {
        StringBuilder sb = new StringBuilder();
        messageTable.forEach(msg -> {
            if (sb.length() == 0) {
                sb.append("msg_id=").append(msg.messageId())
                  .append(", cre_dt_tm=").append(msg.creationDateTime())
                  .append(", nb_of_txs=").append(msg.numberOfTransactions())
                  .append(", ctrl_sum=").append(msg.controlSum())
                  .append(", initg_pty_nm=").append(msg.initiatingParty());
            }
        });
        return sb.toString();
    }

    @Override
    public long getRemittanceCount() throws SQLException {
        return remittanceTable.getRowCount();
    }

    @Override
    public long getTransactionCount() throws SQLException {
        return transactionTable.getRowCount();
    }

    @Override
    public BigDecimal getTotalTransactionAmount() throws SQLException {
        BigDecimal[] total = {BigDecimal.ZERO};
        transactionTable.forEach(tx -> {
            if (tx.instructedAmount() != null) {
                total[0] = total[0].add(tx.instructedAmount());
            }
        });
        return total[0];
    }

    // ── Streaming domain-object access ───────────────────────────────────────

    @Override
    public void streamMessages(Consumer<Message> handler) throws SQLException {
        messageTable.forEach(handler);
    }

    @Override
    public void streamRemittances(String messageId, Consumer<Remittance> handler)
            throws SQLException {
        remittanceTable.forEachByMsgId(messageId, handler);
    }

    @Override
    public void streamTransactions(String remittanceId, Consumer<Transaction> handler)
            throws SQLException {
        transactionTable.forEachByPmtInfId(remittanceId, handler);
    }

    @Override
    public void streamAllTransactions(Consumer<Transaction> handler) throws SQLException {
        transactionTable.forEach(handler);
    }

    // ── Resource management ───────────────────────────────────────────────────

    @Override
    public void close() {
        messageTable.close();
        remittanceTable.close();
        transactionTable.close();
        LOG.debug("ArrowPaymentRepositoryImpl closed");
    }
}
