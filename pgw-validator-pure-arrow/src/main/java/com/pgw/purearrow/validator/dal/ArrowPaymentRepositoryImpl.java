package com.pgw.purearrow.validator.dal;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.dal.PaymentRepository;
import com.pgw.domain.model.Message;
import com.pgw.domain.model.Remittance;
import com.pgw.domain.model.Transaction;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
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
 * <p>
 * Reads ISO 20022 pain.001 data from three in-memory Arrow table objects
 * ({@link ArrowMessageTable}, {@link ArrowRemittanceTable},
 * {@link ArrowTransactionTable}) loaded from Arrow IPC stream files on disk.
 * No DuckDB, no JDBC, no SQL is involved at any point.
 * </p>
 *
 * <p>
 * Validation logic mirrors the SQL predicates in {@code PaymentRepositoryImpl}
 * exactly, translated to pure Java:
 * <ul>
 * <li>String length checks replace {@code LENGTH()} SQL functions.</li>
 * <li>Java {@link Pattern} replaces DuckDB {@code regexp_matches()}.</li>
 * <li>BigDecimal arithmetic replaces SQL {@code SUM} and
 * {@code HAVING ABS(...) > 0.001}.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Instances must be obtained via {@link ArrowPaymentRepositoryLoader}. Call
 * {@link #close()} to release all Arrow off-heap memory when done.
 * </p>
 */
public final class ArrowPaymentRepositoryImpl implements PaymentRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowPaymentRepositoryImpl.class);

    /**
     * Mirrors DuckDB: {@code regexp_matches(iban, '^[A-Z]{2}[0-9]{2}[A-Z0-9]+$')}
     */
    private static final Pattern IBAN_PATTERN = Pattern.compile("^[A-Z]{2}\\d{2}[A-Z0-9]+$");

    /**
     * Threshold for control-sum mismatch; mirrors DuckDB: {@code ABS(...) > 0.001}
     */
    private static final BigDecimal CTRL_SUM_TOLERANCE = new BigDecimal("0.001");

    private final ArrowMessageTable messageTable;
    private final ArrowRemittanceTable remittanceTable;
    private final ArrowTransactionTable transactionTable;

    ArrowPaymentRepositoryImpl(
            ArrowMessageTable messageTable,
            ArrowRemittanceTable remittanceTable,
            ArrowTransactionTable transactionTable) {
        this.messageTable = messageTable;
        this.remittanceTable = remittanceTable;
        this.transactionTable = transactionTable;
    }

    // ── SQL-level bulk validators ────────────────────────────────────────────

    @Override
    public List<Issue> validateMessageFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        messageTable.scanProjected(List.of(
                Pain001ArrowSchema.MSG_ID,
                Pain001ArrowSchema.MSG_INITG_PTY_NM,
                Pain001ArrowSchema.MSG_CRE_DT_TM), batch -> {
                    VarCharVector msgIdVector = batch.varchar(Pain001ArrowSchema.MSG_ID);
                    VarCharVector initgPtyVector = batch.varchar(Pain001ArrowSchema.MSG_INITG_PTY_NM);
                    VarCharVector creDtTmVector = batch.varchar(Pain001ArrowSchema.MSG_CRE_DT_TM);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String id = varchar(msgIdVector, row);
                        if (id != null && id.length() > 35) {
                            issues.add(new Issue(id, "MsgId exceeds maximum length of 35 characters"));
                        }

                        String initgPty = varchar(initgPtyVector, row);
                        if (initgPty == null || initgPty.isEmpty()) {
                            issues.add(new Issue(id, "WARN:Initiating party (InitgPty) is missing"));
                        }

                        String creDtTm = varchar(creDtTmVector, row);
                        if (creDtTm == null || creDtTm.isEmpty()) {
                            issues.add(new Issue(id, "Creation date/time (CreDtTm) is required but missing"));
                        }
                    }
                });
        LOG.debug("validateMessageFields: {} issue(s)", issues.size());
        return issues;
    }

    @Override
    public List<Issue> validateRemittanceFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        remittanceTable.scanProjected(List.of(
                Pain001ArrowSchema.RMT_PMT_INF_ID,
                Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN,
                Pain001ArrowSchema.RMT_PMT_MTD), batch -> {
                    VarCharVector pmtInfIdVector = batch.varchar(Pain001ArrowSchema.RMT_PMT_INF_ID);
                    VarCharVector ibanVector = batch.varchar(Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN);
                    VarCharVector pmtMtdVector = batch.varchar(Pain001ArrowSchema.RMT_PMT_MTD);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String id = varchar(pmtInfIdVector, row);
                        String iban = varchar(ibanVector, row);
                        if (iban != null && !IBAN_PATTERN.matcher(iban).matches()) {
                            issues.add(new Issue(id, "Invalid IBAN format: " + iban));
                        }

                        String pmtMtd = varchar(pmtMtdVector, row);
                        if (pmtMtd == null || pmtMtd.isEmpty()) {
                            issues.add(new Issue(id, "Payment method is required"));
                        }
                    }
                });
        LOG.debug("validateRemittanceFields: {} issue(s)", issues.size());
        return issues;
    }

    @Override
    public List<Issue> validateTransactionFields() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        transactionTable.scanProjected(List.of(
                Pain001ArrowSchema.TX_END_TO_END_ID,
                Pain001ArrowSchema.TX_INSTD_AMT,
                Pain001ArrowSchema.TX_CDTR_NM,
                Pain001ArrowSchema.TX_PMT_INF_ID), batch -> {
                    VarCharVector e2eIdVector = batch.varchar(Pain001ArrowSchema.TX_END_TO_END_ID);
                    DecimalVector amountVector = batch.decimal(Pain001ArrowSchema.TX_INSTD_AMT);
                    VarCharVector creditorVector = batch.varchar(Pain001ArrowSchema.TX_CDTR_NM);
                    VarCharVector pmtInfIdVector = batch.varchar(Pain001ArrowSchema.TX_PMT_INF_ID);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String e2eId = varchar(e2eIdVector, row);
                        BigDecimal amt = decimal(amountVector, row);
                        String pmtInfId = varchar(pmtInfIdVector, row);

                        if (amt == null || amt.compareTo(BigDecimal.ZERO) <= 0) {
                            issues.add(new Issue(e2eId,
                                    "Amount must be positive: PmtInfId=" + pmtInfId
                                            + ", Amount=" + amt));
                        }

                        String cdtrNm = varchar(creditorVector, row);
                        if (cdtrNm == null || cdtrNm.isEmpty()) {
                            issues.add(new Issue(e2eId,
                                    "Creditor name is required: PmtInfId=" + pmtInfId));
                        }
                    }
                });
        LOG.debug("validateTransactionFields: {} issue(s)", issues.size());
        return issues;
    }

    /**
     * Validates control sums at both the remittance level and the message level.
     *
     * <p>
     * Algorithm (mirrors the DuckDB SQL GROUP BY / HAVING logic):
     * <ol>
     * <li>Build {@code Map<pmtInfId, BigDecimal>} for declared remittance ctrl
     * sums.</li>
     * <li>Scan all transactions and accumulate actual sums per
     * {@code pmtInfId}.</li>
     * <li>Compare declared vs actual; flag mismatches where abs diff &gt;
     * 0.001.</li>
     * <li>Build {@code Map<msgId, BigDecimal>} for declared message ctrl sums.</li>
     * <li>Roll up actual sums from step 2 to message level via pmtInfId → msgId
     * join.</li>
     * <li>Compare declared vs actual; flag mismatches where abs diff &gt;
     * 0.001.</li>
     * </ol>
     * </p>
     */
    @Override
    public List<Issue> validateControlSums() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        RemittanceControlData remittanceControlData = loadRemittanceControlData();

        Map<String, BigDecimal> actualRmt = loadActualRemittanceSums();
        addControlSumIssues(issues, remittanceControlData.declaredRmt(), actualRmt,
                "Remittance CtrlSum mismatch: declared=");

        Map<String, BigDecimal> declaredMsg = loadDeclaredMessageControlSums();
        Map<String, BigDecimal> actualMsg = rollUpActualMessageSums(actualRmt, remittanceControlData.pmtInfToMsg());
        addControlSumIssues(issues, declaredMsg, actualMsg, "Message CtrlSum mismatch: declared=");

        LOG.debug("validateControlSums: {} issue(s)", issues.size());
        return issues;
    }

    @Override
    public List<Issue> validateNumberOfTransactions() throws SQLException {
        List<Issue> issues = new ArrayList<>();
        RemittanceCountData remittanceCountData = loadRemittanceCountData();

        Map<String, Long> actualRmt = loadActualRemittanceTransactionCounts();
        addTransactionCountIssues(issues, remittanceCountData.declaredRmtCounts(), actualRmt,
                "Remittance NbOfTxs mismatch: declared=");

        Map<String, Long> declaredMsg = loadDeclaredMessageTransactionCounts();
        Map<String, Long> actualMsg = rollUpActualMessageCounts(actualRmt, remittanceCountData.pmtInfToMsg());
        addTransactionCountIssues(issues, declaredMsg, actualMsg,
                "Message NbOfTxs mismatch: declared=");

        LOG.debug("validateNumberOfTransactions: {} issue(s)", issues.size());
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
        BigDecimal[] sum = { BigDecimal.ZERO };
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
            if (sb.isEmpty()) {
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
        BigDecimal[] total = { BigDecimal.ZERO };
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

    private RemittanceControlData loadRemittanceControlData() {
        Map<String, BigDecimal> declaredRmt = new HashMap<>();
        Map<String, String> pmtInfToMsg = new HashMap<>();

        remittanceTable.scanProjected(List.of(
                Pain001ArrowSchema.RMT_MSG_ID,
                Pain001ArrowSchema.RMT_PMT_INF_ID,
                Pain001ArrowSchema.RMT_CTRL_SUM), batch -> {
                    VarCharVector msgIdVector = batch.varchar(Pain001ArrowSchema.RMT_MSG_ID);
                    VarCharVector pmtInfIdVector = batch.varchar(Pain001ArrowSchema.RMT_PMT_INF_ID);
                    DecimalVector ctrlSumVector = batch.decimal(Pain001ArrowSchema.RMT_CTRL_SUM);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String pmtInfId = varchar(pmtInfIdVector, row);
                        if (pmtInfId == null) {
                            continue;
                        }

                        BigDecimal controlSum = decimal(ctrlSumVector, row);
                        if (controlSum != null) {
                            declaredRmt.put(pmtInfId, controlSum);
                        }

                        String msgId = varchar(msgIdVector, row);
                        if (msgId != null) {
                            pmtInfToMsg.put(pmtInfId, msgId);
                        }
                    }
                });

        return new RemittanceControlData(declaredRmt, pmtInfToMsg);
    }

    private Map<String, BigDecimal> loadActualRemittanceSums() {
        Map<String, BigDecimal> actualRmt = new HashMap<>();
        transactionTable.scanProjected(List.of(
                Pain001ArrowSchema.TX_PMT_INF_ID,
                Pain001ArrowSchema.TX_INSTD_AMT), batch -> {
                    VarCharVector pmtInfIdVector = batch.varchar(Pain001ArrowSchema.TX_PMT_INF_ID);
                    DecimalVector amountVector = batch.decimal(Pain001ArrowSchema.TX_INSTD_AMT);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String pmtInfId = varchar(pmtInfIdVector, row);
                        BigDecimal amount = decimal(amountVector, row);
                        if (pmtInfId != null && amount != null) {
                            actualRmt.merge(pmtInfId, amount, BigDecimal::add);
                        }
                    }
                });
        return actualRmt;
    }

    private Map<String, BigDecimal> loadDeclaredMessageControlSums() {
        Map<String, BigDecimal> declaredMsg = new HashMap<>();
        messageTable.scanProjected(List.of(
                Pain001ArrowSchema.MSG_ID,
                Pain001ArrowSchema.MSG_CTRL_SUM), batch -> {
                    VarCharVector msgIdVector = batch.varchar(Pain001ArrowSchema.MSG_ID);
                    DecimalVector ctrlSumVector = batch.decimal(Pain001ArrowSchema.MSG_CTRL_SUM);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String msgId = varchar(msgIdVector, row);
                        BigDecimal controlSum = decimal(ctrlSumVector, row);
                        if (msgId != null && controlSum != null) {
                            declaredMsg.put(msgId, controlSum);
                        }
                    }
                });
        return declaredMsg;
    }

    private RemittanceCountData loadRemittanceCountData() {
        Map<String, Long> declaredRmtCounts = new HashMap<>();
        Map<String, String> pmtInfToMsg = new HashMap<>();

        remittanceTable.scanProjected(List.of(
                Pain001ArrowSchema.RMT_MSG_ID,
                Pain001ArrowSchema.RMT_PMT_INF_ID,
                Pain001ArrowSchema.RMT_NB_OF_TXS), batch -> {
                    VarCharVector msgIdVector = batch.varchar(Pain001ArrowSchema.RMT_MSG_ID);
                    VarCharVector pmtInfIdVector = batch.varchar(Pain001ArrowSchema.RMT_PMT_INF_ID);
                    VarCharVector countVector = batch.varchar(Pain001ArrowSchema.RMT_NB_OF_TXS);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String pmtInfId = varchar(pmtInfIdVector, row);
                        if (pmtInfId == null) {
                            continue;
                        }

                        Long declaredCount = parseCount(varchar(countVector, row));
                        if (declaredCount != null) {
                            declaredRmtCounts.put(pmtInfId, declaredCount);
                        }

                        String msgId = varchar(msgIdVector, row);
                        if (msgId != null) {
                            pmtInfToMsg.put(pmtInfId, msgId);
                        }
                    }
                });

        return new RemittanceCountData(declaredRmtCounts, pmtInfToMsg);
    }

    private Map<String, Long> loadActualRemittanceTransactionCounts() {
        Map<String, Long> actualRmtCounts = new HashMap<>();
        transactionTable.scanProjected(List.of(Pain001ArrowSchema.TX_PMT_INF_ID), batch -> {
            VarCharVector pmtInfIdVector = batch.varchar(Pain001ArrowSchema.TX_PMT_INF_ID);

            for (int row = 0; row < batch.rowCount(); row++) {
                String pmtInfId = varchar(pmtInfIdVector, row);
                if (pmtInfId != null) {
                    actualRmtCounts.merge(pmtInfId, 1L, Long::sum);
                }
            }
        });
        return actualRmtCounts;
    }

    private Map<String, Long> loadDeclaredMessageTransactionCounts() {
        Map<String, Long> declaredMsgCounts = new HashMap<>();
        messageTable.scanProjected(List.of(
                Pain001ArrowSchema.MSG_ID,
                Pain001ArrowSchema.MSG_NB_OF_TXS), batch -> {
                    VarCharVector msgIdVector = batch.varchar(Pain001ArrowSchema.MSG_ID);
                    VarCharVector countVector = batch.varchar(Pain001ArrowSchema.MSG_NB_OF_TXS);

                    for (int row = 0; row < batch.rowCount(); row++) {
                        String msgId = varchar(msgIdVector, row);
                        Long declaredCount = parseCount(varchar(countVector, row));
                        if (msgId != null && declaredCount != null) {
                            declaredMsgCounts.put(msgId, declaredCount);
                        }
                    }
                });
        return declaredMsgCounts;
    }

    private Map<String, BigDecimal> rollUpActualMessageSums(Map<String, BigDecimal> actualRmt,
            Map<String, String> pmtInfToMsg) {
        Map<String, BigDecimal> actualMsg = new HashMap<>();
        for (Map.Entry<String, BigDecimal> entry : actualRmt.entrySet()) {
            String msgId = pmtInfToMsg.get(entry.getKey());
            if (msgId != null) {
                actualMsg.merge(msgId, entry.getValue(), BigDecimal::add);
            }
        }
        return actualMsg;
    }

    private Map<String, Long> rollUpActualMessageCounts(Map<String, Long> actualRmtCounts,
            Map<String, String> pmtInfToMsg) {
        Map<String, Long> actualMsgCounts = new HashMap<>();
        for (Map.Entry<String, Long> entry : actualRmtCounts.entrySet()) {
            String msgId = pmtInfToMsg.get(entry.getKey());
            if (msgId != null) {
                actualMsgCounts.merge(msgId, entry.getValue(), Long::sum);
            }
        }
        return actualMsgCounts;
    }

    private static void addControlSumIssues(List<Issue> issues,
            Map<String, BigDecimal> declaredSums,
            Map<String, BigDecimal> actualSums,
            String messagePrefix) {
        for (Map.Entry<String, BigDecimal> entry : declaredSums.entrySet()) {
            String id = entry.getKey();
            BigDecimal declared = entry.getValue();
            BigDecimal actual = actualSums.getOrDefault(id, BigDecimal.ZERO);
            BigDecimal diff = declared.subtract(actual).abs();
            if (diff.compareTo(CTRL_SUM_TOLERANCE) > 0) {
                issues.add(new Issue(id,
                        messagePrefix + declared.doubleValue() + ", actual=" + actual.doubleValue()));
            }
        }
    }

    private static void addTransactionCountIssues(List<Issue> issues,
            Map<String, Long> declaredCounts,
            Map<String, Long> actualCounts,
            String messagePrefix) {
        for (Map.Entry<String, Long> entry : declaredCounts.entrySet()) {
            String id = entry.getKey();
            long declared = entry.getValue();
            long actual = actualCounts.getOrDefault(id, 0L);
            if (declared != actual) {
                issues.add(new Issue(id, messagePrefix + declared + ", actual=" + actual));
            }
        }
    }

    private static String varchar(VarCharVector vector, int index) {
        if (vector == null || vector.isNull(index)) {
            return null;
        }
        return vector.getObject(index).toString();
    }

    private static BigDecimal decimal(DecimalVector vector, int index) {
        if (vector == null || vector.isNull(index)) {
            return null;
        }
        return vector.getObject(index);
    }

    private static Long parseCount(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private record RemittanceControlData(Map<String, BigDecimal> declaredRmt,
            Map<String, String> pmtInfToMsg) {
    }

    private record RemittanceCountData(Map<String, Long> declaredRmtCounts,
            Map<String, String> pmtInfToMsg) {
    }
}
