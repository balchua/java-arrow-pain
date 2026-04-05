package com.pgw.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Holds the three Arrow tables parsed from a pain.001.001.09 XML file:
 * <ul>
 * <li><b>Messages</b> — one row per GrpHdr (GroupHeader85)</li>
 * <li><b>Remittances</b> — batches of rows, one per PmtInf
 * (PaymentInstruction30)</li>
 * <li><b>Transactions</b> — batches of rows, one per CdtTrfTxInf
 * (CreditTransferTransaction34)</li>
 * </ul>
 *
 * <p>
 * Message and remittance tables are typically small (1–2 rows, or up to 1M rows
 * for Type C). Transaction tables can be very large and are split into batches
 * of
 * 65,536 rows each.
 * </p>
 *
 * <p>
 * Implements {@link AutoCloseable} to release all Arrow native memory.
 * </p>
 */
public final class ArrowBatchResult implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowBatchResult.class);

    private final VectorSchemaRoot messageRoot;
    private final List<VectorSchemaRoot> remittanceBatches;
    private final List<VectorSchemaRoot> transactionBatches;

    /**
     * Constructs a result from the three table types.
     *
     * @param messageRoot        the single message (GroupHeader) VectorSchemaRoot
     * @param remittanceBatches  batches of remittance (PaymentInformation) rows
     * @param transactionBatches batches of transaction (CdtTrfTxInf) rows
     */
    public ArrowBatchResult(VectorSchemaRoot messageRoot,
            List<VectorSchemaRoot> remittanceBatches,
            List<VectorSchemaRoot> transactionBatches) {
        this.messageRoot = messageRoot;
        this.remittanceBatches = List.copyOf(remittanceBatches);
        this.transactionBatches = List.copyOf(transactionBatches);
    }

    public VectorSchemaRoot getMessageRoot() {
        return messageRoot;
    }

    public List<VectorSchemaRoot> getRemittanceBatches() {
        return remittanceBatches;
    }

    public List<VectorSchemaRoot> getTransactionBatches() {
        return transactionBatches;
    }

    /** Total message rows (typically 1). */
    public int getMessageRowCount() {
        return messageRoot.getRowCount();
    }

    /** Total remittance rows across all batches. */
    public long getRemittanceRowCount() {
        return remittanceBatches.stream().mapToLong(VectorSchemaRoot::getRowCount).sum();
    }

    /** Total transaction rows across all batches. */
    public long getTransactionRowCount() {
        return transactionBatches.stream().mapToLong(VectorSchemaRoot::getRowCount).sum();
    }

    /**
     * Prints a summary of all three Arrow tables.
     */
    public void printSummary() {
        LOG.info("═══════════════════════════════════════════════════════════");
        LOG.info("  Arrow Tables Summary");
        LOG.info("───────────────────────────────────────────────────────────");
        LOG.info("  Message table      : {} rows, {} fields",
                getMessageRowCount(), messageRoot.getFieldVectors().size());
        LOG.info("  Remittance table   : {} rows across {} batch(es)",
                getRemittanceRowCount(), remittanceBatches.size());
        LOG.info("  Transaction table  : {} rows across {} batch(es)",
                getTransactionRowCount(), transactionBatches.size());
        LOG.info("───────────────────────────────────────────────────────────");

        // Sample from message table
        if (messageRoot.getRowCount() > 0) {
            LOG.info("  Message[0]: MsgId={}, CreDtTm={}, NbOfTxs={}, CtrlSum={}, InitgPty={}",
                    messageRoot.getVector(Pain001ArrowSchema.MSG_ID).getObject(0),
                    messageRoot.getVector(Pain001ArrowSchema.MSG_CRE_DT_TM).getObject(0),
                    messageRoot.getVector(Pain001ArrowSchema.MSG_NB_OF_TXS).getObject(0),
                    messageRoot.getVector(Pain001ArrowSchema.MSG_CTRL_SUM).getObject(0),
                    messageRoot.getVector(Pain001ArrowSchema.MSG_INITG_PTY_NM).getObject(0));
        }

        // Sample from first remittance batch
        if (!remittanceBatches.isEmpty() && remittanceBatches.get(0).getRowCount() > 0) {
            VectorSchemaRoot rmt = remittanceBatches.get(0);
            int sampleCount = Math.min(2, rmt.getRowCount());
            for (int i = 0; i < sampleCount; i++) {
                LOG.info("  Remittance[{}]: MsgId={}, PmtInfId={}, PmtMtd={}, Dbtr={}, IBAN={}",
                        i,
                        rmt.getVector(Pain001ArrowSchema.RMT_MSG_ID).getObject(i),
                        rmt.getVector(Pain001ArrowSchema.RMT_PMT_INF_ID).getObject(i),
                        rmt.getVector(Pain001ArrowSchema.RMT_PMT_MTD).getObject(i),
                        rmt.getVector(Pain001ArrowSchema.RMT_DBTR_NM).getObject(i),
                        rmt.getVector(Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN).getObject(i));
            }
        }

        // Sample from first transaction batch
        if (!transactionBatches.isEmpty() && transactionBatches.get(0).getRowCount() > 0) {
            VectorSchemaRoot tx = transactionBatches.get(0);
            int sampleCount = Math.min(3, tx.getRowCount());
            for (int i = 0; i < sampleCount; i++) {
                LOG.info("  Transaction[{}]: PmtInfId={}, InstrId={}, E2E={}, Amt={} {}, Cdtr={}",
                        i,
                        tx.getVector(Pain001ArrowSchema.TX_PMT_INF_ID).getObject(i),
                        tx.getVector(Pain001ArrowSchema.TX_INSTR_ID).getObject(i),
                        tx.getVector(Pain001ArrowSchema.TX_END_TO_END_ID).getObject(i),
                        tx.getVector(Pain001ArrowSchema.TX_INSTD_AMT).getObject(i),
                        tx.getVector(Pain001ArrowSchema.TX_CCY).getObject(i),
                        tx.getVector(Pain001ArrowSchema.TX_CDTR_NM).getObject(i));
            }
        }

        LOG.info("═══════════════════════════════════════════════════════════");
    }

    /**
     * Releases all native Arrow memory held by the three tables.
     */
    @Override
    public void close() {
        closeQuietly(messageRoot);
        remittanceBatches.forEach(ArrowBatchResult::closeQuietly);
        transactionBatches.forEach(ArrowBatchResult::closeQuietly);
        LOG.debug("Closed Arrow result: 1 message root, {} remittance batches, {} transaction batches",
                remittanceBatches.size(), transactionBatches.size());
    }

    private static void closeQuietly(VectorSchemaRoot root) {
        try {
            root.close();
        } catch (Exception e) {
            LOG.warn("Error closing Arrow VectorSchemaRoot: {}", e.getMessage());
        }
    }
}
