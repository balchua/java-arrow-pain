package com.pgw.purearrow.validator.dal;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.domain.model.Transaction;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;

/**
 * In-memory Arrow-backed accessor for the <b>transaction</b> table
 * (one row per ISO 20022 pain.001 CreditTransferTransaction / CdtTrfTxInf).
 *
 * <p>Wraps a list of materialised {@link VectorSchemaRoot} batches loaded by
 * {@link ArrowTableLoader}. Column values are accessed by name using the field
 * constants from {@link Pain001ArrowSchema}.</p>
 *
 * <p>Call {@link #close()} to release all Arrow off-heap memory.</p>
 */
public final class ArrowTransactionTable implements AutoCloseable {

    private final List<VectorSchemaRoot> batches;

    ArrowTransactionTable(List<VectorSchemaRoot> batches) {
        this.batches = batches;
    }

    // ── Row count ─────────────────────────────────────────────────────────────

    /** Total number of transaction rows across all batches. */
    public long getRowCount() {
        return ArrowTableLoader.totalRows(batches);
    }

    // ── Typed column accessors ────────────────────────────────────────────────

    public String getPmtInfId(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_PMT_INF_ID, i);
    }

    public String getInstrId(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_INSTR_ID, i);
    }

    public String getEndToEndId(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_END_TO_END_ID, i);
    }

    public BigDecimal getInstdAmt(VectorSchemaRoot batch, int i) {
        return decimal(batch, Pain001ArrowSchema.TX_INSTD_AMT, i);
    }

    public String getCcy(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_CCY, i);
    }

    public String getCdtrAgtBicfi(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_CDTR_AGT_BICFI, i);
    }

    public String getCdtrNm(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_CDTR_NM, i);
    }

    public String getCdtrAcctIban(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_CDTR_ACCT_IBAN, i);
    }

    public String getRmtInfUstrd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_RMT_INF_USTRD, i);
    }

    public String getRgltyRptgCd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_RGLTY_RPTG_CD, i);
    }

    public String getRmtInfStrdRef(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_RMT_INF_STRD_REF, i);
    }

    public String getRmtInfStrdRefTp(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_RMT_INF_STRD_REF_TP, i);
    }

    public String getPurpCd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_PURP_CD, i);
    }

    public String getUltmtCdtrNm(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_ULTMT_CDTR_NM, i);
    }

    public String getCdtrCtry(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.TX_CDTR_CTRY, i);
    }

    // ── Row iteration ─────────────────────────────────────────────────────────

    /**
     * Iterates over every transaction row in all batches, invoking {@code consumer}
     * with a fully-populated {@link Transaction} domain object for each row.
     */
    public void forEach(Consumer<Transaction> consumer) {
        for (VectorSchemaRoot batch : batches) {
            int rows = batch.getRowCount();
            for (int i = 0; i < rows; i++) {
                consumer.accept(buildTransaction(batch, i));
            }
        }
    }

    /**
     * Iterates over transactions belonging to the given {@code pmtInfId}, invoking
     * {@code consumer} once per matching row. Performs a linear scan.
     */
    public void forEachByPmtInfId(String pmtInfId, Consumer<Transaction> consumer) {
        for (VectorSchemaRoot batch : batches) {
            int rows = batch.getRowCount();
            for (int i = 0; i < rows; i++) {
                if (pmtInfId.equals(getPmtInfId(batch, i))) {
                    consumer.accept(buildTransaction(batch, i));
                }
            }
        }
    }

    // ── Resource management ───────────────────────────────────────────────────

    @Override
    public void close() {
        ArrowTableLoader.closeBatches(batches);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private Transaction buildTransaction(VectorSchemaRoot batch, int i) {
        return new Transaction(
                getPmtInfId(batch, i),
                getInstrId(batch, i),
                getEndToEndId(batch, i),
                getInstdAmt(batch, i),
                getCcy(batch, i),
                getCdtrAgtBicfi(batch, i),
                getCdtrNm(batch, i),
                getCdtrAcctIban(batch, i),
                getRmtInfUstrd(batch, i),
                getRgltyRptgCd(batch, i),
                getRmtInfStrdRef(batch, i),
                getRmtInfStrdRefTp(batch, i),
                getPurpCd(batch, i),
                getUltmtCdtrNm(batch, i),
                getCdtrCtry(batch, i));
    }

    private static String varchar(VectorSchemaRoot root, String field, int i) {
        VarCharVector v = (VarCharVector) root.getVector(field);
        if (v == null || v.isNull(i)) return null;
        return v.getObject(i).toString();
    }

    private static BigDecimal decimal(VectorSchemaRoot root, String field, int i) {
        DecimalVector v = (DecimalVector) root.getVector(field);
        if (v == null || v.isNull(i)) return null;
        return v.getObject(i);
    }
}
