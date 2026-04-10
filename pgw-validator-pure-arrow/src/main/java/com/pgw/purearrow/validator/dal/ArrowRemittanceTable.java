package com.pgw.purearrow.validator.dal;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.domain.model.Remittance;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Consumer;

/**
 * In-memory Arrow-backed accessor for the <b>remittance</b> table
 * (one row per ISO 20022 pain.001 PaymentInformation / PmtInf block).
 *
 * <p>Wraps a list of materialised {@link VectorSchemaRoot} batches loaded by
 * {@link ArrowTableLoader}. Column values are accessed by name using the field
 * constants from {@link Pain001ArrowSchema}.</p>
 *
 * <p>Call {@link #close()} to release all Arrow off-heap memory.</p>
 */
public final class ArrowRemittanceTable implements AutoCloseable {

    private final List<VectorSchemaRoot> batches;

    ArrowRemittanceTable(List<VectorSchemaRoot> batches) {
        this.batches = batches;
    }

    // ── Row count ─────────────────────────────────────────────────────────────

    /** Total number of remittance rows across all batches. */
    public long getRowCount() {
        return ArrowTableLoader.totalRows(batches);
    }

    // ── Typed column accessors ────────────────────────────────────────────────

    public String getMsgId(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_MSG_ID, i);
    }

    public String getPmtInfId(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_PMT_INF_ID, i);
    }

    public String getPmtMtd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_PMT_MTD, i);
    }

    public String getNbOfTxs(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_NB_OF_TXS, i);
    }

    public BigDecimal getCtrlSum(VectorSchemaRoot batch, int i) {
        return decimal(batch, Pain001ArrowSchema.RMT_CTRL_SUM, i);
    }

    public String getSvcLvlCd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_SVC_LVL_CD, i);
    }

    public LocalDate getReqdExctnDt(VectorSchemaRoot batch, int i) {
        DateDayVector v = (DateDayVector) batch.getVector(Pain001ArrowSchema.RMT_REQD_EXCTN_DT);
        if (v == null || v.isNull(i)) return null;
        return LocalDate.ofEpochDay(v.get(i));
    }

    public String getDbtrNm(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_DBTR_NM, i);
    }

    public String getDbtrAcctIban(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN, i);
    }

    public String getDbtrAgtBicfi(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_DBTR_AGT_BICFI, i);
    }

    public String getBtchBookg(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_BTCH_BOOKG, i);
    }

    public String getInstrPrty(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_INSTR_PRTY, i);
    }

    public String getLclInstrmCd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_LCL_INSTRM_CD, i);
    }

    public String getCtgyPurpCd(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_CTGY_PURP_CD, i);
    }

    public String getChrgBr(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_CHRG_BR, i);
    }

    public String getUltmtDbtrNm(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.RMT_ULTMT_DBTR_NM, i);
    }

    // ── Row iteration ─────────────────────────────────────────────────────────

    /**
     * Iterates over every row in all batches and invokes {@code consumer} with a
     * fully-populated {@link Remittance} domain object for each row.
     */
    public void forEach(Consumer<Remittance> consumer) {
        for (VectorSchemaRoot batch : batches) {
            int rows = batch.getRowCount();
            for (int i = 0; i < rows; i++) {
                consumer.accept(new Remittance(
                        getMsgId(batch, i),
                        getPmtInfId(batch, i),
                        getPmtMtd(batch, i),
                        getNbOfTxs(batch, i),
                        getCtrlSum(batch, i),
                        getSvcLvlCd(batch, i),
                        getReqdExctnDt(batch, i),
                        getDbtrNm(batch, i),
                        getDbtrAcctIban(batch, i),
                        getDbtrAgtBicfi(batch, i),
                        getBtchBookg(batch, i),
                        getInstrPrty(batch, i),
                        getLclInstrmCd(batch, i),
                        getCtgyPurpCd(batch, i),
                        getChrgBr(batch, i),
                        getUltmtDbtrNm(batch, i)));
            }
        }
    }

    /**
     * Iterates over remittances belonging to the given {@code msgId}, invoking
     * {@code consumer} once per matching row. Performs a linear scan — suitable
     * for use-cases where only a small subset of rows is expected to match.
     */
    public void forEachByMsgId(String msgId, Consumer<Remittance> consumer) {
        for (VectorSchemaRoot batch : batches) {
            int rows = batch.getRowCount();
            for (int i = 0; i < rows; i++) {
                if (msgId.equals(getMsgId(batch, i))) {
                    consumer.accept(new Remittance(
                            getMsgId(batch, i),
                            getPmtInfId(batch, i),
                            getPmtMtd(batch, i),
                            getNbOfTxs(batch, i),
                            getCtrlSum(batch, i),
                            getSvcLvlCd(batch, i),
                            getReqdExctnDt(batch, i),
                            getDbtrNm(batch, i),
                            getDbtrAcctIban(batch, i),
                            getDbtrAgtBicfi(batch, i),
                            getBtchBookg(batch, i),
                            getInstrPrty(batch, i),
                            getLclInstrmCd(batch, i),
                            getCtgyPurpCd(batch, i),
                            getChrgBr(batch, i),
                            getUltmtDbtrNm(batch, i)));
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
